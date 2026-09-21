#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collect_analyst.py — A股分析师一致预期/评级调整数据采集(东方财富数据源)

为 FinAI2.0 盈利预测修正轴做数据准备。纯数据采集+可行性侦察,原样落盘 parquet。

采集三类表:
  1. research_report_em      个股研报逐条历史(PIT 锚=publishDate),含
                             predict{This,Next,NextTwo}Year{Eps,Pe}、
                             emRatingName/lastEmRatingName/ratingChange、
                             机构/分析师/目标价。历史深度实测 2017-03-30 起。
                             端点: reportapi.eastmoney.com/report/list (qType=0 个股研报,
                             全市场按月分页, pageSize 上限实测 100)
  2. forecast_detail_em      个股盈利预测明细(F10 ycmx): 机构/分析师/预测年度/
                             EPS/PARENT_NETPROFIT/RATING/PUBLISH_DATE。
                             仅覆盖近期窗口(各机构最新一期预测),非全历史。
                             端点: emweb.securities.eastmoney.com/PC_HSF10/ProfitForecast/PageAjax
  3. consensus_snapshot_em   一致预期快照(RPT_WEB_RESPREDICT, 每股一行, 当日截面,
                             非 PIT 历史)。

退市股抽查: 乐视网300104/长生生物002680/康得新002450
  - report/list 按月全局分页天然包含退市股历史行(实测 2018-04 全局含 002450)
  - 另按 code 单独抽查三只, 记录行数与日期分布

纪律: 限速 ~0.6s/请求; 失败重试3次后记 manifest, 不硬造数据。
输出: analyst/{table}_{YYYY}.parquet 按 publishDate/PUBLISH_DATE 年份分片;
      manifest.json 记录行数/覆盖/字段/失败/退市股抽查结果。
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


def parse_ym(s):
    y, m = s.split("-")
    return int(y), int(m)


now = datetime.now(timezone.utc)
_ap = argparse.ArgumentParser(description="采集A股分析师研报/盈利预测/评级数据(东财)")
_ap.add_argument("--out", default="analyst_data", help="输出目录")
_ap.add_argument("--begin", default="2017-01",
                help="起始月份 YYYY-MM (库实测最早 2017-03-30)")
_ap.add_argument("--end", default=f"{now.year}-{now.month:02d}",
                help="结束月份 YYYY-MM (含当月)")
_ap.add_argument("--sleep", type=float, default=0.6, help="每请求间隔秒")
_args = _ap.parse_args()

OUT_DIR = Path(_args.out)
PARQUET_DIR = OUT_DIR / "analyst"
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

REPORT_LIST_URL = "https://reportapi.eastmoney.com/report/list"
CONSENSUS_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"
F10_FORECAST_URL = "https://emweb.securities.eastmoney.com/PC_HSF10/ProfitForecast/PageAjax"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}

# 采集窗口
BEGIN_YEAR_MONTH = parse_ym(_args.begin)
_ey, _em = parse_ym(_args.end)
END_EXCLUSIVE = (_ey + 1, 1) if _em == 12 else (_ey, _em + 1)
DELISTED_SPOT = {"300104": "乐视网", "002680": "长生生物", "002450": "康得新"}
REQUEST_SLEEP = _args.sleep
MAX_RETRY = 3

manifest = {
    "run_at_utc": datetime.now(timezone.utc).isoformat(),
    "tables": {},
    "failures": [],
    "delisted_spot_check": {},
}


def get_json(url, params, referer=None, retries=MAX_RETRY):
    h = dict(HEADERS)
    if referer:
        h["Referer"] = referer
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=30)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(1.5 * (i + 1))
    manifest["failures"].append({"url": url, "params": params, "error": last_err})
    return None


def month_range(beg, end):
    y, m = beg
    while (y, m) < end:
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        yield (y, m), (ny, nm)
        y, m = ny, nm


def collect_research_report():
    """report/list 全市场个股研报按月分页; 返回 list[df] (每月一df)"""
    base = {
        "industryCode": "*", "industry": "*", "rating": "*", "ratingChange": "*",
        "fields": "", "qType": "0", "orgCode": "", "code": "", "rcode": "",
        "pageSize": "100",
    }
    month_frames = []
    month_stats = []
    for (y, m), (ny, nm) in month_range(BEGIN_YEAR_MONTH, END_EXCLUSIVE):
        rng = (f"{y}-{m:02d}-01", f"{ny}-{nm:02d}-01")
        params = dict(base, beginTime=rng[0], endTime=rng[1],
                      pageNo=1, pageNum=1, p=1, pageNumber=1)
        j = get_json(REPORT_LIST_URL, params)
        time.sleep(REQUEST_SLEEP)
        if not j:
            month_stats.append({"month": rng[0], "status": "FAILED"})
            continue
        hits = int(j.get("hits") or 0)
        total_pages = int(j.get("TotalPage") or 0)
        rows = list(j.get("data") or [])
        for pg in range(2, total_pages + 1):
            params.update(pageNo=pg, pageNum=pg, p=pg, pageNumber=pg)
            jj = get_json(REPORT_LIST_URL, params)
            time.sleep(REQUEST_SLEEP)
            if jj:
                rows += jj.get("data") or []
        df = pd.DataFrame(rows)
        got = len(df)
        month_stats.append({"month": rng[0], "hits": hits,
                            "pages": total_pages, "rows": got})
        if got:
            df["_pull_month"] = rng[0]
            month_frames.append(df)
        print(f"[report] {rng[0]} hits={hits} rows={got}", flush=True)
    manifest["tables"]["research_report_em"] = {"monthly": month_stats}
    return month_frames


def collect_consensus_snapshot():
    """RPT_WEB_RESPREDICT 一致预期快照(当日截面)"""
    params = {"reportName": "RPT_WEB_RESPREDICT", "columns": "ALL",
              "pageNumber": "1", "pageSize": "100", "p": "1",
              "pageNo": "1", "pageNum": "1"}
    j = get_json(CONSENSUS_URL, params)
    if not j or not j.get("success"):
        manifest["tables"]["consensus_snapshot_em"] = {"status": "FAILED"}
        return None, []
    pages = int(j["result"]["pages"])
    rows = list(j["result"]["data"])
    for pg in range(2, pages + 1):
        params.update(pageNumber=str(pg), p=str(pg), pageNo=str(pg), pageNum=str(pg))
        jj = get_json(CONSENSUS_URL, params)
        time.sleep(REQUEST_SLEEP)
        if jj and jj.get("success"):
            rows += jj["result"]["data"]
    df = pd.DataFrame(rows)
    manifest["tables"]["consensus_snapshot_em"] = {
        "rows": len(df), "count_field": j["result"].get("count"),
        "note": "当日截面快照, 非PIT历史"}
    print(f"[snapshot] rows={len(df)}", flush=True)
    codes = [c for c in df["SECUCODE"].dropna().unique().tolist()] if not df.empty else []
    return df, codes


def collect_forecast_detail(secucodes):
    """F10 PageAjax 逐股取 ycmx 盈利预测明细(近期窗口, 含净利润)"""
    frames = []
    n_ok = n_empty = n_fail = 0
    for i, secu in enumerate(secucodes):
        # SECUCODE '000001.SZ' -> code param 'SZ000001'
        try:
            exch, code = secu.split(".")[1] + secu.split(".")[0], secu.split(".")[0]
        except Exception:
            exch = None
        if not exch or exch not in ("SZ", "SH", "BJ"):
            n_fail += 1
            continue
        j = get_json(F10_FORECAST_URL, {"code": f"{exch}{code}"},
                     referer="https://emweb.securities.eastmoney.com/")
        time.sleep(REQUEST_SLEEP)
        if j is None:
            n_fail += 1
            continue
        rows = j.get("ycmx") or []
        if rows:
            df = pd.DataFrame(rows)
            df["_secucode"] = secu
            frames.append(df)
            n_ok += 1
        else:
            n_empty += 1
        if (i + 1) % 200 == 0:
            print(f"[ycmx] {i+1}/{len(secucodes)} ok={n_ok} empty={n_empty} fail={n_fail}", flush=True)
    manifest["tables"]["forecast_detail_em"] = {
        "universe": len(secucodes), "stocks_with_rows": n_ok,
        "stocks_empty": n_empty, "stocks_failed": n_fail,
        "note": "F10 ycmx, 仅近期窗口(各机构最新预测), 含PARENT_NETPROFIT"}
    return frames


def delisted_spot_check(global_frames):
    """退市股抽查: 1) code 直查 report/list; 2) 全局月拉结果中是否含该代码"""
    results = {}
    pulled_codes = set()
    for df in global_frames:
        if "stockCode" in df.columns:
            pulled_codes.update(df["stockCode"].dropna().astype(str).tolist())
    base = {"industryCode": "*", "industry": "*", "rating": "*", "ratingChange": "*",
            "fields": "", "qType": "0", "orgCode": "", "rcode": "", "pageSize": "100"}
    for code, name in DELISTED_SPOT.items():
        params = dict(base, code=code, beginTime="2000-01-01", endTime="2027-01-01",
                      pageNo=1, pageNum=1, p=1, pageNumber=1)
        j = get_json(REPORT_LIST_URL, params)
        time.sleep(REQUEST_SLEEP)
        rows = list((j or {}).get("data") or [])
        tp = int((j or {}).get("TotalPage") or 0)
        for pg in range(2, tp + 1):
            params.update(pageNo=pg, pageNum=pg, p=pg, pageNumber=pg)
            jj = get_json(REPORT_LIST_URL, params)
            time.sleep(REQUEST_SLEEP)
            if jj:
                rows += jj.get("data") or []
        dates = sorted(r["publishDate"][:10] for r in rows if r.get("publishDate"))
        # F10 ycmx 对退市股
        jf = get_json(F10_FORECAST_URL, {"code": f"SZ{code}"},
                      referer="https://emweb.securities.eastmoney.com/")
        time.sleep(REQUEST_SLEEP)
        ycmx_n = len((jf or {}).get("ycmx") or [])
        results[code] = {
            "name": name,
            "per_code_rows": len(rows),
            "date_min": dates[0] if dates else None,
            "date_max": dates[-1] if dates else None,
            "present_in_global_monthly_pull": code in pulled_codes,
            "f10_ycmx_rows": ycmx_n,
        }
        print(f"[delisted] {code} {name}: rows={len(rows)} {dates[:1]}..{dates[-1:]}", flush=True)
    manifest["delisted_spot_check"] = results


def write_parquet_by_year(frames, date_col, table):
    """按 date_col 的年份分片落盘 analyst/{table}_{YYYY}.parquet"""
    if not frames:
        return
    big = pd.concat(frames, ignore_index=True)
    big[date_col] = pd.to_datetime(big[date_col], errors="coerce")
    big["_year"] = big[date_col].dt.year
    stats = {}
    for yr, sub in big.dropna(subset=["_year"]).groupby("_year"):
        path = PARQUET_DIR / f"{table}_{int(yr)}.parquet"
        # 同表重跑时覆盖同年分片
        sub = sub.drop(columns=["_year"])
        sub.to_parquet(path, index=False)
        stats[int(yr)] = len(sub)
        print(f"[write] {path.name} rows={len(sub)}", flush=True)
    entry = manifest["tables"].setdefault(table, {})
    entry["rows_total"] = len(big)
    entry["rows_by_year"] = stats
    entry["fields"] = sorted(big.columns.tolist())


def main():
    t0 = time.time()
    print(f"[run] out={OUT_DIR}", flush=True)

    frames = collect_research_report()
    delisted_spot_check(frames)
    write_parquet_by_year(frames, "publishDate", "research_report_em")

    snap, codes = collect_consensus_snapshot()
    if snap is not None and not snap.empty:
        snap.to_parquet(PARQUET_DIR / "consensus_snapshot_em_2026.parquet", index=False)
        manifest["tables"]["consensus_snapshot_em"]["fields"] = sorted(snap.columns.tolist())

    # F10 明细: 快照覆盖股票 + 三只退市抽查股
    universe = list(codes)
    for c in DELISTED_SPOT:
        sc = f"{c}.SZ"
        if sc not in universe:
            universe.append(sc)
    yframes = collect_forecast_detail(universe)
    write_parquet_by_year(yframes, "PUBLISH_DATE", "forecast_detail_em")

    manifest["elapsed_sec"] = round(time.time() - t0, 1)
    with open(OUT_DIR / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"[done] elapsed={manifest['elapsed_sec']}s failures={len(manifest['failures'])}", flush=True)


if __name__ == "__main__":
    main()
