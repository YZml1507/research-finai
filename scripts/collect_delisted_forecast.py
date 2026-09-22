#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collect_delisted_forecast.py — 退市股 cninfo「业绩预告」公告逐股补采

背景：东财 EM 业绩预告库（RPT_PUBLIC_OP_NEWPREDICT / ak.stock_yjyg_em）按现存上市
公司建档，已剔除大部分退市股（抽查 21 只 16 只全空）。巨潮资讯网（cninfo）作为
法定信披平台保留退市公司全部公告原文，用于补齐退市股横截面缺口。

流程：
  1) 退市股清单：akshare 交易所官方退市表
       ak.stock_info_sz_delist() / ak.stock_info_sh_delist()
     按「终止上市日期 ∈ [2015-01-01, 2024-12-31]」过滤（默认窗口，可调）。
     注：akshare 当前无北交所退市表；本窗口内实测无 4/8 字头退市代码。
  2) orgId 解析：cninfo 静态证券表
       https://www.cninfo.com.cn/new/data/szse_stock.json（实为全市场 A/B/CDR 含退市股）
       https://www.cninfo.com.cn/new/data/bj_stock.json
     未命中时按规则构造并回探验证（B 股退市股不入静态表）：
       深市 B 股孪生构造：'gssz' + str(int(code)-200000).zfill(7)  （200018→gssz0000018）
       通用自身构造：    'gssz'/'gssh' + code.zfill(7)            （200152→gssz0200152，900935→gssh0900935）
     每个候选 orgId 以一次 pageSize=1 探针验证：返回公告且 secCode==code 才采纳。
  3) 逐股检索：POST https://www.cninfo.com.cn/new/hisAnnouncement/query
       stock='{code},{orgId}'  column=szse|sse  tabName=fulltext
       searchkey='业绩预'  pageSize=30  翻页至 hasMore=false
     事后按标题正则过滤（见 FORECAST_PAT）：'业绩预告' 之外另有
     「业绩预亏/预增/预减/预盈公告」变体标题（如拉夏贝尔全部预告均为
     「业绩预亏公告」，窄口径会漏掉整类披露）。标题保留原文，预告类型
     归并由下游主控完成，不需要时可按 title 含「业绩预告」再过滤。
     searchkey 命中经全量翻页 ground-truth 抽查验证零漏检
     （300104→32 / 002680→9 / 002450→16，窄口径一致）。
  兜底：orgId 静态表未命中时再按公司名全文检索找回；searchkey 命中为 0
     的股票自动做一次全量翻页复核，区分「真无预告」与「索引漏检」。
  注意：退市股 orgId 为公司级且可能被继受实体共享（如 *ST二重 601268 与
     国机重装 601399 共用 9900010450，org 公告流混杂新老两代码），因此
     采集结果一律按 secCode==ts_code 过滤，空结果以全量复核再定论：
       empty_verified     —— 复核找到该股公告但确无预告类标题
       unresolved_orgId   —— orgId 候选全灭/复核中该股零公告（org 指错）
  4) 落盘：{out}/delisted_forecast/{code}.parquet
       字段：ann_date(YYYY-MM-DD), ts_code(裸码), title(原文,去<em>标签), url, orgId
     manifest.json + manifest.csv：每股状态/公告数/页数/错误登记 + 汇总分布。

用法：
  pip install akshare pandas pyarrow requests
  python3 collect_delisted_forecast.py --out ./out                       # 全量
  python3 collect_delisted_forecast.py --out ./out --only 300104,002680  # 子集
  python3 collect_delisted_forecast.py --out ./out --spotcheck 300104,002680,002450
  python3 collect_delisted_forecast.py --out ./out --em-check            # 东财对照零命中核验

限速：默认 0.5–1.0s 随机抖动/请求；单页失败重试 3 次（指数退避），仍败记 manifest
继续下一只，不中断全局。
"""

import argparse
import json
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

CNINFO_BASE = "https://www.cninfo.com.cn"
HIS_ANN_URL = f"{CNINFO_BASE}/new/hisAnnouncement/query"
STOCK_JSON = {
    "szse": f"{CNINFO_BASE}/new/data/szse_stock.json",   # 实为全市场 A/B/CDR（含退市股）
    "bj": f"{CNINFO_BASE}/new/data/bj_stock.json",
}
EM_YJYG_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
EM_YJYG_REPORT = "RPT_PUBLIC_OP_NEWPREDICT"

CN_TZ = timezone(timedelta(hours=8))
SEARCHKEY = "业绩预"
FORECAST_PAT = re.compile(r"业绩预告|业绩预[亏增减盈升降]")
PAGE_SIZE = 30
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"),
    "Referer": f"{CNINFO_BASE}/new/index",
    "X-Requested-With": "XMLHttpRequest",
}


def market_of(code: str) -> str:
    if code[:1] in ("6", "9"):
        return "SH"
    if code[:1] in ("4", "8"):
        return "BJ"
    return "SZ"


def column_of(code: str) -> str:
    return {"SH": "sse", "BJ": "bj", "SZ": "szse"}[market_of(code)]


def clean_title(t) -> str:
    return re.sub(r"</?em>", "", str(t or "")).strip()


# ---------------------------------------------------------------- 退市股清单

def get_delist_universe(start: str, end: str, sleep: float) -> pd.DataFrame:
    """交易所官方退市表（akshare 封装深交所/上交所 ShowReport）。返回
    {code, name, list_date, delist_date, mkt}，已按窗口过滤。"""
    import akshare as ak  # 延迟导入：--list-file 路径不依赖 akshare

    frames = []
    for func, mkt in ((ak.stock_info_sz_delist, "SZ"),
                      (ak.stock_info_sh_delist, "SH")):
        df = func()
        df.columns = ["code", "name", "list_date", "delist_date"]
        df["mkt"] = mkt
        frames.append(df)
        time.sleep(sleep)
    all_df = pd.concat(frames, ignore_index=True)
    all_df["code"] = all_df["code"].astype(str).str.zfill(6)
    all_df["list_date"] = pd.to_datetime(all_df["list_date"], errors="coerce")
    all_df["delist_date"] = pd.to_datetime(all_df["delist_date"], errors="coerce")
    win = (all_df[(all_df["delist_date"] >= start) & (all_df["delist_date"] <= end)]
           .drop_duplicates("code")          # 源表自带少量重复行（如 600555）
           .sort_values(["delist_date", "code"]).reset_index(drop=True))
    return win, all_df.reset_index(drop=True)


# ---------------------------------------------------------------- orgId 解析

def load_org_map(session: requests.Session) -> dict:
    """cninfo 静态证券表 code→orgId。szse_stock.json 实为全市场表（含 SH 代码与
    已退市股）；bj_stock.json 补北交所。"""
    org = {}
    for name, url in STOCK_JSON.items():
        r = session.get(url, timeout=30)
        r.raise_for_status()
        for x in r.json().get("stockList", []):
            org[str(x["code"]).zfill(6)] = x["orgId"]
    return org


def org_candidates(code: str, org_map: dict) -> list:
    """orgId 候选链：静态表 > 规则构造。B 股退市股不入静态表，按公司孪生/自身
    规则构造（深市 B 股 code-200000 即孪生 A 码；gssh/gssz+code.zfill(7) 对老
    编码恒真）。"""
    cands = []
    if code in org_map:
        cands.append(org_map[code])
    prefix = "gssh" if market_of(code) == "SH" else "gssz"
    cands.append(prefix + code.zfill(7))
    if code[:1] == "2":                      # 深 B 孪生：200xxx → 000xxx
        cands.append("gssz" + str(int(code) - 200000).zfill(7))
    return list(dict.fromkeys(cands))        # 保序去重


def probe_org(session: requests.Session, code: str, org_id: str,
              sleep_range) -> tuple:
    """探针验证：候选 orgId 查询第一页，返回 (是否采纳, 总公告数)。采纳标准：
    有公告即采纳（total>0）。注意退市股 orgId 可能被继受实体共享，首页可
    能全是新实体公告——不要求本页 secCode 匹配，是否真属于该股由采集端
    secCode 过滤与 empty 全量复核判定。"""
    data = {"pageNum": 1, "pageSize": 5, "column": column_of(code),
            "tabName": "fulltext", "plate": "", "stock": f"{code},{org_id}",
            "searchkey": "", "secid": "", "category": "", "trade": "", "seDate": ""}
    try:
        j = post_json(session, HIS_ANN_URL, data, sleep_range)
    except Exception:
        return False, -1
    total = j.get("totalAnnouncement") or 0
    return total > 0, total


def resolve_org_by_name(session: requests.Session, code: str, name: str,
                        sleep_range) -> str:
    """orgId 兜底：公司名全文检索，取首个 secCode==code 命中的 orgId。
    退市股部分代码不入静态表（如 601268 *ST二重 → 9900010450）。"""
    keys = [name]
    stripped = re.sub(r"[*＊]|退市|退$|ST|B$", "", name).strip()
    if stripped and stripped != name:
        keys.append(stripped)
    for key in keys:
        try:
            data = {"pageNum": 1, "pageSize": 30, "column": column_of(code),
                    "tabName": "fulltext", "plate": "", "stock": "",
                    "searchkey": key, "secid": "", "category": "",
                    "trade": "", "seDate": ""}
            j = post_json(session, HIS_ANN_URL, data, sleep_range)
        except Exception:
            continue
        for a in (j.get("announcements") or []):
            if a.get("secCode") == code and a.get("orgId"):
                return a["orgId"]
    return ""


# ---------------------------------------------------------------- 查询与采集

def post_json(session: requests.Session, url: str, data: dict, sleep_range,
              is_post: bool = True):
    """带限速+重试的 JSON 请求。每次请求前 sleep（含重试），间隔 0.5–1.0s。"""
    last_err = None
    for att in range(1, MAX_RETRIES + 1):
        time.sleep(random.uniform(*sleep_range))
        try:
            r = (session.post if is_post else session.get)(url, data=data if is_post else None,
                                                         params=None if is_post else data,
                                                         timeout=25)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001 —— 网络层异常类型杂，统一重试
            last_err = e
            wait = (2 ** att) + random.random()
            print(f"    [retry {att}/{MAX_RETRIES}] {type(e).__name__} "
                  f"{str(e)[:120]}; sleep {wait:.1f}s", flush=True)
            time.sleep(wait)
    raise last_err


def query_announcements(session: requests.Session, code: str, org_id: str,
                        searchkey: str, sleep_range, max_pages: int = 0) -> tuple:
    """翻页拉取公告。searchkey 非空时为全文检索模式；max_pages>0 时截断翻页
    （empty 复核用上限控制）。返回 (全部命中行, 页数)。"""
    rows, page = [], 1
    while True:
        data = {"pageNum": page, "pageSize": PAGE_SIZE, "column": column_of(code),
                "tabName": "fulltext", "plate": "", "stock": f"{code},{org_id}",
                "searchkey": searchkey, "secid": "", "category": "", "trade": "",
                "seDate": ""}
        j = post_json(session, HIS_ANN_URL, data, sleep_range)
        anns = j.get("announcements") or []
        rows.extend(anns)
        if not j.get("hasMore") or not anns or (max_pages and page >= max_pages):
            return rows, page
        page += 1


def to_records(code: str, org_id: str, anns: list) -> pd.DataFrame:
    """过滤「标题含业绩预告」并规整为产出 schema。按 announcementId 去重。"""
    seen, recs = set(), []
    for a in anns:
        if a.get("secCode") != code:   # 共享 orgId 下混入继受实体公告，逐行过滤
            continue
        aid = a.get("announcementId") or (a.get("adjunctUrl"), a.get("announcementTime"))
        if aid in seen:
            continue
        seen.add(aid)
        title = clean_title(a.get("announcementTitle"))
        if not FORECAST_PAT.search(title):
            continue
        ts = a.get("announcementTime")
        ann_date = (datetime.fromtimestamp(ts / 1000, tz=CN_TZ).strftime("%Y-%m-%d")
                    if ts else None)
        adjunct = a.get("adjunctUrl") or ""
        recs.append({
            "ann_date": ann_date,
            "ts_code": code,
            "title": title,
            "url": ("https://static.cninfo.com.cn/" + adjunct) if adjunct else None,
            "orgId": a.get("orgId") or org_id,
        })
    return pd.DataFrame.from_records(
        recs, columns=["ann_date", "ts_code", "title", "url", "orgId"])


# ---------------------------------------------------------------- 对照工具

def em_yjyg_count(session: requests.Session, code: str) -> int:
    """东财业绩预告底表按 SECURITY_CODE 直查命中数（对照用：退市股应为 0）。"""
    time.sleep(random.uniform(0.5, 1.0))
    r = session.get(EM_YJYG_URL, params={
        "reportName": EM_YJYG_REPORT, "columns": "ALL",
        "pageSize": "5", "pageNumber": "1",
        "filter": f'(SECURITY_CODE="{code}")'},
        headers={"User-Agent": HEADERS["User-Agent"]}, timeout=25)
    res = r.json().get("result")
    return (res or {}).get("count") or 0


# ---------------------------------------------------------------- 主流程

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="./out", help="输出根目录")
    ap.add_argument("--start", default="2015-01-01", help="退市日窗口下界")
    ap.add_argument("--end", default="2024-12-31", help="退市日窗口上界")
    ap.add_argument("--list-file", default=None,
                    help="复用已落盘的退市清单 parquet（跳过 akshare 拉取）")
    ap.add_argument("--only", default=None, help="只跑给定代码，逗号分隔")
    ap.add_argument("--sleep-lo", type=float, default=0.5)
    ap.add_argument("--sleep-hi", type=float, default=1.0)
    ap.add_argument("--spotcheck", default=None,
                    help="对给定代码做全量翻页 ground-truth 校验（不写正式产出）")
    ap.add_argument("--em-check", action="store_true",
                    help="对清单内代码抽查东财 yjyg 命中数（对照用）")
    ap.add_argument("--no-verify-empty", dest="verify_empty",
                    action="store_false", help="关闭 empty 股全量翻页复核")
    ap.add_argument("--verify-max-pages", type=int, default=100,
                    help="empty 复核全量翻页页数上限（默认100页=3000条）")
    args = ap.parse_args()

    out_dir = Path(args.out)
    fc_dir = out_dir / "delisted_forecast"
    fc_dir.mkdir(parents=True, exist_ok=True)
    sleep_range = (min(args.sleep_lo, args.sleep_hi), max(args.sleep_lo, args.sleep_hi))

    session = requests.Session()
    session.headers.update(HEADERS)

    # ---- 退市股清单 ----
    if args.list_file:
        universe = pd.read_parquet(args.list_file).reset_index(drop=True)
        all_df = universe
    else:
        universe, all_df = get_delist_universe(args.start, args.end,
                                             sleep_range[1])
        all_df.to_parquet(out_dir / "delist_all.parquet", index=False)
        universe.to_parquet(out_dir / "delist_universe.parquet", index=False)
    if args.only:
        keep = {c.strip().zfill(6) for c in args.only.split(",")}
        universe = universe[universe["code"].isin(keep)].reset_index(drop=True)
    print(f"[universe] 窗口 {args.start}~{args.end} 退市股 {len(universe)} 只"
          f"（全史 {len(all_df)} 只）", flush=True)

    # ---- orgId 底表 ----
    org_map = load_org_map(session)
    print(f"[orgId] 静态表 {len(org_map)} 条", flush=True)

    # ---- 抽查：全量翻页 ground-truth（验证 searchkey 完备性）----
    if args.spotcheck:
        report = {}
        for code in [c.strip().zfill(6) for c in args.spotcheck.split(",")]:
            org_id = None
            for cand in org_candidates(code, org_map):
                ok, _ = probe_org(session, code, cand, sleep_range)
                if ok:
                    org_id = cand
                    break
            if not org_id:
                print(f"[spotcheck] {code} orgId 未解析，跳过", flush=True)
                continue
            anns, pages = query_announcements(session, code, org_id, "", sleep_range)
            truth = to_records(code, org_id, anns)
            sk, _ = query_announcements(session, code, org_id, SEARCHKEY, sleep_range)
            sk_df = to_records(code, org_id, sk)
            missing = sorted(set(truth.title) - set(sk_df.title))
            report[code] = {"orgId": org_id, "total_ann_pages": pages,
                            "truth_n": len(truth), "searchkey_n": len(sk_df),
                            "missed_titles": missing}
            print(f"[spotcheck] {code} 全量{pages}页 truth={len(truth)} "
                  f"searchkey={len(sk_df)} 漏={missing or '无'}", flush=True)
        (out_dir / "spotcheck_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=1))
        print(f"[spotcheck] 报告写入 {out_dir/'spotcheck_report.json'}", flush=True)

    # ---- 东财对照 ----
    em_counts = {}
    if args.em_check:
        for code in universe["code"]:
            try:
                em_counts[code] = em_yjyg_count(session, code)
            except Exception as e:  # noqa: BLE001
                em_counts[code] = f"err:{type(e).__name__}"
        hits = {k: v for k, v in em_counts.items() if isinstance(v, int) and v > 0}
        print(f"[em-check] 东财 yjyg 命中>0 的退市股: {len(hits)}/{len(em_counts)} "
              f"{dict(list(hits.items())[:10])}", flush=True)
        (out_dir / "em_check.json").write_text(
            json.dumps(em_counts, ensure_ascii=False, indent=1))

    # ---- 逐股采集 ----
    per_stock, t0 = [], time.time()
    for i, row in universe.iterrows():
        code, mkt = row["code"], row["mkt"]
        rec = {"code": code, "name": row["name"], "mkt": mkt,
               "delist_date": str(row["delist_date"])[:10],
               "list_date": str(row["list_date"])[:10],
               "orgId": None, "status": "failed", "n_ann": 0,
               "pages": 0, "error": None}
        try:
            org_id = None
            for cand in org_candidates(code, org_map):
                ok, total = probe_org(session, code, cand, sleep_range)
                if ok:
                    org_id = cand
                    break
            if not org_id:
                org_id = resolve_org_by_name(session, code, row["name"],
                                             sleep_range)
                if org_id:
                    ok, _ = probe_org(session, code, org_id, sleep_range)
                    if not ok:
                        org_id = None
            if not org_id:
                rec["status"] = "unresolved_orgId"
            else:
                rec["orgId"] = org_id
                anns, pages = query_announcements(session, code, org_id,
                                                  SEARCHKEY, sleep_range)
                df = to_records(code, org_id, anns)
                rec["pages"] = pages
                if len(df) == 0 and args.verify_empty:
                    # searchkey 零命中 → 全量翻页复核，区分真无预告与索引漏检。
                    # 复核中若根本见不到 secCode==code 的行，说明 orgId 指错
                    # 实体，回退 unresolved_orgId。
                    all_anns, pages2 = query_announcements(
                        session, code, org_id, "", sleep_range,
                        max_pages=args.verify_max_pages)
                    df = to_records(code, org_id, all_anns)
                    rec["pages"] += pages2
                    rec["verified"] = True
                    if not any(a.get("secCode") == code for a in all_anns):
                        rec["status"] = "unresolved_orgId"
                        rec["error"] = "verify: 全量复核未见该股公告"
                        per_stock.append(rec)
                        print(f"[{i + 1}/{len(universe)}] {code} {row['name']} "
                              f"{rec['status']}", flush=True)
                        continue
                rec["n_ann"] = len(df)
                df.to_parquet(fc_dir / f"{code}.parquet", index=False)
                rec["status"] = "ok" if len(df) else (
                    "empty_verified" if rec.get("verified") else "empty")
        except Exception as e:  # noqa: BLE001
            rec["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        per_stock.append(rec)
        print(f"[{i + 1}/{len(universe)}] {code} {row['name']} "
              f"{rec['status']} n={rec['n_ann']} pages={rec['pages']}",
              flush=True)

    manifest_df = pd.DataFrame(per_stock)
    manifest_df.to_csv(out_dir / "manifest.csv", index=False)

    dist = manifest_df["n_ann"].value_counts(
        bins=[-1, 0, 5, 10, 20, 50, 10 ** 9]).sort_index()
    summary = {
        "generated_at": datetime.now(tz=CN_TZ).isoformat(),
        "delist_window": [args.start, args.end],
        "stocks_total": len(manifest_df),
        "ok": int((manifest_df.status == "ok").sum()),
        "empty": int(manifest_df.status.str.startswith("empty").sum()),
        "unresolved_orgId": int((manifest_df.status == "unresolved_orgId").sum()),
        "failed": int((manifest_df.status == "failed").sum()),
        "announcements_total": int(manifest_df.n_ann.sum()),
        "n_ann_distribution": {str(k): int(v) for k, v in dist.items()},
        "n_ann_max": int(manifest_df.n_ann.max() or 0),
        "n_ann_median": float(manifest_df.n_ann.median() or 0),
        "elapsed_sec": round(time.time() - t0, 1),
        "em_check": em_counts or None,
        "spotcheck": args.spotcheck or None,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps({**summary, "per_stock": per_stock},
                   ensure_ascii=False, indent=1))
    print(f"[done] {summary}", flush=True)


if __name__ == "__main__":
    main()
