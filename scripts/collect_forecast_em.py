#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collect_forecast_em.py — 东方财富业绩预告(yjyg) / 业绩快报(yjkb) / 业绩报表(yjbb) 全 A 全史采集

数据源：akshare 免费接口（免 key），底层为东财 datacenter-web API
  ak.stock_yjyg_em(date)  业绩预告：列含 股票代码/股票简称/预测指标/业绩变动/预测数值/
                                  业绩变动幅度/业绩变动原因/预告类型(预增预减扭亏首亏续亏
                                  续盈略增略减不确定)/上年同期值/公告日期
  ak.stock_yjkb_em(date)  业绩快报：每股收益/营业收入组/净利润组/每股净资产/净资产收益率/
                                  所处行业/公告日期
  ak.stock_yjbb_em(date)  业绩报表（正式披露）：营业总收入组/净利润组/每股经营现金流量/
                                  销售毛利率/所处行业/最新公告日期

切分方式：按**报告期**逐期拉取 —— 每年 4 期（0331/0630/0930/1231）。
实测东财 2009 年末期即有数据，历史深度覆盖 2010 至今。
退市股覆盖：东财该库按公告存档，历史期记录中退市股通常仍在，
采集后可用已知退市股（乐视网 300104 / 康得新 002450 / 长生生物 002680 等）抽查核验。

输出:
  <out>/yjyg/{period}.parquet     —— 原样落盘不清洗（period 形如 20101231）
  <out>/yjkb/{period}.parquet
  <out>/yjbb/{period}.parquet
  <out>/manifest.csv + manifest.json —— 每期行数/状态/失败登记/字段清单/公告日字段存在性

用法:
  pip install akshare pandas pyarrow
  python3 collect_forecast_em.py --out ./out --start-year 2010

限速: 基准 sleep 0.8s/期；瞬断/限频指数退避（单期最多 8 次，最长 ~120s/次）。
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import akshare as ak
import pandas as pd
import requests

API_FUNCS = {
    "yjyg": "stock_yjyg_em",   # 业绩预告
    "yjkb": "stock_yjkb_em",   # 业绩快报
    "yjbb": "stock_yjbb_em",   # 业绩报表（正式披露，补充）
}
# 各接口底层东财 API 原始参数（取自 akshare 源码）。用途：akshare 在该期**真无数据**
# 时不对空信封做判空，`data_json["result"]["pages"]` 直接抛 TypeError——
# 用 raw_probe 区分「该期真空」与「解析器/网络真坏」，避免把空期误记 failed。
RAW_SPEC = {
    "yjyg": {
        "url": "https://datacenter.eastmoney.com/securities/api/data/v1/get",
        "report_name": "RPT_PUBLIC_OP_NEWPREDICT",
        "filter_tmpl": "(REPORT_DATE='{d}')",
    },
    "yjkb": {
        "url": "https://datacenter.eastmoney.com/securities/api/data/v1/get",
        "report_name": "RPT_FCI_PERFORMANCEE",
        "filter_tmpl": ('(SECURITY_TYPE_CODE in ("058001001","058001008"))'
                        '(TRADE_MARKET_CODE!="069001017")'
                        "(REPORT_DATE='{d}')"),
    },
    "yjbb": {
        "url": "https://datacenter-web.eastmoney.com/api/data/v1/get",
        "report_name": "RPT_LICO_FN_CPD",
        "filter_tmpl": "(REPORTDATE='{d}')",
    },
}
# 各接口的公告日期字段名（PIT 对齐关键）
ANN_DATE_FIELD = {"yjyg": "公告日期", "yjkb": "公告日期", "yjbb": "最新公告日期"}

QUARTER_ENDS = ("0331", "0630", "0930", "1231")
DEFAULT_SLEEP = 0.8
MAX_RETRIES = 8


def call_api(func_name: str, period: str, sleep: float) -> pd.DataFrame:
    """调 akshare 接口，瞬断/限频自适应退避；无权限概念（免费源），一切异常都重试到底后抛出。"""
    delay = 2.0
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = getattr(ak, func_name)(date=period)
            time.sleep(sleep)
            return df if df is not None else pd.DataFrame()
        except Exception as e:  # noqa: BLE001 —— akshare 底层异常类型杂（requests/json/KeyError）
            last_err = e
            wait = delay + random.random()
            print(f"  [retry {attempt}/{MAX_RETRIES}] {func_name} {period}: "
                  f"{type(e).__name__} {str(e)[:150]}; sleep {wait:.1f}s", flush=True)
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
            delay = min(delay * 2, 120)
    raise last_err  # pragma: no cover


def raw_probe_has_data(api_name: str, period: str) -> bool:
    """原始请求判空：result=null 或 pages=0 → 该期真无数据。探针出错返回 True（保守：仍按 failed 记）。"""
    spec = RAW_SPEC[api_name]
    d = f"{period[:4]}-{period[4:6]}-{period[6:]}"
    params = {
        "pageSize": "1", "pageNumber": "1",
        "reportName": spec["report_name"], "columns": "ALL",
        "filter": spec["filter_tmpl"].format(d=d),
    }
    try:
        r = requests.get(spec["url"], params=params, timeout=30)
        res = r.json().get("result")
        if res is None:
            return False
        return (res.get("pages") or 0) > 0
    except Exception:  # noqa: BLE001
        return True


def collect_api(api_name: str, out_dir: Path, periods: list, sleep: float,
                skip_existing: bool = False) -> list:
    """逐期采集并落盘 parquet，返回 manifest 行列表。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    func_name = API_FUNCS[api_name]
    rows = []
    for period in periods:
        rec = {"api": api_name, "period": period, "rows": 0,
               "status": "ok", "error": "", "n_cols": 0}
        t0 = time.time()
        if skip_existing and (out_dir / f"{period}.parquet").exists():
            df_old = pd.read_parquet(out_dir / f"{period}.parquet")
            rec.update(rows=len(df_old), n_cols=len(df_old.columns),
                       status="reused", elapsed_s=round(time.time() - t0, 1))
            print(f"{api_name} {period}: reused rows={rec['rows']}", flush=True)
            rows.append(rec)
            continue
        try:
            df = call_api(func_name, period, sleep)
            if df.empty:
                rec["status"] = "empty"
            else:
                rec["rows"] = len(df)
                rec["n_cols"] = len(df.columns)
                df.to_parquet(out_dir / f"{period}.parquet", index=False)
        except Exception as e:  # noqa: BLE001
            # akshare 对真空期抛 TypeError('NoneType' not subscriptable) 之类；
            # 用原始探针复核：真无数据 → 记 empty（verified_empty），否则记 failed。
            if not raw_probe_has_data(api_name, period):
                rec["status"] = "empty"
                rec["error"] = "verified_empty_via_raw_probe"
            else:
                rec["status"] = "failed"
                rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
                print(f"  [FAILED] {api_name} {period}: {rec['error']}", flush=True)
        rec["elapsed_s"] = round(time.time() - t0, 1)
        print(f"{api_name} {period}: {rec['status']} rows={rec['rows']} "
              f"({rec['elapsed_s']}s)", flush=True)
        rows.append(rec)
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="输出目录")
    ap.add_argument("--start-year", type=int, default=2010)
    ap.add_argument("--end-year", type=int, default=datetime.now().year)
    ap.add_argument("--apis", default="yjyg,yjkb,yjbb",
                    help=f"逗号分隔，可选：{','.join(API_FUNCS)}")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP)
    ap.add_argument("--skip-existing", action="store_true",
                    help="已有 parquet 的期次直接复用（不重拉），只补缺的期")
    args = ap.parse_args()

    periods = [f"{y}{md}" for y in range(args.start_year, args.end_year + 1)
               for md in QUARTER_ENDS]

    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    wanted = [a.strip() for a in args.apis.split(",") if a.strip()]
    unknown = set(wanted) - set(API_FUNCS)
    if unknown:
        raise SystemExit(f"未知 api: {unknown}; 可选 {sorted(API_FUNCS)}")

    manifest_rows = []
    api_summary = {}
    for api_name in wanted:
        subdir = out_root / api_name
        rows = collect_api(api_name, subdir, periods, args.sleep,
                           args.skip_existing)
        manifest_rows.extend(rows)
        # 用第一期成功落盘的 parquet 读回字段清单与公告日字段存在性
        cols, ann_field = [], None
        for r in rows:
            if r["status"] == "ok" and r["rows"] > 0:
                cols = pd.read_parquet(subdir / f"{r['period']}.parquet").columns.tolist()
                ann_field = ANN_DATE_FIELD.get(api_name)
                break
        api_summary[api_name] = {
            "status": "done",
            "func": API_FUNCS[api_name],
            "total_rows": sum(r["rows"] for r in rows),
            "nonempty_periods": sum(1 for r in rows
                                  if r["status"] in ("ok", "reused") and r["rows"] > 0),
            "empty_periods": [r["period"] for r in rows if r["status"] == "empty"],
            "failed_periods": [{"period": r["period"], "error": r["error"]}
                               for r in rows if r["status"] == "failed"],
            "columns": cols,
            "ann_date_field": ann_field,
            "ann_date_field_present": ann_field in cols if ann_field else None,
        }

    pd.DataFrame(manifest_rows).to_csv(out_root / "manifest.csv", index=False)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "eastmoney via akshare",
        "akshare_version": ak.__version__,
        "start_year": args.start_year, "end_year": args.end_year,
        "n_periods": len(periods),
        "apis": api_summary,
        "rows": manifest_rows,
    }
    (out_root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(api_summary, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
