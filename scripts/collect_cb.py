#!/usr/bin/env python3
"""collect_cb.py — A-share convertible bond full-history data collection via akshare.

Collects, into ./cb_data/:
  cb_issue_cninfo.parquet   cninfo issuance table (raw, universe source, incl. delisted)
  cb_list_ths.parquet       THS bond list (raw)
  cb_meta.parquet           assembled meta: code/name/market/issue size/conv start/list/delist/stock
  cb_meta_em/{code}.parquet raw per-bond EM info frames (LISTING_DATE/DELIST_DATE/...)
  cb_daily/{sym}.parquet    full daily OHLCV per bond (sina; delisted included)
  cb_premium/{code}.parquet daily premium analysis per bond (raw column names kept)
  manifest.jsonl            one record per (dataset, symbol) attempt
  cb_cov_comparison.parquet EM comparison table (raw, best-effort)

No data cleaning; everything is written as returned by akshare.
Resumable: existing non-empty parquet outputs are skipped; 'fail' entries are retried on rerun.
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

OUT = Path("cb_data")
DAILY_DIR = OUT / "cb_daily"
PREM_DIR = OUT / "cb_premium"
META_EM_DIR = OUT / "cb_meta_em"
MANIFEST = OUT / "manifest.jsonl"

SLEEP_LO, SLEEP_HI = 0.3, 0.8
RETRIES = 3
RETRY_BACKOFF = (2.0, 5.0)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def manifest_log(dataset: str, symbol: str, status: str, rows: int = 0, error: str = "") -> None:
    with MANIFEST.open("a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "ts": now_iso(),
                    "dataset": dataset,
                    "symbol": symbol,
                    "status": status,
                    "rows": rows,
                    "error": error,
                },
                ensure_ascii=False,
            )
            + "\n"
        )


def sleep_jitter() -> None:
    time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))


def fetch_with_retry(fn, label: str, **kwargs):
    """Call fn(**kwargs) up to RETRIES times. Returns (df, error_str_or_None)."""
    err = None
    for attempt in range(RETRIES):
        try:
            df = fn(**kwargs)
            return df, None
        except Exception as e:  # noqa: BLE001 - record anything, keep collecting
            err = f"{type(e).__name__}: {e}"[:500]
            if attempt < RETRIES - 1:
                time.sleep(RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)])
    return None, err


def done_ids(dataset: str) -> set:
    """Symbols already recorded ok/empty in the manifest (skip on rerun)."""
    ids = set()
    if MANIFEST.exists():
        with MANIFEST.open(encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("dataset") == dataset and rec.get("status") in ("ok", "empty"):
                    ids.add(rec["symbol"])
    return ids


def mkt_prefix(market: str, code: str) -> str:
    m = str(market)
    if "深" in m:
        return "sz"
    if "上" in m:
        return "sh"
    if code.startswith(("10", "11")):
        return "sh"
    if code.startswith("12"):
        return "sz"
    return "sz"  # 404xxx delisted-board and anything else: try sz, failure is recorded


def build_universe(ak, refresh: bool) -> pd.DataFrame:
    """Return universe df: code, symbol(prefixed), name, market, source flags."""
    issue_path = OUT / "cb_issue_cninfo.parquet"
    ths_path = OUT / "cb_list_ths.parquet"
    cmp_path = OUT / "cb_cov_comparison.parquet"

    if issue_path.exists() and not refresh:
        issue = pd.read_parquet(issue_path)
    else:
        issue, err = fetch_with_retry(
            ak.bond_cov_issue_cninfo, "cb_issue_cninfo", start_date="19900101", end_date="20351231"
        )
        if issue is None:
            manifest_log("cb_issue_cninfo", "-", "fail", 0, err or "")
            raise RuntimeError(f"cninfo issue table failed: {err}")
        issue.to_parquet(issue_path)
        manifest_log("cb_issue_cninfo", "-", "ok", len(issue))
        sleep_jitter()

    if ths_path.exists() and not refresh:
        ths = pd.read_parquet(ths_path)
    else:
        ths, err = fetch_with_retry(ak.bond_zh_cov_info_ths, "cb_list_ths")
        if ths is None:
            manifest_log("cb_list_ths", "-", "fail", 0, err or "")
            ths = pd.DataFrame(columns=["债券代码"])
        else:
            ths.to_parquet(ths_path)
            manifest_log("cb_list_ths", "-", "ok", len(ths))
        sleep_jitter()

    # Best-effort third list source (EM comparison table; live bonds only)
    if not cmp_path.exists() or refresh:
        cmp_df, err = fetch_with_retry(ak.bond_cov_comparison, "cb_cov_comparison")
        if cmp_df is None:
            manifest_log("cb_cov_comparison", "-", "fail", 0, err or "")
        else:
            cmp_df.to_parquet(cmp_path)
            manifest_log("cb_cov_comparison", "-", "ok", len(cmp_df))
        sleep_jitter()

    issue["债券代码"] = issue["债券代码"].astype(str).str.zfill(6)
    rows = {}
    for _, r in issue.iterrows():
        key = (r["债券代码"], str(r.get("交易市场", "")))
        rows[key] = {
            "code": r["债券代码"],
            "market": r.get("交易市场", ""),
            "name_cninfo": r.get("债券简称", ""),
            "src_cninfo": True,
            "src_ths": False,
        }
    for _, r in ths.iterrows():
        code = str(r.get("债券代码", "")).zfill(6)
        # reuse the cninfo key when the code already exists there (same code, its market)
        existing = [k for k in rows if k[0] == code]
        if existing:
            for k in existing:
                rows[k]["src_ths"] = True
            key = existing[0]
        else:
            sym = mkt_prefix("", code)
            key = (code, "上交所" if sym == "sh" else "深交所")
            rows[key] = {
                "code": code,
                "market": key[1],
                "name_cninfo": "",
                "src_cninfo": False,
                "src_ths": True,
            }
        if not rows[key]["name_cninfo"]:
            rows[key]["name_cninfo"] = str(r.get("债券简称", ""))

    uni = pd.DataFrame(rows.values())
    uni["symbol"] = uni.apply(lambda r: mkt_prefix(r["market"], r["code"]) + r["code"], axis=1)
    uni = uni.sort_values("symbol").reset_index(drop=True)
    return uni, issue, ths


def collect_meta_em(ak, uni: pd.DataFrame, limit: int | None) -> None:
    done = done_ids("cb_meta_em")
    codes = sorted(set(uni["code"]))
    n = 0
    for code in codes:
        path = META_EM_DIR / f"{code}.parquet"
        if code in done or (path.exists() and path.stat().st_size > 0):
            continue
        df, err = fetch_with_retry(ak.bond_zh_cov_info, "cb_meta_em", symbol=code)
        if df is None:
            manifest_log("cb_meta_em", code, "fail", 0, err or "")
        elif df.empty:
            manifest_log("cb_meta_em", code, "empty", 0)
        else:
            df.to_parquet(path)
            manifest_log("cb_meta_em", code, "ok", len(df))
        n += 1
        sleep_jitter()
        if limit and n >= limit:
            break
    print(f"[cb_meta_em] attempted {n} this run", flush=True)


def collect_daily(ak, uni: pd.DataFrame, limit: int | None) -> None:
    done = done_ids("cb_daily")
    n = 0
    for _, r in uni.iterrows():
        sym = r["symbol"]
        path = DAILY_DIR / f"{sym}.parquet"
        if sym in done or (path.exists() and path.stat().st_size > 0):
            continue
        df, err = fetch_with_retry(ak.bond_zh_hs_cov_daily, "cb_daily", symbol=sym)
        if df is None:
            manifest_log("cb_daily", sym, "fail", 0, err or "")
        elif df.empty:
            manifest_log("cb_daily", sym, "empty", 0)
        else:
            df.to_parquet(path)
            manifest_log("cb_daily", sym, "ok", len(df))
        n += 1
        sleep_jitter()
        if limit and n >= limit:
            break
    print(f"[cb_daily] attempted {n} this run", flush=True)


def collect_premium(ak, uni: pd.DataFrame, limit: int | None) -> None:
    done = done_ids("cb_premium")
    codes = sorted(set(uni["code"]))
    n = 0
    for code in codes:
        path = PREM_DIR / f"{code}.parquet"
        if code in done or (path.exists() and path.stat().st_size > 0):
            continue
        df, err = fetch_with_retry(ak.bond_zh_cov_value_analysis, "cb_premium", symbol=code)
        if df is None:
            manifest_log("cb_premium", code, "fail", 0, err or "")
        elif df.empty:
            manifest_log("cb_premium", code, "empty", 0)
        else:
            df.to_parquet(path)
            manifest_log("cb_premium", code, "ok", len(df))
        n += 1
        sleep_jitter()
        if limit and n >= limit:
            break
    print(f"[cb_premium] attempted {n} this run", flush=True)


def assemble_meta(uni: pd.DataFrame, issue: pd.DataFrame) -> pd.DataFrame:
    """cb_meta.parquet: one row per (code, market) universe entry."""
    em_rows = {}
    for code in uni["code"].unique():
        p = META_EM_DIR / f"{code}.parquet"
        if p.exists() and p.stat().st_size > 0:
            d = pd.read_parquet(p)
            if len(d):
                em_rows[code] = d.iloc[0].to_dict()

    out = []
    for _, r in uni.iterrows():
        code = r["code"]
        em = em_rows.get(code, {})
        rec = {
            "code": code,
            "symbol": r["symbol"],
            "name": r.get("name_cninfo", ""),
            "market": r.get("market", ""),
            "src_cninfo": r.get("src_cninfo", False),
            "src_ths": r.get("src_ths", False),
            "meta_source": "em" if em else "cninfo_only",
            # required fields (EM when present)
            "listing_date": em.get("LISTING_DATE"),
            "delist_date": em.get("DELIST_DATE"),
            "stock_code": em.get("CONVERT_STOCK_CODE"),
            "conv_start_date_em": em.get("TRANSFER_START_DATE"),
            "issue_scale_yi": em.get("ACTUAL_ISSUE_SCALE"),
            "expire_date": em.get("EXPIRE_DATE"),
            "em_name": em.get("SECURITY_NAME_ABBR"),
        }
        out.append(rec)
    meta = pd.DataFrame(out)

    # overlay cninfo issue fields (raw names kept under cn_*)
    iss = issue.copy()
    iss["债券代码"] = iss["债券代码"].astype(str).str.zfill(6)
    iss = iss.rename(
        columns={
            "发行起始日": "cn_issue_start",
            "实际发行总量": "cn_issue_amount_wan",
            "转股开始日期": "cn_conv_start",
            "转股终止日期": "cn_conv_end",
            "公告日期": "cn_ann_date",
            "初始转股价格": "cn_init_conv_price",
            "网上申购日期": "cn_subscribe_date",
            "交易市场": "cn_market",
        }
    )
    keep = [
        "债券代码",
        "cn_market",
        "cn_ann_date",
        "cn_issue_start",
        "cn_issue_amount_wan",
        "cn_conv_start",
        "cn_conv_end",
        "cn_init_conv_price",
        "cn_subscribe_date",
    ]
    iss = iss[keep].drop_duplicates(subset=["债券代码", "cn_market"], keep="first")
    meta = meta.merge(
        iss,
        left_on=["code", "market"],
        right_on=["债券代码", "cn_market"],
        how="left",
    ).drop(columns=["债券代码", "cn_market"])
    meta.to_parquet(OUT / "cb_meta.parquet")
    return meta


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="max items per dataset (testing)")
    ap.add_argument("--refresh", action="store_true", help="refetch list sources")
    ap.add_argument("--meta-only", action="store_true", help="skip daily/premium")
    args = ap.parse_args()

    import akshare as ak

    for d in (OUT, DAILY_DIR, PREM_DIR, META_EM_DIR):
        d.mkdir(parents=True, exist_ok=True)

    uni, issue, ths = build_universe(ak, args.refresh)
    uni.to_parquet(OUT / "cb_universe.parquet")
    print(f"universe: {len(uni)} (code, market) entries", flush=True)

    collect_meta_em(ak, uni, args.limit)
    if not args.meta_only:
        collect_daily(ak, uni, args.limit)
        collect_premium(ak, uni, args.limit)

    meta = assemble_meta(uni, issue)
    print(f"cb_meta.parquet: {len(meta)} rows", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
