#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""collect_esop.py — A股正向事件族公告历史采集（cninfo 公告全文检索）

事件族: 员工持股计划(ESOP) / 股权激励 / 定向增发 / 非公开发行
用途: FinAI2.0 事件驱动 alpha 轴的正向事件补族（文献先验: 员工持股/股权激励
      公告在 A 股存在正向漂移）。标题原文保留，细类归并（草案/进展/完成/失效）
      由下游主控完成。

数据源:
  cninfo 公告全文检索 POST http://www.cninfo.com.cn/new/hisAnnouncement/query
  （单路全市场: column 参数不做市场过滤，含北交所/退市股，不剔）

实测口径 (承袭 collect_letters.py 2026-09-21 实测):
  - pageSize 硬上限 30（传更大值仍返回 30 条）
  - column 参数不做市场过滤（szse/sse/bj 返回同一 total，仅影响排序），
    全市场一并返回
  - 深分页上限约 150 页（≈4500 条），超限后固定返回最后一页内容 ->
    按月分片；分片内 reported_total > 4000 则自动二分时间窗（半月/旬）
  - searchkey 命中标题或正文；isHLtitle=false 取原始标题（不带 <em>）
  - announcementTime 为毫秒时间戳，UTC 日期即公告日
  - 关键词会同时命中事件原件与衍生公告（进展/完成/修订/失效/问询回复等）；
    原样全部落盘，不丢原始行

输出 (out dir, 默认 ./esop_events):
  {kw}_{YYYYMM}.parquet    按月分片事件表:
                           ann_date(YYYY-MM-DD), ts_code(裸码6位), kw(四关键词之一),
                           title(公告标题原文), url(公告PDF静态链接)
  parts/                   月内二分时的一分片缓存（续跑复用）
  manifest.jsonl           每分片一条登记: kw/period/reported_total/fetched/failed_pages/status
  .done_*                  续跑断点标记

用法:
  python3 collect_esop.py --out ./esop_events            # 2015-01 ~ 2024-12 全量（可续跑）
  python3 collect_esop.py --out ./esop_events --kw 股权激励 --start 2020-01-01 --end 2020-12-31

限速: 每请求 sleep 0.5~1.0s 抖动; 每页失败重试 3 次(退避 2s/5s/12s)后记入 manifest。
"""

import argparse
import datetime as dt
import json
import math
import random
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests

API = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
STATIC_BASE = "http://static.cninfo.com.cn/"
PAGE_SIZE = 30          # 硬上限
PAGE_CAP = 150          # 深分页上限（经验值）
SPLIT_GUARD = 4000      # 分片 reported_total 超过此值则二分时间窗
KEYWORDS = ["员工持股计划", "股权激励", "定向增发", "非公开发行"]
SLEEP_LO, SLEEP_HI = 0.5, 1.0
RETRIES = 3
RETRY_BACKOFF = (2.0, 5.0, 12.0)
EM_PAT = re.compile(r"</?em>")


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def month_shards(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        nxt = (cur.replace(day=28) + dt.timedelta(days=7)).replace(day=1)
        yield cur, min(nxt - dt.timedelta(days=1), end)
        cur = nxt


def norm_rows(rows, kw):
    """cninfo 原始行 -> 产出表行 (ann_date, ts_code, kw, title, url)。"""
    out = []
    for a in rows:
        ms = a.get("announcementTime")
        try:
            ann_date = dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc).date().isoformat()
        except (TypeError, ValueError, OSError):
            ann_date = ""
        code = str(a.get("secCode") or "").strip()
        adjunct = str(a.get("adjunctUrl") or "").strip()
        out.append({
            "ann_date": ann_date,
            "ts_code": code.zfill(6) if code and code.isdigit() else code,
            "kw": kw,
            "title": EM_PAT.sub("", str(a.get("announcementTitle") or "")).strip(),
            "url": (STATIC_BASE + adjunct) if adjunct else "",
        })
    return out


class Collector:
    def __init__(self, out: Path):
        self.out = out
        out.mkdir(parents=True, exist_ok=True)
        (out / "parts").mkdir(exist_ok=True)
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        })
        self.mf = (out / "manifest.jsonl").open("a", encoding="utf-8")
        self.done_path = out / ".done_units"
        self.done = set()
        if self.done_path.exists():
            self.done = set(self.done_path.read_text(encoding="utf-8").split())

    def log(self, **rec):
        self.mf.write(json.dumps({"ts": now_iso(), **rec}, ensure_ascii=False) + "\n")
        self.mf.flush()

    def mark_done(self, key):
        self.done.add(key)
        with self.done_path.open("a", encoding="utf-8") as f:
            f.write(key + "\n")

    def fetch_page(self, kw, d1, d2, page):
        data = {
            "pageNum": page, "pageSize": PAGE_SIZE, "column": "szse",
            "tabName": "fulltext", "plate": "", "stock": "", "searchkey": kw,
            "secid": "", "category": "", "trade": "",
            "seDate": f"{d1.isoformat()}~{d2.isoformat()}",
            "sortName": "", "sortType": "", "isHLtitle": "false",
        }
        last_err = ""
        for i in range(RETRIES):
            try:
                r = self.s.post(API, data=data, timeout=30)
                if r.status_code == 200:
                    return r.json(), ""
                last_err = f"http {r.status_code}"
            except Exception as e:  # noqa: BLE001
                last_err = f"{type(e).__name__}: {e}"
            time.sleep(RETRY_BACKOFF[min(i, len(RETRY_BACKOFF) - 1)])
        return None, last_err

    def fetch_shard(self, kw, d1, d2):
        """抓一个时间窗的全部页；返回 (rows, reported_total, failed_pages)。"""
        failed = []
        rows = []
        time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))
        d, _ = self.fetch_page(kw, d1, d2, 1)
        if d is None:
            return rows, -1, [1]
        total = d.get("totalAnnouncement") or d.get("totalRecordNum") or 0
        rows.extend(d.get("announcements") or [])
        pages = min(math.ceil(total / PAGE_SIZE), PAGE_CAP)
        for p in range(2, pages + 1):
            time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))
            d, _ = self.fetch_page(kw, d1, d2, p)
            if d is None or not (d.get("announcements") or []):
                failed.append(p)
                continue
            rows.extend(d["announcements"])
        return rows, total, failed

    def collect_period(self, kw, d1, d2, depth):
        """采集 [d1,d2] 窗口；超深分页安全线则二分。返回规范化行 list。"""
        label = f"{d1.isoformat()}~{d2.isoformat()}"
        key = f"{kw}|{label}"
        part_file = self.out / "parts" / f"{kw}_{d1:%Y%m%d}_{d2:%Y%m%d}.parquet"
        if key in self.done and part_file.exists():
            return pd.read_parquet(part_file).to_dict("records")
        if key in self.done:
            return []

        rows, total, failed = self.fetch_shard(kw, d1, d2)
        if total > SPLIT_GUARD and depth < 2 and (d2 - d1).days > 1:
            mid = d1 + dt.timedelta(days=(d2 - d1).days // 2)
            self.log(kw=kw, period=label, status="split", reported_total=total, depth=depth)
            return (self.collect_period(kw, d1, mid, depth + 1)
                    + self.collect_period(kw, mid + dt.timedelta(days=1), d2, depth + 1))

        status = "ok" if not failed and total >= 0 else ("fail" if total < 0 else "partial")
        recs = norm_rows(rows, kw)
        self.log(kw=kw, period=label, status=status, reported_total=total,
                 fetched=len(recs), failed_pages=failed, depth=depth)
        if status == "ok":
            if depth > 0:  # 月内子分片落 parts/ 供续跑复用
                pd.DataFrame(recs).to_parquet(part_file, index=False)
            self.mark_done(key)
        print(f"[{now_iso()}] {kw} {label}: total={total} fetched={len(recs)} {status}", flush=True)
        return recs

    def collect_month(self, kw, d1, d2):
        """整月采集入口：产出 esop_events/{kw}_{YYYYMM}.parquet。"""
        mkey = f"month|{kw}|{d1:%Y%m}"
        shard_file = self.out / f"{kw}_{d1:%Y%m}.parquet"
        if mkey in self.done and shard_file.exists():
            return
        recs = self.collect_period(kw, d1, d2, depth=0)
        df = pd.DataFrame(recs, columns=["ann_date", "ts_code", "kw", "title", "url"])
        if len(df):
            df = df.drop_duplicates(subset=["ann_date", "ts_code", "title", "url"])
            df.to_parquet(shard_file, index=False)
        elif not shard_file.exists():
            df.to_parquet(shard_file, index=False)  # 空月也落盘，标记已采集
        self.log(kw=kw, period=f"{d1:%Y%m}", status="month_done",
                 rows=int(len(df)), file=shard_file.name)
        self.mark_done(mkey)

    def collect(self, kws, start: dt.date, end: dt.date):
        for kw in kws:
            for d1, d2 in month_shards(start, end):
                self.collect_month(kw, d1, d2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./esop_events")
    ap.add_argument("--kw", nargs="*", default=KEYWORDS, choices=KEYWORDS)
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default="2024-12-31")
    a = ap.parse_args()
    start = dt.date.fromisoformat(a.start)
    end = dt.date.fromisoformat(a.end)
    Collector(Path(a.out)).collect(a.kw, start, end)


if __name__ == "__main__":
    sys.exit(main())
