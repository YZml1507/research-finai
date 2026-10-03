#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""collect_letters.py — A股监管函件事件历史采集（cninfo 公告全文检索）

数据源:
  A. cninfo 公告全文检索 POST http://www.cninfo.com.cn/new/hisAnnouncement/query （主路径，全市场含北交所/退市股）
  B. SZSE 问询函件官方列表 GET https://www.szse.cn/api/report/ShowReport/data?CATALOGID=main_wxhj
     （权威补充：官方函件类别 hjlb + 发函日期 fhrq + 函件/回复 PDF 链接；主板 tab2/创业板 tab3）
  C. SSE 监管问询官方列表 GET http://query.sse.com.cn/commonSoaQuery.do?sqlId=BS_KCB_GGLL
     &channelId=10743,10744,10012（官方 extWTFL 细类 + createTime 发函时间）

实测口径 (2026-09-21 实测):
  cninfo:
  - pageSize 硬上限 30（传更大值仍返回 30 条）
  - column 参数不做市场过滤（szse/sse/bj 返回同一 total，仅影响排序），
    全市场一并返回；市场由 pageColumn / 代码前缀在下游还原
  - 深分页上限约 150 页（≈4500 条），超限后固定返回最后一页内容 ->
    按月分片（实测单月峰值 ~1100 条，远低于上限）；分片内仍 >4000 则自动
    退化为半月/日分片
  - searchkey 命中标题或正文；isHLtitle=false 取原始标题（不带 <em>）
  - announcementTime 为毫秒时间戳，UTC 日期即公告日
  - 关键词会同时命中函件原件与公司回复/核查意见等衍生公告；原样全部落盘，
    衍生记录在 build 阶段标记剔除，不丢原始行
  szse: pagesize 固定 20/页；TABKEY 惰性加载（未选中 tab 返回 recordcount=0）；
    fhrq=发函日期（YYYY-MM-DD）；ck/hfck 为 encode-open PDF 链接
  sse: pageHelp.pageSize 实测可到 100；createTime/cmsOpDate 为发布时间（多为当日晚间）；
    extWTFL 为官方问题分类（问询函/并购重组审核意见函/...）；需带 Referer

输出 (out dir):
  raw/{kw}_{YYYYMM}.parquet        cninfo 原始行（含回复/衍生公告，原样）
  raw_szse/wxhj_{tab}.parquet      SZSE 官方问询函件列表原始行
  raw_sse/inquiries.parquet        SSE 官方监管问询列表原始行
  letters/{letter_type}_{YYYY}.parquet
      cninfo 事件表：ann_date, ts_code, letter_type(归并大类: 问询函|关注函|监管函|警示函|其他),
      letter_subtype(原文细类: 年报问询函/审核问询函/监管关注函/...),
      exchange(SSE|SZSE|BSE|UNK), title, url, announcement_id, page_column, dup_event
  letters_official_szse.parquet    SZSE 权威事件表（source=szse_wxhj）
  letters_official_sse.parquet     SSE 权威事件表（source=sse_inquiries）
  letters/_nonletter_{YYYY}.parquet  命中关键词但非函件原件的记录（回复/核查/进展等）
  letters_proxy/{letter_type}_{YYYY}.parquet
      回复/进展公告推断的函件事件（ann_date=公告披露日, proxy_kind=回复|进展|其他）
      ——2025 起交易所不再公开函件原件，此表为近年唯一可回看的函件存在证据
  manifest.jsonl                   每个 (kw, period)/(src,part) 分片一条采集登记
  manifest.json                    build 汇总：类型/年度覆盖/缺口登记

用法:
  python3 collect_letters.py --out ./letters_out --collect            # cninfo 全量（可续跑）
  python3 collect_letters.py --out ./letters_out --official           # 交易所官方列表（可续跑）
  python3 collect_letters.py --out ./letters_out --build              # 由 raw 生成全部事件表

限速: 每请求 sleep 0.5~1.0s 抖动; 每页失败重试 3 次(指数退避)后记入 manifest。
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
PAGE_SIZE = 30          # 硬上限
PAGE_CAP = 150          # 深分页上限（经验值）
SPLIT_GUARD = 4000      # 分片 total 超过此值则继续细分
KEYWORDS = ["问询函", "关注函", "监管函", "警示函"]
START = dt.date(2015, 1, 1)
SLEEP_LO, SLEEP_HI = 0.5, 1.0
RETRIES = 3
RETRY_BACKOFF = (2.0, 5.0, 12.0)
STATIC_BASE = "http://static.cninfo.com.cn/"

PAGE_COL_TO_EXCH = {
    "SZZB": "SZSE", "SZCY": "SZSE", "SZMB": "SZSE", "SZSB": "SZSE",
    "SHZB": "SSE", "SHKCB": "SSE", "SHB": "SSE",
    "BJS": "BSE",
    "NEEQ": "NEEQ", "HK": "HK", "FUND": "FUND", "BOND": "BOND",
}

# 非函件原件的衍生公告：命中关键词但事件属性不同（回复/核查意见/进展/延期/更正/说明/意见书等）
NONLETTER_PAT = re.compile(
    r"回复|答复|回函|复函|延期|进展|核查意见|核查说明|专项核查|专项说明|补充说明|说明|"
    r"法律意见|财务顾问意见|会计师意见|保荐意见|独立董事意见|专项意见|意见书|"
    r"整改|取消|更正|撤销|落实|问询情况|问询问题|问询回复|函证|鉴证|审计报告|评估报告"
)
# 细类词表：标题中紧贴关键词的描述语，最长匹配优先（与交易所官方口径对齐）
SUBTYPE_DESCR = [
    "非许可类重组", "许可类重组", "重大资产重组", "并购重组", "重组", "审核", "首轮",
    "二轮", "三轮", "四轮", "第二轮", "第三轮", "年报", "年度报告",
    "半年报", "三季报", "一季度报告", "定期报告", "业绩预告", "业绩快报",
    "监管关注", "监管工作", "信息披露监管", "自律监管", "股价异动", "异常波动",
    "权益变动", "二次", "补充", "问询", "提请关注", "关注", "监管", "警示",
]
SUBTYPE_PAT = re.compile(r"(问询函|关注函|监管函|警示函)")
EM_PAT = re.compile(r"</?em>")


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def month_shards(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        nxt = (cur.replace(day=28) + dt.timedelta(days=7)).replace(day=1)
        yield cur, min(nxt - dt.timedelta(days=1), end)
        cur = nxt


class Collector:
    def __init__(self, out: Path):
        self.out = out
        (out / "raw").mkdir(parents=True, exist_ok=True)
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        })
        self.mf = (out / "manifest.jsonl").open("a", encoding="utf-8")
        self.done_path = out / ".done_shards"
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
        """抓一个分片的全部页；返回 (rows, reported_total, failed_pages)."""
        failed = []
        rows = []
        time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))
        d, err = self.fetch_page(kw, d1, d2, 1)
        if d is None:
            return rows, -1, [1]
        total = d.get("totalAnnouncement") or d.get("totalRecordNum") or 0
        for a in (d.get("announcements") or []):
            rows.append(a)
        pages = min(math.ceil(total / PAGE_SIZE), PAGE_CAP)
        for p in range(2, pages + 1):
            time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))
            d, err = self.fetch_page(kw, d1, d2, p)
            if d is None or not (d.get("announcements") or []):
                failed.append(p)
                continue
            rows.extend(d["announcements"])
        return rows, total, failed

    def collect(self, start: dt.date, end: dt.date):
        for kw in KEYWORDS:
            for d1, d2 in month_shards(start, end):
                self.collect_period(kw, d1, d2, depth=0)

    def collect_period(self, kw, d1, d2, depth):
        label = f"{d1.isoformat()}~{d2.isoformat()}"
        key = f"{kw}|{label}"
        if key in self.done:
            return
        shard_file = self.out / "raw" / f"{kw}_{d1:%Y%m}.parquet" if depth == 0 else \
            self.out / "raw" / f"{kw}_{d1:%Y%m%d}_{d2:%Y%m%d}.parquet"
        rows, total, failed = self.fetch_shard(kw, d1, d2)
        if total > SPLIT_GUARD and depth < 2 and (d2 - d1).days > 1:
            # 超过深分页安全线：二分时间窗
            mid = d1 + dt.timedelta(days=(d2 - d1).days // 2)
            self.log(kw=kw, period=label, status="split", reported_total=total, depth=depth)
            self.collect_period(kw, d1, mid, depth + 1)
            self.collect_period(kw, mid + dt.timedelta(days=1), d2, depth + 1)
            return
        status = "ok" if not failed and total >= 0 else ("fail" if total < 0 else "partial")
        if rows:
            df = pd.DataFrame(rows)
            df["kw"] = kw
            df["shard"] = label
            df.to_parquet(shard_file, index=False)
        self.log(kw=kw, period=label, status=status, reported_total=total,
                 fetched=len(rows), failed_pages=failed, depth=depth)
        if status == "ok":
            self.mark_done(key)
        print(f"[{now_iso()}] {kw} {label}: total={total} fetched={len(rows)} {status}", flush=True)


SZSE_API = "https://www.szse.cn/api/report/ShowReport/data"
# SZSE 官方目录：main_wxhj=问询函件(主板 tab2/创业板 tab3)；1800_jgxxgk=监管措施(监管函/警示函等)
SZSE_CATS = {"wxhj": ("main_wxhj", {"tab2": "主板", "tab3": "创业板"}),
             "jgxx": ("1800_jgxxgk", {"tab1": "监管措施"})}
SSE_API = "http://query.sse.com.cn/commonSoaQuery.do"


def merged_type(text: str) -> str:
    """归并大类：按字面值，警示函 > 监管函(含监管关注函) > 关注函 > 问询函 > 其他"""
    t = text or ""
    if "警示函" in t:
        return "警示函"
    if "监管函" in t or "监管关注函" in t:
        return "监管函"
    if "关注函" in t:
        return "关注函"
    if "问询函" in t:
        return "问询函"
    return "其他"


def fetch_json(sess, url, params=None, data=None, referer=None):
    headers = {"Referer": referer} if referer else {}
    last_err = ""
    for i in range(RETRIES):
        try:
            if data is None:
                r = sess.get(url, params=params, headers=headers, timeout=30)
            else:
                r = sess.post(url, data=data, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json(), ""
            last_err = f"http {r.status_code}"
        except Exception as e:  # noqa: BLE001
            last_err = f"{type(e).__name__}: {e}"
        time.sleep(RETRY_BACKOFF[min(i, len(RETRY_BACKOFF) - 1)])
    return None, last_err


def collect_szse(sess, out: Path, log):
    rawdir = out / "raw_szse"
    rawdir.mkdir(exist_ok=True)
    done_path = out / ".done_official"
    done = set(done_path.read_text(encoding="utf-8").split()) if done_path.exists() else set()
    for cat_name, (catalog_id, tabs) in SZSE_CATS.items():
        for tab, tabname in tabs.items():
            key = f"szse|{cat_name}|{tab}"
            if key in done:
                continue
            rows, failed = [], []
            d, err = fetch_json(sess, SZSE_API,
                                params={"SHOWTYPE": "JSON", "CATALOGID": catalog_id,
                                        "TABKEY": tab, "PAGENO": 1, "random": str(random.random())},
                                referer="https://www.szse.cn/disclosure/supervision/inquire/index.html")
            pages = total = 0
            if d:
                for t in d:
                    if t.get("metadata", {}).get("tabkey") == tab:
                        pages = int(t["metadata"].get("pagecount") or 0)
                        total = int(t["metadata"].get("recordcount") or 0)
                        rows.extend(t.get("data") or [])
            for p in range(2, pages + 1):
                time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))
                d, err = fetch_json(sess, SZSE_API,
                                    params={"SHOWTYPE": "JSON", "CATALOGID": catalog_id,
                                            "TABKEY": tab, "PAGENO": p, "random": str(random.random())},
                                    referer="https://www.szse.cn/disclosure/supervision/inquire/index.html")
                got = 0
                if d:
                    for t in d:
                        if t.get("metadata", {}).get("tabkey") == tab:
                            got = len(t.get("data") or [])
                            rows.extend(t.get("data") or [])
                if got == 0:
                    failed.append(p)
            if rows:
                pd.DataFrame(rows).assign(cat=cat_name, tab=tab, tab_name=tabname).to_parquet(
                    rawdir / f"{cat_name}_{tab}.parquet", index=False)
            status = "ok" if not failed and d else ("fail" if not d else "partial")
            log(src=f"szse_{cat_name}", part=tab, status=status, reported_total=total,
                fetched=len(rows), failed_pages=failed)
            if status == "ok":
                with done_path.open("a", encoding="utf-8") as f:
                    f.write(key + "\n")
            print(f"[{now_iso()}] szse {cat_name} {tab} {tabname}: "
                  f"total={total} fetched={len(rows)} {status}", flush=True)


def collect_sse(sess, out: Path, log):
    rawdir = out / "raw_sse"
    rawdir.mkdir(exist_ok=True)
    done_path = out / ".done_official"
    done = set(done_path.read_text(encoding="utf-8").split()) if done_path.exists() else set()
    if "sse|inquiries" in done:
        return
    base = {"siteId": "28", "sqlId": "BS_KCB_GGLL", "extGGLX": "", "stockcode": "",
            "channelId": "10743,10744,10012", "extGGDL": "",
            "order": "createTime|desc,stockcode|asc", "isPagination": "true",
            "pageHelp.pageSize": "100", "pageHelp.beginPage": "1",
            "pageHelp.cacheSize": "1", "pageHelp.endPage": "5", "type": ""}
    rows, failed = [], []
    d, err = fetch_json(sess, SSE_API, params={**base, "pageHelp.pageNo": "1"},
                        referer="http://www.sse.com.cn/disclosure/credibility/supervision/inquiries/")
    total = 0
    if d:
        ph = d.get("pageHelp", {})
        total = int(ph.get("total") or 0)
        rows.extend(ph.get("data") or [])
    pages = math.ceil(total / 100)
    for p in range(2, pages + 1):
        time.sleep(random.uniform(SLEEP_LO, SLEEP_HI))
        d, err = fetch_json(sess, SSE_API, params={**base, "pageHelp.pageNo": str(p)},
                            referer="http://www.sse.com.cn/disclosure/credibility/supervision/inquiries/")
        got = d.get("pageHelp", {}).get("data") if d else None
        if not got:
            failed.append(p)
            continue
        rows.extend(got)
    if rows:
        pd.DataFrame(rows).to_parquet(rawdir / "inquiries.parquet", index=False)
    status = "ok" if not failed and d else ("fail" if not d else "partial")
    log(src="sse_inquiries", part="all", status=status, reported_total=total,
        fetched=len(rows), failed_pages=failed)
    if status == "ok":
        with done_path.open("a", encoding="utf-8") as f:
            f.write("sse|inquiries\n")
    print(f"[{now_iso()}] sse inquiries: total={total} fetched={len(rows)} {status}", flush=True)


ENCODE_OPEN = re.compile(r"encode-open='([^']+)'")


def build_official(out: Path):
    outdir = out / "letters"
    outdir.mkdir(exist_ok=True)
    frames = []
    for f in sorted((out / "raw_szse").glob("wxhj_*.parquet")):
        frames.append(pd.read_parquet(f))
    for f in sorted((out / "raw_szse").glob("jgxx_*.parquet")):
        g = pd.read_parquet(f)
        g = g.rename(columns={"gkxx_gsdm": "gsdm", "gkxx_gsjc": "gsjc",
                              "gkxx_gdrq": "fhrq", "gkxx_jgcs": "hjlb",
                              "hjnr": "ck"})
        g["hfck"] = ""
        g["gkxx_sjdx"] = g.get("gkxx_sjdx", "")
        frames.append(g)
    if frames:
        z = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["gsdm", "fhrq", "hjlb", "ck"])
        z["ann_date"] = pd.to_datetime(z["fhrq"]).dt.date
        z["ts_code"] = z["gsdm"].astype(str).str.zfill(6)
        z["letter_subtype"] = z["hjlb"]
        z["letter_type"] = z["hjlb"].map(merged_type)
        z["exchange"] = "SZSE"
        z["title"] = z["gsjc"] + " " + z["hjlb"]
        z["url"] = z["ck"].map(lambda s: "https://www.szse.cn" + m.group(1)
                               if (m := ENCODE_OPEN.search(str(s))) else "")
        z["reply_url"] = z["hfck"].map(lambda s: "https://www.szse.cn" + m.group(1)
                                     if (m := ENCODE_OPEN.search(str(s))) else "")
        z["announcement_id"] = z["url"].str.extract(r"([0-9A-F]{32})\.pdf", expand=False)
        z["source"] = "szse_" + z["cat"].fillna("wxhj")
        z[["ann_date", "ts_code", "letter_type", "letter_subtype", "exchange",
           "title", "url", "reply_url", "announcement_id", "gsjc", "source"]]\
            .rename(columns={"gsjc": "sec_name"}).to_parquet(
                out / "letters_official_szse.parquet", index=False)
        print("szse_official:", len(z))
    f = out / "raw_sse" / "inquiries.parquet"
    if f.exists():
        s = pd.read_parquet(f).drop_duplicates(subset=["docId"])
        s["ann_date"] = pd.to_datetime(s["createTime"]).dt.date
        s["ts_code"] = s["stockcode"].astype(str).str.zfill(6)
        s["letter_subtype"] = s["extWTFL"]
        s["letter_type"] = s["extWTFL"].map(merged_type)
        s["exchange"] = "SSE"
        s["title"] = s["docTitle"]
        s["url"] = "http://" + s["docURL"].astype(str)
        s["reply_url"] = ""
        s["announcement_id"] = s["docId"].astype(str)
        s["source"] = "sse_inquiries"
        s[["ann_date", "ts_code", "letter_type", "letter_subtype", "exchange",
           "title", "url", "reply_url", "announcement_id", "extGSJC", "source"]]\
            .rename(columns={"extGSJC": "sec_name"}).to_parquet(
                out / "letters_official_sse.parquet", index=False)
        print("sse_official:", len(s))


def exchange_of(sec_code, page_column):
    pc = (page_column or "").upper()
    if pc in PAGE_COL_TO_EXCH:
        return PAGE_COL_TO_EXCH[pc]
    c = str(sec_code or "")
    if re.match(r"^(60|68|90|110|113|132|5)", c):
        return "SSE"
    if re.match(r"^(00|30|20|12|15|16|18)", c):
        return "SZSE"
    if re.match(r"^(4|8|92)", c):
        return "BSE"
    return "UNK"


def classify(title: str):
    """返回 (letter_type 归并大类, letter_subtype 原文细类, is_letter)."""
    t = EM_PAT.sub("", title or "")
    m = SUBTYPE_PAT.search(t)
    subtype = ""
    if m:
        kw = m.group(1)
        subtype = kw
        cands = [d for d in SUBTYPE_DESCR if kw not in d and not kw.startswith(d)]
        for d in cands:  # 最长匹配优先：找到标题中「描述语+关键词」紧邻形态
            if d + kw in t:
                subtype = d + kw
                break
        else:
            # 退而求其次：描述语与关键词同句出现但不相邻
            for d in cands:
                if d in t and len(d) > 1:
                    subtype = d + kw
                    break
    lt = merged_type(t)
    is_letter = bool(m) and not NONLETTER_PAT.search(t)
    return lt, subtype, is_letter


def build(out: Path):
    raws = sorted((out / "raw").glob("*.parquet"))
    if not raws:
        print("no raw parquet found", file=sys.stderr)
        return
    df = pd.concat([pd.read_parquet(p) for p in raws], ignore_index=True)
    df["title"] = df["announcementTitle"].map(lambda t: EM_PAT.sub("", str(t or "")))
    df = df.drop_duplicates(subset=["announcementId"], keep="first")  # 跨关键词/跨分片去重
    df["ann_date"] = pd.to_datetime(df["announcementTime"], unit="ms", utc=True).dt.date
    df["ts_code"] = df["secCode"].astype(str).str.zfill(6)
    df["announcement_id"] = df["announcementId"]
    df["page_column"] = df["pageColumn"]
    df["url"] = STATIC_BASE + df["adjunctUrl"].astype(str)
    res = df["title"].map(classify)
    df["letter_type"] = [r[0] for r in res]
    df["letter_subtype"] = [r[1] for r in res]
    df["is_letter"] = [r[2] for r in res]
    df["exchange"] = [exchange_of(c, p) for c, p in zip(df["secCode"], df["pageColumn"])]
    df["dup_event"] = df.duplicated(subset=["ts_code", "ann_date", "letter_type"], keep=False)

    letters = df[df["is_letter"]].copy()
    nonletters = df[~df["is_letter"]].copy()
    outdir = out / "letters"
    outdir.mkdir(exist_ok=True)
    summary = {"built_at": now_iso(), "raw_rows": int(len(df)),
               "letter_rows": int(len(letters)), "nonletter_rows": int(len(nonletters)),
               "by_type": {}, "by_year": {}, "files": []}
    for lt, g in letters.groupby("letter_type"):
        summary["by_type"][lt] = int(len(g))
        for y, gy in g.groupby(g["ann_date"].map(lambda d: int(d.year))):
            f = outdir / f"{lt}_{y}.parquet"
            gy[["ann_date", "ts_code", "letter_type", "letter_subtype", "exchange",
                "title", "url", "announcement_id", "page_column", "dup_event",
                "secName", "kw", "shard"]].to_parquet(f, index=False)
            summary["files"].append({"file": f.name, "rows": int(len(gy))})
            summary["by_year"].setdefault(str(y), 0)
            summary["by_year"][str(y)] += int(len(gy))
    for y, gy in nonletters.groupby(nonletters["ann_date"].map(lambda d: int(d.year))):
        f = outdir / f"_nonletter_{y}.parquet"
        gy[["ann_date", "ts_code", "letter_type", "letter_subtype", "exchange",
            "title", "url", "announcement_id", "page_column",
            "secName", "kw", "shard"]].to_parquet(f, index=False)

    # 回复/进展代理事件：标题含函件词 + 回复/回函/答复/进展/落实 的衍生公告。
    # 2025 起交易所不再公开函件原件（官方列表与 cninfo 原件同步断崖），
    # 回复公告成为函件存在的唯一可回看证据；ann_date 为披露日（通常滞后 0~30 天）。
    proxy = nonletters[nonletters["title"].str.contains(
        r"回复|回函|答复|进展|落实|复函", regex=True, na=False)].copy()
    proxy["proxy_kind"] = proxy["title"].map(
        lambda t: "回复" if re.search(r"回复|回函|答复|复函", t)
        else ("进展" if re.search(r"进展|落实", t) else "其他"))
    proxydir = out / "letters_proxy"
    proxydir.mkdir(exist_ok=True)
    summary["proxy_rows"] = int(len(proxy))
    for (lt, y), gy in proxy.groupby(
            [proxy["letter_type"], proxy["ann_date"].map(lambda d: int(d.year))]):
        f = proxydir / f"{lt}_{y}.parquet"
        gy[["ann_date", "ts_code", "letter_type", "letter_subtype", "exchange",
            "title", "url", "announcement_id", "page_column", "proxy_kind",
            "secName", "kw", "shard"]].to_parquet(f, index=False)
    (out / "manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("letter_rows", "nonletter_rows", "by_type")},
                     ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./letters_out")
    ap.add_argument("--start", default=START.isoformat())
    ap.add_argument("--end", default=dt.date.today().isoformat())
    ap.add_argument("--collect", action="store_true", help="cninfo 分片采集")
    ap.add_argument("--official", action="store_true", help="SZSE/SSE 官方列表采集")
    ap.add_argument("--build", action="store_true")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if args.collect:
        c = Collector(out)
        c.collect(dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end))
    if args.official:
        c = Collector(out)  # 复用其 session/log
        collect_szse(c.s, out, c.log)
        collect_sse(c.s, out, c.log)
    if args.build:
        build(out)
        build_official(out)


if __name__ == "__main__":
    main()
