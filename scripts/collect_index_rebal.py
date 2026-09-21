#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
collect_index_rebal.py — 中证指数定期调样历史采集（沪深300 / 中证500 / 中证红利）

数据源：中证指数官网公告系统 (csindex.com.cn)
  列表：POST /csindex-home/announcement/queryAnnouncementByVo
        body: {lang, page:{page,rows}, relatedTopics:["index_rebalance"],
               typeList:["announcement"], searchInput, startDate, endDate, indexCode}
  详情：GET  /csindex-home/announcement/queryAnnouncementById?id=<id>
        -> enclosureList[].fileUrl + content 内嵌 <a href> 附件 / 内嵌 <table> 名单

附件三种形态（年代相关）：
  1) xls/xlsx 工作簿（~2015-2022）：sheet=调入/调出/备选名单，长表，含指数代码列
  2) PDF（2023+ 起）：「<指数名>指数样本调整名单：」调出/调入双栏 + 「<指数名>指数备选名单：」
  3) 公告正文内嵌 HTML 表格（2015 年部分公告无附件链接）

输出：
  outdir/index_rebal/{index_code}_{effective_date}.parquet
      列: index_code,index_name,review_date,effective_date,effective_rule,
          action(add|remove|standby),rank,stock_code,stock_name,
          announcement_id,announcement_title,source_url,source_kind
  outdir/manifest.csv + manifest.json  —— 每期登记 + 失败/缺口登记（不硬造）
  outdir/raw/ —— 原始公告 JSON 与附件（可追溯）

用法：python3 collect_index_rebal.py --out ./out --start 2015-01-01 [--end 2026-12-31]
限速：默认每个请求 sleep 0.7s。
"""

import argparse
import io
import json
import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests

BASE = "https://www.csindex.com.cn"
LIST_URL = BASE + "/csindex-home/announcement/queryAnnouncementByVo"
DETAIL_URL = BASE + "/csindex-home/announcement/queryAnnouncementById"

UA = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Referer": "https://www.csindex.com.cn/",
}

# 目标指数：代码 -> 规范名
TARGET_INDICES = {"000300": "沪深300", "000905": "中证500", "000922": "中证红利"}
# 附件中可能出现的名称写法 -> 代码（键已去空白）
NAME2CODE = {
    "沪深300": "000300", "沪深300指数": "000300", "沪深 300": "000300",
    "中证500": "000905", "中证500指数": "000905", "中证 500": "000905",
    "中证红利": "000922", "中证红利指数": "000922", "中证 红利": "000922",
}

# 主批次公告标题特征：头版必含沪深300，且不是精明/临时/月度等系列
HEADLINE_RE = re.compile(r"沪深\s*300")
EXCLUDE_RE = re.compile(r"精明|临时|月度|香港新股|港股通综合")
# 正文里用于区分定期审议 vs 事件驱动（临时/个股事件）调整
ADHOC_RE = re.compile(r"鉴于|退市|吸收合并|换股|临时调整")
PERIODIC_RE = re.compile(r"定期调整|定期审议|专家委员会审议|样本调整")

SLEEP = 0.7


def norm_name(s: str) -> str:
    """去掉名称中的空白/全角空格用于匹配"""
    return re.sub(r"[\s 　]+", "", unicodedata.normalize("NFKC", str(s)))


def norm_code(v) -> str:
    """统一证券/指数代码为字符串；纯数字补零到6位"""
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if re.fullmatch(r"\d+", s) and len(s) < 6:
        s = s.zfill(6)
    return s


class CSIClient:
    def __init__(self, sleep=SLEEP):
        self.s = requests.Session()
        self.s.headers.update(UA)
        self.sleep = sleep
        self.errors = []

    def _get(self, url, **kw):
        for attempt in range(4):
            try:
                r = self.s.get(url, timeout=40, **kw)
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                err = e
            time.sleep(self.sleep * (attempt + 2))
        self.errors.append(("GET", url, str(locals().get("err", r.status_code))))
        return None

    def _post(self, url, payload):
        for attempt in range(4):
            try:
                r = self.s.post(url, json=payload, timeout=40)
                if r.status_code == 200:
                    return r
            except requests.RequestException as e:
                err = e
            time.sleep(self.sleep * (attempt + 2))
        self.errors.append(("POST", url, str(locals().get("err", r.status_code))))
        return None

    def list_announcements(self, start, end, extra=None):
        """翻页拉取 index_rebalance + announcement 公告"""
        items, page = [], 1
        while True:
            body = {
                "lang": "cn",
                "page": {"key": "", "page": page, "rows": 100, "sortBy": ""},
                "relatedTopics": ["index_rebalance"],
                "typeList": ["announcement"],
                "startDate": start, "endDate": end,
            }
            if extra:
                body.update(extra)
            time.sleep(self.sleep)
            r = self._post(LIST_URL, body)
            if r is None:
                break
            d = r.json()
            items += d.get("data") or []
            if len(items) >= d.get("total", 0) or not d.get("data"):
                break
            page += 1
        return items

    def detail(self, ann_id):
        time.sleep(self.sleep)
        r = self._get(DETAIL_URL, params={"id": ann_id})
        if r is None:
            return None
        return r.json().get("data")

    def download(self, url):
        time.sleep(self.sleep)
        r = self._get(url)
        return r.content if r is not None else None


# ---------- 生效日 / 分类 ----------

def extract_effective_date(content_html):
    """从公告正文抽取生效/调整日期。返回 (date_str, rule_phrase)"""
    txt = re.sub(r"<[^>]+>", " ", content_html)
    txt = unicodedata.normalize("NFKC", re.sub(r"[\s 　]+", "", txt))
    pats = [
        r"(?:于|自)\s*(20\d{2})年(\d{1,2})月(\d{1,2})日\s*[^。，,]{0,12}?(?:生效|调整|实施)",
        r"决定于\s*(20\d{2})年(\d{1,2})月(\d{1,2})日[^。，,]{0,6}调整",
        r"(20\d{2})年(\d{1,2})月(\d{1,2})日\s*(?:收市后)?生效",
    ]
    for p in pats:
        m = re.search(p, txt)
        if m:
            y, mo, d = m.groups()[:3]
            start = max(0, m.start() - 30)
            return f"{y}-{int(mo):02d}-{int(d):02d}", txt[start:m.end()]
    return None, None


def classify_event(title, content_html):
    """periodic / adhoc / other"""
    t = re.sub(r"<[^>]+>", " ", content_html or "")
    if ADHOC_RE.search(t) and not PERIODIC_RE.search(t):
        return "adhoc"
    if re.search(r"定期", title) or re.search(r"专家委员会审议|定期", t):
        return "periodic"
    return "other"


def extract_attachment_urls(detail):
    """enclosureList + 正文 <a href> 文件链接，去重保序"""
    urls = []
    for e in detail.get("enclosureList") or []:
        if e.get("fileUrl"):
            urls.append((e.get("fileName") or "", e["fileUrl"]))
    for m in re.finditer(r'href="([^"]+)"', detail.get("content") or ""):
        u = m.group(1)
        if re.search(r"\.(xls|xlsx|pdf|docx?|zip)(\?|$)", u, re.I) and "csindex" in u:
            urls.append(("", u))
    seen, out = set(), []
    for name, u in urls:
        u = u.replace("notice%2F", "notice/")  # OSS 路径里的 %2F 需还原
        if u not in seen:
            seen.add(u)
            out.append((name, u))
    return out


# ---------- 解析器 ----------

def parse_workbook(blob, ann_meta):
    """xls/xlsx 工作簿 -> records。sheet: 调入/调出/备选名单"""
    recs = []
    xl = pd.ExcelFile(io.BytesIO(blob))
    sheet_action = {}
    for s in xl.sheet_names:
        sn = norm_name(s)
        if sn.startswith("调入"):
            sheet_action[s] = "add"
        elif sn.startswith("调出"):
            sheet_action[s] = "remove"
        elif "备选" in sn:
            sheet_action[s] = "standby"
    for sheet, action in sheet_action.items():
        raw = xl.parse(sheet, header=None)
        hdr_idx = raw[raw.apply(lambda r: r.astype(str).str.contains("指数代码|指数名称").any(), axis=1)].index
        if len(hdr_idx) == 0:
            continue
        df = xl.parse(sheet, header=int(hdr_idx[0]))
        df.columns = [norm_name(c) for c in df.columns]
        cidx = next((c for c in df.columns if c in ("指数代码", "指数名称")), df.columns[0])
        cname = next((c for c in df.columns if "指数" in c and ("简称" in c or "名称" in c)), None)
        cstk = next((c for c in df.columns if c in ("证券代码", "股票代码", "成份券代码")), None)
        cstkname = next((c for c in df.columns if c in ("证券简称", "股票名称", "证券名称", "成份券名称")), None)
        crank = next((c for c in df.columns if "排序" in c or "序号" in c), None)
        if cstk is None:
            continue
        for _, row in df.iterrows():
            icode = norm_code(row.get(cidx, ""))
            iname = norm_name(row.get(cname, "")) if cname else ""
            tgt = icode if icode in TARGET_INDICES else NAME2CODE.get(iname)
            if tgt not in TARGET_INDICES:
                continue
            scode = norm_code(row.get(cstk, ""))
            if not re.fullmatch(r"[0-9A-Z]{4,8}(\.[A-Z]{2})?", scode):
                continue
            recs.append(dict(
                index_code=tgt, index_name=TARGET_INDICES[tgt],
                action=action,
                rank=int(row[crank]) if crank and pd.notna(row.get(crank)) else None,
                stock_code=scode,
                stock_name=str(row.get(cstkname, "")).strip() if cstkname else "",
                **ann_meta,
            ))
    return recs


CODE_RE = r"(?:[0-9]{6}|[0-9]{4,5}\.[A-Z]{2})"
# 区段锚点：「…指数样本(股)调整名单：」/「…指数备选名单：」
ANCHOR_RE = re.compile(r"指数\s*(备选名单|样本股?调整名单|样本调整名单)\s*[:：]")
# 锚点前的指数名（窗口内向前取，尾部可含数字，如 沪深 300 / 中证红利）
NAME_TAIL_RE = re.compile(
    r"[\u4e00-\u9fa5][\u4e00-\u9fa5A-Za-z]{0,10}?\s*\d{0,4}\s*$")
# 名称字符：中文/字母/常见标点和 ' A'/' B' 后缀；不含数字以免吞掉下一个代码
NAME_RE = r"[\u4e00-\u9fa5A-Za-z（）()·&\-\*ST]+(?:\s(?!\d)[\u4e00-\u9fa5A-Za-z]+)*"


def _lookup_index(name):
    nn = norm_name(name)
    for k, v in sorted(NAME2CODE.items(), key=lambda kv: -len(norm_name(kv[0]))):
        if nn.endswith(norm_name(k)):
            return v
    return None


def parse_text_sections(text, ann_meta):
    """按名单区段切分纯文本，解析 调出/调入 双栏 与 备选 排名栏。PDF 与公告正文共用。
    区段边界取「指数名起点」而非锚点起点，避免上一段的名单行被标题吞掉。"""
    recs = []
    anchors = []
    for m in ANCHOR_RE.finditer(text):
        win_start = max(0, m.start() - 30)
        window = text[win_start:m.start()]
        nm = NAME_TAIL_RE.search(window)
        name_start = win_start + nm.start() if nm else m.start()
        name = text[name_start:m.start()] if nm else ""
        anchors.append(dict(name=name, name_start=name_start, kind=m.group(1),
                            data_start=m.end()))
    for i, a in enumerate(anchors):
        seg_end = anchors[i + 1]["name_start"] if i + 1 < len(anchors) else len(text)
        seg = text[a["data_start"]:seg_end]
        tgt = _lookup_index(a["name"])
        if tgt not in TARGET_INDICES:
            continue
        if "备选" in a["kind"]:
            for mm in re.finditer(rf"(\d{{1,3}})\s+({CODE_RE})\s+({NAME_RE})", seg):
                recs.append(dict(index_code=tgt, index_name=TARGET_INDICES[tgt],
                                 action="standby", rank=int(mm.group(1)),
                                 stock_code=norm_code(mm.group(2)),
                                 stock_name=mm.group(3).strip(), **ann_meta))
        else:
            for mm in re.finditer(
                    rf"({CODE_RE})\s+({NAME_RE})\s+({CODE_RE})\s+({NAME_RE})", seg):
                recs.append(dict(index_code=tgt, index_name=TARGET_INDICES[tgt],
                                 action="remove", rank=None,
                                 stock_code=norm_code(mm.group(1)),
                                 stock_name=mm.group(2).strip(), **ann_meta))
                recs.append(dict(index_code=tgt, index_name=TARGET_INDICES[tgt],
                                 action="add", rank=None,
                                 stock_code=norm_code(mm.group(3)),
                                 stock_name=mm.group(4).strip(), **ann_meta))
    return recs


def parse_pdf(blob, ann_meta):
    """中证「部分指数样本调整名单」PDF -> records"""
    import pdfplumber
    with pdfplumber.open(io.BytesIO(blob)) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    return parse_text_sections(text, ann_meta)


def parse_html_tables(content_html, ann_meta):
    """公告正文内嵌名单（2015 年公告）：去标签成纯文本后按区段解析。"""
    import html as htmlmod
    txt = htmlmod.unescape(re.sub(r"<[^>]+>", " ", content_html))
    txt = re.sub(r"[\s 　]+", " ", unicodedata.normalize("NFKC", txt))
    return parse_text_sections(txt, ann_meta)


# ---------- 主流程 ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./out")
    ap.add_argument("--start", default="2015-01-01")
    ap.add_argument("--end", default="2026-12-31")
    ap.add_argument("--sleep", type=float, default=SLEEP)
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 个候选（调试）")
    args = ap.parse_args()

    outdir = Path(args.out)
    rawdir = outdir / "raw"
    pdir = outdir / "index_rebal"
    for d in (outdir, rawdir, pdir):
        d.mkdir(parents=True, exist_ok=True)

    cli = CSIClient(sleep=args.sleep)

    # 1) 枚举指数调样公告
    items = cli.list_announcements(args.start, args.end)
    (rawdir / "announcement_list.json").write_text(json.dumps(items, ensure_ascii=False, indent=1))
    print(f"[list] {len(items)} announcements")

    # 2) 标题粗筛：候选 = 标题含沪深300且非排除词；另加中证红利/红利字样的公告（可能含000922附件）
    cands, seen = [], set()
    for it in items:
        t = it["title"]
        hit = (HEADLINE_RE.search(t) and not EXCLUDE_RE.search(t)) or re.search(r"中证红利|红利", t)
        if hit and it["id"] not in seen:
            seen.add(it["id"])
            cands.append(it)
    cands.sort(key=lambda x: x["publishDate"])
    if args.limit:
        cands = cands[:args.limit]
    print(f"[cand] {len(cands)} candidates")

    manifest = []
    all_records = []  # (event_key, df)

    for it in cands:
        ann_id, pub = it["id"], it["publishDate"]
        row = {"announcement_id": ann_id, "review_date": pub, "title": it["title"],
               "event_class": None, "effective_date": None, "effective_rule": None,
               "attachment_urls": "", "parse_status": "", "indices_found": "",
               "n_add": 0, "n_remove": 0, "n_standby": 0, "note": ""}
        det = cli.detail(ann_id)
        if det is None:
            row["parse_status"] = "detail_fetch_failed"
            manifest.append(row)
            continue
        (rawdir / f"detail_{ann_id}.json").write_text(json.dumps(det, ensure_ascii=False))
        content = det.get("content") or ""
        row["event_class"] = classify_event(it["title"], content)
        eff, rule = extract_effective_date(content)
        row["effective_date"] = eff
        row["effective_rule"] = rule

        if row["event_class"] != "periodic":
            row["parse_status"] = "skipped_non_periodic"
            manifest.append(row)
            continue

        urls = extract_attachment_urls(det)
        row["attachment_urls"] = ";".join(u for _, u in urls)

        recs = []
        statuses = []
        for fi, (fname, url) in enumerate(urls):
            blob = cli.download(url)
            if blob is None:
                statuses.append(f"download_failed:{url[-50:]}")
                continue
            ext = url.split("?")[0].rsplit(".", 1)[-1].lower()
            (rawdir / f"att_{ann_id}_{fi}.{ext}").write_bytes(blob)
            ann_meta = {"review_date": pub, "effective_date": eff, "effective_rule": rule,
                        "announcement_id": ann_id, "announcement_title": it["title"],
                        "source_url": url, "source_kind": ext}
            try:
                if ext in ("xls", "xlsx"):
                    got = parse_workbook(blob, ann_meta)
                elif ext == "pdf":
                    got = parse_pdf(blob, ann_meta)
                else:
                    got = []
                for r in got:
                    r["_fidx"] = fi
                recs += got
                statuses.append(f"{ext}:{len(got)}")
            except Exception as e:
                statuses.append(f"{ext}:parse_error:{e}")
        # 无附件或附件无目标指数 → 尝试正文内嵌表格
        if not any(r["index_code"] in TARGET_INDICES for r in recs):
            got = parse_html_tables(content, {
                "review_date": pub, "effective_date": eff, "effective_rule": rule,
                "announcement_id": ann_id, "announcement_title": it["title"],
                "source_url": f"{BASE}/#/about/newsDetail?id={ann_id}", "source_kind": "html_table"})
            for r in got:
                r["_fidx"] = len(urls)  # 正文视为最后一版
            recs += got
            if got:
                statuses.append(f"html_table:{len(got)}")

        df = pd.DataFrame(recs)
        if len(df):
            # 同一期可能挂多个附件版本（修正稿）：同 (index,action) 用后下载到的文件整体覆盖
            if "_fidx" in df:
                maxf = df.groupby(["index_code", "action"])["_fidx"].transform("max")
                df = df[df["_fidx"] == maxf].drop(columns=["_fidx"])
                df = df.drop_duplicates(subset=["index_code", "action", "stock_code", "rank"],
                                        keep="last")
            for icode, sub in df.groupby("index_code"):
                f = pdir / f"{icode}_{eff or pub}.parquet"
                sub.to_parquet(f, index=False)
                all_records.append(sub)
            row["indices_found"] = ",".join(sorted(df["index_code"].unique()))
            row["n_add"] = int((df["action"] == "add").sum())
            row["n_remove"] = int((df["action"] == "remove").sum())
            row["n_standby"] = int((df["action"] == "standby").sum())
            missing = set(TARGET_INDICES) - set(df["index_code"].unique())
            if missing:
                row["note"] = "indices_absent_in_attachment:" + ",".join(sorted(missing))
            row["parse_status"] = "ok" if not missing else "partial"
        else:
            row["parse_status"] = "no_target_rows"
        row["parse_status"] += "|" + ";".join(statuses)
        manifest.append(row)
        print(f"[{pub}] {ann_id} {row['event_class']} eff={eff} found={row['indices_found']} "
              f"add={row['n_add']} rm={row['n_remove']} stb={row['n_standby']} {row['note']}")

    mdf = pd.DataFrame(manifest)
    mdf.to_csv(outdir / "manifest.csv", index=False, encoding="utf-8-sig")
    summary = {
        "source": "csindex.com.cn announcement system",
        "range": [args.start, args.end],
        "announcements_listed": len(items),
        "candidates": len(cands),
        "periodic_events": int((mdf["event_class"] == "periodic").sum()) if len(mdf) else 0,
        "non_periodic_skipped": mdf[mdf["event_class"] != "periodic"]["title"].tolist() if len(mdf) else [],
        "per_index_events": {},
        "http_errors": cli.errors,
    }
    if all_records:
        all_df = pd.concat(all_records, ignore_index=True)
        for ic in TARGET_INDICES:
            evs = all_df[all_df["index_code"] == ic].groupby("effective_date").size()
            summary["per_index_events"][ic] = {k: int(v) for k, v in evs.items()}
        all_df.to_parquet(outdir / "index_rebal" / "_all.parquet", index=False)
    (outdir / "manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1))
    print(json.dumps(summary["per_index_events"], ensure_ascii=False, indent=1))


if __name__ == "__main__":
    sys.exit(main())
