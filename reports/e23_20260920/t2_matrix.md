# T2 假设 × 数据面矩阵（e23, 2026-09-20，只读盘点）

## 数据面字段清单
- `data/daily_basic_alla/{YYYYMMDD}.parquet`（2431 日截面，2015-01-05→2024-12-31，全 A ~3700-5200 只/日）：
  `ts_code, trade_date, close, dv_ratio, dv_ttm, total_mv, circ_mv, free_share, turnover_rate, pe, pb`
  ——dv/市值/估值/换手齐备，**无 OHLC/量额**、无行业、无 ST 标签。
- `data/financial_pit_alla/{code}.parquet`（5474 只，PIT）：
  `code, pub_date, stat_date, roe, net_profit_yoy, deducted_net_profit_yoy, debt_to_assets, cash_flow_per_share, eps, source`
  ——PIT 语义=pub_date≤决策日可见；字段覆盖 ROE/增速（含扣非 yoy）/杠杆/每股经营现金流/EPS。
- `data/dividend_events_alla/{code}.parquet`（全 A 分红事件）：
  `ts_code, end_date, ann_date, div_proc, stk_div, stk_bo_rate, stk_co_rate, cash_div, cash_div_tax, record_date, ex_date, pay_date, div_listdate, imp_ann_date, code`
  ——公告/除权/派息/登记日期齐备，ann_date PIT 严。
- `data/c3_universe/{sym}/{year}.parquet`（692 只含 7 退市票，2015–2024）：
  `date, OHLC, preclose, volume, amount, turn, pctChg, tradestatus, isST, code, source, adjust_mode, market_cap, dividend_yield` + `exdiv/{sym}.parquet`。
- 其他在位：`data/c3_pool/pool_yearly.parquet`、`data/pool_meta/stock_industry.parquet`（当前快照行业）、`data/stock_basic_cache.parquet`、`data/rates/gc001_daily.parquet`、宽度序列。

## 假设 × 数据面

| 假设 | 需要字段 | 可得？ | PIT 安全？ | 宇宙 | 还缺什么 |
|---|---|---|---|---|---|
| D5 应计/盈利质量（剔伪红利） | 经营现金流、净利润、应计 | **部分**：fina_alla 有 `cash_flow_per_share`+eps，可近似现金流覆盖度=CFPS/EPS；无总资产/应计项原始字段 | ✅ pub_date | 全 A（fina）×692（bars） | 应计比率本体需资产负债表字段（补采） |
| D6 三指标择时（期限利差+银行间量） | 10Y/短端利率、银行间成交量 | ❌ 仓内只有 GC001 利率 | — | — | **需新采集**（中债收益率、银行间量）——数据层缺口，评测不能离线做 |
| FCF 现金流因子 | 自由现金流（经营现金流−资本开支） | **近似部分**：有 cash_flow_per_share，无资本开支字段 | ✅ pub_date | 全 A 财报×692 bars | 资本开支字段（补采）或用 CFPS 近似降级口径 |
| 动量/反转（692 池） | 日 OHLC | ✅ c3_universe 完整 | ✅（价格无外源泄漏） | 692（含退市） | 无；可直接立项 |
| 低波 BAB | 日收益波动率+ beta（需指数基准） | ✅ bars + sh.000300/宽基在日 | ✅ | 692 或全 A 截面（dv_alla 只有 close 无全序列） | 全 A 版需 bars 扩展；692 版零成本 |
| D4 行业中性 | 行业分类 | **部分**：`stock_industry.parquet` 为当前快照标签（非 PIT，C3 已登记简化） | ⚠️ 非 PIT | 692 | PIT 行业史（补采）或接受快照简化并登记 |
| PEAD 扣非 | 扣非净利润同比 | ✅ `deducted_net_profit_yoy` 已采（fina_alla） | ✅ pub_date | 692（bars）/全 A（fina） | 公告事件时间戳精度（fina pub_date 日级，够用日线） |
| 换手率/流动性因子 | turnover_rate, amount | ✅ daily_basic 全 A + c3 bars | ✅ | 全 A 截面 / 692 | 无 |

## 结论
**零采集可立**：692 池上的动量/反转、低波、换手因子、扣非 PEAD、FCPS 近似 FCF、现金流覆盖度版 D5。
**需补采才能严肃做**：D6 宏观择时（利率/银行间量）、严格应计异象、PIT 行业史。
**宇宙注意**：非红利假设若上全 A 宇宙需扩 bars（现只有 692+487 并集有 bars）；daily_basic 截面可做选股但无撮合价格 ⇒ 引擎级回测仍以 692 为界。
