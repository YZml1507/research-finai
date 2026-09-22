# 61. e62 Kaggle GPU ML 特征扩展收单判负（一致预期/基金覆盖无交互增量）

日期：2026-09-22 ｜ 状态：收单判负登记

## 设计
- 底盘：e58 事件+价格 26 特征（XGBRegressor GPU, max_depth5/lr.05/n300）
- 扩展臂 +7 特征：s1_eps_rev90, s2_np_rev90, s3_fy_slope, s4_pe_chg, fwd_ep（CSMAR 一致预期面板）、fund_cov, fund_cov_chg（基金覆盖聚合半年频，avail+120d lag）
- 矩阵 422,292×38；walk-forward 24 月窗 + 20 交易日 embargo；月度 Rank IC，2017-2024 共 89 月
- Kaggle GPU kernel `mengxinyz/finai-e62-xgb-gpu` v5（v1-v3 因 index 对齐 bug 作废）

## 结果
| 臂 | IC | t | ICIR_ann |
|---|---|---|---|
| base 26 特征 | 0.0997 | 8.18 | 3.01 |
| ext +7 | 0.0992 | 7.75 | 2.85 |

ΔIC = −0.0005，t 差 −0.43。**新特征零增量且轻微为负**。

## 结论
1. 一致预期成品面板（e60 已判弱）在 ML 交互项内依然无肉——不是"单变量弱但交互有用"的情形；
2. 基金覆盖半年频聚合同理无增量；
3. XGBoost 基臂 IC 0.0997/t8.18 略强于 e58 LGBM 0.083/2.59——模型选择本身的差异，e58 结论仍成立（basket-bound）；
4. 元教训再次坐实：该面板的信息要么已在价格里、要么精度在聚合中丢失。方向不变——若再投数据，只投**逐分析师明细库**。
