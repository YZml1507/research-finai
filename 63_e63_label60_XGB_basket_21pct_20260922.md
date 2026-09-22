# 63. e63: label fwd60 + XGBoost GPU → top500 mensual 21.5% CAGR (hallazgo fuerte)

Fecha: 2026-09-22 | Estado: hallazgo a validar (NO promovido)

## Experimento
Grid Kaggle GPU (XGBRegressor, 7 configs × labels fwd20/fwd60) sobre la
matriz e58 (26 features precio+eventos, walk-forward 24m + embargo 20td).

## IC por configuración
- label60 mejor: depth6/lr.05 → **IC 0.147, t 10.06** (n=87 meses)
- label20 mejor: depth5/lr.03 → IC 0.089, t 6.33 (n=89)

Consistente con e58_horizon (fwd80 IC > fwd20): el alpha es slow-moving.

## Validación a nivel cartera (basket_bt, costos ~11.5bp/lado, top500)
| scores | freq | CAGR | MDD | turnover anual |
|---|---|---|---|---|
| h20 | mensual | 4.8% | 0.38 | 17.2x |
| h20 | trimestral | 6.6% | 0.40 | 6.0x |
| h60 | mensual | **21.5%** | 0.28 | 14.3x |
| h60 | trimestral | 11.2% | 0.31 | 5.9x |

## Implicancia
El mismo set de features entrenado contra fwd60 cuadruplica el CAGR neto
del basket vs la config e58 original (6.1%). Candidato a nueva config T3
(≥200万): requiere prereg + corrida e58 completa con label60 + chequeo de
sensibilidad de costos (turnover 14x/año) + OOS antes de promover.

## e64 (mismo batch)
fund_portfolio fund-level (Kaggle tushare snapshot, ann_date PIT):
F1-F6 todos 判弱 (|t|≤2.65). El mejor dataset gratuito de holdings
confirma que crowding simple no alcanza como factor standalone.
