# 融資策略方向（資金分層 + 只動可動資金）

## 目標

在 **Bitfinex funding** 上，於可自動化範圍內 **盡量提高實際年化收益**，核心原則：

1. **已經成功借出的高利率長單 = 成果池，不動**
2. **只有可重新部署的閒置資金，才進入 base / preposition / spike 計算**
3. **盡量讓資金留在高利率長天期（120d）**
4. **高利率 spike 可能只有幾秒到幾分鐘，所以平常就預掛一部分 120d 作為「等待網」**
5. **預掛單不頻繁重掛，避免一直重排隊**

---

## 一、資金分層

每輪執行時，帳戶資金分成三類：

| 類別 | 定義 | 處理方式 |
|------|------|---------|
| **locked_high_rate_loans** | active credit 且 `period ≥ LOCKED_MIN_PERIOD_DAYS` 且 `rate ≥ LOCKED_MIN_RATE` | 不動、不列入配比計算、僅在到期 / 提前還款 / 狀態變化時才離開此類 |
| **active_other_loans** | 其他 active credits（2d、低利率長單等） | 不動，等自然到期釋出 |
| **available_capital** | `wallet.available_balance` + 「可撤的非 preposition 掛單金額」 | 唯一需要重新分配的資金池 |

注意：所有百分比都是對 `available_capital` 算，**不是** 對總資產算。

---

## 二、策略模式

每輪：先決定 spike level，再決定怎麼分配 `available_capital`。

### Spike level 0（無 spike） — Base mode

| 桶 | 佔 available_capital | 說明 |
|----|---------------------|------|
| 2d（ladder） | `BASE_SPLIT_2D` = 70% | 市場均價到估計頂，分 10 階梯子 |
| 120d preposition | `BASE_SPLIT_120D_PREPOSITION` = 25% | 單一大單 @ `preposition_target_rate` |
| reserve（機動） | `BASE_SPLIT_RESERVE` = 5% | 留在 wallet，不掛 |

### Spike level 1（一般 spike）

| 桶 | 佔 available_capital |
|----|---------------------|
| 2d | 40% |
| 30d | 20% |
| 120d（含 preposition）| 40% |

### Spike level 2（強 spike）

| 桶 | 佔 available_capital |
|----|---------------------|
| 2d | 10% |
| 30d | 20% |
| 120d（含 preposition）| 70% |

**共通規則**：已經在場上的 preposition 120d 掛單會抵掉 120d 桶的預算，只補差額，不重下整桶。

---

## 三、Preposition 目標利率

以最近 3 天 hourly funding candle（`trade:1h:{currency}:a30:p2:p30`）的 HIGH 計算 p99，套用：

```
target_rate = clamp(p99_hourly_high × 0.98, [floor, ceil])
           = min(max(0.00040, p99_hourly_high × 0.98), 0.00048)
```

意義：
- **不低於 0.00040** — 0.00040 已經是滿意的高價位
- **跟著市場高價區浮動** — p99 * 0.98，略低於極端值
- **不超過 0.00048** — 避免被單次異常 spike 把掛價拉太高
- **用 candle HIGH 而非每筆 trade** — HIGH 本身就是每小時的峰值，正好對應「預掛在高價區」的意圖；且 candle 端點不會被 10000 筆 trade 上限截斷

資料不足或 API 失敗時 fallback 到 `PREPOSITION_RATE_FLOOR`（保守）。

### Preposition 留在場上的保留規則（不對稱）

現有 preposition 掛單（`period == PREPOSITION_PERIOD` 且 `rate + PREPOSITION_TOLERANCE ≥ target_rate`）**不撤**。

**不對稱設計**：
- 掛得比目標**高**的單 → 保留。高單只會在 spike 時被吃到，成交等於多賺；主動撤掉改掛低價等於自己壓低報價
- 掛得比目標**低**（超過 tolerance）的單 → 撤掉重掛。低單成交會讓我們少賺，必須重配

tolerance 仍保留小緩衝，避免 target_rate 在 1-2bp 間微幅漂移就來回重掛。

---

## 四、Spike 判定邏輯

資料來源：`/v2/trades/{currency}/hist`（公開成交），拉最近 24h。

### Level 1（一般 spike）

- 最近 1 分鐘平均成交利率 > 過去 24h 平均 × `SPIKE_L1_MULTIPLIER`（預設 1.8）
- **且** 最近 1 分鐘內出現至少 1 筆 `period ≥ SPIKE_L1_MIN_LONG_PERIOD`（預設 30d）的成交

### Level 2（強 spike）

- 滿足 L1 所有條件
- **且** 最近 1 分鐘最高成交利率 ≥ `SPIKE_L2_MIN_RATE`（預設 0.00035）
- **且** 最近 1 分鐘內出現 ≥ `SPIKE_L2_MIN_LONG_TRADES`（預設 2）筆 `period ≥ SPIKE_L2_MIN_LONG_PERIOD`（預設 120d）的成交

Spike **只影響這輪 `available_capital` 的配比**，不會重撤 locked loans 或 preposition。

---

## 五、Fallback：sub-minimum bucket 合併

當 `available_capital` 太小，切完配比後某個桶 < Bitfinex 最小下限（預設 150 USD）時，該桶會被合併到「首選桶」：

- **Base mode**：首選 = 2d（快成交優先）。若 preposition topup < 150 → 併入 2d。
- **Spike mode**：首選 = 120d（長天期優先）。若 2d/30d < 150 → 併入 120d。

若首選桶自己也 < 150，則全部金額連同滾入下一優先桶；全部桶合併後仍 < 150，則本輪 skip，錢留 wallet，**不會誤撤任何掛單**。

---

## 六、主流程

```
每次執行：
  1. 抓 active credits / pending offers / wallet balance
  2. classify_loans → locked_high_rate / active_other（此二類不動）
  3. compute_preposition_target_rate（近 N 天 hourly candle HIGH 的 p99）
  4. classify_offers → preposition（keep）/ other（可撤候選）
  5. available_capital = wallet + sum(other_offers)
  6. 拉 24h 公開成交 → detect_spike_level → 0 / 1 / 2
  7. build_base_orders 或 build_spike_orders（依 level）
  8. 逐一取消 other_offers（不動 preposition）
  9. 逐一送出新單
```

**絕不做的事**：
- 取消 preposition 容忍帶內的掛單
- 重配整戶資金
- 因 spike 就撤已成交的 locked 高息長單
- 丟掉 sub-minimum 金額讓它死在 wallet（有 fallback 合併）

---

## 七、環境變數

### 資金分層
| 變數 | 預設 | 用途 |
|------|------|------|
| `LOCKED_MIN_PERIOD_DAYS` | 60 | locked 判定的最小天期 |
| `LOCKED_MIN_RATE` | 0.00040 | locked 判定的最小利率 |

### Base mode 配比
| 變數 | 預設 | 用途 |
|------|------|------|
| `BASE_SPLIT_2D` | 0.70 | 2d 佔比 |
| `BASE_SPLIT_120D_PREPOSITION` | 0.25 | 預掛 120d 佔比 |
| `BASE_SPLIT_RESERVE` | 0.05 | 機動資金佔比 |

### Preposition
| 變數 | 預設 | 用途 |
|------|------|------|
| `PREPOSITION_PERIOD` | 120 | 預掛天期 |
| `PREPOSITION_RATE_FLOOR` | 0.00040 | target_rate 下限 |
| `PREPOSITION_RATE_CEIL` | 0.00048 | target_rate 上限 |
| `PREPOSITION_P99_MULT` | 0.98 | 對 p99 的乘數 |
| `PREPOSITION_LOOKBACK_DAYS` | 3 | p99 採樣的回看天數（hourly candle）|
| `PREPOSITION_TOLERANCE` | 0.00002 | 保留現有預掛單的利率容忍帶 |

### Spike 判定
| 變數 | 預設 | 用途 |
|------|------|------|
| `SPIKE_L1_MULTIPLIER` | 1.8 | L1 觸發：recent-1m / 24h-avg 比值 |
| `SPIKE_L1_MIN_LONG_PERIOD` | 30 | L1 需要出現的最小成交天期 |
| `SPIKE_L2_MIN_RATE` | 0.00035 | L2 觸發：recent-1m 最高成交利率 |
| `SPIKE_L2_MIN_LONG_PERIOD` | 120 | L2 需要出現的最小成交天期 |
| `SPIKE_L2_MIN_LONG_TRADES` | 2 | L2 需要的該天期成交筆數 |
| `SPIKE_RECENT_WINDOW_SEC` | 60 | 「最近 1 分鐘」視窗 |
| `SPIKE_BASELINE_WINDOW_SEC` | 86400 | 「過去 24h」基線視窗 |

### Spike 配比
| 變數 | 預設 | 格式 |
|------|------|------|
| `SPIKE_SPLIT_L1` | `0.40,0.20,0.40` | 2d, 30d, 120d |
| `SPIKE_SPLIT_L2` | `0.10,0.20,0.70` | 2d, 30d, 120d |

### 其他
| 變數 | 預設 | 用途 |
|------|------|------|
| `FUND_CURRENCY` | fUSD | 幣種 |
| `BITFINEX_MIN_FUNDING_ORDER_USD` | 150 | 交易所單筆最小下單 |
| `MINIMUM_FUNDS` | 500 | 梯子每階 chunk 下限 |
| `RATE_ADJUSTMENT_RATIO` | 1.11 | 2d/30d 梯子寬度 |

---

## 八、已捨棄的舊邏輯

下列項目在新策略下已不使用（但相關函式仍保留在 `start.py`，未來需要可再用）：

- **固定四桶配比**（`NORMAL_MARGIN_SPLIT` / `HIGH_RATE_MARGIN_SPLIT`）：被資金分層 + spike level 配比取代。
- **高利率模式門檻**（`HIGH_RATE_APY_MIN`）：spike 判定改用近 1 分鐘 vs 24h 的動態結構，不再用單一 APY 門檻。
- **`cancel_all_funding_offers`**：改用 `cancel_funding_offer(id)` 精準撤單，避免誤撤 preposition。
- **歷史 K 線壓頂**（`FUNDING_HIST_*`）：preposition target rate 直接用近 N 天 hourly funding candle HIGH 的 p99，不再需要 K 線 slack。

---

**最後更新**：2026-04-21
