# 融資策略方向（資金分層 + 只動可動資金）

## 目標

在 **Bitfinex funding** 上，於可自動化範圍內 **盡量提高實際年化收益**，核心原則：

1. **已借出的 Funding Credits 不可由 Bot 調度**：僅做多筆與面值加總之資訊 log，不列入配比母體
2. **只有可重新部署的閒置資金，才進入 base / preposition / spike 計算**
3. **盡量讓資金留在高利率長天期（120d）**
4. **高利率 spike 可能只有幾秒到幾分鐘，所以平常就預掛一部分 120d 作為「等待網」**
5. **避免無謂重掛**：若現有「可撤」掛單與本輪新計畫在容差內一致，會 **skip cancel+resubmit**；但 **逾時預掛刷新**、**利率偏離容忍帶**、**KEEP_MAX 裁切** 時仍會撤單重掛。

### 程式變數與白話（對應 `start.py`）

| 符號／桶名 | 白話 |
|------------|------|
| `kept_prep` | 本輪判定 **留在簿上、不會被取消** 的我方 **120d 預掛**（利率／容忍／刷新／`KEEP_MAX` 規則都過關時） |
| `cancel_queue` | 本輪判定 **要先取消**，再拿釋放的錢依照新計畫重下的我方掛單 |
| `kept_prep_notional` | **本輪保留在簿**的 `kept_prep` 那些掛單，面值加總（約當美金） |
| `cancel_queue_notional` | **本輪預備先撤銷**的 `cancel_queue` 那些掛單，面值加總（約當美金）；撤完後這筆數字多半會進錢包再參與重掛 |
| `available_capital` | 錢包可用 + **`cancel_queue_notional`**（理論：撤掉 `cancel_queue` 後馬上能多用的那一側部位） |
| `split_budget_notional` | `available_capital` + **`kept_prep_notional`**，**Base／Spike 的百分比一律乘這個數** |

---

## 一、資金分層

每輪執行時，與自動化調度直接相關的「可動部位」主要以掛單與錢包表達；**已借出的 credits** 另列如下（不做子類拆分）：

| 類別 | 定義 | 處理方式 |
|------|------|---------|
| **Funding Credits（active）** | 目前 **狀態為 active** 的已放款（來自 `get_funding_credits`） | **不撤、不列入 **`split_budget_notional`****；程式僅 log **`Credits(active): n=… notional=…`**（筆數 + 面值加總） |
| **available_capital** | **`wallet.available_balance`** + 本輪 **`classify_offers`** 分進 **`cancel_queue`** 的我方掛單面值總和 | 見上表「程式變數與白話」；不等於名目配比分母 **`split_budget_notional`** |

注意（與程式一致）：**Base／Spike 的拆分比例係對 **`split_budget_notional`** 計算，不是對整帳總資產，也不是只看 `available_capital`。其中：

 **`split_budget_notional` = `available_capital` + `kept_prep_notional`**  

- **`kept_prep_notional`**：**本輪分類為 **keep** 的 120d 預掛**規模總和（已由簿上餘額表達，本輪未取消前不計入 `available_capital`）。  
- 意義：**尚未變成 Funding Credit 借出**、仍在你掌控下的 **「free + 可撤掛單 + keep 預掛」** 一併當作分桶母體，使 **45%／50%／5%** 與你直覺上「全部還沒借出去的錢」一致。

---

### 仍不列入 `split_budget_notional` 的錢

- **進行中的 Funding Credits**：已借出、記在貸款端，除非到期或提前結清，否則不進入本輪分桶。

---

### 實際能「新下單」花掉的上限

- **`build_*_orders` 產生的新單名義**，仍受 **`available_capital`** 約束：**kept_prep** 上的資金除非你把它改列入 **cancel_queue** 並撤掉，同一輪內無法「再花一輪錢」重複下注。
- 因此當 **`target_2d + prep_topup`（或 spike 的 **120d_prep_topup** / 短端）** 合計仍超過 **`available_capital`** 時，程式會**先保長端 incremental top-up**、再**裁切 2d／30d**（與 `start.py` 一致）。

---

### Base 配比、reserve（5%）、與池子縮放

- **設計**：`BASE_SPLIT_2D` + `BASE_SPLIT_120D_PREPOSITION` + `BASE_SPLIT_RESERVE` = **1**。名目上 **2d** 與 **120d（含已掛 + topup）** 的目標規模為 **`BASE_SPLIT_* × split_budget_notional`**；**120d topup** = **`max(0, 該目標 − kept_prep_notional)`**，避免重複堆疊同一長單。  
- **Reserve（5%）**：名目上是 **`BASE_SPLIT_RESERVE × split_budget_notional`** 這一塊不強制打成 **2d／120d** 新單；實際 **wallet** 餘額還受「是否裁切 2d」「topup 是否為 0」等影響。  
- **放款／成交進 credits 愈多** → 通常 **`split_budget_notional` 變小** → 同名目比例下的 **美元錨點**也變小（含 **5% reserve** 的美元意義），與「高利盡量沉在 **credits**」一致。  
- **若 `kept_prep_notional` 已高於長端名目目標**：**topup = 0**，但 **2d 名目**仍按 **`BASE_SPLIT_2D × split_budget_notional`** 變大；若 **liquid `available_capital` 不夠**，**2d 會被裁切到僅能花完自由資金**。

資金自 **credits** 回流時，**`split_budget_notional` 回升**，名目美元亦跟著回升。

---

### Spike 模式與 reserve

**Spike level 1／2** 的 **`SPIKE_SPLIT_*`** 同樣乘在 **`split_budget_notional`** 上；**仍無**額外的 `BASE_SPLIT_RESERVE` 維度。實際新單支出同樣 **≤ `available_capital`**。

---

## 二、策略模式

每輪：先決定 spike level，再決定怎麼切 **`split_budget_notional`**（見第一節）；實際送單仍以 **`available_capital`** 為支出上限。

### Spike level 0（無 spike） — Base mode

| 桶 | 佔 `split_budget_notional` | 說明 |
|----|---------------------|------|
| 2d（ladder） | `BASE_SPLIT_2D` = 45% | `compute_ladder_range` + 分階；花費來自 **`available_capital`**，可被裁切 |
| 120d preposition | `BASE_SPLIT_120D_PREPOSITION` = 50% | 已由 **keep** 滿足之部分不重下；不足處為 **topup** 新單 |
| reserve | `BASE_SPLIT_RESERVE` = 5% | 名目配比分母一部分；程式不另送 reserve 標籤單 |

### Spike level 1（一般 spike）

| 桶 | 佔 `split_budget_notional` |
|----|---------------------|
| 2d | 40% |
| 30d | 0%（預設不配；權重合併至 120d） |
| 120d（含 preposition）| 60% |

### Spike level 2（強 spike）

| 桶 | 佔 `split_budget_notional` |
|----|---------------------|
| 2d | 10% |
| 30d | 0%（預設不配；權重合併至 120d） |
| 120d（含 preposition）| 90% |

**共通規則**：**keep**（**`kept_prep_notional`**）先抵 **`BASE_SPLIT_120D_*`/`SPIKE` 中段長端**對應的美元目標，只補 **topup**，不重複堆整桶。

---

## 三、Preposition 目標利率（touch-only）

資料：最近 **`PREPOSITION_TOUCH_LOOKBACK_DAYS`**（預設 10）天的 hourly funding candle（`trade:1h:{currency}:a30:p2:p30`）的 **HIGH**；拉取時使用 **`start` + `end=現在`**，避免只帶 `start` 時回傳錯誤歷史區間。與 **`submit_funding_offer` 的 `rate`、`PREPOSITION_RATE_CEIL`** 須為同一刻度。

對每個長度為 **`PREPOSITION_TOUCH_WINDOW_HOURS`**（預設 48，約兩天）的滑動視窗，統計窗內有多少小時 **`HIGH ≥ 閾值 r`**；對所有滑動窗取**平均值**這個「觸碰次數」。在所有滿足「**平均值 ≥ `PREPOSITION_MIN_AVG_TOUCHES`**」的 `r` 裡取**最大**的：

```
r*      = largest such r (rolling-window criterion)
raw     = r* × PREPOSITION_P99_MULT
target  = min(raw, PREPOSITION_RATE_CEIL)
```

成功路徑**不再**對 `target` 施加「下限 clamp」；只靠 touch 算出 `raw`，再用 **`CEIL` 做上沿**。

- **`PREPOSITION_FALLBACK_RATE`**：僅在 **資料不足、candle API 異常 touch 約束無解、或算出無效 raw** 時用作本輪目標利率（並須 `≤ PREPOSITION_RATE_CEIL`）；預設偏低，避免算不出 touch 時掛離譜高利。
- **`PREPOSITION_RATE_CEIL`**：僅在 **touch 成功** 時限制 `raw`；預設偏高，讓市場尖峰算出來的 `raw` 多半能原樣掛出；若 `raw` 仍超過 ceil 才壓頂並 WARNING。
- **`PREPOSITION_KEEP_MAX_RATE`**：仍可用於裁掉 **利率過高** 的預掛（見下文）。

資料不足／touch 約束無解／無效結果 → **`PREPOSITION_FALLBACK_RATE`**。

### Preposition 留在場上 vs 逾時重掛

符合 **120d 預掛** 且 **`rate + tolerance ≥ target`** 的掛單預設**保留**（含比目標高的限價）。

若 REST 回傳的 **`MTS_CREATED`** 起算已超過 **`PREPOSITION_REFRESH_HOURS`**（預設 **24**，約一天），該筆視為「盤上放太久」，改列為**可撤**，下一輪會取消並依**重新計算的 touch 目標**再掛（避免永遠卡在同一張單）。

**`PREPOSITION_REFRESH_HOURS=0`** 可關閉此行為。

其他：`PREPOSITION_KEEP_MAX_RATE` 仍可用於強制裁掉過高利率的預掛。

### 短天期梯子（2d / 30d）

對各天期區間從 **同一批近 24h 公開成交**（與 spike 共用）篩出利率樣本：

- **`rate_low`** = 該區間成交利率的 **`LADDER_LOW_PCT` 分位**（預設 30）
- **`rate_high`** = **`LADDER_HIGH_PCT` 分位**（預設 95）

若樣本數 **&lt; `LADDER_MIN_SAMPLES`**（預設 20），改用以 **`get_market_funding_book`** 彙整的訂單簿 **加權均價／最高檔** 作為 fallback。若 `high ≤ low`，程式會將上緣暫設為 **約 +10%** 以強制構成區間。

階梯筆數隨 **`MINIMUM_FUNDS`**、**`BITFINEX_MIN_*`**、`budget` 變動，非固定「恰好 10 階」（大額時目標約 10+ 檔）。

## 四、Spike 判定邏輯

資料來源：`/v2/trades/{currency}/hist`（公開成交），自 **`now - SPIKE_BASELINE_WINDOW_SEC`** 起往現在拉取（預設 **24h**；單次最多 **10000** 筆，極端 busy 時較舊樣本可能被截斷並打 Warning）。

**兩段不重疊視窗**（與 `detect_spike_level` 一致）：

- **recent**：`[now - SPIKE_RECENT_WINDOW_SEC, now]`（預設 **最後 60 秒**）
- **baseline**：`[now - SPIKE_BASELINE_WINDOW_SEC, now - SPIKE_RECENT_WINDOW_SEC)`（預設 **往前 24h 但排除最近 1 分鐘**）

### Level 1（一般 spike）

- **recent** 內成交利率的**算術平均** &gt; **baseline** 內成交利率的**算術平均** × `SPIKE_L1_MULTIPLIER`（預設 1.8）
- **且** **recent** 內至少 1 筆 `period ≥ SPIKE_L1_MIN_LONG_PERIOD`（預設 30d）

### Level 2（強 spike）

- 滿足 L1 所有條件
- **且** **recent** 內最高成交利率 ≥ `SPIKE_L2_MIN_RATE`（預設 0.00035）
- **且** **recent** 內 ≥ `SPIKE_L2_MIN_LONG_TRADES`（預設 2）筆 `period ≥ SPIKE_L2_MIN_LONG_PERIOD`（預設 120d）

Spike **只改**本輪 `build_*_orders` 的 **2d／30d／120d 配比**；**不會**動 **已借出的 Funding Credits**。**kept_prep** 內預掛也不會因此被額外加撤；若該張 120d 已被 `classify_offers` 丟進 **`cancel_queue`**，則隨 **`cancel_queue`** 一併撤銷並重下。

---

## 五、Fallback：sub-minimum bucket 合併

當 `available_capital` 太小，切完配比後某個桶 < Bitfinex 最小下限（預設 150 USD）時，該桶會被合併到「首選桶」：

- **Base mode**：首選 = 2d。**prep_topup** < 最小單金額 → 併入 2d 桶。
- **Spike mode**：首選 = 120d（長天期優先）。若 2d/30d < 150 → 併入 120d。

若首選桶自己也 < 150，則全部金額連同滾入下一優先桶；合併後仍無法做出任何可下單，`build_*_orders` 會得到 **空列表**，主流程在 **進入撤單前** 即 `return`，本輪 **不會取消任何掛單**。

另：**`_plan_matches_existing`** 為真時會 **整段跳過 cancel + submit**，簿上掛單完全不動。

---

## 六、主流程（`lending_bot_strategy`）

```
 1. 抓 funding credits / offers / wallet；任一路失敗則整輪 skip
 2. （可選）粗算：**wallet + 同幣種所有進行中的 funding offers 面值**，若連交易所最小單都湊不到則提早 skip。**此為較鬆的上界（含將被 classify 為 keep 者）**，為避免在低流動情境仍打公開 API；不致誤因「將撤銷之 120d」未列入而下錯判。
 3. `_active_credits_rollups`：active credits 筆數與面值加總（僅 log；不撤貸款）
 4. compute_preposition_target_rate（touch；第三節）
 5. classify_offers → **`kept_prep`**（留住）／**`cancel_queue`**（將撤銷並重掛）
 6. available_capital = wallet + sum(cancel_queue)（見「程式變數與白話」）
 7. 若 available_capital 低於交易所最小單 → skip（不撤不送）
 8. 拉近 24h 公開成交 + get_market_funding_book → compute_ladder_range（2d、30d）
 9. detect_spike_level → 0 / 1 / 2
10. build_base_orders 或 build_spike_orders（內：**`split_budget_notional`** = **`available_capital`** + **`kept_prep_notional`**；比例皆乘此值）
11. 若 `new_orders` 為空 → **立即 return（不撤銷、不重掛）**
12. 若現有 **`cancel_queue`** 已與 `new_orders` 在容差內一致 → **skip cancel + skip submit**
13. 逐一取消 **`cancel_queue`** 內掛單；**kept_prep** 不撤
14. 非 `DRY_RUN` 時重查 wallet，必要時 **縮放** `new_orders` 總額後送出
```

**原則**：
- 不會為 spike 去動已借出的 Funding Credits
- 名目拆分乘 **`split_budget_notional`**。**已借出的 credits** 不在此集合內。實際 **cancel／submit** 只可動 **`available_capital`**。**kept_prep** 上的資金要等到 `classify_offers` 將其改判進 **`cancel_queue`** 並撤銷後，才併進錢包與下一輪 **`available_capital`**。
- **kept_prep** 可跨輪留簿；被列入 **cancel_queue** 的（含逾時、利率、tolerance、`KEEP_MAX` 等）本輪撤掉再重掛（若未提早 return、也未 no-op）。

---

## 七、環境變數

### Base mode 配比
| 變數 | 預設 | 用途 |
|------|------|------|
| `BASE_SPLIT_2D` | 0.45 | 2d 佔 **`split_budget_notional`** |
| `BASE_SPLIT_120D_PREPOSITION` | 0.50 | 預掛 120d 佔 **`split_budget_notional`** |
| `BASE_SPLIT_RESERVE` | 0.05 | Base 其名目區塊（乘 **`split_budget_notional`**）；實務上靠「新單不撲滿 **`available_capital`**」反映在 **wallet** |

### Preposition
| 變數 | 預設 | 用途 |
|------|------|------|
| `PREPOSITION_PERIOD` | 120 | 預掛天期 |
| `PREPOSITION_RATE_CEIL` | 0.0050 | touch 成功時 `raw` 上限（偏高，方便吃到尖峰；約 182% 簡單年化才會碰到） |
| `PREPOSITION_FALLBACK_RATE` | 0.00040 | touch 失敗時專用（偏低、約 15% 簡單年化；與 CEIL 無關，不會自動變高） |
| `PREPOSITION_P99_MULT` | 0.98 | raw 對 r* 的乘數 |
| `PREPOSITION_TOUCH_LOOKBACK_DAYS` | 10 | hourly candle 回看天數 |
| `PREPOSITION_TOUCH_WINDOW_HOURS` | 48 | 滑動窗長（小時） |
| `PREPOSITION_MIN_AVG_TOUCHES` | 5.0 | 每窗平均至少幾小時 HIGH≥r |
| `PREPOSITION_TOLERANCE` | 0.00002 | 保留現有預掛單的利率容忍帶 |
| `PREPOSITION_REFRESH_HOURS` | 24 | 預掛單自 MTS_CREATED 起超過此小時數則撤並重掛；0=關閉 |
| `PREPOSITION_KEEP_MAX_RATE` | （未設定） | 超過此利率的預掛改列為可撤 |
| `PREPOSITION_MIN_SAMPLES` | 50 | 至少幾根 hourly candles（≥ `TOUCH_WINDOW+1`） |

### Spike 判定
| 變數 | 預設 | 用途 |
|------|------|------|
| `SPIKE_L1_MULTIPLIER` | 1.8 | L1：**recent** 平均 / **baseline** 平均（baseline 不含 recent 視窗） |
| `SPIKE_L1_MIN_LONG_PERIOD` | 30 | L1 需要出現的最小成交天期 |
| `SPIKE_L2_MIN_RATE` | 0.00035 | L2：**recent** 視窗內成交利率最高值閾值 |
| `SPIKE_L2_MIN_LONG_PERIOD` | 120 | L2 需要出現的最小成交天期 |
| `SPIKE_L2_MIN_LONG_TRADES` | 2 | L2 需要的該天期成交筆數 |
| `SPIKE_RECENT_WINDOW_SEC` | 60 | 「最近 1 分鐘」視窗 |
| `SPIKE_BASELINE_WINDOW_SEC` | 86400 | 「過去 24h」基線視窗 |

### Spike 配比（對 **`split_budget_notional`**）
| 變數 | 預設 | 格式 |
|------|------|------|
| `SPIKE_SPLIT_L1` | `0.40,0.0,0.60` | 2d, 30d, 120d（中段可為 0） |
| `SPIKE_SPLIT_L2` | `0.10,0.0,0.90` | 2d, 30d, 120d |

### 梯子與公開成交抽樣
| 變數 | 預設 | 用途 |
|------|------|------|
| `LADDER_LOW_PCT` | 30.0 | 2d/30d 梯子下緣（近 24h 成交該區間之分位） |
| `LADDER_HIGH_PCT` | 95.0 | 2d/30d 梯子上緣 |
| `LADDER_MIN_SAMPLES` | 20 | 低於此樣本數則改採 order book fallback |

### 其他
| 變數 | 預設 | 用途 |
|------|------|------|
| `FUND_CURRENCY` | fUSD | 幣種（可設 `USD`，程式會正規成 `fUSD`） |
| `BITFINEX_MIN_FUNDING_ORDER_USD` | 150 | 交易所單筆最小下單 |
| `MINIMUM_FUNDS` | 500 | 梯子每階金額下限（與最小單取 max） |
| `DRY_RUN` | 關 | 設為 `1`/`true` 時只 log 不送單 |

---

## 八、已捨棄的舊邏輯

下列項目在新策略下已不使用；其中 **`NORMAL_MARGIN_SPLIT` / `HIGH_RATE_MARGIN_SPLIT` 等常數已自 `start.py` 移除**（僅本文件保留名稱，供對照舊版行為）。

- **固定四桶配比**（`NORMAL_MARGIN_SPLIT` / `HIGH_RATE_MARGIN_SPLIT`）：被資金分層 + spike level 配比取代。
- **高利率模式門檻**（`HIGH_RATE_APY_MIN`）：spike 判定改用近 1 分鐘 vs 24h 的動態結構，不再用單一 APY 門檻。
- **`cancel_all_funding_offers`**：改用 `cancel_funding_offer(id)` 精準撤單，避免誤撤 preposition。
- **歷史 K 線壓頂**（`FUNDING_HIST_*`）：已移除。
- **Preposition percentile / PR99 / `PREPOSITION_RATE_FLOOR`**：preposition 改為唯一演算法 touch（見第三節）；僅 **`PREPOSITION_FALLBACK_RATE`** 在失敗時使用；2d／30d 梯子仍沿用成交分位（`LADDER_*`）。

---

**最後更新**：2026-05-22
