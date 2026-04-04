from __future__ import annotations

import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from bfxapi import Client

load_dotenv()

_MS_DAY = 86400_000
_DASH_AUTH_KEY = "_dash_auth_ok"


def _dashboard_password() -> Optional[str]:
    raw = os.getenv("DASHBOARD_PASSWORD")
    if raw is None or not str(raw).strip():
        return None
    return str(raw)


def _require_dashboard_auth() -> None:
    expected = _dashboard_password()
    if expected is None:
        return
    if st.session_state.get(_DASH_AUTH_KEY):
        return

    st.markdown(
        '<p class="dashboard-title">Bitfinex 融資儀表板</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="dashboard-sub">請輸入密碼以繼續</p>',
        unsafe_allow_html=True,
    )
    with st.form("dashboard_login_form", clear_on_submit=False):
        entered = st.text_input("密碼", type="password", autocomplete="current-password")
        submitted = st.form_submit_button("登入", width="stretch")
        if submitted:
            try:
                ok = hmac.compare_digest(
                    entered.encode("utf-8"),
                    expected.encode("utf-8"),
                )
            except Exception:
                ok = False
            if ok:
                st.session_state[_DASH_AUTH_KEY] = True
                st.rerun()
            st.error("密碼錯誤")
    st.stop()


def _daily_rate_to_apy_pct(rate: float) -> float:
    return round(max(rate, 0.0) * 365.0 * 100.0, 4)


@st.cache_data(ttl=30)
def _public_funding_tickers():
    bfx = Client()
    out = {}
    for sym, label in (("fUSD", "USD"), ("fUST", "USDt")):
        try:
            out[label] = bfx.rest.public.get_f_ticker(sym)
        except Exception as exc:
            out[label] = exc
    return out


def _inject_css() -> None:
    st.markdown(
        """
        <style>
          :root {
            --fuly-bg: #121212;
            --fuly-surface: #1a1a1a;
            --fuly-border: #2d2d2d;
            --fuly-green: #5cdb82;
            --fuly-green-deep: #2f7a47;
            --fuly-green-mid: #3fa862;
            --fuly-gold: #f1b12d;
            --fuly-gold-deep: #b8860b;
            --fuly-orange: #f05d38;
            --fuly-orange-deep: #b03d22;
            --fuly-muted: #9ca3af;
          }
          html, body, [class*="css"]  {
            font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Noto Sans TC", sans-serif !important;
          }
          .stApp {
            background: var(--fuly-bg);
          }
          [data-testid="stHeader"] {
            background: rgba(18, 18, 18, 0.92);
            border-bottom: 1px solid var(--fuly-border);
          }
          .dashboard-title {
            font-size: clamp(1.5rem, 3vw, 2rem);
            font-weight: 700;
            letter-spacing: -0.03em;
            margin: 0 0 0.25rem 0;
            color: #ffffff;
          }
          .dashboard-sub {
            color: var(--fuly-muted);
            font-size: 0.9rem;
            margin: 0;
          }
          [data-testid="stMetricDelta"] {
            font-size: 0.78rem;
          }
          .asset-label {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            font-size: 0.95rem;
            font-weight: 600;
            color: #ffffff;
            margin-bottom: 0.75rem;
            padding: 0.35rem 0.85rem;
            background: var(--fuly-surface);
            border-radius: 999px;
            border: 1px solid var(--fuly-border);
          }
          .asset-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--fuly-green);
            box-shadow: 0 0 14px rgba(92, 219, 130, 0.55);
          }
          .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            background: var(--fuly-surface);
            padding: 8px;
            border-radius: 14px;
            border: 1px solid var(--fuly-border);
          }
          .stTabs [data-baseweb="tab"] {
            border-radius: 10px;
            padding: 0.55rem 1.1rem;
            font-weight: 600;
            color: var(--fuly-muted) !important;
          }
          .stTabs [aria-selected="true"] {
            background: var(--fuly-gold) !important;
            color: #121212 !important;
          }
          div[data-testid="stDataFrame"] > div {
            border: 1px solid var(--fuly-border) !important;
            border-radius: 14px;
            overflow: hidden;
          }
          [data-testid="stDataFrame"] [class*="glideDataEditor"] {
            --gdg-bg-header: #1a1a1a !important;
            --gdg-bg-cell: #141414 !important;
          }
          .stButton > button {
            border-radius: 12px;
            font-weight: 700;
            border: none;
            background: var(--fuly-gold);
            color: #121212;
            transition: filter 0.15s, transform 0.1s;
          }
          .stButton > button:hover {
            filter: brightness(1.08);
            color: #121212;
          }
          hr {
            border-color: var(--fuly-border) !important;
            opacity: 1;
          }
          div[data-testid="stVerticalBlockBorderWrapper"] {
            background: var(--fuly-surface);
            border: 1px solid var(--fuly-border) !important;
            border-radius: 16px;
            padding: 1.1rem 1.25rem !important;
            margin-bottom: 1rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=45)
def _snapshot():
    key, sec = os.getenv("BF_API_KEY"), os.getenv("BF_API_SECRET")
    if not key or not str(key).strip() or not sec or not str(sec).strip():
        return None
    bfx = Client(api_key=key, api_secret=sec)
    wallets = bfx.rest.auth.get_wallets()
    credits = bfx.rest.auth.get_funding_credits()
    offers = bfx.rest.auth.get_funding_offers()
    ledgers = {}
    for cur in ("USD", "UST"):
        try:
            ledgers[cur] = bfx.rest.auth.get_ledgers(cur, limit=500)
        except Exception:
            ledgers[cur] = []
    return {
        "wallets": wallets,
        "credits": credits,
        "offers": offers,
        "ledgers": ledgers,
    }


def _funding_wallets(wallets):
    return [w for w in wallets if str(w.wallet_type).lower() == "funding"]


def _rate_to_apy_pct(rate: float) -> float:
    return round(rate * 100 * 365, 4)


def _mts_to_local(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone()


def _credit_expiry_ms(c) -> int:
    return int(c.mts_opening + c.period * _MS_DAY)


def _credit_time_left(c) -> str:
    end = _credit_expiry_ms(c)
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    sec = max(0, (end - now) // 1000)
    if sec < 3600:
        return f"{sec // 60} 分鐘內"
    if sec < 86400:
        return f"{sec // 3600} 小時內"
    return f"{sec // 86400} 天內"


def _ledger_is_internal_transfer(desc: str) -> bool:
    d = (desc or "").lower()
    if "transfer of" in d:
        return True
    if "from wallet" in d and "to deposit on wallet" in d:
        return True
    if "from wallet" in d and "to wallet" in d:
        return True
    return False


def _ledger_is_funding_income(desc: str) -> bool:
    if not desc or _ledger_is_internal_transfer(desc):
        return False
    d = desc.lower()
    return any(
        k in d
        for k in (
            "margin funding payment",
            "margin swap",
            "funding payment",
            "interest payment",
            "interest on",
        )
    )


def _build_credits_df(credits):
    if not credits:
        return pd.DataFrame()
    rows = []
    for c in credits:
        sym = (c.symbol or "").replace("f", "") or c.symbol
        rows.append(
            {
                "幣種": sym,
                "金額": round(abs(c.amount), 4),
                "天數": int(c.period),
                "年化率 %": _rate_to_apy_pct(c.rate),
                "期限": _credit_time_left(c),
                "ID": int(c.id),
            }
        )
    return pd.DataFrame(rows)


def _build_offers_df(offers):
    if not offers:
        return pd.DataFrame()
    rows = []
    for o in offers:
        sym = (o.symbol or "").replace("f", "") or o.symbol
        rows.append(
            {
                "幣種": sym,
                "金額": round(abs(o.amount), 4),
                "天數": int(o.period),
                "年化率 %": _rate_to_apy_pct(o.rate),
                "狀態": o.offer_status,
                "ID": int(o.id),
            }
        )
    return pd.DataFrame(rows)


def _build_earnings_df(ledgers_map, days_back: int = 90):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days_back)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    rows = []
    for cur, ledgers in ledgers_map.items():
        for L in ledgers:
            if L.mts < cutoff_ms:
                continue
            if L.amount <= 0:
                continue
            if not _ledger_is_funding_income(L.description):
                continue
            rows.append(
                {
                    "幣種": cur,
                    "收益": round(L.amount, 6),
                    "日期": _mts_to_local(L.mts).strftime("%Y/%m/%d"),
                    "時間": _mts_to_local(L.mts).strftime("%H:%M:%S"),
                    "說明": L.description[:100],
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("日期", ascending=False).reset_index(drop=True)


def _build_monthly_earnings_summary(ledgers_map, days_back: int) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    cutoff_ms = int((now - timedelta(days=days_back)).timestamp() * 1000)
    rows: list[dict] = []
    for cur, ledgers in ledgers_map.items():
        for L in ledgers:
            if L.mts < cutoff_ms or L.amount <= 0:
                continue
            if not _ledger_is_funding_income(L.description):
                continue
            dt = _mts_to_local(L.mts)
            rows.append({"幣種": cur, "ym": dt.strftime("%Y-%m"), "收益": float(L.amount)})
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    pt = df.pivot_table(index="ym", columns="幣種", values="收益", aggfunc="sum", fill_value=0.0)
    pt = pt.reset_index()
    pt.columns.name = None
    rename = {"ym": "月份", "UST": "USDt"}
    pt = pt.rename(columns={k: rename.get(k, k) for k in pt.columns})
    for col in ("USD", "USDt"):
        if col not in pt.columns:
            pt[col] = 0.0
    pt = pt[["月份", "USD", "USDt"]].copy()
    pt["合計"] = pt["USD"] + pt["USDt"]
    for c in ("USD", "USDt", "合計"):
        pt[c] = pt[c].round(6)
    return pt.sort_values("月份", ascending=False).reset_index(drop=True)


def _weighted_apy(credits, symbol_suffix: str) -> Optional[float]:
    sym = f"f{symbol_suffix}"
    subset = [c for c in credits if (c.symbol or "").upper() == sym.upper()]
    if not subset:
        return None
    num = sum(abs(c.amount) * c.rate for c in subset)
    den = sum(abs(c.amount) for c in subset)
    if den <= 0:
        return None
    return round((num / den) * 100 * 365, 2)


def _sum_30d_earnings(ledgers_map) -> float:
    now = datetime.now(timezone.utc)
    cutoff_ms = int((now - timedelta(days=30)).timestamp() * 1000)
    s = 0.0
    for ledgers in ledgers_map.values():
        for L in ledgers:
            if L.mts < cutoff_ms or L.amount <= 0:
                continue
            if _ledger_is_funding_income(L.description):
                s += L.amount
    return round(s, 6)


def _df_credit_config():
    return {
        "幣種": st.column_config.TextColumn("幣種", width="small"),
        "金額": st.column_config.NumberColumn("金額", format="%.4f", help="該筆融資部位"),
        "天數": st.column_config.NumberColumn("天數", format="%d", width="small"),
        "年化率 %": st.column_config.NumberColumn("年化率 %", format="%.2f", width="small"),
        "期限": st.column_config.TextColumn("期限", width="medium"),
        "ID": st.column_config.NumberColumn("ID", format="%d", width="small"),
    }


def _df_offer_config():
    return {
        "幣種": st.column_config.TextColumn("幣種", width="small"),
        "金額": st.column_config.NumberColumn("金額", format="%.4f"),
        "天數": st.column_config.NumberColumn("天數", format="%d", width="small"),
        "年化率 %": st.column_config.NumberColumn("年化率 %", format="%.2f", width="small"),
        "狀態": st.column_config.TextColumn("狀態", width="medium"),
        "ID": st.column_config.NumberColumn("ID", format="%d", width="small"),
    }


def _df_earnings_config():
    return {
        "幣種": st.column_config.TextColumn("幣種", width="small"),
        "收益": st.column_config.NumberColumn("收益", format="%.6f"),
        "日期": st.column_config.TextColumn("日期", width="small"),
        "時間": st.column_config.TextColumn("時間", width="small"),
        "說明": st.column_config.TextColumn("說明", width="large"),
    }


def _df_monthly_earnings_config():
    return {
        "月份": st.column_config.TextColumn("月份", width="small"),
        "USD": st.column_config.NumberColumn("USD", format="%.6f"),
        "USDt": st.column_config.NumberColumn("USDt", format="%.6f"),
        "合計": st.column_config.NumberColumn("合計", format="%.6f"),
    }


def _render_fuly_metric_row(
    labels: list[str],
    values: list[str],
    captions: Optional[list[str]] = None,
) -> None:
    caps = captions if captions is not None else ["", "", "", ""]
    while len(caps) < 4:
        caps.append("")
    styles = (
        {
            "bg": "linear-gradient(145deg, #3fa862 0%, #2f7a47 100%)",
            "border": "rgba(92, 219, 130, 0.42)",
            "lbl": "rgba(255,255,255,0.92)",
            "val": "#ffffff",
            "cap": "rgba(255,255,255,0.85)",
        },
        {
            "bg": "linear-gradient(145deg, #5cdb82 0%, #45a865 100%)",
            "border": "rgba(92, 219, 130, 0.5)",
            "lbl": "rgba(255,255,255,0.92)",
            "val": "#ffffff",
            "cap": "rgba(255,255,255,0.85)",
        },
        {
            "bg": "linear-gradient(145deg, #f05d38 0%, #b03d22 100%)",
            "border": "rgba(240, 93, 56, 0.45)",
            "lbl": "rgba(255,255,255,0.92)",
            "val": "#ffffff",
            "cap": "rgba(255,255,255,0.85)",
        },
        {
            "bg": "linear-gradient(145deg, #f1b12d 0%, #b8860b 100%)",
            "border": "rgba(241, 177, 45, 0.5)",
            "lbl": "rgba(18,18,18,0.88)",
            "val": "#121212",
            "cap": "rgba(18,18,18,0.78)",
        },
    )
    parts: list[str] = []
    for i in range(4):
        s = styles[i]
        cap_html = ""
        if caps[i]:
            cap_html = (
                f'<div style="font-size:0.78rem;margin-top:0.38rem;line-height:1.35;color:{s["cap"]};">'
                f"{caps[i]}</div>"
            )
        parts.append(
            f'<div style="flex:1 1 140px;min-width:118px;background:{s["bg"]};'
            f'border:1px solid {s["border"]};border-radius:14px;padding:1rem 1.05rem;'
            f'min-height:5.65rem;box-sizing:border-box;">'
            f'<div style="font-size:0.72rem;font-weight:600;text-transform:uppercase;'
            f'letter-spacing:0.06em;color:{s["lbl"]};">{labels[i]}</div>'
            f'<div style="font-size:1.32rem;font-weight:800;font-variant-numeric:tabular-nums;'
            f'color:{s["val"]};margin-top:0.4rem;line-height:1.2;">{values[i]}</div>'
            f"{cap_html}</div>"
        )
    st.markdown(
        f'<div style="display:flex;flex-wrap:wrap;gap:12px;width:100%;">{"".join(parts)}</div>',
        unsafe_allow_html=True,
    )


def _render_market_section_bottom() -> None:
    st.markdown(
        """
        <p class="dashboard-title" style="font-size:1.25rem;margin:0 0 0.35rem 0;">市場現況</p>
        <p style="color:#9ca3af;font-size:0.9rem;margin:0 0 0.85rem;">
        公開 ticker · 約 30 秒快取<br/>
        <span style="color:#6b7280;font-size:0.82rem;">
        FRR、Bid、Ask 皆為換算年化%；Bid＝市場端最佳出借、Ask＝借款方最佳</span></p>
        """,
        unsafe_allow_html=True,
    )
    _mt = _public_funding_tickers()
    mc1, mc2 = st.columns(2)
    for col, lbl in ((mc1, "USD"), (mc2, "USDt")):
        with col:
            with st.container(border=True):
                tk = _mt.get(lbl)
                st.markdown(
                    f'<div class="asset-label"><span class="asset-dot"></span>市場 · {lbl}</div>',
                    unsafe_allow_html=True,
                )
                if isinstance(tk, Exception):
                    st.warning(f"無法取得：{tk}")
                elif tk is None:
                    st.info("無資料")
                else:
                    rel = tk.daily_change_relative * 100.0
                    _render_fuly_metric_row(
                        ["FRR 年化", "Bid（出借）年化", "Ask（借款）年化", "24h 漲跌"],
                        [
                            f"{_daily_rate_to_apy_pct(tk.frr):.2f}%",
                            f"{_daily_rate_to_apy_pct(tk.bid):.2f}%",
                            f"{_daily_rate_to_apy_pct(tk.ask):.2f}%",
                            f"{rel:.2f}%",
                        ],
                        captions=[
                            "",
                            f"{tk.bid_period} 天 · 規模 {tk.bid_size:,.2f}",
                            f"{tk.ask_period} 天 · 規模 {tk.ask_size:,.2f}",
                            "相對前一日 ticker",
                        ],
                    )


def main():
    st.set_page_config(
        page_title="Bitfinex 融資儀表板",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    _inject_css()
    _require_dashboard_auth()

    h1, h2, h3 = st.columns([3, 1, 1], vertical_alignment="center")
    with h1:
        st.markdown('<p class="dashboard-title">Bitfinex 融資儀表板</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="dashboard-sub">資料約 45 秒快取一次 · 與交易所同步可能有延遲</p>',
            unsafe_allow_html=True,
        )
    with h2:
        st.markdown("")
        if st.button("重新整理", width="stretch"):
            _snapshot.clear()
            _public_funding_tickers.clear()
            st.rerun()
    with h3:
        st.markdown("")
        if _dashboard_password() is not None and st.button("登出", width="stretch"):
            st.session_state.pop(_DASH_AUTH_KEY, None)
            st.rerun()

    try:
        data = _snapshot()
    except Exception as e:
        st.error(f"無法連線或讀取帳戶：{e}")
        data = None

    if data is None:
        st.warning(
            "請在 `.env` 設定 `BF_API_KEY` 與 `BF_API_SECRET` 以查看帳戶與借貸明細；"
            "頁面最下方仍可查看公開市場現況。"
        )

    if data is not None:
        wallets = data["wallets"]
        credits = data["credits"]
        offers = data["offers"]
        ledgers_map = data["ledgers"]

        fw = _funding_wallets(wallets)
        by_cur = {w.currency.upper(): w for w in fw}

        for cur in ("USD", "UST"):
            w = by_cur.get(cur)
            if not w:
                continue
            apy = _weighted_apy(credits, cur)
            earn30 = None
            if cur in ledgers_map:
                emap = {cur: ledgers_map[cur]}
                earn30 = _sum_30d_earnings(emap)
            label = "USD" if cur == "USD" else "USDt"
            with st.container(border=True):
                st.markdown(
                    f'<div class="asset-label"><span class="asset-dot"></span>{label}</div>',
                    unsafe_allow_html=True,
                )
                _render_fuly_metric_row(
                    ["總餘額", "可用", "近 30 天收益（估算）", "加權年化 %"],
                    [
                        f"{w.balance:,.4f}",
                        f"{w.available_balance:,.4f}",
                        f"{earn30 if earn30 is not None else 0:,.4f}",
                        f"{apy:.2f}%" if apy is not None else "—",
                    ],
                )

        tab_a, tab_b, tab_c, tab_d = st.tabs(["已借出", "每月收益", "每日收益", "掛單中"])

        with tab_a:
            cdf = _build_credits_df(credits)
            if cdf.empty:
                st.info("目前沒有進行中的融資 credit。")
            else:
                st.dataframe(
                    cdf,
                    width="stretch",
                    hide_index=True,
                    column_config=_df_credit_config(),
                )

        with tab_b:
            c_sl_m, _ = st.columns([1, 3])
            with c_sl_m:
                days_m = st.slider(
                    "統計區間（天數）",
                    7,
                    365,
                    90,
                    key="monthly_earn_days",
                    help="僅統計此天數內分錄，再依日曆月加總",
                )
            st.caption(
                "依日曆月加總；僅含已辨識的融資利息分錄。單次 API 最多約 500 筆 ledger，久遠月份可能不完整。"
            )
            mdf = _build_monthly_earnings_summary(ledgers_map, days_back=days_m)
            if mdf.empty:
                st.info("此期間內無可匯總的每月收益。")
            else:
                st.dataframe(
                    mdf,
                    width="stretch",
                    hide_index=True,
                    column_config=_df_monthly_earnings_config(),
                )

        with tab_c:
            c_sl, _ = st.columns([1, 3])
            with c_sl:
                days = st.slider("天數", 7, 365, 90, help="篩選最近幾天內的收益分錄")
            edf = _build_earnings_df(ledgers_map, days_back=days)
            if edf.empty:
                st.info("此期間內沒有符合條件的收益分錄（或 API 未回傳 ledger）。")
            else:
                st.dataframe(
                    edf,
                    width="stretch",
                    hide_index=True,
                    column_config=_df_earnings_config(),
                )

        with tab_d:
            odf = _build_offers_df(offers)
            if odf.empty:
                st.info("目前沒有融資掛單。")
            else:
                st.dataframe(
                    odf,
                    width="stretch",
                    hide_index=True,
                    column_config=_df_offer_config(),
                )

    st.divider()
    _render_market_section_bottom()


if __name__ == "__main__":
    main()
