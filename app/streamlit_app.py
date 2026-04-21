# app/streamlit_app.py
# Deploy gratis en share.streamlit.io

import streamlit as st
import json
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

st.set_page_config(
    page_title="Portfolio Autopilot",
    layout="wide",
    page_icon="📊"
)

# ── Helpers ──────────────────────────────────────────────

@st.cache_data(ttl=300)  # Cache 5 min
def load_positions():
    f = Path("data/positions/current.json")
    return json.load(open(f)) if f.exists() else {}

@st.cache_data(ttl=300)
def load_rebalances():
    files = sorted(Path("data/rebalances").glob("*.json"))
    return [json.load(open(f)) for f in files] if files else []

@st.cache_data(ttl=300)
def load_thesis():
    files = sorted(Path("data/thesis").glob("*.json"))
    return [json.load(open(f)) for f in files[-30:]] if files else []

# ── UI ───────────────────────────────────────────────────

st.title("📊 Portfolio Autopilot")

rebalances = load_rebalances()
last_rebalance = rebalances[-1] if rebalances else None

if last_rebalance:
    st.caption(
        f"Último rebalanceo: {last_rebalance['timestamp'][:10]} | "
        f"Próximo: automático vía GitHub Actions"
    )

tab1, tab2, tab3 = st.tabs(["Portfolio", "Thesis", "Historial"])

# ── TAB 1: Portfolio actual ───────────────────────────────
with tab1:
    positions = load_positions()
    
    if not positions:
        st.info("Sin posiciones todavía. Ejecuta el primer rebalanceo.")
    else:
        # Métricas top
        if last_rebalance:
            m = last_rebalance["metrics"]
            c = last_rebalance["changes"]
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Posiciones", len(positions))
            col2.metric("EV 12M", f"{m['expected_return']:.1%}")
            col3.metric("Risk Score", f"{m['risk_score']:.1%}")
            col4.metric(
                "Último turnover", 
                f"{c['turnover']:.1%}",
                help="One-sided, cap 30%"
            )
        
        st.divider()
        
        # Allocation chart
        col1, col2 = st.columns([1, 1])
        
        with col1:
            fig = go.Figure(go.Pie(
                labels=list(positions.keys()),
                values=[p["weight"] for p in positions.values()],
                hole=0.45,
                textinfo="label+percent",
                textfont_size=12
            ))
            fig.update_layout(
                title="Allocation actual",
                height=380,
                showlegend=False,
                margin=dict(t=40, b=0, l=0, r=0)
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Tabla de posiciones
            rows = []
            for ticker, pos in sorted(
                positions.items(), 
                key=lambda x: -x[1]["weight"]
            ):
                rows.append({
                    "Ticker": ticker,
                    "Weight": f"{pos['weight']:.1%}",
                    "Entry": f"${pos.get('entry_price', 0):.2f}",
                    "EV 12M": f"${pos.get('ev_12m', 0):.2f}" if pos.get('ev_12m') else "-",
                    "Entry Date": pos.get("entry_date", "-")
                })
            
            st.dataframe(
                pd.DataFrame(rows), 
                use_container_width=True,
                hide_index=True
            )
        
        # Kill conditions (importante para el audit trail)
        st.subheader("Kill Conditions activas")
        for ticker, pos in positions.items():
            if pos.get("kill_condition"):
                st.warning(
                    f"**{ticker}** ({pos['weight']:.1%}): "
                    f"{pos['kill_condition']}"
                )
        
        # Último commentary
        if last_rebalance and last_rebalance.get("commentary"):
            st.subheader("Último rebalanceo - Commentary")
            st.markdown(last_rebalance["commentary"])

# ── TAB 2: Thesis ────────────────────────────────────────
with tab2:
    thesis_list = load_thesis()
    
    if not thesis_list:
        st.info("Sin thesis todavía.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            tickers = ["Todos"] + sorted(set(t["ticker"] for t in thesis_list))
            sel_ticker = st.selectbox("Ticker", tickers)
        with col2:
            actions = ["Todas", "OPEN", "ADD", "TRIM", "CLOSE"]
            sel_action = st.selectbox("Acción", actions)
        
        filtered = thesis_list
        if sel_ticker != "Todos":
            filtered = [t for t in filtered if t["ticker"] == sel_ticker]
        if sel_action != "Todas":
            filtered = [t for t in filtered if t["action"] == sel_action]
        
        for thesis in reversed(filtered):
            ev_pct = thesis.get("expected_return_pct", 0)
            ratio = thesis.get("upside_downside_ratio", 0)
            
            with st.expander(
                f"**{thesis['ticker']}** | {thesis['action']} | "
                f"{thesis['timestamp'][:10]} | "
                f"EV: {ev_pct:+.1f}% | U/D: {ratio:.2f}x"
            ):
                col1, col2, col3 = st.columns(3)
                col1.metric("Weight", f"{thesis['weight']:.1%}")
                col1.metric("EV 12M", f"{ev_pct:+.1f}%")
                col2.metric("Bear Down", f"{thesis.get('bear_downside_pct', 0):.1f}%")
                col2.metric("U/D Ratio", f"{ratio:.2f}x")
                col3.metric("Precio", f"${thesis.get('price_at_thesis', 0):.2f}")
                
                st.error(f"**Kill:** {thesis.get('kill_condition', 'N/A')}")
                st.markdown(thesis.get("thesis_text", "N/A"))

# ── TAB 3: Historial ─────────────────────────────────────
with tab3:
    if not rebalances:
        st.info("Sin historial todavía.")
    else:
        # Turnover history chart
        df = pd.DataFrame([{
            "Fecha": r["timestamp"][:10],
            "Turnover": r["changes"]["turnover"],
            "EV_12M": r["metrics"]["expected_return"],
            "Posiciones": len(r["portfolio"])
        } for r in rebalances])
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["Fecha"],
            y=df["Turnover"],
            name="Turnover usado",
            marker_color="steelblue"
        ))
        fig.add_hline(
            y=0.30, 
            line_dash="dash", 
            line_color="red",
            annotation_text="Cap 30%"
        )
        fig.update_layout(
            title="Turnover por rebalanceo",
            yaxis_tickformat=".0%",
            height=300
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # EV evolution
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=df["Fecha"],
            y=df["EV_12M"],
            mode="lines+markers",
            name="EV 12M portfolio",
            line=dict(color="green", width=2)
        ))
        fig2.update_layout(
            title="Expected Value 12M (evolución)",
            yaxis_tickformat=".1%",
            height=300
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        # Historial de commentaries
        st.subheader("Archivo de rebalanceos")
        for r in reversed(rebalances):
            with st.expander(
                f"Rebalanceo {r['timestamp'][:10]} | "
                f"+{len(r['changes']['added'])} "
                f"-{len(r['changes']['dropped'])} nombres"
            ):
                st.markdown(r.get("commentary", "Sin commentary"))
