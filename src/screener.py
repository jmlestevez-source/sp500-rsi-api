# src/screener.py
"""
Pre-filtro cuantitativo ANTES del LLM.
Reduce 1000 → ~100 candidatos usando solo datos reales.
"""

import numpy as np
import pandas as pd


def compute_quant_scores(
    fundamentals: dict[str, dict],
    price_history: pd.DataFrame,
) -> list[dict]:
    """
    Calcula score cuantitativo puro para cada ticker.
    No usa LLM, solo métricas reales.
    """
    scores = []

    for ticker, info in fundamentals.items():
        if not info.get("_data_ok"):
            continue

        score = 0.0
        flags = []

        # ── Factor momentum (datos históricos reales) ──
        if ticker in price_history.columns:
            series = price_history[ticker].dropna()
            n      = len(series)

            if n >= 20:
                m1m = (
                    series.iloc[-1] / series.iloc[-21] - 1
                ) if n >= 21 else 0
                m3m = (
                    series.iloc[-1] / series.iloc[-63] - 1
                ) if n >= 63 else 0
                m6m = (
                    series.iloc[-1] / series.iloc[-126] - 1
                ) if n >= 126 else 0
                m12m = (
                    series.iloc[-1] / series.iloc[-252] - 1
                ) if n >= 252 else 0

                # 12m excluyendo último mes (estándar)
                m12m_ex = (
                    series.iloc[-21] / series.iloc[-252] - 1
                ) if n >= 252 else m12m

                vol = series.pct_change().std() * np.sqrt(252)

                # Scoring momentum
                if m1m > 0.02:   score += 10; flags.append("M1m+")
                if m3m > 0.05:   score += 15; flags.append("M3m+")
                if m6m > 0.10:   score += 15; flags.append("M6m+")
                if m12m_ex > 0.15: score += 20; flags.append("M12m+")

                # Penalizar alta volatilidad
                if vol > 0.60:  score -= 15; flags.append("HiVol")
                elif vol < 0.25: score += 5;  flags.append("LoVol")

                # 52W position (no comprar lo que ha caído mucho)
                h52 = info.get("52w_high")
                l52 = info.get("52w_low")
                p   = info.get("price")
                if h52 and l52 and p and h52 > l52:
                    position = (p - l52) / (h52 - l52)
                    if position > 0.70: score += 10
                    elif position < 0.30: score -= 5

        # ── Factor calidad fundamental ──
        fpe    = info.get("forward_pe")
        growth = info.get("revenue_growth") or 0
        egrw   = info.get("earnings_growth") or 0
        gm     = info.get("gross_margins") or 0
        om     = info.get("operating_margins") or 0
        roe    = info.get("roe") or 0
        de     = info.get("debt_to_equity")
        cr     = info.get("current_ratio") or 0
        evebitda = info.get("ev_to_ebitda")

        # PE razonable
        if fpe:
            if 0 < fpe < 20:   score += 20; flags.append("LowPE")
            elif 20 <= fpe < 35: score += 10; flags.append("OkPE")
            elif fpe > 60:     score -= 15; flags.append("HighPE")

        # Crecimiento
        if growth > 0.20:    score += 20; flags.append("HiGrow")
        elif growth > 0.10:  score += 12
        elif growth > 0.05:  score += 6
        elif growth < 0:     score -= 10; flags.append("NegGrow")

        # Earnings growth
        if egrw > 0.15:  score += 10
        elif egrw < 0:   score -= 8

        # Calidad márgenes
        if gm > 0.50:    score += 15; flags.append("HiGM")
        elif gm > 0.30:  score += 8
        elif gm < 0.15:  score -= 5

        if om > 0.20:    score += 10
        elif om < 0:     score -= 15; flags.append("NegOM")

        # ROE
        if roe > 0.20:   score += 10
        elif roe > 0.10: score += 5
        elif roe < 0:    score -= 10

        # Deuda
        if de is not None:
            if de < 0.5:    score += 10; flags.append("LowDebt")
            elif de > 2.0:  score -= 10; flags.append("HiDebt")

        # Liquidez
        if cr > 2.0:     score += 5
        elif cr < 1.0:   score -= 5

        # EV/EBITDA
        if evebitda:
            if 0 < evebitda < 15:  score += 10
            elif evebitda > 40:    score -= 10

        # ── Market cap mínimo: excluir micro-caps ──
        mcap = info.get("market_cap") or 0
        if mcap < 500_000_000:  # < 500M
            score -= 30
            flags.append("MicroCap")
        elif mcap > 10_000_000_000:
            score += 5
            flags.append("LargeCap")

        scores.append({
            "ticker":       ticker,
            "quant_score":  round(score, 2),
            "flags":        flags,
            "forward_pe":   fpe,
            "revenue_growth": growth,
            "gross_margins": gm,
            "sector":       info.get("sector", ""),
            "market_cap":   mcap,
            "price":        info.get("price"),
        })

    scores.sort(key=lambda x: x["quant_score"], reverse=True)
    return scores


def apply_sector_diversification(
    scores: list[dict],
    top_n: int = 100,
    max_per_sector: int = 20,
) -> list[dict]:
    """
    Aplica diversificación sectorial al pre-filtro.
    Evita que un solo sector domine los candidatos.
    """
    sector_counts: dict[str, int] = {}
    selected = []

    for s in scores:
        sector = s.get("sector") or "Unknown"
        count  = sector_counts.get(sector, 0)

        if count < max_per_sector:
            selected.append(s)
            sector_counts[sector] = count + 1

        if len(selected) >= top_n:
            break

    return selected


def prescreening(
    fundamentals: dict[str, dict],
    price_history: pd.DataFrame,
    top_n: int = 100,
) -> tuple[list[str], list[str]]:
    """
    Pipeline completo de pre-filtro.
    Returns:
        candidates: lista de tickers seleccionados
        no_data:    lista de tickers sin datos
    """
    no_data = [
        t for t, d in fundamentals.items()
        if not d.get("_data_ok")
    ]

    if no_data:
        print(
            f"    ⚠ Sin datos fundamentales: "
            f"{len(no_data)} tickers"
        )

    scores    = compute_quant_scores(fundamentals, price_history)
    selected  = apply_sector_diversification(scores, top_n)
    candidates = [s["ticker"] for s in selected]

    print(
        f"    Pre-filtro: {len(fundamentals)} → "
        f"{len(candidates)} candidatos | "
        f"{len(no_data)} sin datos"
    )
    return candidates, no_data
