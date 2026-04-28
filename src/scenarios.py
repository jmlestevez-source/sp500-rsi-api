# src/scenarios.py
"""
Escenarios probabilísticos por ticker.
Si el LLM falla o hay rate limit, usa cálculo
cuantitativo directo con los datos reales.
"""

from src.llm import call_llm_json


def _quantitative_scenario(
    ticker:     str,
    stock_data: dict,
) -> dict:
    """
    Genera escenario sin LLM usando solo datos reales.
    Se usa como fallback cuando el LLM no responde.
    """
    price   = stock_data.get("price") or 0
    fpe     = stock_data.get("forward_pe")    or 20
    growth  = stock_data.get("revenue_growth") or 0
    gm      = stock_data.get("gross_margins")  or 0
    beta    = stock_data.get("beta")           or 1.0
    h52     = stock_data.get("52w_high")       or price * 1.3
    l52     = stock_data.get("52w_low")        or price * 0.7

    # Probabilidades basadas en posición en rango 52s
    if price > 0 and h52 > l52:
        pos_52 = (price - l52) / (h52 - l52)
    else:
        pos_52 = 0.5

    # Momentum implícito: está más cerca del high o del low
    if pos_52 > 0.7:
        prob_bull = 0.35
        prob_base = 0.45
        prob_bear = 0.20
    elif pos_52 < 0.3:
        prob_bull = 0.25
        prob_base = 0.40
        prob_bear = 0.35
    else:
        prob_bull = 0.30
        prob_base = 0.45
        prob_bear = 0.25

    # Upside basado en crecimiento
    if growth > 0.20:
        bull_mult_12m = 1.40
        base_mult_12m = 1.15
    elif growth > 0.10:
        bull_mult_12m = 1.30
        base_mult_12m = 1.10
    elif growth > 0:
        bull_mult_12m = 1.20
        base_mult_12m = 1.05
    else:
        bull_mult_12m = 1.10
        base_mult_12m = 0.98

    # Downside basado en beta y PE
    if beta and beta > 1.5:
        bear_mult_12m = 0.68
    elif beta and beta > 1.0:
        bear_mult_12m = 0.73
    else:
        bear_mult_12m = 0.78

    # Targets 12m
    bull_12m = round(price * bull_mult_12m, 2)
    base_12m = round(price * base_mult_12m, 2)
    bear_12m = round(price * bear_mult_12m, 2)

    # Escalar proporcionalmente para otros horizontes
    def scale(mult_12m: float, months: int) -> float:
        # Retorno proporcional al tiempo (simplificado)
        fraction = months / 12
        adj = 1 + (mult_12m - 1) * fraction
        return round(price * adj, 2)

    targets_1m  = {
        "bull": scale(bull_mult_12m,  1),
        "base": scale(base_mult_12m,  1),
        "bear": scale(bear_mult_12m,  1),
    }
    targets_3m  = {
        "bull": scale(bull_mult_12m,  3),
        "base": scale(base_mult_12m,  3),
        "bear": scale(bear_mult_12m,  3),
    }
    targets_6m  = {
        "bull": scale(bull_mult_12m,  6),
        "base": scale(base_mult_12m,  6),
        "bear": scale(bear_mult_12m,  6),
    }
    targets_12m = {
        "bull": bull_12m,
        "base": base_12m,
        "bear": bear_12m,
    }

    def ev(t: dict) -> float:
        return (
            prob_bull * t["bull"]
            + prob_base * t["base"]
            + prob_bear * t["bear"]
        )

    ev_12m = ev(targets_12m)
    bd     = (bear_12m - price) / price if price else -0.25
    wu     = (ev_12m  - price) / price if price else 0
    ratio  = abs(wu / bd) if bd != 0 else 0

    sector  = stock_data.get("sector",   "")
    growth_p = f"{growth*100:.0f}%" if growth else "N/D"

    return {
        "ticker":                 ticker,
        "current_price":          price,
        "prob_bull":              prob_bull,
        "prob_base":              prob_base,
        "prob_bear":              prob_bear,
        "targets_1m":             targets_1m,
        "targets_3m":             targets_3m,
        "targets_6m":             targets_6m,
        "targets_12m":            targets_12m,
        "ev_1m":                  ev(targets_1m),
        "ev_3m":                  ev(targets_3m),
        "ev_6m":                  ev(targets_6m),
        "ev_12m":                 ev_12m,
        "bear_case_downside_12m": bd,
        "upside_downside_ratio":  ratio,
        "bull_thesis": (
            f"Crecimiento de ingresos {growth_p} "
            f"con márgenes brutos {gm*100:.0f}% "
            f"impulsando expansión de múltiplos."
        ),
        "base_thesis": (
            f"Ejecución en línea con consenso. "
            f"PE forward {fpe:.1f}x justificado "
            f"por crecimiento actual."
        ),
        "bear_thesis": (
            f"Compresión de múltiplos si el "
            f"crecimiento desacelera. Beta {beta:.1f} "
            f"amplifica correcciones de mercado."
        ),
        "kill_condition": (
            f"Caída de ingresos por debajo de "
            f"${price * 0.85:.2f} con revisión "
            f"negativa de guidance."
        ),
        "key_catalyst": (
            f"Próximo earnings report. "
            f"Sector: {sector}."
        ),
        "_source": "quantitative_fallback",
    }


def build_scenario(
    ticker:     str,
    stock_data: dict,
    macro_context: str,
) -> dict:
    price   = stock_data.get("price") or 0
    fpe     = stock_data.get("forward_pe")
    growth  = stock_data.get("revenue_growth")
    margins = stock_data.get("gross_margins")
    roe     = stock_data.get("roe")
    de      = stock_data.get("debt_to_equity")
    sector  = stock_data.get("sector", "")
    low_52  = stock_data.get("52w_low")
    high_52 = stock_data.get("52w_high")
    macro_s = macro_context[:120]

    if not price:
        return _quantitative_scenario(ticker, stock_data)

    bear_floor = round(price * 0.65, 2)
    bear_min   = round(price * 0.75, 2)
    bull_ceil  = round(price * 1.60, 2)

    prompt = (
        f"Build 3 scenarios for {ticker} "
        f"at ${price:.2f}.\n"
        f"Data: fwdPE={fpe}, growth={growth}, "
        f"margins={margins}, ROE={roe}, D/E={de}, "
        f"sector={sector}, "
        f"52w=[{low_52},{high_52}]\n"
        f"Macro: {macro_s}\n\n"
        "RULES:\n"
        "1. Probabilities MUST sum to 1.0\n"
        f"2. Bear 12m MUST be between "
        f"${bear_floor} and ${bear_min}\n"
        f"3. Bull 12m MUST be below ${bull_ceil}\n"
        "4. All text in Spanish.\n"
        "5. kill_condition: specific metric + "
        "numeric threshold + event.\n\n"
        "Return ONLY JSON:\n"
        '{"prob_bull":<f>,"prob_base":<f>,'
        '"prob_bear":<f>,'
        '"targets_1m":{"bull":<p>,"base":<p>,'
        '"bear":<p>},'
        '"targets_3m":{"bull":<p>,"base":<p>,'
        '"bear":<p>},'
        '"targets_6m":{"bull":<p>,"base":<p>,'
        '"bear":<p>},'
        '"targets_12m":{"bull":<p>,"base":<p>,'
        '"bear":<p>},'
        '"bull_thesis":"<es>",'
        '"base_thesis":"<es>",'
        '"bear_thesis":"<es>",'
        '"kill_condition":"<specific>",'
        '"key_catalyst":"<event>"}'
    )

    try:
        r = call_llm_json(
            prompt, task="scenario", max_tokens=500
        )

        # Normalizar probabilidades
        total_p = (
            r.get("prob_bull", 0.30)
            + r.get("prob_base", 0.45)
            + r.get("prob_bear", 0.25)
        )
        if abs(total_p - 1.0) > 0.01 and total_p > 0:
            r["prob_bull"] /= total_p
            r["prob_base"] /= total_p
            r["prob_bear"] /= total_p

        # Corregir bear case irreal
        bear_12m = (
            r.get("targets_12m", {}).get("bear", price * 0.75)
        )
        if price > 0 and bear_12m > price * 0.90:
            for h in ["targets_1m", "targets_3m",
                      "targets_6m", "targets_12m"]:
                if h in r and "bear" in r[h]:
                    r[h]["bear"] = round(
                        r[h]["bear"] * 0.75, 2
                    )

        def ev(t: dict) -> float:
            return (
                r["prob_bull"] * t.get("bull", price)
                + r["prob_base"] * t.get("base", price)
                + r["prob_bear"] * t.get(
                    "bear", price * 0.75
                )
            )

        ev_12m   = ev(r.get("targets_12m", {}))
        bear_12m = r.get("targets_12m", {}).get(
            "bear", price * 0.75
        )
        bd    = (bear_12m - price) / price if price else -0.25
        wu    = (ev_12m   - price) / price if price else 0
        ratio = abs(wu / bd) if bd != 0 else 0

        return {
            "ticker":                 ticker,
            "current_price":          price,
            "prob_bull":              r.get("prob_bull", 0.30),
            "prob_base":              r.get("prob_base", 0.45),
            "prob_bear":              r.get("prob_bear", 0.25),
            "targets_1m":             r.get("targets_1m",  {}),
            "targets_3m":             r.get("targets_3m",  {}),
            "targets_6m":             r.get("targets_6m",  {}),
            "targets_12m":            r.get("targets_12m", {}),
            "ev_1m":          ev(r.get("targets_1m",  {})),
            "ev_3m":          ev(r.get("targets_3m",  {})),
            "ev_6m":          ev(r.get("targets_6m",  {})),
            "ev_12m":                 ev_12m,
            "bear_case_downside_12m": bd,
            "upside_downside_ratio":  ratio,
            "bull_thesis":  r.get("bull_thesis",  "N/D"),
            "base_thesis":  r.get("base_thesis",  "N/D"),
            "bear_thesis":  r.get("bear_thesis",  "N/D"),
            "kill_condition": r.get("kill_condition", "N/D"),
            "key_catalyst":   r.get("key_catalyst",   "N/D"),
            "_source": "llm",
        }

    except Exception as e:
        print(
            f"    LLM falló para {ticker}: {e}. "
            f"Usando fallback cuantitativo."
        )
        return _quantitative_scenario(ticker, stock_data)
