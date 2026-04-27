# src/scenarios.py
"""
Construcción de escenarios probabilísticos por ticker.
Sin cambios lógicos respecto al original,
con mejoras en validación.
"""

from src.llm import call_llm_json


def build_scenario(
    ticker: str,
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
    macro_s = macro_context[:150]

    bear_floor = round(price * 0.65, 2)
    bull_ceil  = round(price * 1.60, 2)
    bear_min   = round(price * 0.75, 2)

    prompt = (
        f"Build 3 scenarios for {ticker} "
        f"at current price ${price:.2f}.\n\n"
        f"Data: fwdPE={fpe}, rev_growth={growth}, "
        f"gross_margins={margins}, ROE={roe}, "
        f"D/E={de}, sector={sector}, "
        f"52w_range=[{low_52}, {high_52}]\n\n"
        f"Macro: {macro_s}\n\n"
        "RULES:\n"
        "1. Probabilities MUST sum to 1.0\n"
        f"2. Bear target MUST be between "
        f"${bear_floor} and ${bear_min} "
        f"(-25% to -35% from current)\n"
        f"3. Bull target MUST be below ${bull_ceil}\n"
        "4. kill_condition: SPECIFIC metric + "
        "numeric threshold + specific event. "
        "Example: 'NRR below 115% in Q2 2025 "
        "combined with annual revenue guidance cut'. "
        "NOT: 'guidance miss'.\n"
        "5. All thesis text in Spanish.\n\n"
        "Return ONLY this JSON:\n"
        "{\n"
        '"prob_bull": <float>,\n'
        '"prob_base": <float>,\n'
        '"prob_bear": <float>,\n'
        '"targets_1m": {"bull":<p>,"base":<p>,"bear":<p>},\n'
        '"targets_3m": {"bull":<p>,"base":<p>,"bear":<p>},\n'
        '"targets_6m": {"bull":<p>,"base":<p>,"bear":<p>},\n'
        '"targets_12m": {"bull":<p>,"base":<p>,"bear":<p>},\n'
        '"bull_thesis": "<2 specific sentences in Spanish>",\n'
        '"base_thesis": "<2 specific sentences in Spanish>",\n'
        '"bear_thesis": "<2 specific sentences in Spanish>",\n'
        '"kill_condition": "<metric+threshold+event>",\n'
        '"key_catalyst": "<real event with approx date>"\n'
        "}"
    )

    try:
        r = call_llm_json(
            prompt, task="scenario", max_tokens=700
        )

        # Normalizar probabilidades
        total_p = (
            r.get("prob_bull", 0.25)
            + r.get("prob_base", 0.50)
            + r.get("prob_bear", 0.25)
        )
        if abs(total_p - 1.0) > 0.01 and total_p > 0:
            r["prob_bull"] /= total_p
            r["prob_base"] /= total_p
            r["prob_bear"] /= total_p

        # Validar y corregir bear case irreal
        bear_12m = r.get("targets_12m", {}).get("bear", price * 0.75)
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
                + r["prob_bear"] * t.get("bear", price * 0.75)
            )

        ev_12m   = ev(r.get("targets_12m", {}))
        bear_12m = r.get("targets_12m", {}).get(
            "bear", price * 0.75
        )
        bd = (bear_12m - price) / price if price else -0.25
        wu = (ev_12m - price) / price if price else 0
        ratio = abs(wu / bd) if bd != 0 else 0

        return {
            "ticker":                 ticker,
            "current_price":          price,
            "prob_bull":              r.get("prob_bull", 0.25),
            "prob_base":              r.get("prob_base", 0.50),
            "prob_bear":              r.get("prob_bear", 0.25),
            "targets_1m":             r.get("targets_1m", {}),
            "targets_3m":             r.get("targets_3m", {}),
            "targets_6m":             r.get("targets_6m", {}),
            "targets_12m":            r.get("targets_12m", {}),
            "ev_1m":                  ev(r.get("targets_1m", {})),
            "ev_3m":                  ev(r.get("targets_3m", {})),
            "ev_6m":                  ev(r.get("targets_6m", {})),
            "ev_12m":                 ev_12m,
            "bear_case_downside_12m": bd,
            "upside_downside_ratio":  ratio,
            "bull_thesis":  r.get("bull_thesis",  "N/D"),
            "base_thesis":  r.get("base_thesis",  "N/D"),
            "bear_thesis":  r.get("bear_thesis",  "N/D"),
            "kill_condition": r.get("kill_condition", "N/D"),
            "key_catalyst":   r.get("key_catalyst",   "N/D"),
        }

    except Exception as e:
        print(f"    Error scenario {ticker}: {e}")
        return {
            "ticker":                 ticker,
            "current_price":          price,
            "ev_12m":                 price,
            "bear_case_downside_12m": -0.30,
            "upside_downside_ratio":  0,
            "kill_condition":         "Error en generación",
            "key_catalyst":           "N/D",
            "targets_12m": {
                "bull": price * 1.2,
                "base": price,
                "bear": price * 0.7,
            },
            "prob_bull":   0.25,
            "prob_base":   0.50,
            "prob_bear":   0.25,
            "bull_thesis": "N/D",
            "base_thesis": "N/D",
            "bear_thesis": "N/D",
        }
