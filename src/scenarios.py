# src/scenarios.py
"""
Escenarios probabilísticos por ticker.
Fallback cuantitativo mejorado con kill conditions
basadas en datos reales del ticker.
"""

from src.llm import call_llm_json


def _smart_kill_condition(
    ticker:     str,
    stock_data: dict,
) -> str:
    """
    Genera una kill condition basada en datos reales.
    Usa métricas específicas del ticker, no genéricas.
    """
    price   = stock_data.get("price")       or 0
    fpe     = stock_data.get("forward_pe")
    growth  = stock_data.get("revenue_growth")
    gm      = stock_data.get("gross_margins")
    om      = stock_data.get("operating_margins")
    roe     = stock_data.get("roe")
    de      = stock_data.get("debt_to_equity")
    beta    = stock_data.get("beta")        or 1.0
    l52     = stock_data.get("52w_low")     or price * 0.7
    sector  = stock_data.get("sector", "")

    conditions = []

    # 1. Stop loss técnico: 20% por debajo de entrada
    if price > 0:
        stop = round(price * 0.80, 2)
        conditions.append(
            f"Precio cierra por debajo de ${stop} "
            f"(stop loss -20% desde entrada ${price:.2f})"
        )

    # 2. Deterioro de márgenes operativos
    if om and om > 0:
        # Alerta si márgenes caen más de 5pp
        threshold = round((om - 0.05) * 100, 1)
        if threshold > 0:
            conditions.append(
                f"Margen operativo cae por debajo de "
                f"{threshold}% (actual: "
                f"{om*100:.1f}%) en dos trimestres "
                f"consecutivos"
            )

    # 3. Caída de crecimiento de ingresos
    if growth and growth > 0.05:
        # Si el crecimiento cae a la mitad
        half = round(growth * 50, 1)
        conditions.append(
            f"Crecimiento de ingresos interanual "
            f"cae por debajo de {half}% "
            f"(actual: {growth*100:.1f}%)"
        )
    elif growth and growth <= 0:
        conditions.append(
            "Tercer trimestre consecutivo con "
            "caída de ingresos interanual"
        )

    # 4. Rotura del mínimo de 52 semanas
    if l52 and price > 0:
        conditions.append(
            f"Precio rompe mínimo de 52 semanas "
            f"(${l52:.2f}) con volumen superior "
            f"al promedio"
        )

    # 5. Apalancamiento (solo si es relevante)
    if de and de > 0.5:
        # Alerta si sube un 50% desde nivel actual
        de_limit = round(de * 1.5, 1)
        conditions.append(
            f"Ratio deuda/equity supera {de_limit}x "
            f"(actual: {de:.1f}x) sin justificación "
            f"por adquisición estratégica"
        )

    # Combinar las 2 más relevantes
    if len(conditions) >= 2:
        return f"{conditions[0]}. O: {conditions[1]}."
    elif conditions:
        return conditions[0]
    else:
        return (
            f"Precio cierra por debajo de "
            f"${price * 0.80:.2f} durante 5 "
            f"sesiones consecutivas."
        )


def _smart_thesis(
    ticker:     str,
    stock_data: dict,
    scenario_type: str,
) -> str:
    """
    Genera tesis basada en datos reales del ticker.
    """
    price   = stock_data.get("price")       or 0
    fpe     = stock_data.get("forward_pe")
    growth  = stock_data.get("revenue_growth")
    gm      = stock_data.get("gross_margins")
    om      = stock_data.get("operating_margins")
    roe     = stock_data.get("roe")
    de      = stock_data.get("debt_to_equity")
    sector  = stock_data.get("sector",  "")
    industry = stock_data.get("industry", "")

    # Formatear valores
    fpe_s    = f"PE fwd {fpe:.1f}x"       if fpe    else "PE N/D"
    growth_s = f"crecimiento {growth*100:.1f}%" if growth else "crecimiento N/D"
    gm_s     = f"margen bruto {gm*100:.1f}%"   if gm     else ""
    om_s     = f"margen operativo {om*100:.1f}%" if om    else ""
    roe_s    = f"ROE {roe*100:.1f}%"            if roe    else ""
    de_s     = f"D/E {de:.1f}x"                 if de     else ""

    metrics = ", ".join(
        m for m in [fpe_s, growth_s, gm_s, roe_s]
        if m
    )

    if scenario_type == "bull":
        return (
            f"Aceleración del {growth_s} "
            f"con expansión de márgenes en "
            f"{sector}. {fpe_s} tiene espacio "
            f"para re-ratearse al alza si la "
            f"ejecución confirma la tendencia."
        )
    elif scenario_type == "base":
        return (
            f"Ejecución en línea con consenso: "
            f"{metrics}. Negocio estable en "
            f"{industry or sector} sin catalizador "
            f"fuerte ni riesgo inminente."
        )
    else:  # bear
        risks = []
        if de and de > 1.0:
            risks.append(
                f"apalancamiento elevado ({de_s})"
            )
        if growth and growth < 0.05:
            risks.append(
                f"crecimiento débil ({growth_s})"
            )
        if fpe and fpe > 30:
            risks.append(
                f"valoración exigente ({fpe_s})"
            )
        if not risks:
            risks.append("desaceleración macro")
            risks.append("compresión de múltiplos")

        risk_str = " y ".join(risks[:2])
        return (
            f"Riesgo de {risk_str} provocando "
            f"revisión a la baja de estimaciones "
            f"y caída del precio."
        )


def _quantitative_scenario(
    ticker:     str,
    stock_data: dict,
) -> dict:
    """
    Genera escenario sin LLM usando datos reales.
    Fallback mejorado con kill conditions inteligentes.
    """
    price   = stock_data.get("price")          or 0
    fpe     = stock_data.get("forward_pe")     or 20
    growth  = stock_data.get("revenue_growth") or 0
    gm      = stock_data.get("gross_margins")  or 0
    beta    = stock_data.get("beta")           or 1.0
    h52     = stock_data.get("52w_high")       or price * 1.3
    l52     = stock_data.get("52w_low")        or price * 0.7

    # Posición en rango 52 semanas
    if price > 0 and h52 > l52:
        pos_52 = (price - l52) / (h52 - l52)
    else:
        pos_52 = 0.5

    # Probabilidades
    if pos_52 > 0.7:
        prob_bull, prob_base, prob_bear = 0.35, 0.45, 0.20
    elif pos_52 < 0.3:
        prob_bull, prob_base, prob_bear = 0.25, 0.40, 0.35
    else:
        prob_bull, prob_base, prob_bear = 0.30, 0.45, 0.25

    # Multiplicadores según crecimiento
    if growth > 0.20:
        bull_m, base_m = 1.40, 1.15
    elif growth > 0.10:
        bull_m, base_m = 1.30, 1.10
    elif growth > 0:
        bull_m, base_m = 1.20, 1.05
    else:
        bull_m, base_m = 1.10, 0.98

    # Downside según beta
    if beta > 1.5:
        bear_m = 0.68
    elif beta > 1.0:
        bear_m = 0.73
    else:
        bear_m = 0.78

    # Targets
    bull_12m = round(price * bull_m, 2)
    base_12m = round(price * base_m, 2)
    bear_12m = round(price * bear_m, 2)

    def scale(mult: float, months: int) -> float:
        fraction = months / 12
        return round(
            price * (1 + (mult - 1) * fraction), 2
        )

    targets = {}
    for label, months in [
        ("1m", 1), ("3m", 3), ("6m", 6), ("12m", 12)
    ]:
        targets[label] = {
            "bull": scale(bull_m, months),
            "base": scale(base_m, months),
            "bear": scale(bear_m, months),
        }

    def ev(t: dict) -> float:
        return (
            prob_bull * t["bull"]
            + prob_base * t["base"]
            + prob_bear * t["bear"]
        )

    ev_12m = ev(targets["12m"])
    bd     = (
        (bear_12m - price) / price
        if price else -0.25
    )
    wu     = (
        (ev_12m - price) / price
        if price else 0
    )
    ratio  = abs(wu / bd) if bd != 0 else 0

    return {
        "ticker":                 ticker,
        "current_price":          price,
        "prob_bull":              prob_bull,
        "prob_base":              prob_base,
        "prob_bear":              prob_bear,
        "targets_1m":             targets["1m"],
        "targets_3m":             targets["3m"],
        "targets_6m":             targets["6m"],
        "targets_12m":            targets["12m"],
        "ev_1m":                  ev(targets["1m"]),
        "ev_3m":                  ev(targets["3m"]),
        "ev_6m":                  ev(targets["6m"]),
        "ev_12m":                 ev_12m,
        "bear_case_downside_12m": bd,
        "upside_downside_ratio":  ratio,
        "bull_thesis": _smart_thesis(
            ticker, stock_data, "bull"
        ),
        "base_thesis": _smart_thesis(
            ticker, stock_data, "base"
        ),
        "bear_thesis": _smart_thesis(
            ticker, stock_data, "bear"
        ),
        "kill_condition": _smart_kill_condition(
            ticker, stock_data
        ),
        "key_catalyst": (
            f"Próximo earnings report. "
            f"Sector: {stock_data.get('sector', 'N/D')}."
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
        return _quantitative_scenario(
            ticker, stock_data
        )

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
        f"2. Bear 12m between "
        f"${bear_floor}-${bear_min}\n"
        f"3. Bull 12m below ${bull_ceil}\n"
        "4. All text in Spanish\n"
        "5. kill_condition MUST be a SPECIFIC "
        "metric with a NUMERIC threshold "
        "that can be objectively verified. "
        "Examples: 'Margen operativo cae por "
        "debajo del 15% en dos trimestres' or "
        "'Precio cierra por debajo de $X durante "
        "5 sesiones'. NOT generic phrases like "
        "'D/E > 100' or 'deterioro en guidance'.\n\n"
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
        '"bull_thesis":"<2 frases en español>",'
        '"base_thesis":"<2 frases en español>",'
        '"bear_thesis":"<2 frases en español>",'
        '"kill_condition":"<métrica + umbral + '
        'condición verificable>",'
        '"key_catalyst":"<evento real>"}'
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
        bear_12m = r.get(
            "targets_12m", {}
        ).get("bear", price * 0.75)
        if price > 0 and bear_12m > price * 0.90:
            for h in [
                "targets_1m", "targets_3m",
                "targets_6m", "targets_12m"
            ]:
                if h in r and "bear" in r[h]:
                    r[h]["bear"] = round(
                        r[h]["bear"] * 0.75, 2
                    )

        # Validar kill condition del LLM
        kc = r.get("kill_condition", "")
        kc_bad = (
            not kc
            or len(kc) < 20
            or kc == "N/D"
            or "D/E >" in kc and any(
                x in kc for x in [
                    "100", "800", "500"
                ]
            )
        )
        if kc_bad:
            # LLM generó basura → usar nuestro cálculo
            r["kill_condition"] = _smart_kill_condition(
                ticker, stock_data
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
        bear_12m = r.get(
            "targets_12m", {}
        ).get("bear", price * 0.75)
        bd    = (
            (bear_12m - price) / price
            if price else -0.25
        )
        wu    = (
            (ev_12m - price) / price
            if price else 0
        )
        ratio = abs(wu / bd) if bd != 0 else 0

        return {
            "ticker":                 ticker,
            "current_price":          price,
            "prob_bull":    r.get("prob_bull", 0.30),
            "prob_base":    r.get("prob_base", 0.45),
            "prob_bear":    r.get("prob_bear", 0.25),
            "targets_1m":   r.get("targets_1m",  {}),
            "targets_3m":   r.get("targets_3m",  {}),
            "targets_6m":   r.get("targets_6m",  {}),
            "targets_12m":  r.get("targets_12m", {}),
            "ev_1m":        ev(r.get("targets_1m",  {})),
            "ev_3m":        ev(r.get("targets_3m",  {})),
            "ev_6m":        ev(r.get("targets_6m",  {})),
            "ev_12m":                 ev_12m,
            "bear_case_downside_12m": bd,
            "upside_downside_ratio":  ratio,
            "bull_thesis":  r.get("bull_thesis",  "N/D"),
            "base_thesis":  r.get("base_thesis",  "N/D"),
            "bear_thesis":  r.get("bear_thesis",  "N/D"),
            "kill_condition": r.get(
                "kill_condition", "N/D"
            ),
            "key_catalyst": r.get(
                "key_catalyst", "N/D"
            ),
            "_source": "llm",
        }

    except Exception as e:
        print(
            f"    LLM falló para {ticker}: {e}. "
            f"Usando fallback cuantitativo."
        )
        return _quantitative_scenario(
            ticker, stock_data
        )
