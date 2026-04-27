# src/email_report.py
"""
Generación del email HTML completo con:
- Performance real vs SPY
- Operaciones ejecutadas con P&L
- Tickers sin datos (advertencia)
- Portfolio actual con rendimientos
- Kill conditions
- Tesis
"""

import json
from pathlib import Path
from datetime import datetime


def _color(val: float, invert: bool = False) -> str:
    """Verde positivo, rojo negativo."""
    if invert:
        val = -val
    return "#28a745" if val >= 0 else "#dc3545"


def _pct_badge(val: float, suffix: str = "%") -> str:
    color = _color(val)
    sign  = "+" if val >= 0 else ""
    return (
        f"<span style='color:{color};"
        f"font-weight:bold'>"
        f"{sign}{val:.2f}{suffix}</span>"
    )


def generate_email_report(
    result:          dict,
    all_thesis:      list,
    summary:         str,
    positions:       dict,
    perf_metrics:    dict,
    no_data_tickers: list[str],
    new_trades:      list[dict],
) -> None:
    today   = datetime.now().strftime("%Y-%m-%d")
    added   = result["added_names"]
    dropped = result["dropped_names"]

    parts = []
    if added:   parts.append(f"+{','.join(added)}")
    if dropped: parts.append(f"-{','.join(dropped)}")
    changes_str = " ".join(parts) if parts else "sin cambios"

    subject = (
        f"Portfolio {today} | "
        f"EV {result['expected_return']:.1%} | "
        f"{changes_str}"
    )

    port_ret = perf_metrics.get("portfolio_return_pct", 0)
    spy_ret  = perf_metrics.get("spy_return_pct",       0)
    alpha    = perf_metrics.get("alpha_pct",             0)

    # ── Sección 1: KPIs superiores ────────────────────────

    kpi_style = (
        "background:#f8f9fa;padding:15px;"
        "border-radius:8px;flex:1;min-width:110px;"
        "text-align:center"
    )
    num_style = "font-size:22px;font-weight:bold"

    kpis = (
        # Portfolio return
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};"
        f"color:{_color(port_ret)}'>"
        f"{'+'if port_ret>=0 else ''}{port_ret:.2f}%</div>"
        "<div style='color:#666;font-size:13px'>"
        "Ret. Portfolio</div></div>"

        # SPY return
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};"
        f"color:{_color(spy_ret)}'>"
        f"{'+'if spy_ret>=0 else ''}{spy_ret:.2f}%</div>"
        "<div style='color:#666;font-size:13px'>"
        "Ret. SPY (mismo periodo)</div></div>"

        # Alpha
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};"
        f"color:{_color(alpha)}'>"
        f"{'+'if alpha>=0 else ''}{alpha:.2f}%</div>"
        "<div style='color:#666;font-size:13px'>"
        "Alpha vs SPY</div></div>"

        # EV 12M
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};"
        f"color:{_color(result[\"expected_return\"])}'>"
        f"{result['expected_return']:.1%}</div>"
        "<div style='color:#666;font-size:13px'>"
        "VE 12M</div></div>"

        # Turnover
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};color:#fd7e14'>"
        f"{result['turnover_used']:.1%}</div>"
        "<div style='color:#666;font-size:13px'>"
        "Turnover</div></div>"

        # N posiciones
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style}'>"
        f"{len(result['weights'])}</div>"
        "<div style='color:#666;font-size:13px'>"
        "Posiciones</div></div>"
    )

    # ── Sección 2: Advertencia tickers sin datos ──────────

    no_data_section = ""
    if no_data_tickers:
        items = "".join(
            f"<li><code>{t}</code></li>"
            for t in sorted(no_data_tickers)
        )
        no_data_section = (
            "<div style='background:#fff3cd;"
            "border:1px solid #ffc107;"
            "padding:15px;border-radius:8px;"
            "margin:15px 0'>"
            "<h3 style='color:#856404;margin-top:0'>"
            f"⚠ {len(no_data_tickers)} tickers sin datos "
            f"fundamentales</h3>"
            "<p style='color:#856404;margin:5px 0'>"
            "Estos tickers fueron excluidos del análisis "
            "por no disponer de datos en Yahoo Finance. "
            "Verificar que los símbolos son correctos.</p>"
            f"<ul style='columns:4;color:#856404'>"
            f"{items}</ul></div>"
        )

    # ── Sección 3: Operaciones ejecutadas ─────────────────

    trades_rows = ""
    for t in new_trades:
        action    = t.get("action",   "")
        ticker    = t.get("ticker",   "")
        price     = t.get("price_at_trade", 0) or 0
        w_before  = t.get("weight_before",  0) or 0
        w_after   = t.get("weight_after",   0) or 0
        pnl       = t.get("pnl_pct")
        days      = t.get("pnl_days")

        action_colors = {
            "OPEN":  ("#28a745", "ABRIR"),
            "ADD":   ("#17a2b8", "AÑADIR"),
            "TRIM":  ("#fd7e14", "REDUCIR"),
            "CLOSE": ("#dc3545", "CERRAR"),
        }
        color, label = action_colors.get(
            action, ("#666", action)
        )

        pnl_str = ""
        if pnl is not None:
            pnl_str = (
                f"<strong style='color:{_color(pnl)}'>"
                f"{'+'if pnl>=0 else ''}{pnl:.2f}%</strong>"
                f" ({days}d)" if days else
                f"<strong style='color:{_color(pnl)}'>"
                f"{'+'if pnl>=0 else ''}{pnl:.2f}%</strong>"
            )

        trades_rows += (
            f"<tr style='border-bottom:1px solid #eee'>"
            f"<td style='padding:8px'>"
            f"<span style='background:{color};"
            f"color:white;padding:2px 8px;"
            f"border-radius:3px;font-size:12px'>"
            f"{label}</span></td>"
            f"<td style='padding:8px'>"
            f"<strong>{ticker}</strong></td>"
            f"<td style='padding:8px'>${price:.2f}</td>"
            f"<td style='padding:8px'>"
            f"{w_before:.1%} → {w_after:.1%}</td>"
            f"<td style='padding:8px'>{pnl_str}</td>"
            f"</tr>"
        )

    trades_section = ""
    if trades_rows:
        trades_section = (
            "<h2>Operaciones ejecutadas</h2>"
            "<table style='width:100%;"
            "border-collapse:collapse'>"
            "<thead>"
            "<tr style='background:#343a40;color:white'>"
            "<th style='padding:10px;text-align:left'>"
            "Acción</th>"
            "<th style='padding:10px;text-align:left'>"
            "Ticker</th>"
            "<th style='padding:10px;text-align:left'>"
            "Precio</th>"
            "<th style='padding:10px;text-align:left'>"
            "Peso</th>"
            "<th style='padding:10px;text-align:left'>"
            "P&L</th>"
            "</tr></thead>"
            f"<tbody>{trades_rows}</tbody></table>"
        )

    # ── Sección 4: Operaciones cerradas (histórico) ───────

    closed_summary = perf_metrics.get(
        "closed_trades_summary", {}
    )
    closed_section = ""
    if closed_summary:
        win_rate = closed_summary.get("win_rate_pct", 0)
        avg_pnl  = closed_summary.get("avg_pnl_pct",  0)
        best     = closed_summary.get("best_trade",    0)
        worst    = closed_summary.get("worst_trade",   0)
        count    = closed_summary.get("count",         0)

        closed_section = (
            "<h2>Historial operaciones cerradas</h2>"
            "<div style='display:flex;gap:15px;"
            "flex-wrap:wrap;margin:10px 0'>"
            f"<div style='{kpi_style}'>"
            f"<div style='{num_style}'>{count}</div>"
            "<div style='color:#666;font-size:13px'>"
            "Operaciones cerradas</div></div>"
            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};"
            f"color:{_color(win_rate-50)}'>"
            f"{win_rate:.1f}%</div>"
            "<div style='color:#666;font-size:13px'>"
            "Win rate</div></div>"
            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};"
            f"color:{_color(avg_pnl)}'>"
            f"{'+'if avg_pnl>=0 else ''}"
            f"{avg_pnl:.2f}%</div>"
            "<div style='color:#666;font-size:13px'>"
            "P&L promedio</div></div>"
            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};color:#28a745'>"
            f"+{best:.2f}%</div>"
            "<div style='color:#666;font-size:13px'>"
            "Mejor trade</div></div>"
            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};color:#dc3545'>"
            f"{worst:.2f}%</div>"
            "<div style='color:#666;font-size:13px'>"
            "Peor trade</div></div>"
            "</div>"
        )

    # ── Sección 5: Portfolio actual con rendimientos ──────

    pos_details = perf_metrics.get("positions_detail", [])
    pos_map     = {p["ticker"]: p for p in pos_details}

    rows = ""
    for t, w in sorted(
        result["weights"].items(), key=lambda x: -x[1]
    ):
        pos    = positions.get(t, {})
        detail = pos_map.get(t, {})
        ev     = pos.get("ev_12m")
        entry  = detail.get("entry_price") or 0
        curr   = detail.get("current_price") or 0
        ret    = detail.get("ret_pct",   0) or 0
        days   = detail.get("days_held", 0) or 0

        ev_str  = f"${ev:.2f}"   if ev    else "-"
        ret_str = (
            f"<span style='color:{_color(ret)};font-weight:bold'>"
            f"{'+'if ret>=0 else ''}{ret:.2f}%</span>"
        )

        rows += (
            f"<tr style='border-bottom:1px solid #eee'>"
            f"<td style='padding:8px'><strong>{t}</strong></td>"
            f"<td style='padding:8px'>{w:.1%}</td>"
            f"<td style='padding:8px'>${entry:.2f}</td>"
            f"<td style='padding:8px'>${curr:.2f}</td>"
            f"<td style='padding:8px'>{ret_str}</td>"
            f"<td style='padding:8px'>{days}d</td>"
            f"<td style='padding:8px'>{ev_str}</td>"
            f"</tr>"
        )

    # ── Sección 6: Best/Worst ─────────────────────────────

    best  = perf_metrics.get("best_position")
    worst = perf_metrics.get("worst_position")

    bw_section = ""
    if best and worst:
        bw_section = (
            "<div style='display:flex;gap:15px;"
            "margin:10px 0'>"
            "<div style='flex:1;background:#d4edda;"
            "padding:12px;border-radius:8px'>"
            f"<strong>🏆 Mejor: {best['ticker']}</strong>"
            f"<br>Ret: "
            f"{_pct_badge(best['ret_pct'])} | "
            f"Peso: {best['weight']:.1%} | "
            f"{best['days_held']}d</div>"
            "<div style='flex:1;background:#f8d7da;"
            "padding:12px;border-radius:8px'>"
            f"<strong>📉 Peor: {worst['ticker']}</strong>"
            f"<br>Ret: "
            f"{_pct_badge(worst['ret_pct'])} | "
            f"Peso: {worst['weight']:.1%} | "
            f"{worst['days_held']}d</div>"
            "</div>"
        )

    # ── Sección 7: Kill conditions ────────────────────────

    kills = ""
    for t, p in positions.items():
        kc = p.get("kill_condition", "")
        if kc:
            kills += (
                f"<tr style='border-bottom:1px solid #eee'>"
                f"<td style='padding:8px'>"
                f"<strong>{t}</strong></td>"
                f"<td style='padding:8px'>"
                f"{p['weight']:.1%}</td>"
                f"<td style='padding:8px'>{kc}</td>"
                f"</tr>"
            )

    kills_section = ""
    if kills:
        kills_section = (
            "<h2>Kill Conditions activas</h2>"
            "<table style='width:100%;"
            "border-collapse:collapse'>"
            "<thead><tr style='background:#dc3545;"
            "color:white'>"
            "<th style='padding:10px;text-align:left'>"
            "Ticker</th>"
            "<th style='padding:10px;text-align:left'>"
            "Peso</th>"
            "<th style='padding:10px;text-align:left'>"
            "Condición</th>"
            "</tr></thead>"
            f"<tbody>{kills}</tbody></table>"
        )

    # ── Sección 8: Tesis ──────────────────────────────────

    thesis_html = ""
    for th in all_thesis:
        ev_pct   = th.get("expected_return_pct", 0)
        bear_pct = th.get("bear_downside_pct",   0)
        ratio    = th.get("upside_downside_ratio", 0)
        kill     = th.get("kill_condition",       "N/D")
        text     = th.get("thesis_text",          "N/D")
        accion   = th.get("accion", th.get("action", ""))

        thesis_html += (
            "<div style='border:1px solid #ddd;"
            "padding:15px;margin:10px 0;"
            "border-radius:5px'>"
            f"<h3>{th['ticker']} | {accion} | "
            f"{th['weight']:.1%}</h3>"
            f"<p>VE 12M: {_pct_badge(ev_pct)} | "
            f"Bajista: {bear_pct:.1f}% | "
            f"Ratio U/D: {ratio:.2f}x</p>"
            "<p style='background:#fff3cd;"
            "padding:10px;border-radius:3px'>"
            f"<strong>Kill condition:</strong> {kill}</p>"
            "<div style='white-space:pre-wrap;"
            "font-family:Georgia,serif;line-height:1.6'>"
            f"{text}</div></div>"
        )

    thesis_section = (
        f"<h2>Tesis de posición</h2>{thesis_html}"
        if thesis_html else ""
    )

    # ── HTML final ────────────────────────────────────────

    ev_port  = result["expected_return"]
    risk_adj = result["risk_adjusted_return"]

    body = (
        "<!DOCTYPE html><html>"
        "<body style='font-family:Arial,sans-serif;"
        "max-width:860px;margin:0 auto;padding:20px'>"
        "<h1 style='color:#1a1a2e'>"
        f"Rebalanceo Portfolio {today}</h1>"

        # KPIs
        "<h2>Métricas de rendimiento</h2>"
        f"<div style='display:flex;gap:15px;"
        f"margin:20px 0;flex-wrap:wrap'>{kpis}</div>"
        f"{bw_section}"

        # Advertencia sin datos
        f"{no_data_section}"

        # Commentary
        "<h2>Commentary</h2>"
        "<div style='background:#f8f9fa;padding:20px;"
        "border-radius:8px;white-space:pre-wrap;"
        "font-family:Georgia,serif;line-height:1.8'>"
        f"{summary}</div>"

        # Operaciones
        f"{trades_section}"

        # Historial cerradas
        f"{closed_section}"

        # Portfolio actual
        "<h2>Portfolio actual</h2>"
        "<table style='width:100%;"
        "border-collapse:collapse'>"
        "<thead>"
        "<tr style='background:#1a1a2e;color:white'>"
        "<th style='padding:10px;text-align:left'>"
        "Ticker</th>"
        "<th style='padding:10px;text-align:left'>"
        "Peso</th>"
        "<th style='padding:10px;text-align:left'>"
        "Entrada</th>"
        "<th style='padding:10px;text-align:left'>"
        "Precio actual</th>"
        "<th style='padding:10px;text-align:left'>"
        "Ret.</th>"
        "<th style='padding:10px;text-align:left'>"
        "Días</th>"
        "<th style='padding:10px;text-align:left'>"
        "VE 12M</th>"
        "</tr></thead>"
        f"<tbody>{rows}</tbody></table>"

        # Kill conditions
        f"{kills_section}"

        # Tesis
        f"{thesis_section}"

        # Footer
        "<hr style='margin:30px 0'>"
        "<p style='color:#999;font-size:12px'>"
        "No es consejo de inversión, es como estoy "
        "gestionando mi propio capital."
        "</p></body></html>"
    )

    Path("data").mkdir(parents=True, exist_ok=True)
    with open(
        "data/email_report.json", "w", encoding="utf-8"
    ) as f:
        json.dump(
            {"subject": subject, "body": body},
            f, indent=2, ensure_ascii=False,
        )
    print("✓ Email report guardado")
