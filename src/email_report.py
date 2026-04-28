# src/email_report.py
"""
Generación del email HTML completo y envío via Gmail.
"""

import os
import json
import smtplib
from pathlib              import Path
from datetime             import datetime
from email.mime.text      import MIMEText
from email.mime.multipart import MIMEMultipart


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
    no_data_tickers: list,
    new_trades:      list,
) -> None:
    today   = datetime.now().strftime("%Y-%m-%d")
    added   = result["added_names"]
    dropped = result["dropped_names"]

    # Subject
    parts = []
    if added:
        parts.append(f"+{','.join(added)}")
    if dropped:
        parts.append(f"-{','.join(dropped)}")
    changes_str = " ".join(parts) if parts else "sin cambios"

    ev_val  = result["expected_return"]
    subject = (
        f"Portfolio {today} | "
        f"EV {ev_val:.1%} | "
        f"{changes_str}"
    )

    port_ret = perf_metrics.get("portfolio_return_pct", 0.0)
    spy_ret  = perf_metrics.get("spy_return_pct",       0.0)
    alpha    = perf_metrics.get("alpha_pct",             0.0)

    kpi_style = (
        "background:#f8f9fa;padding:15px;"
        "border-radius:8px;flex:1;min-width:110px;"
        "text-align:center"
    )
    num_style = "font-size:22px;font-weight:bold"

    # ── KPIs ──────────────────────────────────────────────

    port_color  = _color(port_ret)
    spy_color   = _color(spy_ret)
    alpha_color = _color(alpha)
    ev_color    = _color(ev_val)

    port_sign  = "+" if port_ret >= 0 else ""
    spy_sign   = "+" if spy_ret  >= 0 else ""
    alpha_sign = "+" if alpha    >= 0 else ""

    port_str   = f"{port_sign}{port_ret:.2f}%"
    spy_str    = f"{spy_sign}{spy_ret:.2f}%"
    alpha_str  = f"{alpha_sign}{alpha:.2f}%"
    ev_str     = f"{ev_val:.1%}"
    to_str     = f"{result['turnover_used']:.1%}"
    np_str     = str(len(result["weights"]))
    ra_val     = result["risk_adjusted_return"]
    ra_str     = f"{ra_val:.2f}x"

    kpis = (
        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};color:{port_color}'>"
        f"{port_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"Ret. Portfolio</div></div>"

        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};color:{spy_color}'>"
        f"{spy_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"Ret. SPY (mismo periodo)</div></div>"

        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};color:{alpha_color}'>"
        f"{alpha_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"Alpha vs SPY</div></div>"

        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};color:{ev_color}'>"
        f"{ev_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"VE 12M</div></div>"

        f"<div style='{kpi_style}'>"
        f"<div style='{num_style};color:#fd7e14'>"
        f"{to_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"Turnover</div></div>"

        f"<div style='{kpi_style}'>"
        f"<div style='{num_style}'>{np_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"Posiciones</div></div>"

        f"<div style='{kpi_style}'>"
        f"<div style='{num_style}'>{ra_str}</div>"
        f"<div style='color:#666;font-size:13px'>"
        f"Ret. Ajustado Riesgo</div></div>"
    )

    # ── Best / Worst ───────────────────────────────────────

    best  = perf_metrics.get("best_position")
    worst = perf_metrics.get("worst_position")

    bw_section = ""
    if best and worst:
        best_ret   = best.get("ret_pct",   0.0)
        worst_ret  = worst.get("ret_pct",  0.0)
        best_w     = best.get("weight",    0.0)
        worst_w    = worst.get("weight",   0.0)
        best_days  = best.get("days_held", 0)
        worst_days = worst.get("days_held", 0)
        best_t     = best.get("ticker",    "")
        worst_t    = worst.get("ticker",   "")

        best_badge  = _pct_badge(best_ret)
        worst_badge = _pct_badge(worst_ret)

        bw_section = (
            "<div style='display:flex;gap:15px;"
            "margin:10px 0'>"

            "<div style='flex:1;background:#d4edda;"
            "padding:12px;border-radius:8px'>"
            f"<strong>Mejor: {best_t}</strong><br>"
            f"Ret: {best_badge} | "
            f"Peso: {best_w:.1%} | "
            f"{best_days}d</div>"

            "<div style='flex:1;background:#f8d7da;"
            "padding:12px;border-radius:8px'>"
            f"<strong>Peor: {worst_t}</strong><br>"
            f"Ret: {worst_badge} | "
            f"Peso: {worst_w:.1%} | "
            f"{worst_days}d</div>"

            "</div>"
        )

    # ── Sin datos ──────────────────────────────────────────

    no_data_section = ""
    if no_data_tickers:
        items    = "".join(
            f"<li><code>{t}</code></li>"
            for t in sorted(no_data_tickers)
        )
        nd_count = len(no_data_tickers)
        no_data_section = (
            "<div style='background:#fff3cd;"
            "border:1px solid #ffc107;"
            "padding:15px;border-radius:8px;"
            "margin:15px 0'>"
            "<h3 style='color:#856404;margin-top:0'>"
            f"Advertencia: {nd_count} tickers "
            f"sin datos fundamentales</h3>"
            "<p style='color:#856404;margin:5px 0'>"
            "Estos tickers fueron excluidos del "
            "análisis por no disponer de datos en "
            "Yahoo Finance.</p>"
            "<ul style='columns:4;color:#856404'>"
            f"{items}</ul></div>"
        )

    # ── Operaciones ejecutadas ─────────────────────────────

    trades_rows = ""
    for t in new_trades:
        action   = t.get("action",        "")
        ticker   = t.get("ticker",        "")
        price    = t.get("price_at_trade", 0) or 0
        w_before = t.get("weight_before",  0) or 0
        w_after  = t.get("weight_after",   0) or 0
        pnl      = t.get("pnl_pct")
        days     = t.get("pnl_days")

        action_map = {
            "OPEN":  ("#28a745", "ABRIR"),
            "ADD":   ("#17a2b8", "AÑADIR"),
            "TRIM":  ("#fd7e14", "REDUCIR"),
            "CLOSE": ("#dc3545", "CERRAR"),
        }
        a_color, a_label = action_map.get(
            action, ("#666", action)
        )

        pnl_str = "-"
        if pnl is not None:
            pnl_color = _color(pnl)
            pnl_sign  = "+" if pnl >= 0 else ""
            pnl_val   = f"{pnl_sign}{pnl:.2f}%"
            days_str  = f" ({days}d)" if days else ""
            pnl_str   = (
                f"<strong style='color:{pnl_color}'>"
                f"{pnl_val}</strong>{days_str}"
            )

        w_str = f"{w_before:.1%} &rarr; {w_after:.1%}"

        trades_rows += (
            f"<tr style='border-bottom:"
            f"1px solid #eee'>"
            f"<td style='padding:8px'>"
            f"<span style='background:{a_color};"
            f"color:white;padding:2px 8px;"
            f"border-radius:3px;font-size:12px'>"
            f"{a_label}</span></td>"
            f"<td style='padding:8px'>"
            f"<strong>{ticker}</strong></td>"
            f"<td style='padding:8px'>"
            f"${price:.2f}</td>"
            f"<td style='padding:8px'>{w_str}</td>"
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
            "<tr style='background:#343a40;"
            "color:white'>"
            "<th style='padding:10px;"
            "text-align:left'>Acción</th>"
            "<th style='padding:10px;"
            "text-align:left'>Ticker</th>"
            "<th style='padding:10px;"
            "text-align:left'>Precio</th>"
            "<th style='padding:10px;"
            "text-align:left'>Peso</th>"
            "<th style='padding:10px;"
            "text-align:left'>P&amp;L</th>"
            "</tr></thead>"
            f"<tbody>{trades_rows}</tbody>"
            "</table>"
        )

    # ── Historial operaciones cerradas ─────────────────────

    closed_summary = perf_metrics.get(
        "closed_trades_summary", {}
    )
    closed_section = ""
    if closed_summary:
        win_rate  = closed_summary.get("win_rate_pct", 0.0)
        avg_pnl   = closed_summary.get("avg_pnl_pct",  0.0)
        best_tv   = closed_summary.get("best_trade",   0.0)
        worst_tv  = closed_summary.get("worst_trade",  0.0)
        count     = closed_summary.get("count",        0)

        wr_color = _color(win_rate - 50)
        ap_color = _color(avg_pnl)
        ap_sign  = "+" if avg_pnl  >= 0 else ""
        bt_sign  = "+" if best_tv  >= 0 else ""

        closed_section = (
            "<h2>Historial operaciones cerradas</h2>"
            "<div style='display:flex;gap:15px;"
            "flex-wrap:wrap;margin:10px 0'>"

            f"<div style='{kpi_style}'>"
            f"<div style='{num_style}'>{count}</div>"
            f"<div style='color:#666;font-size:13px'>"
            f"Operaciones cerradas</div></div>"

            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};"
            f"color:{wr_color}'>"
            f"{win_rate:.1f}%</div>"
            f"<div style='color:#666;font-size:13px'>"
            f"Win rate</div></div>"

            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};"
            f"color:{ap_color}'>"
            f"{ap_sign}{avg_pnl:.2f}%</div>"
            f"<div style='color:#666;font-size:13px'>"
            f"P&amp;L promedio</div></div>"

            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};"
            f"color:#28a745'>"
            f"{bt_sign}{best_tv:.2f}%</div>"
            f"<div style='color:#666;font-size:13px'>"
            f"Mejor trade</div></div>"

            f"<div style='{kpi_style}'>"
            f"<div style='{num_style};"
            f"color:#dc3545'>"
            f"{worst_tv:.2f}%</div>"
            f"<div style='color:#666;font-size:13px'>"
            f"Peor trade</div></div>"

            "</div>"
        )

    # ── Portfolio actual con rendimientos ──────────────────

    pos_details = perf_metrics.get("positions_detail", [])
    pos_map     = {p["ticker"]: p for p in pos_details}

    rows = ""
    for t, w in sorted(
        result["weights"].items(),
        key=lambda x: -x[1],
    ):
        pos    = positions.get(t, {})
        detail = pos_map.get(t, {})

        ev_12m = pos.get("ev_12m")
        entry  = detail.get("entry_price",   0) or 0
        curr   = detail.get("current_price", 0) or 0
        ret    = detail.get("ret_pct",       0) or 0
        days   = detail.get("days_held",     0) or 0

        ev_disp = f"${ev_12m:.2f}" if ev_12m else "-"
        ret_c   = _color(ret)
        ret_sgn = "+" if ret >= 0 else ""
        ret_str = (
            f"<span style='color:{ret_c};"
            f"font-weight:bold'>"
            f"{ret_sgn}{ret:.2f}%</span>"
        )

        rows += (
            f"<tr style='border-bottom:"
            f"1px solid #eee'>"
            f"<td style='padding:8px'>"
            f"<strong>{t}</strong></td>"
            f"<td style='padding:8px'>{w:.1%}</td>"
            f"<td style='padding:8px'>"
            f"${entry:.2f}</td>"
            f"<td style='padding:8px'>"
            f"${curr:.2f}</td>"
            f"<td style='padding:8px'>{ret_str}</td>"
            f"<td style='padding:8px'>{days}d</td>"
            f"<td style='padding:8px'>{ev_disp}</td>"
            f"</tr>"
        )

    # ── Kill conditions ────────────────────────────────────

    kills = ""
    for t, p in positions.items():
        kc    = p.get("kill_condition", "")
        w_val = p.get("weight", 0)
        if kc:
            kills += (
                f"<tr style='border-bottom:"
                f"1px solid #eee'>"
                f"<td style='padding:8px'>"
                f"<strong>{t}</strong></td>"
                f"<td style='padding:8px'>"
                f"{w_val:.1%}</td>"
                f"<td style='padding:8px'>{kc}</td>"
                f"</tr>"
            )

    kills_section = ""
    if kills:
        kills_section = (
            "<h2>Kill Conditions activas</h2>"
            "<table style='width:100%;"
            "border-collapse:collapse'>"
            "<thead>"
            "<tr style='background:#dc3545;"
            "color:white'>"
            "<th style='padding:10px;"
            "text-align:left'>Ticker</th>"
            "<th style='padding:10px;"
            "text-align:left'>Peso</th>"
            "<th style='padding:10px;"
            "text-align:left'>Condición</th>"
            "</tr></thead>"
            f"<tbody>{kills}</tbody></table>"
        )

    # ── Tesis ──────────────────────────────────────────────

    thesis_html = ""
    for th in all_thesis:
        ev_pct   = th.get("expected_return_pct",  0.0)
        bear_pct = th.get("bear_downside_pct",     0.0)
        ratio    = th.get("upside_downside_ratio", 0.0)
        kill     = th.get("kill_condition",        "N/D")
        text     = th.get("thesis_text",           "N/D")
        accion   = th.get("accion", th.get("action", ""))
        t_ticker = th.get("ticker", "")
        t_weight = th.get("weight", 0.0)

        ev_c     = _color(ev_pct)
        ev_sign  = "+" if ev_pct >= 0 else ""
        ev_badge = (
            f"<strong style='color:{ev_c}'>"
            f"{ev_sign}{ev_pct:.1f}%</strong>"
        )

        thesis_html += (
            "<div style='border:1px solid #ddd;"
            "padding:15px;margin:10px 0;"
            "border-radius:5px'>"

            f"<h3>{t_ticker} | {accion} | "
            f"{t_weight:.1%}</h3>"

            f"<p>VE 12M: {ev_badge} | "
            f"Bajista: {bear_pct:.1f}% | "
            f"Ratio U/D: {ratio:.2f}x</p>"

            "<p style='background:#fff3cd;"
            "padding:10px;border-radius:3px'>"
            f"<strong>Kill condition:</strong> "
            f"{kill}</p>"

            "<div style='white-space:pre-wrap;"
            "font-family:Georgia,serif;"
            "line-height:1.6'>"
            f"{text}</div></div>"
        )

    thesis_section = (
        f"<h2>Tesis de posición</h2>{thesis_html}"
        if thesis_html else ""
    )

    # ── HTML final ─────────────────────────────────────────

    body = (
        "<!DOCTYPE html><html>"
        "<body style='font-family:Arial,sans-serif;"
        "max-width:860px;margin:0 auto;padding:20px'>"

        "<h1 style='color:#1a1a2e'>"
        f"Rebalanceo Portfolio {today}</h1>"

        "<h2>Métricas de rendimiento</h2>"
        "<div style='display:flex;gap:15px;"
        "margin:20px 0;flex-wrap:wrap'>"
        f"{kpis}</div>"
        f"{bw_section}"

        f"{no_data_section}"

        "<h2>Commentary</h2>"
        "<div style='background:#f8f9fa;"
        "padding:20px;border-radius:8px;"
        "white-space:pre-wrap;"
        "font-family:Georgia,serif;"
        "line-height:1.8'>"
        f"{summary}</div>"

        f"{trades_section}"
        f"{closed_section}"

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

        f"{kills_section}"
        f"{thesis_section}"

        "<hr style='margin:30px 0'>"
        "<p style='color:#999;font-size:12px'>"
        "No es consejo de inversión, es como estoy "
        "gestionando mi propio capital."
        "</p></body></html>"
    )

    # Guardar JSON
    Path("data").mkdir(parents=True, exist_ok=True)
    with open(
        "data/email_report.json", "w", encoding="utf-8"
    ) as f:
        json.dump(
            {"subject": subject, "body": body},
            f, indent=2, ensure_ascii=False,
        )
    print("  ✓ Email report guardado")


# ── Envío via Gmail ───────────────────────────────────────────────────────────

def send_email_report() -> bool:
    """
    Envía el email report guardado via Gmail SMTP.

    Variables de entorno requeridas:
      EMAIL_USERNAME  → tu cuenta Gmail
      EMAIL_PASSWORD  → App Password de Gmail
      EMAIL_TO        → destinatario (opcional, 
    """
    username = os.getenv("EMAIL_USERNAME")
    password = os.getenv("EMAIL_PASSWORD")
    to_email = os.getenv("EMAIL_TO")

    if not username or not password:
        print(
            "  ⚠ EMAIL_USERNAME o EMAIL_PASSWORD "
            "no configurados."
        )
        return False

    # Cargar el report generado
    report_path = Path("data/email_report.json")
    if not report_path.exists():
        print(f"  ⚠ No existe {report_path}")
        return False

    report    = json.load(
        open(report_path, encoding="utf-8")
    )
    subject   = report["subject"]
    body_html = report["body"]

    # Destinatario: EMAIL_TO si existe, sino el propio remitente
    to_email = os.getenv("EMAIL_TO", username)

    print(f"  De:    {username}")
    print(f"  Para:  {to_email}")
    print(f"  Asunto: {subject[:70]}...")

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = username
        msg["To"]      = to_email

        text_plain = (
            "Este email requiere un cliente "
            "que soporte HTML."
        )
        msg.attach(
            MIMEText(text_plain, "plain", "utf-8")
        )
        msg.attach(
            MIMEText(body_html, "html", "utf-8")
        )

        with smtplib.SMTP_SSL(
            "smtp.gmail.com", 465, timeout=30
        ) as server:
            server.login(username, password)
            server.sendmail(
                username,
                to_email,
                msg.as_string(),
            )

        print("  ✓ Email enviado correctamente")
        return True

    except smtplib.SMTPAuthenticationError as e:
        print(f"  ✗ Error de autenticación: {e}")
        print(
            "  Verifica que EMAIL_PASSWORD sea "
            "un App Password de Gmail."
        )
        return False

    except Exception as e:
        print(f"  ✗ Error enviando email: {e}")
        return False
