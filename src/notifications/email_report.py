# src/notifications/email_report.py

import json
import os
from pathlib import Path
from datetime import datetime

def generate_email_report() -> dict:
    """
    Genera el contenido del email con el rebalanceo completo.
    Se ejecuta al final de rebalance.py y guarda en data/
    para que el workflow lo lea.
    """
    
    # Cargar último rebalanceo
    rebalance_files = sorted(Path("data/rebalances").glob("*.json"))
    if not rebalance_files:
        return {"subject": "Error: sin datos", "body": "No se encontraron datos"}
    
    latest = json.load(open(rebalance_files[-1]))
    
    # Cargar thesis del día
    today = datetime.now().strftime("%Y-%m-%d")
    thesis_files = list(Path("data/thesis").glob(f"{today}_*.json"))
    thesis_today = [json.load(open(f)) for f in thesis_files]
    
    # Cargar posiciones
    positions = json.load(open("data/positions/current.json"))
    
    # ── Construir subject ──────────────────────────────────
    added = latest["changes"]["added"]
    dropped = latest["changes"]["dropped"]
    
    changes_str = ""
    if added:
        changes_str += f"+{','.join(added)}"
    if dropped:
        changes_str += f" -{','.join(dropped)}"
    if not changes_str:
        changes_str = "sin cambios"
    
    subject = (
        f"📊 Portfolio Rebalance {today} | "
        f"EV {latest['metrics']['expected_return']:.1%} | "
        f"{changes_str}"
    )
    
    # ── Construir body en HTML ─────────────────────────────
    
    # Portfolio table
    portfolio_rows = ""
    for ticker, weight in sorted(
        latest["portfolio"].items(),
        key=lambda x: -x[1]
    ):
        pos = positions.get(ticker, {})
        ev = pos.get("ev_12m")
        ev_str = f"${ev:.2f}" if ev else "-"
        
        portfolio_rows += f"""
        <tr>
            <td><strong>{ticker}</strong></td>
            <td>{weight:.1%}</td>
            <td>${pos.get('entry_price', 0):.2f}</td>
            <td>{ev_str}</td>
        </tr>
        """
    
    # Thesis del día
    thesis_html = ""
    for t in thesis_today:
        ev_pct = t.get("expected_return_pct", 0)
        color = "#28a745" if ev_pct > 0 else "#dc3545"
        
        thesis_html += f"""
        <div style="border:1px solid #ddd; padding:15px; margin:10px 0; border-radius:5px;">
            <h3 style="margin:0 0 10px 0;">
                {t['ticker']} | {t['action']} | {t['weight']:.1%}
            </h3>
            <table>
                <tr>
                    <td>EV 12M:</td>
                    <td style="color:{color}"><strong>{ev_pct:+.1f}%</strong></td>
                    <td>Bear downside:</td>
                    <td>{t.get('bear_downside_pct', 0):.1f}%</td>
                    <td>U/D Ratio:</td>
                    <td>{t.get('upside_downside_ratio', 0):.2f}x</td>
                </tr>
            </table>
            <p style="background:#fff3cd; padding:10px; border-radius:3px;">
                <strong>Kill condition:</strong> {t.get('kill_condition', 'N/A')}
            </p>
            <div style="white-space:pre-wrap; font-family:Georgia,serif; line-height:1.6;">
                {t.get('thesis_text', 'N/A')}
            </div>
        </div>
        """
    
    # Kill conditions activas (todas las posiciones)
    kills_html = ""
    for ticker, pos in positions.items():
        if pos.get("kill_condition"):
            kills_html += f"""
            <tr>
                <td><strong>{ticker}</strong></td>
                <td>{pos['weight']:.1%}</td>
                <td>{pos['kill_condition']}</td>
            </tr>
            """
    
    body = f"""
    <!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; max-width:800px; margin:0 auto;">
        
        <h1 style="color:#1a1a2e;">📊 Portfolio Rebalance - {today}</h1>
        
        <!-- Métricas -->
        <div style="display:flex; gap:20px; margin:20px 0;">
            <div style="background:#f8f9fa; padding:15px; border-radius:8px; flex:1; text-align:center;">
                <div style="font-size:24px; font-weight:bold; color:#28a745;">
                    {latest['metrics']['expected_return']:.1%}
                </div>
                <div style="color:#666;">EV 12M</div>
            </div>
            <div style="background:#f8f9fa; padding:15px; border-radius:8px; flex:1; text-align:center;">
                <div style="font-size:24px; font-weight:bold;">
                    {latest['metrics']['risk_adjusted_return']:.2f}x
                </div>
                <div style="color:#666;">Risk-Adj Return</div>
            </div>
            <div style="background:#f8f9fa; padding:15px; border-radius:8px; flex:1; text-align:center;">
                <div style="font-size:24px; font-weight:bold; color:#fd7e14;">
                    {latest['changes']['turnover']:.1%}
                </div>
                <div style="color:#666;">Turnover (cap 30%)</div>
            </div>
            <div style="background:#f8f9fa; padding:15px; border-radius:8px; flex:1; text-align:center;">
                <div style="font-size:24px; font-weight:bold;">
                    {len(latest['portfolio'])}
                </div>
                <div style="color:#666;">Posiciones</div>
            </div>
        </div>
        
        <!-- Commentary -->
        <h2>📝 Commentary</h2>
        <div style="background:#f8f9fa; padding:20px; border-radius:8px; 
                    white-space:pre-wrap; font-family:Georgia,serif; line-height:1.8;">
            {latest.get('commentary', 'Sin commentary')}
        </div>
        
        <!-- Portfolio -->
        <h2>📈 Portfolio actual</h2>
        <table style="width:100%; border-collapse:collapse;">
            <thead>
                <tr style="background:#1a1a2e; color:white;">
                    <th style="padding:10px; text-align:left;">Ticker</th>
                    <th style="padding:10px; text-align:left;">Weight</th>
                    <th style="padding:10px; text-align:left;">Entry</th>
                    <th style="padding:10px; text-align:left;">EV 12M</th>
                </tr>
            </thead>
            <tbody>{portfolio_rows}</tbody>
        </table>
        
        <!-- Thesis del día -->
        {"<h2>🎯 Thesis generadas hoy</h2>" + thesis_html if thesis_html else ""}
        
        <!-- Kill conditions -->
        <h2>⚠️ Kill Conditions activas</h2>
        <table style="width:100%; border-collapse:collapse;">
            <thead>
                <tr style="background:#dc3545; color:white;">
                    <th style="padding:10px; text-align:left;">Ticker</th>
                    <th style="padding:10px; text-align:left;">Weight</th>
                    <th style="padding:10px; text-align:left;">Kill Condition</th>
                </tr>
            </thead>
            <tbody>{kills_html}</tbody>
        </table>
        
        <hr style="margin:30px 0;">
        <p style="color:#999; font-size:12px;">
            Not advice, just how I'm sizing my own book. | 
            <a href="https://github.com/{os.getenv('GITHUB_REPOSITORY', 'tu-repo')}">
                Ver audit trail completo
            </a>
        </p>
        
    </body>
    </html>
    """
    
    # Guardar para que el workflow lo lea
    report = {"subject": subject, "body": body}
    Path("data/email_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2)
    )
    
    return report


if __name__ == "__main__":
    report = generate_email_report()
    print(f"Subject: {report['subject']}")
    print("Report generado en data/email_report.json")
