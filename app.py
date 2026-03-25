"""
S&P 500 RSI Monitor API
API Flask para generar informes de RSI del S&P 500
"""

from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from datetime import datetime
import logging
import traceback
import warnings

warnings.filterwarnings('ignore')

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Lista reducida pero confiable de tickers S&P 500
SP500_TICKERS = [
    # Mega Caps - Technology
    ("AAPL", "Information Technology"),
    ("MSFT", "Information Technology"),
    ("NVDA", "Information Technology"),
    ("GOOGL", "Communication Services"),
    ("AMZN", "Consumer Discretionary"),
    ("META", "Communication Services"),
    ("TSLA", "Consumer Discretionary"),
    
    # Technology
    ("AVGO", "Information Technology"),
    ("AMD", "Information Technology"),
    ("INTC", "Information Technology"),
    ("CSCO", "Information Technology"),
    ("ORCL", "Information Technology"),
    ("ADBE", "Information Technology"),
    ("CRM", "Information Technology"),
    ("QCOM", "Information Technology"),
    ("TXN", "Information Technology"),
    ("INTU", "Information Technology"),
    ("IBM", "Information Technology"),
    ("AMAT", "Information Technology"),
    ("MU", "Information Technology"),
    
    # Communication Services
    ("NFLX", "Communication Services"),
    ("DIS", "Communication Services"),
    ("CMCSA", "Communication Services"),
    ("VZ", "Communication Services"),
    ("T", "Communication Services"),
    ("TMUS", "Communication Services"),
    
    # Health Care
    ("UNH", "Health Care"),
    ("JNJ", "Health Care"),
    ("LLY", "Health Care"),
    ("ABBV", "Health Care"),
    ("MRK", "Health Care"),
    ("TMO", "Health Care"),
    ("ABT", "Health Care"),
    ("DHR", "Health Care"),
    ("PFE", "Health Care"),
    ("BMY", "Health Care"),
    ("AMGN", "Health Care"),
    ("MDT", "Health Care"),
    ("GILD", "Health Care"),
    ("CVS", "Health Care"),
    
    # Financials
    ("JPM", "Financials"),
    ("V", "Financials"),
    ("MA", "Financials"),
    ("BAC", "Financials"),
    ("WFC", "Financials"),
    ("GS", "Financials"),
    ("MS", "Financials"),
    ("BLK", "Financials"),
    ("SPGI", "Financials"),
    ("C", "Financials"),
    ("SCHW", "Financials"),
    ("AXP", "Financials"),
    ("PGR", "Financials"),
    ("CB", "Financials"),
    
    # Consumer Discretionary
    ("HD", "Consumer Discretionary"),
    ("MCD", "Consumer Discretionary"),
    ("NKE", "Consumer Discretionary"),
    ("LOW", "Consumer Discretionary"),
    ("SBUX", "Consumer Discretionary"),
    ("TJX", "Consumer Discretionary"),
    ("BKNG", "Consumer Discretionary"),
    
    # Consumer Staples
    ("PG", "Consumer Staples"),
    ("KO", "Consumer Staples"),
    ("PEP", "Consumer Staples"),
    ("COST", "Consumer Staples"),
    ("WMT", "Consumer Staples"),
    ("PM", "Consumer Staples"),
    ("MO", "Consumer Staples"),
    ("MDLZ", "Consumer Staples"),
    
    # Industrials
    ("CAT", "Industrials"),
    ("GE", "Industrials"),
    ("RTX", "Industrials"),
    ("HON", "Industrials"),
    ("UPS", "Industrials"),
    ("BA", "Industrials"),
    ("DE", "Industrials"),
    ("LMT", "Industrials"),
    
    # Energy
    ("XOM", "Energy"),
    ("CVX", "Energy"),
    ("COP", "Energy"),
    ("SLB", "Energy"),
    ("EOG", "Energy"),
    
    # Utilities
    ("NEE", "Utilities"),
    ("DUK", "Utilities"),
    ("SO", "Utilities"),
    
    # Real Estate
    ("AMT", "Real Estate"),
    ("PLD", "Real Estate"),
    ("CCI", "Real Estate"),
    
    # Materials
    ("LIN", "Materials"),
    ("APD", "Materials"),
    ("SHW", "Materials"),
]


def calculate_rsi(prices, period=14):
    """Calcula el RSI de forma segura."""
    try:
        if len(prices) < period + 1:
            return None
            
        delta = prices.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        last_rsi = rsi.iloc[-1]
        
        if pd.isna(last_rsi) or pd.isinf(last_rsi):
            return None
            
        return float(last_rsi)
        
    except Exception as e:
        logger.error(f"Error calculando RSI: {e}")
        return None


def get_rsi_data():
    """Obtiene datos RSI para todos los tickers."""
    results = []
    tickers_list = [t[0] for t in SP500_TICKERS]
    sectors_dict = {t[0]: t[1] for t in SP500_TICKERS}
    
    logger.info(f"Procesando {len(tickers_list)} tickers...")
    
    # Procesar en lotes pequeños
    batch_size = 10
    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i:i + batch_size]
        logger.info(f"Lote {i//batch_size + 1}: procesando {len(batch)} tickers")
        
        for ticker in batch:
            try:
                # Descargar datos individuales para mayor confiabilidad
                data = yf.download(
                    ticker,
                    period='3mo',
                    progress=False,
                    auto_adjust=True
                )
                
                if data.empty or len(data) < 20:
                    logger.warning(f"{ticker}: datos insuficientes")
                    continue
                
                close_prices = data['Close']
                rsi = calculate_rsi(close_prices)
                
                if rsi is not None:
                    results.append({
                        'ticker': ticker,
                        'sector': sectors_dict[ticker],
                        'rsi': round(rsi, 2),
                        'oversold': rsi < 30
                    })
                    logger.info(f"✓ {ticker}: RSI = {rsi:.2f}")
                else:
                    logger.warning(f"{ticker}: RSI no calculable")
                    
            except Exception as e:
                logger.error(f"Error con {ticker}: {str(e)}")
                continue
    
    logger.info(f"Completado: {len(results)} tickers procesados exitosamente")
    return pd.DataFrame(results)


def generate_report(analysis, df):
    """Genera el reporte formateado para Telegram (optimizado para < 4096 caracteres)."""
    if df.empty or analysis is None:
        return "❌ No se pudieron obtener datos"
    
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    
    # Estadísticas generales
    total = len(df)
    oversold_count = int(df['oversold'].sum())
    oversold_pct = round(oversold_count / total * 100, 1)
    sectors_affected = len(analysis[analysis['oversold'] > 0])
    total_sectors = len(analysis)
    
    # Barra compacta
    def get_bar(pct, width=6):
        filled = int(pct / 100 * width)
        return '█' * filled + '░' * (width - filled)
    
    # Construir mensaje compacto
    msg = f"""📊 <b>S&P 500 RSI</b>
━━━━━━━━━━━━━━━━━━━━
📅 {now}

📈 <b>RESUMEN</b>
Analizados: <b>{total}</b> | Sobreventa: <b>{oversold_count}</b> (<b>{oversold_pct}%</b>)
Sectores afectados: <b>{sectors_affected}/{total_sectors}</b>

📉 <b>POR SECTOR</b>
"""
    
    # Solo mostrar sectores con datos relevantes (>0% o top sectores)
    top_sectors = analysis.head(8)  # Solo top 8 sectores
    
    for _, row in top_sectors.iterrows():
        sector_short = row['sector'][:15]  # Acortar nombres
        bar = get_bar(row['pct_oversold'], width=5)
        msg += f"<code>{sector_short:15}</code> {bar} {row['pct_oversold']}%\n"
    
    # Top sobrevendidas (reducido a 5)
    if oversold_count > 0:
        top_oversold = df[df['oversold']].nsmallest(5, 'rsi')
        msg += f"\n🔴 <b>TOP 5 SOBREVENTA</b>\n"
        for _, stock in top_oversold.iterrows():
            msg += f"• {stock['ticker']}: <b>{stock['rsi']:.1f}</b>\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━\n💡 <i>RSI &lt; 30 = sobreventa</i>"
    
    return msg

@app.route('/')
def index():
    """Endpoint raíz."""
    return jsonify({
        "service": "S&P 500 RSI Monitor API",
        "status": "online",
        "version": "2.0",
        "endpoints": {
            "/health": "Health check",
            "/rsi-report": "Genera informe RSI"
        }
    })


@app.route('/health')
def health():
    """Health check."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    })


@app.route('/rsi-report')
def rsi_report():
    """Endpoint principal - genera informe RSI."""
    try:
        logger.info("=== INICIANDO GENERACIÓN DE INFORME ===")
        
        # Obtener datos
        df = get_rsi_data()
        
        if df.empty:
            raise ValueError("No se obtuvieron datos válidos")
        
        # Generar mensaje
        message = generate_report(df)
        
        # Estadísticas
        stats = {
            "total_tickers": int(len(df)),
            "total_oversold": int(df['oversold'].sum()),
            "pct_oversold": round(float(df['oversold'].sum() / len(df) * 100), 1)
        }
        
        logger.info(f"=== INFORME COMPLETADO: {stats['total_tickers']} tickers ===")
        
        return jsonify({
            "success": True,
            "message": message,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"ERROR: {error_msg}")
        logger.error(traceback.format_exc())
        
        return jsonify({
            "success": False,
            "error": error_msg,
            "message": f"⚠️ <b>Error</b>\n\n{error_msg}",
            "timestamp": datetime.now().isoformat()
        }), 500


if __name__ == '__main__':
    port = 5000
    logger.info(f"Iniciando servidor en puerto {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
