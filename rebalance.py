# rebalance.py - versión cloud + OpenRouter

import os
import json
import yaml
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Imports del proyecto
from src.llm.openrouter_client import OpenRouterClient, TaskType
from src.scoring.fundamental_scorer import FundamentalScorer
from src.scoring.scenario_builder import ScenarioBuilder
from src.optimizer.portfolio_optimizer import PortfolioOptimizer
from src.thesis.thesis_generator import ThesisGenerator

load_dotenv()

def load_config():
    with open("config/portfolio_config.yaml") as f:
        return yaml.safe_load(f)

def load_current_positions() -> dict:
    f = Path("data/positions/current.json")
    return json.load(open(f)) if f.exists() else {}

def load_universe() -> list:
    with open("data/universe/tickers.json") as f:
        return json.load(f)

def get_macro_context(client: OpenRouterClient) -> str:
    """
    Macro context via LLM + datos reales de FRED (gratuito)
    """
    import yfinance as yf
    
    # Datos macro reales gratuitos via yfinance proxies
    spy = yf.Ticker("SPY")
    vix = yf.Ticker("^VIX")
    tnx = yf.Ticker("^TNX")   # 10Y yield
    dxy = yf.Ticker("DX-Y.NYB")  # Dollar index
    
    try:
        spy_info = spy.history(period="5d")
        spy_return_5d = (
            spy_info["Close"].iloc[-1] / spy_info["Close"].iloc[0] - 1
        ) * 100
        
        vix_current = vix.history(period="1d")["Close"].iloc[-1]
        tnx_current = tnx.history(period="1d")["Close"].iloc[-1]
        
        market_data = f"""
        DATOS DE MERCADO ACTUALES:
        - SPY 5d return: {spy_return_5d:.2f}%
        - VIX: {vix_current:.1f} ({'fear' if vix_current > 25 else 'neutral' if vix_current > 15 else 'complacency'})
        - 10Y Treasury yield: {tnx_current:.2f}%
        - Fecha: {datetime.now().strftime('%Y-%m-%d')}
        """
    except Exception:
        market_data = f"Fecha: {datetime.now().strftime('%Y-%m-%d')}"
    
    prompt = f"""
    {market_data}
    
    Basándote en estos datos y tu conocimiento actualizado, describe el 
    contexto macro para un portfolio long-only equity en 150 palabras máximo.
    
    Incluye:
    1. Fed stance y expectativas de tipos (cuantificado)
    2. Estado del ciclo económico
    3. Risk appetite actual (VIX como referencia)
    4. Sectores con viento de cola vs headwind ahora mismo
    5. Top 2 riesgos macro próximos 3 meses
    
    Sé específico y cuantitativo. Sin frases genéricas.
    """
    
    return client.complete(
        prompt=prompt,
        task=TaskType.MACRO,
        max_tokens=400
    )

def run_rebalance():
    print(f"\n{'='*60}")
    print(f"REBALANCE - {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")
    
    # Setup
    config = load_config()
    api_key = os.getenv("OPENROUTER_API_KEY")
    
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY no encontrada en .env")
    
    llm = OpenRouterClient(
        api_key=api_key,
        site_url="https://github.com/tu-usuario/portfolio-autopilot",
        app_name="Portfolio Autopilot"
    )
    
    current_positions = load_current_positions()
    universe = load_universe()
    
    print(f"Universe: {len(universe)} tickers")
    print(f"Posiciones actuales: {len(current_positions)} nombres\n")
    
    # 1. Macro
    print("📊 Macro context...")
    macro = get_macro_context(llm)
    print(f"✓ Macro listo\n")
    
    # 2. Scoring del universo
    print("🔍 Scoring universe...")
    scorer = FundamentalScorer(llm_client=llm)
    top_scores = scorer.score_universe(
        tickers=universe,
        macro_context=macro,
        top_n=40,
        batch_size=10    # Procesar en batches para respetar rate limits
    )
    print(f"✓ Top: {top_scores[0].ticker} ({top_scores[0].composite_score:.1f})\n")
    
    # 3. Scenarios
    print("📐 Building scenarios...")
    builder = ScenarioBuilder(llm_client=llm)
    scenarios = {}
    
    for i, score in enumerate(top_scores):
        print(f"  [{i+1}/{len(top_scores)}] {score.ticker}...")
        scenario = builder.build_scenario(
            ticker=score.ticker,
            stock_data=score.data_snapshot,
            macro_context=macro,
            current_portfolio=list(current_positions.values())
        )
        scenarios[score.ticker] = scenario
        
        # Pausa entre requests para respetar rate limits
        time.sleep(1.5)
    
    print(f"✓ {len(scenarios)} scenarios completados\n")
    
    # 4. Optimizer (local, sin LLM)
    print("⚙️  Optimizing portfolio...")
    optimizer = PortfolioOptimizer(config)
    current_weights = {t: p["weight"] for t, p in current_positions.items()}
    
    result = optimizer.optimize(
        scenarios=list(scenarios.values()),
        current_positions=current_weights
    )
    
    print(f"✓ Portfolio: {len(result.weights)} nombres")
    print(f"  Turnover: {result.turnover_used:.1%}")
    print(f"  EV 12M: {result.expected_return:.1%}\n")
    
    # 5. Thesis
    print("📝 Generating thesis...")
    thesis_gen = ThesisGenerator(llm_client=llm)
    all_thesis = []
    
    for ticker, weight in result.weights.items():
        if ticker in result.added_names:
            action = "OPEN"
        elif ticker in result.dropped_names:
            action = "CLOSE"
        else:
            old_w = current_weights.get(ticker, 0)
            diff = abs(weight - old_w)
            if diff >= config["turnover"]["min_position_change"]:
                action = "ADD" if weight > old_w else "TRIM"
            else:
                action = "HOLD"
        
        # Solo generar thesis para cambios reales
        if action != "HOLD":
            thesis = thesis_gen.generate_position_thesis(
                ticker=ticker,
                scenario=scenarios[ticker],
                weight=weight,
                action=action,
                macro_summary=macro
            )
            all_thesis.append(thesis)
            print(f"  ✓ {ticker} [{action}]")
            time.sleep(2)
    
    # 6. Summary
    print("\n📣 Rebalance commentary...")
    summary = thesis_gen.generate_rebalance_summary(
        optimization_result=result,
        all_thesis=all_thesis,
        macro_summary=macro
    )
    
    # 7. Guardar todo
    save_results(result, all_thesis, summary, scenarios)
    
    # 8. Report de uso de API
    print(f"\n📊 API Usage: {llm.usage_report()}")
    
    print(f"\n{'='*60}")
    print("✅ Rebalance complete")
    print(f"{'='*60}\n")
    
    print("\nCOMMENTARY:")
    print(summary)
    
    return result

def save_results(result, all_thesis, summary, scenarios):
    """Guarda todo en data/ para el audit trail via git"""
    timestamp = datetime.now().isoformat()
    date = timestamp[:10]
    
    # Posiciones actuales
    positions = {}
    for ticker, weight in result.weights.items():
        s = scenarios.get(ticker)
        positions[ticker] = {
            "weight": weight,
            "entry_date": date,
            "entry_price": s.current_price if s else None,
            "ev_12m": s.ev_12m if s else None,
            "kill_condition": s.kill_condition if s else None
        }
    
    Path("data/positions").mkdir(parents=True, exist_ok=True)
    with open("data/positions/current.json", "w") as f:
        json.dump(positions, f, indent=2)
    
    # Rebalance completo
    rebalance = {
        "timestamp": timestamp,
        "portfolio": result.weights,
        "changes": {
            "added": result.added_names,
            "dropped": result.dropped_names,
            "turnover": result.turnover_used
        },
        "metrics": {
            "expected_return": result.expected_return,
            "risk_score": result.risk_score,
            "risk_adjusted_return": result.risk_adjusted_return
        },
        "commentary": summary
    }
    
    Path("data/rebalances").mkdir(parents=True, exist_ok=True)
    with open(f"data/rebalances/{date}_rebalance.json", "w") as f:
        json.dump(rebalance, f, indent=2)
    
    print("✓ Resultados guardados")

if __name__ == "__main__":
    run_rebalance()
