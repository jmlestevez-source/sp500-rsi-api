# rebalance.py
"""
Orquestador principal del sistema de rebalanceo.
Pipeline completo optimizado para Russell 1000.
Timeout global: 50 minutos (para GitHub Actions de 60min).
"""

import os
import json
import yaml
import time
import signal
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from src.llm import call_llm, request_counts
from src.universe import load_universe, update_universe
from src.data_fetcher import (
    fetch_fundamentals_parallel,
    fetch_price_history,
    fetch_macro_data,
    cache,
)
from src.screener  import prescreening
from src.scorer    import score_batch
from src.scenarios import build_scenario
from src.optimizer import optimize_portfolio
from src.thesis    import generate_thesis
from src.performance import (
    compute_performance_metrics,
    update_performance,
    record_trades,
)
from src.email_report import (
    generate_email_report,
    send_email_report,
)


# ── Timeout global ────────────────────────────────────────────────────────────

MAX_RUNTIME_SECONDS = 50 * 60  # 50 minutos


class TimeoutError(Exception):
    pass


def _timeout_handler(signum, frame):
    raise TimeoutError("Tiempo máximo excedido")


# ── Config y estado ───────────────────────────────────────────────────────────

def load_config() -> dict:
    with open("config/portfolio_config.yaml") as f:
        return yaml.safe_load(f)


def load_current_positions() -> dict:
    f = Path("data/positions/current.json")
    return json.load(open(f)) if f.exists() else {}


def save_results(
    result:    dict,
    scenarios: dict,
) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    ts    = datetime.now().isoformat()

    positions = {}
    for ticker, weight in result["weights"].items():
        s = scenarios.get(ticker, {})
        positions[ticker] = {
            "weight":         weight,
            "entry_date":     today,
            "entry_price":    s.get("current_price"),
            "ev_12m":         s.get("ev_12m"),
            "kill_condition": s.get("kill_condition"),
        }

    Path("data/positions").mkdir(
        parents=True, exist_ok=True
    )
    with open("data/positions/current.json", "w") as f:
        json.dump(positions, f, indent=2)

    rebalance = {
        "timestamp": ts,
        "portfolio": result["weights"],
        "changes": {
            "added":    result["added_names"],
            "dropped":  result["dropped_names"],
            "turnover": result["turnover_used"],
        },
        "metrics": {
            "expected_return":      result["expected_return"],
            "risk_score":           result["risk_score"],
            "risk_adjusted_return": result["risk_adjusted_return"],
        },
    }

    Path("data/rebalances").mkdir(parents=True, exist_ok=True)
    rb_path = Path(f"data/rebalances/{today}_rebalance.json")
    with open(rb_path, "w") as f:
        json.dump(rebalance, f, indent=2)

    print("  ✓ Resultados guardados")
    return positions


def get_macro_context(macro_data: dict) -> str:
    lines = []
    for label, d in macro_data.items():
        price = d.get("price",  0)
        r5d   = d.get("ret_5d", 0)
        lines.append(
            f"- {label}: {price:.2f} ({r5d:+.1f}% 5d)"
        )

    system = (
        "Eres un analista macro experto. "
        "Responde siempre en español. "
        "Sé conciso, específico y cuantitativo."
    )
    prompt = (
        f"Datos de mercado reales:\n"
        f"{chr(10).join(lines)}\n\n"
        "Describe el contexto macro actual para un "
        "portfolio long-only en máximo 120 palabras.\n"
        "Incluye: 1) Fed y tipos 2) Fase ciclo "
        "3) VIX/riesgo 4) Sectores 5) Top 2 riesgos. "
        "Sin frases genéricas."
    )
    return call_llm(
        prompt, task="macro", system=system,
        max_tokens=300,
    )


def _generate_commentary(
    result:        dict,
    macro_context: str,
    perf_metrics:  dict,
) -> str:
    weights  = result["weights"]
    added    = result["added_names"]
    dropped  = result["dropped_names"]
    port_ret = perf_metrics.get("portfolio_return_pct", 0.0)
    spy_ret  = perf_metrics.get("spy_return_pct",       0.0)
    alpha    = perf_metrics.get("alpha_pct",             0.0)

    lines = "\n".join(
        f"  {t}: {w:.1%}"
        for t, w in sorted(
            weights.items(), key=lambda x: -x[1]
        )
    )

    system = (
        "Eres un gestor de portfolio profesional. "
        "Escribe en primera persona. "
        "Directo, cuantitativo. Responde en español."
    )

    to_val = result["turnover_used"]
    ev_val = result["expected_return"]

    prompt = (
        "Commentary del rebalanceo semanal. "
        "Máximo 300 palabras.\n\n"
        f"Portfolio:\n{lines}\n\n"
        f"Cambios: +{added} -{dropped}\n"
        f"Turnover: {to_val:.1%}\n"
        f"EV 12M: {ev_val:.1%}\n\n"
        f"Performance: Portfolio {port_ret:+.2f}% | "
        f"SPY {spy_ret:+.2f}% | Alpha {alpha:+.2f}%\n\n"
        f"Macro: {macro_context[:200]}\n\n"
        "Estructura: 1) Macro 2) Compras/ventas "
        "3) Mantenidas 4) Performance vs SPY "
        "5) Vigilar\n\n"
        "Termina con: 'No es consejo de inversión.'"
    )

    try:
        return call_llm(
            prompt, task="commentary",
            system=system, max_tokens=500,
        )
    except Exception as e:
        return f"Error: {e}"


def _check_time(
    start_time:  float,
    step_name:   str,
    max_seconds: int = MAX_RUNTIME_SECONDS,
) -> None:
    """Verifica que no hemos excedido el tiempo máximo."""
    elapsed = time.time() - start_time
    remaining = max_seconds - elapsed
    if remaining < 120:  # menos de 2 minutos
        raise TimeoutError(
            f"Abortando en {step_name}: "
            f"quedan solo {remaining:.0f}s "
            f"de {max_seconds}s máximos"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def run_rebalance(
    force_universe_update: bool = False,
) -> dict:
    start_time = time.time()

    # Configurar timeout por señal (solo Linux/Mac)
    try:
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(MAX_RUNTIME_SECONDS)
    except (AttributeError, ValueError):
        pass  # Windows no soporta SIGALRM

    print(f"\n{'='*60}")
    print(
        "REBALANCEO PORTFOLIO — "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
    )
    print(f"{'='*60}\n")

    # Verificar credenciales
    groq_key   = os.getenv("GROQ_API_KEY")
    gemini_key = os.getenv("GEMINI_API_KEY")
    if not groq_key and not gemini_key:
        raise Exception("Necesitas GROQ_API_KEY o GEMINI_API_KEY")
    if groq_key:
        print("✓ Groq disponible")
    if gemini_key:
        print("✓ Gemini disponible (backup)")

    email_user = os.getenv("EMAIL_USERNAME")
    if email_user:
        print(f"✓ Email configurado: {email_user}")
    else:
        print("⚠ Email no configurado")

    cache.cleanup_old(keep_days=2)
    config            = load_config()
    current_positions = load_current_positions()
    print(f"Posiciones actuales: {len(current_positions)}\n")

    try:
        # ── PASO 1: Universo ──────────────────────────────
        print("=" * 50)
        print("PASO 1: Universo Russell 1000")
        print("=" * 50)

        if force_universe_update:
            universe_tickers = update_universe()
        else:
            universe_tickers = load_universe()

        print(f"  Total: {len(universe_tickers)} tickers\n")

        # ── PASO 2: Macro ─────────────────────────────────
        print("=" * 50)
        print("PASO 2: Contexto macro")
        print("=" * 50)

        macro_data    = fetch_macro_data()
        macro_context = get_macro_context(macro_data)
        print("  ✓ Macro generado\n")

        _check_time(start_time, "PASO 3")

        # ── PASO 3: Histórico de precios ──────────────────
        print("=" * 50)
        print("PASO 3: Histórico de precios")
        print("=" * 50)

        price_history = fetch_price_history(
            universe_tickers,
            period="1y",
            chunk_size=100,
        )
        print()

        _check_time(start_time, "PASO 4")

        # ── PASO 4: Fundamentales ─────────────────────────
        print("=" * 50)
        print("PASO 4: Fundamentales (paralelo)")
        print("=" * 50)

        fundamentals = fetch_fundamentals_parallel(
            universe_tickers,
            max_workers=10,
        )
        print()

        _check_time(start_time, "PASO 5")

        # ── PASO 5: Pre-filtro cuantitativo ───────────────
        print("=" * 50)
        print("PASO 5: Pre-filtro cuantitativo")
        print("=" * 50)

        prescreen_n = config.get(
            "screening", {}
        ).get("prescreen_top_n", 100)

        candidates, no_data_tickers = prescreening(
            fundamentals,
            price_history,
            top_n=prescreen_n,
        )
        print(
            f"  {len(candidates)} candidatos | "
            f"{len(no_data_tickers)} sin datos\n"
        )

        _check_time(start_time, "PASO 6")

        # ── PASO 6: Scoring LLM ──────────────────────────
        print("=" * 50)
        print("PASO 6: Scoring LLM (batches)")
        print("=" * 50)

        batch_size = config.get(
            "screening", {}
        ).get("llm_batch_size", 8)

        stocks_to_score = [
            {**fundamentals[t], "ticker": t}
            for t in candidates
            if t in fundamentals
        ]

        scored = score_batch(
            stocks_to_score,
            macro_context,
            batch_size=batch_size,
        )
        scored.sort(
            key=lambda x: x.get("composite_score", 0),
            reverse=True,
        )

        max_pos    = config["portfolio"]["max_positions"]
        top_scored = scored[:max_pos * 2]

        print(
            f"  ✓ Top {len(top_scored)} candidatos\n"
        )

        _check_time(start_time, "PASO 7")

        # ── PASO 7: Escenarios ────────────────────────────
        print("=" * 50)
        print("PASO 7: Escenarios")
        print("=" * 50)

        scenarios: dict = {}
        total_sc         = len(top_scored)

        for i, s in enumerate(top_scored):
            _check_time(start_time, f"Escenario {i+1}")
            ticker    = s["ticker"]
            fund_data = fundamentals.get(ticker, {})
            merged    = {**fund_data, **s}
            print(f"  [{i+1}/{total_sc}] {ticker}...")
            scenarios[ticker] = build_scenario(
                ticker, merged, macro_context,
            )

        print(f"  ✓ {len(scenarios)} escenarios\n")

        _check_time(start_time, "PASO 8")

        # ── PASO 8: Optimización ──────────────────────────
        print("=" * 50)
        print("PASO 8: Optimizando portfolio")
        print("=" * 50)

        current_weights = {
            t: p["weight"]
            for t, p in current_positions.items()
        }

        result = optimize_portfolio(
            list(scenarios.values()),
            current_weights,
            config,
        )

        print(
            f"  ✓ {len(result['weights'])} posiciones | "
            f"Turnover {result['turnover_used']:.1%} | "
            f"EV {result['expected_return']:.1%}\n"
        )

        # ── PASO 9: Registrar operaciones ─────────────────
        print("=" * 50)
        print("PASO 9: Operaciones")
        print("=" * 50)

        new_trades = record_trades(
            result, scenarios, current_positions,
        )
        print(f"  ✓ {len(new_trades)} operaciones\n")

        # ── PASO 10: Guardar posiciones ───────────────────
        print("=" * 50)
        print("PASO 10: Guardando posiciones")
        print("=" * 50)

        positions = save_results(result, scenarios)
        print()

        # ── PASO 11: Performance ──────────────────────────
        print("=" * 50)
        print("PASO 11: Performance vs SPY")
        print("=" * 50)

        perf_metrics = compute_performance_metrics(
            positions, scenarios,
        )
        update_performance(result, positions, scenarios)

        port_ret = perf_metrics.get("portfolio_return_pct", 0.0)
        spy_ret  = perf_metrics.get("spy_return_pct",       0.0)
        alpha    = perf_metrics.get("alpha_pct",             0.0)

        p_s = "+" if port_ret >= 0 else ""
        s_s = "+" if spy_ret  >= 0 else ""
        a_s = "+" if alpha    >= 0 else ""

        print(
            f"  Portfolio: {p_s}{port_ret:.2f}% | "
            f"SPY: {s_s}{spy_ret:.2f}% | "
            f"Alpha: {a_s}{alpha:.2f}%\n"
        )

        _check_time(start_time, "PASO 12")

        # ── PASO 12: Tesis ────────────────────────────────
        print("=" * 50)
        print("PASO 12: Tesis")
        print("=" * 50)

        all_thesis = []
        min_change = config["turnover"]["min_position_change"]

        for ticker, weight in result["weights"].items():
            old_w = current_weights.get(ticker, 0.0)
            diff  = weight - old_w

            if ticker in result["added_names"]:
                action = "OPEN"
            elif diff >= min_change:
                action = "ADD"
            elif diff <= -min_change:
                action = "TRIM"
            else:
                action = "HOLD"

            if action != "HOLD" and ticker in scenarios:
                _check_time(start_time, f"Tesis {ticker}")
                thesis = generate_thesis(
                    ticker, scenarios[ticker],
                    weight, action, macro_context,
                )
                all_thesis.append(thesis)
                labels = {
                    "OPEN": "ABRIR",
                    "ADD":  "AÑADIR",
                    "TRIM": "REDUCIR",
                }
                print(
                    f"  ✓ {ticker} "
                    f"[{labels.get(action, action)}]"
                )

        for ticker in result["dropped_names"]:
            if ticker in scenarios:
                _check_time(start_time, f"Tesis {ticker}")
                thesis = generate_thesis(
                    ticker, scenarios[ticker],
                    0.0, "CLOSE", macro_context,
                )
                all_thesis.append(thesis)
                print(f"  ✓ {ticker} [CERRAR]")

        print()

        # ── PASO 13: Commentary ───────────────────────────
        print("=" * 50)
        print("PASO 13: Commentary")
        print("=" * 50)

        summary = _generate_commentary(
            result, macro_context, perf_metrics,
        )
        result["commentary"] = summary

        today   = datetime.now().strftime("%Y-%m-%d")
        rb_file = Path(
            f"data/rebalances/{today}_rebalance.json"
        )
        if rb_file.exists():
            try:
                rb = json.load(open(rb_file))
                rb["commentary"] = summary
                with open(rb_file, "w") as f:
                    json.dump(rb, f, indent=2)
            except Exception:
                pass

        print("  ✓ Commentary generado\n")

        # ── PASO 14: Email ────────────────────────────────
        print("=" * 50)
        print("PASO 14: Email report")
        print("=" * 50)

        generate_email_report(
            result          = result,
            all_thesis      = all_thesis,
            summary         = summary,
            positions       = positions,
            perf_metrics    = perf_metrics,
            no_data_tickers = no_data_tickers,
            new_trades      = new_trades,
        )

        send_email_report()
        print()

    except TimeoutError as e:
        print(f"\n⚠ TIMEOUT: {e}")
        print("Intentando enviar email con lo que hay...")

        # Intentar enviar email parcial
        try:
            if "result" in dir() and "positions" in dir():
                generate_email_report(
                    result          = result,
                    all_thesis      = all_thesis if "all_thesis" in dir() else [],
                    summary         = summary if "summary" in dir() else "Timeout",
                    positions       = positions,
                    perf_metrics    = perf_metrics if "perf_metrics" in dir() else {},
                    no_data_tickers = no_data_tickers if "no_data_tickers" in dir() else [],
                    new_trades      = new_trades if "new_trades" in dir() else [],
                )
                send_email_report()
        except Exception as e2:
            print(f"  Error email parcial: {e2}")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        # Cancelar alarma
        try:
            signal.alarm(0)
        except (AttributeError, ValueError):
            pass

    # Resumen final
    elapsed = time.time() - start_time
    mins    = int(elapsed // 60)
    secs    = int(elapsed  % 60)

    print(f"\n{'='*60}")
    print(f"✅ Completado en {mins}m {secs}s")
    print(f"{'='*60}")

    print("\n📊 Llamadas API:")
    for model, count in sorted(
        request_counts.items(),
        key=lambda x: -x[1],
    ):
        print(f"   {model}: {count}")

    if "summary" in dir():
        print(f"\n{summary}\n")

    return result if "result" in dir() else {}


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--update-universe",
        action="store_true",
        help="Forzar actualización Russell 1000",
    )
    args = parser.parse_args()

    run_rebalance(
        force_universe_update=args.update_universe,
    )
