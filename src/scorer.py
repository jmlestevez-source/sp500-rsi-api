# src/scorer.py
"""
Scoring LLM en batches: una llamada por N tickers
en lugar de una por ticker.
"""

import time
from src.llm import call_llm_json


def score_batch(
    stocks: list[dict],
    macro_context: str,
    batch_size: int = 8,
) -> list[dict]:
    """
    Puntúa stocks en batches para minimizar
    llamadas LLM.
    stocks: lista de dicts con fundamentales
    Devuelve lista con composite_score añadido.
    """
    results = []
    total   = len(stocks)

    for i in range(0, total, batch_size):
        batch = stocks[i:i + batch_size]
        batch_results = _score_single_batch(
            batch, macro_context
        )
        results.extend(batch_results)

        completed = min(i + batch_size, total)
        print(
            f"    Scoring: [{completed}/{total}] "
            f"tickers procesados"
        )

    return results


def _score_single_batch(
    batch: list[dict],
    macro_context: str,
) -> list[dict]:
    """Puntúa un batch con una sola llamada LLM."""

    # Construir tabla de datos
    rows = []
    for s in batch:
        fpe    = s.get("forward_pe",      "N/A")
        growth = s.get("revenue_growth",  "N/A")
        gm     = s.get("gross_margins",   "N/A")
        roe    = s.get("roe",             "N/A")
        de     = s.get("debt_to_equity",  "N/A")
        sector = s.get("sector",          "N/A")
        mcap   = s.get("market_cap",       0)
        mcap_b = f"{mcap/1e9:.1f}B" if mcap else "N/A"

        rows.append(
            f"{s['ticker']}: fwdPE={fpe}, "
            f"rev_growth={growth}, gross_margin={gm}, "
            f"ROE={roe}, D/E={de}, "
            f"sector={sector}, mcap={mcap_b}"
        )

    data_table = "\n".join(rows)
    tickers    = [s["ticker"] for s in batch]

    # JSON template para forzar formato
    json_keys = ", ".join(
        f'"{t}": {{"fundamental_score": <0-100>, '
        f'"forward_setup_score": <0-100>}}'
        for t in tickers
    )

    prompt = (
        f"Score these {len(batch)} stocks from 0-100 "
        f"on two dimensions.\n\n"
        f"MACRO CONTEXT: {macro_context[:120]}\n\n"
        f"STOCK DATA:\n{data_table}\n\n"
        f"fundamental_score: quality of business "
        f"(margins, ROE, balance sheet, moat)\n"
        f"forward_setup_score: attractiveness right "
        f"now (valuation vs growth, momentum, "
        f"risk/reward)\n\n"
        f"Return ONLY this JSON:\n"
        f"{{{json_keys}}}"
    )

    try:
        result = call_llm_json(
            prompt, task="batch_scoring", max_tokens=600
        )

        scored = []
        for s in batch:
            t     = s["ticker"]
            scores = result.get(t, {})

            fscore  = float(scores.get("fundamental_score",  50))
            fwscore = float(scores.get("forward_setup_score", 50))

            # Combinar con quant_score si existe
            quant = float(s.get("quant_score", 0))
            # Normalizar quant a 0-100
            quant_norm = min(max((quant + 30) / 130 * 100, 0), 100)

            composite = (
                fscore     * 0.25
                + fwscore  * 0.35
                + quant_norm * 0.40  # peso al cuantitativo
            )

            scored.append({
                **s,
                "fundamental_score":   fscore,
                "forward_setup_score": fwscore,
                "composite_score":     composite,
            })

        return scored

    except Exception as e:
        print(f"    Error batch scoring: {e}")
        # Fallback: usar solo quant_score
        return [
            {
                **s,
                "fundamental_score":   50,
                "forward_setup_score": 50,
                "composite_score":     float(
                    s.get("quant_score", 0)
                ),
            }
            for s in batch
        ]
