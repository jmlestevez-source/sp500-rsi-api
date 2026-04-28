# src/performance.py
"""
Tracking de rendimiento del portfolio vs SPY.
Corregido: manejo de precios null, reset de portfolio.
"""

import json
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime


HISTORY_PATH   = Path("data/performance/history.json")
TRADE_LOG_PATH = Path("data/trades/trade_log.json")


def load_trade_log() -> list[dict]:
    if TRADE_LOG_PATH.exists():
        try:
            return json.load(open(TRADE_LOG_PATH))
        except Exception:
            return []
    return []


def save_trade_log(trades: list[dict]) -> None:
    TRADE_LOG_PATH.parent.mkdir(
        parents=True, exist_ok=True
    )
    with open(TRADE_LOG_PATH, "w") as f:
        json.dump(trades, f, indent=2)


def _get_current_price(ticker: str) -> float:
    """Obtiene precio actual real de un ticker."""
    try:
        hist = yf.Ticker(ticker).history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return 0.0


def record_trades(
    result:            dict,
    scenarios:         dict,
    current_positions: dict,
) -> list[dict]:
    """
    Registra operaciones ejecutadas.
    Calcula P&L de cerradas con precios reales.
    """
    trades     = load_trade_log()
    today      = datetime.now().strftime("%Y-%m-%d")
    new_trades = []

    # Posiciones abiertas / modificadas
    for ticker, new_weight in result["weights"].items():
        old_weight = current_positions.get(
            ticker, {}
        ).get("weight", 0)
        scenario   = scenarios.get(ticker, {})
        price      = scenario.get("current_price", 0)

        # Obtener precio real si no lo tenemos
        if not price or price == 0:
            price = _get_current_price(ticker)

        if ticker in result["added_names"]:
            action = "OPEN"
        elif new_weight > old_weight + 0.01:
            action = "ADD"
        elif new_weight < old_weight - 0.01:
            action = "TRIM"
        else:
            action = "HOLD"

        if action in ("OPEN", "ADD", "TRIM"):
            trade = {
                "date":           today,
                "ticker":         ticker,
                "action":         action,
                "price_at_trade": price,
                "weight_before":  old_weight,
                "weight_after":   new_weight,
                "ev_12m":         scenario.get("ev_12m"),
                "status":         "OPEN",
                "pnl_pct":        None,
                "pnl_days":       None,
            }
            new_trades.append(trade)

    # Posiciones cerradas
    for ticker in result["dropped_names"]:
        old_pos    = current_positions.get(ticker, {})
        entry_p    = old_pos.get("entry_price") or 0
        entry_date = old_pos.get("entry_date",  today)
        old_weight = old_pos.get("weight",      0)

        # Obtener precio actual REAL para el P&L
        scenario = scenarios.get(ticker, {})
        exit_p   = scenario.get("current_price", 0)

        if not exit_p or exit_p == 0:
            exit_p = _get_current_price(ticker)

        # Calcular P&L solo si tenemos ambos precios
        pnl_pct = None
        if entry_p and entry_p > 0 and exit_p and exit_p > 0:
            pnl_pct = round(
                (exit_p - entry_p) / entry_p * 100, 2
            )
        elif entry_p and entry_p > 0 and exit_p == 0:
            # No tenemos precio de salida: no calcular
            pnl_pct = None

        # Días en posición
        pnl_days = None
        try:
            d0 = datetime.strptime(entry_date, "%Y-%m-%d")
            d1 = datetime.strptime(today,      "%Y-%m-%d")
            pnl_days = (d1 - d0).days
        except Exception:
            pass

        trade = {
            "date":           today,
            "ticker":         ticker,
            "action":         "CLOSE",
            "price_at_trade": exit_p,
            "weight_before":  old_weight,
            "weight_after":   0,
            "entry_price":    entry_p,
            "entry_date":     entry_date,
            "exit_price":     exit_p,
            "pnl_pct":        pnl_pct,
            "pnl_days":       pnl_days,
            "status":         "CLOSED",
        }
        new_trades.append(trade)

    # Actualizar log
    closed_tickers = set(result["dropped_names"])
    for t in trades:
        if (
            t["ticker"] in closed_tickers
            and t.get("status") == "OPEN"
        ):
            t["status"] = "CLOSED"

    trades.extend(new_trades)
    save_trade_log(trades)
    return new_trades


def load_performance_history() -> dict:
    if HISTORY_PATH.exists():
        try:
            return json.load(open(HISTORY_PATH))
        except Exception:
            pass
    return {
        "inception_date": None,
        "snapshots":      [],
    }


def save_performance_history(h: dict) -> None:
    HISTORY_PATH.parent.mkdir(
        parents=True, exist_ok=True
    )
    with open(HISTORY_PATH, "w") as f:
        json.dump(h, f, indent=2)


def update_performance(
    result:    dict,
    positions: dict,
    scenarios: dict,
) -> dict:
    history = load_performance_history()
    today   = datetime.now().strftime("%Y-%m-%d")

    if not history["inception_date"]:
        history["inception_date"] = today

    # Precio SPY
    spy_price = None
    try:
        spy_hist  = yf.Ticker("^GSPC").history(
            period="2d"
        )
        spy_price = float(spy_hist["Close"].iloc[-1])
    except Exception:
        pass

    snapshot = {
        "date":           today,
        "weights":        result["weights"],
        "expected_return": result["expected_return"],
        "spy_price":       spy_price,
        "n_positions":     len(result["weights"]),
    }

    history["snapshots"].append(snapshot)
    save_performance_history(history)
    return history


def compute_performance_metrics(
    positions: dict,
    scenarios: dict,
) -> dict:
    """
    Calcula rendimiento real vs SPY.
    Solo cuenta posiciones con precios válidos.
    """
    metrics = {
        "positions_detail":      [],
        "portfolio_return_pct":  0.0,
        "spy_return_pct":        0.0,
        "alpha_pct":             0.0,
        "best_position":         None,
        "worst_position":        None,
        "closed_trades_summary": {},
    }

    if not positions:
        return metrics

    total_weighted_return = 0.0
    total_weight          = 0.0
    position_returns      = []

    for ticker, pos in positions.items():
        entry_price = pos.get("entry_price") or 0
        entry_date  = pos.get("entry_date",  "")
        weight      = pos.get("weight",       0)
        scenario    = scenarios.get(ticker,   {})
        curr_price  = scenario.get(
            "current_price", 0
        )

        # Si no tenemos precio actual, intentar obtenerlo
        if not curr_price or curr_price == 0:
            curr_price = _get_current_price(ticker)

        # Si no tenemos precio de entrada, usar actual
        if not entry_price or entry_price == 0:
            entry_price = curr_price

        # Calcular retorno solo si ambos precios > 0
        if entry_price > 0 and curr_price > 0:
            ret_pct = (
                curr_price - entry_price
            ) / entry_price * 100
        else:
            ret_pct = 0.0

        # Días en posición
        days = 0
        try:
            d0   = datetime.strptime(
                entry_date, "%Y-%m-%d"
            )
            days = (datetime.now() - d0).days
        except Exception:
            pass

        pos_detail = {
            "ticker":        ticker,
            "weight":        weight,
            "entry_price":   entry_price,
            "entry_date":    entry_date,
            "current_price": curr_price,
            "ret_pct":       round(ret_pct, 2),
            "days_held":     days,
            "ev_12m":        pos.get("ev_12m"),
        }
        position_returns.append(pos_detail)

        total_weighted_return += ret_pct * weight
        total_weight          += weight

    metrics["positions_detail"] = sorted(
        position_returns,
        key=lambda x: x["ret_pct"],
        reverse=True,
    )

    if total_weight > 0:
        metrics["portfolio_return_pct"] = round(
            total_weighted_return / total_weight, 2
        )

    if position_returns:
        metrics["best_position"]  = max(
            position_returns,
            key=lambda x: x["ret_pct"],
        )
        metrics["worst_position"] = min(
            position_returns,
            key=lambda x: x["ret_pct"],
        )

    # SPY en el mismo periodo
    if positions:
        dates = []
        for pos in positions.values():
            ed = pos.get("entry_date", "")
            if ed:
                try:
                    dates.append(
                        datetime.strptime(ed, "%Y-%m-%d")
                    )
                except Exception:
                    pass

        if dates:
            oldest = min(dates)
            try:
                spy_hist = yf.Ticker("^GSPC").history(
                    start=oldest.strftime("%Y-%m-%d"),
                    end=datetime.now().strftime("%Y-%m-%d"),
                )
                if len(spy_hist) >= 2:
                    spy_ret = (
                        float(spy_hist["Close"].iloc[-1])
                        / float(spy_hist["Close"].iloc[0])
                        - 1
                    ) * 100
                    metrics["spy_return_pct"] = round(
                        spy_ret, 2
                    )
                    metrics["alpha_pct"] = round(
                        metrics["portfolio_return_pct"]
                        - spy_ret, 2
                    )
            except Exception:
                pass

    # Historial cerradas: solo con P&L válido
    trades = load_trade_log()
    closed = [
        t for t in trades
        if t.get("status") == "CLOSED"
        and t.get("pnl_pct") is not None
    ]
    if closed:
        pnls = [t["pnl_pct"] for t in closed]
        metrics["closed_trades_summary"] = {
            "count":       len(closed),
            "avg_pnl_pct": round(
                sum(pnls) / len(pnls), 2
            ),
            "win_rate_pct": round(
                sum(1 for p in pnls if p > 0)
                / len(pnls) * 100, 1
            ),
            "best_trade":  round(max(pnls), 2),
            "worst_trade": round(min(pnls), 2),
        }

    return metrics
