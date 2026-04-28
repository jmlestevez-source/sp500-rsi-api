# src/data_fetcher.py
"""
Descarga de datos optimizada:
- Histórico de precios en batch (yf.download)
- Fundamentales en paralelo con timeout corto
- Caché diaria
- Skip de tickers problemáticos
"""

import time
import pickle
import threading
import concurrent.futures
import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime


CACHE_DIR = Path("data/cache")


class DataCache:
    def __init__(self):
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, key: str) -> Path:
        today = datetime.now().strftime("%Y-%m-%d")
        safe  = (
            key.replace("/", "_")
            .replace("^", "X")
            .replace(".", "_")
        )
        return CACHE_DIR / f"{safe}_{today}.pkl"

    def get(self, key: str):
        p = self._path(key)
        if p.exists():
            try:
                with self._lock:
                    return pickle.load(open(p, "rb"))
            except Exception:
                pass
        return None

    def set(self, key: str, data) -> None:
        try:
            with self._lock:
                pickle.dump(
                    data, open(self._path(key), "wb")
                )
        except Exception:
            pass

    def cleanup_old(self, keep_days: int = 2) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        count = 0
        for f in CACHE_DIR.glob("*.pkl"):
            if today not in f.name:
                try:
                    f.unlink()
                    count += 1
                except Exception:
                    pass
        if count:
            print(f"  Caché: {count} archivos antiguos eliminados")


cache = DataCache()


class RateLimiter:
    def __init__(self, calls_per_second: float = 2.0):
        self.min_interval = 1.0 / calls_per_second
        self.last_called  = 0.0
        self._lock        = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            elapsed = time.time() - self.last_called
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_called = time.time()


yf_limiter = RateLimiter(calls_per_second=2.0)


# ── Fundamentales ─────────────────────────────────────────────────────────────

def fetch_fundamentals(ticker: str) -> dict:
    """Descarga fundamentales de un ticker con caché."""
    cached = cache.get(f"fund_{ticker}")
    if cached is not None:
        return cached

    yf_limiter.wait()
    result = {"ticker": ticker, "_data_ok": False}

    try:
        info = yf.Ticker(ticker).info

        # Si info está vacío o tiene error, salir rápido
        if not info or info.get("regularMarketPrice") is None:
            price = info.get("currentPrice")
            if price is None:
                result["_error"] = "no_price"
                cache.set(f"fund_{ticker}", result)
                return result

        fields = {
            "price":             ("currentPrice",
                                  "regularMarketPrice"),
            "forward_pe":        ("forwardPE",),
            "trailing_pe":       ("trailingPE",),
            "revenue_growth":    ("revenueGrowth",),
            "earnings_growth":   ("earningsGrowth",),
            "gross_margins":     ("grossMargins",),
            "operating_margins": ("operatingMargins",),
            "free_cashflow":     ("freeCashflow",),
            "market_cap":        ("marketCap",),
            "ev_to_ebitda":      ("enterpriseToEbitda",),
            "debt_to_equity":    ("debtToEquity",),
            "roe":               ("returnOnEquity",),
            "beta":              ("beta",),
            "52w_high":          ("fiftyTwoWeekHigh",),
            "52w_low":           ("fiftyTwoWeekLow",),
            "sector":            ("sector",),
            "industry":          ("industry",),
        }

        for field, keys in fields.items():
            for k in keys:
                v = info.get(k)
                if v is not None:
                    result[field] = v
                    break

        result["_data_ok"] = (
            result.get("price") is not None
        )
        cache.set(f"fund_{ticker}", result)

    except Exception as e:
        err = str(e)[:100]
        result["_error"] = err
        cache.set(f"fund_{ticker}", result)

    return result


def fetch_fundamentals_parallel(
    tickers:     list[str],
    max_workers: int = 10,
) -> dict[str, dict]:
    """
    Descarga fundamentales en paralelo.
    Timeout por ticker: 15s.
    """
    results: dict[str, dict] = {}
    total = len(tickers)
    done  = 0
    t0    = time.time()

    print(f"  Descargando fundamentales de {total} tickers...")

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        future_map = {
            executor.submit(fetch_fundamentals, t): t
            for t in tickers
        }

        for future in concurrent.futures.as_completed(
            future_map
        ):
            ticker = future_map[future]
            done  += 1
            try:
                data = future.result(timeout=15)
                results[ticker] = data
            except concurrent.futures.TimeoutError:
                results[ticker] = {
                    "ticker":   ticker,
                    "_data_ok": False,
                    "_error":   "timeout",
                }
            except Exception as e:
                results[ticker] = {
                    "ticker":   ticker,
                    "_data_ok": False,
                    "_error":   str(e)[:100],
                }

            if done % 100 == 0 or done == total:
                elapsed = time.time() - t0
                rate    = done / elapsed if elapsed else 0
                eta     = (total - done) / rate if rate else 0
                print(
                    f"    [{done}/{total}] "
                    f"{elapsed:.0f}s "
                    f"({rate:.1f}/s, "
                    f"ETA {eta:.0f}s)"
                )

    ok  = sum(1 for d in results.values() if d.get("_data_ok"))
    bad = total - ok
    elapsed = time.time() - t0
    print(
        f"  ✓ {ok}/{total} OK | {bad} sin datos | "
        f"{elapsed:.0f}s total"
    )
    return results


# ── Histórico de precios ──────────────────────────────────────────────────────

def fetch_price_history(
    tickers:    list[str],
    period:     str = "1y",
    chunk_size: int = 100,
) -> pd.DataFrame:
    """
    Descarga histórico en chunks con yf.download().
    Mucho más rápido que ticker por ticker.
    """
    cache_key = f"hist_{period}_{len(tickers)}"
    cached    = cache.get(cache_key)
    if cached is not None:
        print(f"  Histórico desde caché: {cached.shape}")
        return cached

    all_closes = []
    total      = len(tickers)
    n_chunks   = (total + chunk_size - 1) // chunk_size
    t0         = time.time()

    print(
        f"  Descargando histórico {period}: "
        f"{total} tickers en {n_chunks} chunks..."
    )

    for i in range(0, total, chunk_size):
        chunk     = tickers[i:i + chunk_size]
        chunk_num = i // chunk_size + 1

        try:
            # yf.download acepta lista directamente
            hist = yf.download(
                chunk,
                period=period,
                progress=False,
                threads=True,
                auto_adjust=True,
                timeout=30,
            )

            if hist.empty:
                print(f"    Chunk {chunk_num}: vacío")
                continue

            if isinstance(hist.columns, pd.MultiIndex):
                close = hist["Close"]
            else:
                close = hist[["Close"]]
                close.columns = [chunk[0]]

            all_closes.append(close)
            print(
                f"    Chunk {chunk_num}/{n_chunks}: "
                f"{close.shape[1]} tickers OK"
            )

        except Exception as e:
            print(f"    Chunk {chunk_num} error: {e}")

        time.sleep(0.3)

    if not all_closes:
        print("  ⚠ Sin datos históricos")
        return pd.DataFrame()

    combined = pd.concat(all_closes, axis=1)
    combined = combined.loc[
        :, ~combined.columns.duplicated()
    ]

    elapsed = time.time() - t0
    print(
        f"  ✓ Histórico: {combined.shape[0]} días, "
        f"{combined.shape[1]} tickers, "
        f"{elapsed:.0f}s"
    )

    cache.set(cache_key, combined)
    return combined


# ── Macro ─────────────────────────────────────────────────────────────────────

def fetch_macro_data() -> dict:
    """Datos de mercado para contexto macro."""
    cached = cache.get("macro")
    if cached:
        return cached

    result = {}
    tickers = {
        "SPY": "^GSPC",
        "VIX": "^VIX",
        "TNX": "^TNX",
        "QQQ": "QQQ",
        "IWM": "IWM",
    }

    for label, ticker in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="20d")
            if not hist.empty and len(hist) >= 2:
                price = float(hist["Close"].iloc[-1])
                idx5  = min(5, len(hist) - 1)
                ret_5d = (
                    price / float(hist["Close"].iloc[-idx5]) - 1
                ) * 100
                ret_20d = (
                    price / float(hist["Close"].iloc[0]) - 1
                ) * 100
                result[label] = {
                    "price":   price,
                    "ret_5d":  round(ret_5d, 2),
                    "ret_20d": round(ret_20d, 2),
                }
        except Exception:
            pass

    cache.set("macro", result)
    return result
