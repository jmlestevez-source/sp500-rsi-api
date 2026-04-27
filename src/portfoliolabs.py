# src/portfoliolabs.py
"""
Extrae datos financieros de PortfolioLabs para confirmar
los fundamentales de las posiciones seleccionadas.

USO: Solo se ejecuta para los ~20 tickers del portfolio
     final, no para los 1000 del universo.

REQUISITOS en GitHub Actions:
  - actions/setup-python con selenium instalado
  - uso de webdriver-manager para gestionar chromedriver
"""

import io
import time
import json
from pathlib import Path

import pandas as pd

# Imports opcionales — no rompe si no están instalados
try:
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import (
        expected_conditions as EC,
    )
    SELENIUM_OK = True
except ImportError:
    SELENIUM_OK = False


CACHE_PATH = Path("data/cache/portfoliolabs")


def _make_driver() -> "webdriver.Chrome":
    """
    Crea el driver de Chrome.
    Intenta webdriver-manager primero,
    luego asume chromedriver en PATH.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    try:
        from webdriver_manager.chrome import (
            ChromeDriverManager,
        )
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(
            service=service, options=options
        )
    except Exception:
        return webdriver.Chrome(options=options)


def _clean_value(val) -> float | None:
    """Limpia un valor financiero a float en billones."""
    if isinstance(val, str):
        val = val.strip()
        if val in ("—", "–", "-", "", "N/A"):
            return None
        mult = 1.0
        if "B" in val:
            mult = 1.0
            val  = val.replace("B", "")
        elif "M" in val:
            mult = 0.001
            val  = val.replace("M", "")
        elif "T" in val:
            mult = 1000.0
            val  = val.replace("T", "")
        val = val.replace("$", "").replace(",", "").strip()
        try:
            return float(val) * mult
        except Exception:
            return None
    if pd.isna(val):
        return None
    try:
        return float(val)
    except Exception:
        return None


def _extract_metric(
    df: pd.DataFrame,
    metric_name: str,
) -> dict[int, float]:
    """
    Extrae una métrica de la tabla.
    Returns dict {year: value}.
    """
    first_col = df.columns[0]

    # Buscar fila por nombre de métrica
    mask = df[first_col].astype(str).str.contains(
        metric_name, case=False, na=False
    )
    row = df[mask]

    if row.empty:
        return {}

    row_s  = row.iloc[0]
    result = {}

    for col in row_s.index:
        col_str = str(col).strip()
        # Solo columnas que sean años (4 dígitos)
        if col_str.isdigit() and len(col_str) == 4:
            v = _clean_value(row_s[col])
            if v is not None:
                result[int(col_str)] = v

    return result


def _fetch_income_statement(
    ticker: str,
    driver: "webdriver.Chrome",
    timeout: int = 15,
) -> pd.DataFrame | None:
    """
    Navega a PortfolioLabs y extrae el Income Statement.
    """
    url = (
        f"https://portfolioslab.com/symbol/"
        f"{ticker.upper()}"
    )

    try:
        driver.get(url)

        # Esperar a que cargue la tabla
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.TAG_NAME, "table")
            )
        )
        time.sleep(2)  # JS adicional

        soup = BeautifulSoup(
            driver.page_source, "html.parser"
        )

        # Buscar encabezado Income Statement
        header = soup.find(
            lambda tag: (
                tag.name in ("h2", "h3", "h4", "h5")
                and "Income Statement" in tag.get_text()
            )
        )

        if not header:
            # Intentar buscar directamente la tabla
            # con datos de revenue
            tables = soup.find_all("table")
            for table in tables:
                text = table.get_text()
                if "Revenue" in text or "EBIT" in text:
                    df = pd.read_html(
                        io.StringIO(str(table)),
                        header=0,
                    )[0]
                    return df
            return None

        table = header.find_next("table")
        if not table:
            return None

        df = pd.read_html(
            io.StringIO(str(table)), header=0
        )[0]
        return df

    except Exception as e:
        print(f"    Error PL {ticker}: {e}")
        return None


def fetch_portfoliolabs_batch(
    tickers: list[str],
    metrics: list[str] | None = None,
) -> dict[str, dict]:
    """
    Extrae datos de PortfolioLabs para una lista de tickers.
    Reutiliza un solo driver para todos (más eficiente).

    Args:
        tickers: lista de tickers del portfolio final
        metrics: métricas a extraer.
                 Default: ["Total Revenue", "EBIT",
                           "Net Income", "Gross Profit"]

    Returns:
        dict {ticker: {metric: {year: value}}}
    """
    if not SELENIUM_OK:
        print(
            "  ⚠ Selenium no instalado. "
            "Saltando PortfolioLabs."
        )
        return {}

    if metrics is None:
        metrics = [
            "Total Revenue",
            "Gross Profit",
            "EBIT",
            "Net Income",
        ]

    CACHE_PATH.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict] = {}
    driver  = None

    try:
        print(
            f"  Iniciando Selenium para "
            f"{len(tickers)} tickers..."
        )
        driver = _make_driver()

        for i, ticker in enumerate(tickers):
            print(
                f"  [{i+1}/{len(tickers)}] "
                f"PortfolioLabs: {ticker}"
            )

            # Caché por ticker (persiste entre runs)
            cache_file = CACHE_PATH / f"{ticker}.json"
            if cache_file.exists():
                try:
                    cached = json.load(open(cache_file))
                    results[ticker] = cached
                    print(f"    (desde caché)")
                    continue
                except Exception:
                    pass

            df = _fetch_income_statement(ticker, driver)

            if df is None:
                results[ticker] = {"_error": "no_data"}
                print(f"    Sin datos")
                continue

            ticker_data: dict[str, dict] = {}
            for metric in metrics:
                values = _extract_metric(df, metric)
                if values:
                    ticker_data[metric] = values

            results[ticker] = ticker_data

            # Guardar caché (válido 7 días — datos trimestrales)
            with open(cache_file, "w") as f:
                json.dump(ticker_data, f, indent=2)

            print(
                f"    OK: "
                f"{list(ticker_data.keys())}"
            )
            time.sleep(1.5)  # Pausa entre requests

    except Exception as e:
        print(f"  Error Selenium: {e}")
    finally:
        if driver:
            driver.quit()

    return results


def compare_with_yfinance(
    ticker:         str,
    yf_data:        dict,
    pl_data:        dict,
) -> dict:
    """
    Compara datos de Yahoo Finance con PortfolioLabs.
    Devuelve dict con divergencias encontradas.
    """
    divergences = {}

    # Revenue growth: comparar con el año más reciente de PL
    pl_revenue = pl_data.get("Total Revenue", {})
    if pl_revenue and len(pl_revenue) >= 2:
        years      = sorted(pl_revenue.keys())
        latest_yr  = years[-1]
        prev_yr    = years[-2]
        v_latest   = pl_revenue[latest_yr]
        v_prev     = pl_revenue[prev_yr]

        if v_prev and v_prev != 0:
            pl_growth = (v_latest - v_prev) / abs(v_prev)
            yf_growth = yf_data.get("revenue_growth") or 0

            diff = abs(pl_growth - yf_growth)
            if diff > 0.05:   # divergencia > 5%
                divergences["revenue_growth"] = {
                    "yfinance":      round(yf_growth, 4),
                    "portfoliolabs": round(pl_growth, 4),
                    "diff":          round(diff,      4),
                }

    # EBIT margins
    pl_ebit    = pl_data.get("EBIT",          {})
    pl_revenue = pl_data.get("Total Revenue", {})
    if pl_ebit and pl_revenue:
        years = sorted(
            set(pl_ebit.keys()) & set(pl_revenue.keys())
        )
        if years:
            yr  = years[-1]
            ebit_v = pl_ebit.get(yr,    0) or 0
            rev_v  = pl_revenue.get(yr, 0) or 0
            if rev_v and rev_v != 0:
                pl_margin = ebit_v / rev_v
                yf_margin = (
                    yf_data.get("operating_margins") or 0
                )
                diff = abs(pl_margin - yf_margin)
                if diff > 0.03:  # divergencia > 3%
                    divergences["operating_margins"] = {
                        "yfinance":      round(yf_margin, 4),
                        "portfoliolabs": round(pl_margin, 4),
                        "diff":          round(diff,      4),
                    }

    return divergences


def enrich_with_portfoliolabs(
    portfolio_tickers: list[str],
    fundamentals:      dict[str, dict],
) -> dict[str, dict]:
    """
    Pipeline completo de enriquecimiento.
    1. Descarga datos de PortfolioLabs
    2. Compara con Yahoo Finance
    3. Añade divergencias a los fundamentales
    4. Devuelve fundamentales enriquecidos

    Diseñado para ejecutarse DESPUÉS de la selección
    final (solo ~20 tickers).
    """
    if not SELENIUM_OK:
        print(
            "  Selenium no disponible, "
            "saltando enriquecimiento PL"
        )
        return fundamentals

    print(
        f"\n🔍 Enriqueciendo {len(portfolio_tickers)} "
        f"posiciones con PortfolioLabs..."
    )

    pl_data = fetch_portfoliolabs_batch(portfolio_tickers)

    enriched = dict(fundamentals)  # copia superficial

    for ticker in portfolio_tickers:
        pl = pl_data.get(ticker, {})
        yf = fundamentals.get(ticker, {})

        if not pl or "_error" in pl:
            continue

        divergences = compare_with_yfinance(
            ticker, yf, pl
        )

        # Añadir datos PL y divergencias al dict
        enriched[ticker] = {
            **yf,
            "_portfoliolabs":  pl,
            "_pl_divergences": divergences,
        }

        if divergences:
            div_str = ", ".join(
                f"{k}: YF={v['yfinance']:.2%} "
                f"vs PL={v['portfoliolabs']:.2%}"
                for k, v in divergences.items()
            )
            print(
                f"  ⚠ {ticker} divergencia: {div_str}"
            )
        else:
            print(f"  ✓ {ticker} datos consistentes")

    return enriched
