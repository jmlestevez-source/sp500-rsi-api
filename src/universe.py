# src/universe.py
"""
Obtiene los tickers del Russell 1000 desde Wikipedia.
Corrige símbolos para compatibilidad con Yahoo Finance.
"""

import os
import io
import json
import base64
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

WIKI_URL   = "https://en.wikipedia.org/wiki/Russell_1000_Index"
TABLE_ID   = "constituents"
LOCAL_PATH = Path("data/universe/tickers.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,"
        "application/xml;q=0.9,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.5",
}

# Tickers que hay que eliminar completamente
# (clases duplicadas o no cotizables en Yahoo)
EXCLUDE_TICKERS = {
    "BF.A", "BF-A",
    "CWEN.A", "CWEN-A",
    "HEI.A", "HEI-A",
    "LEN.B", "LEN-B",
    "UHAL.B", "UHAL-B",
    "BRK.A", "BRK-A",
    "FOX",  # duplicado de FOXA
    "GOOG",  # duplicado de GOOGL
    "NWS",   # duplicado de NWSA
}

# Reemplazos específicos además del . → -
TICKER_FIXES = {
    "BRK.B":  "BRK-B",
    "BF.B":   "BF-B",
    "CWEN":   "CWEN",
}


def _fix_ticker(raw: str) -> str:
    """
    Corrige un ticker para Yahoo Finance.
    1. Aplica fixes específicos conocidos
    2. Reemplaza . por - (estándar Yahoo)
    3. Excluye tickers problemáticos
    """
    raw = raw.strip().upper()

    # Fix específico primero
    if raw in TICKER_FIXES:
        return TICKER_FIXES[raw]

    # Excluir
    if raw in EXCLUDE_TICKERS:
        return ""

    # Reemplazo genérico: . → - (Yahoo usa guiones)
    fixed = raw.replace(".", "-")

    # Excluir el resultado también
    if fixed in EXCLUDE_TICKERS:
        return ""

    return fixed


def _fetch_html(url: str, retries: int = 3) -> str:
    """Descarga HTML con User-Agent real."""
    for attempt in range(retries):
        try:
            resp = requests.get(
                url, headers=HEADERS, timeout=30
            )
            if resp.status_code == 200:
                return resp.text
            print(
                f"  HTTP {resp.status_code} "
                f"intento {attempt + 1}"
            )
        except Exception as e:
            print(f"  Error intento {attempt + 1}: {e}")

        time.sleep(2 ** attempt)

    raise Exception(
        f"No se pudo descargar {url} "
        f"tras {retries} intentos"
    )


def _parse_table(html: str) -> list[dict]:
    """Parsea la tabla de componentes."""
    try:
        from bs4 import BeautifulSoup
        soup  = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": TABLE_ID})

        if table is None:
            for t in soup.find_all("table"):
                hdrs = " ".join(
                    th.get_text(strip=True).lower()
                    for th in t.find_all("th")
                )
                if "symbol" in hdrs or "ticker" in hdrs:
                    table = t
                    break

        if table is None:
            raise ValueError("Tabla no encontrada")

        df = pd.read_html(
            io.StringIO(str(table)), flavor="lxml"
        )[0]
        return _df_to_components(df)

    except ImportError:
        pass

    tables = pd.read_html(
        io.StringIO(html),
        attrs={"id": TABLE_ID},
        flavor="lxml",
    )
    if not tables:
        raise ValueError("Tabla no encontrada")
    return _df_to_components(tables[0])


def _df_to_components(df: pd.DataFrame) -> list[dict]:
    """Convierte DataFrame a lista de componentes."""
    print(f"  Columnas: {list(df.columns)}")

    col_map: dict[str, str] = {}
    for col in df.columns:
        cl = str(col).lower().strip()
        if "symbol" in cl or "ticker" in cl:
            col_map["symbol"] = col
        elif "company" in cl or "security" in cl:
            if "company" not in col_map:
                col_map["company"] = col
        elif "sector" in cl and "sub" not in cl:
            col_map["sector"] = col
        elif "sub" in cl or "industry" in cl:
            col_map["sub_industry"] = col

    if "symbol" not in col_map:
        cols = list(df.columns)
        if len(cols) >= 2:
            col_map["symbol"]  = cols[1]
            col_map["company"] = cols[0]
        else:
            raise ValueError(
                f"No se encontró columna Symbol: "
                f"{list(df.columns)}"
            )

    today      = datetime.now().strftime("%Y-%m-%d")
    components = []
    seen       = set()

    for _, row in df.iterrows():
        raw = str(row.get(col_map["symbol"], "")).strip()
        if not raw or raw.lower() in ("nan", "symbol"):
            continue

        ticker = _fix_ticker(raw)
        if not ticker:
            continue
        if ticker in seen:
            continue
        seen.add(ticker)

        company = str(
            row.get(col_map.get("company", ""), "")
        ).strip()
        sector = str(
            row.get(col_map.get("sector", ""), "")
        ).strip()
        sub = str(
            row.get(col_map.get("sub_industry", ""), "")
        ).strip()

        components.append({
            "ticker":       ticker,
            "company":      company,
            "sector":       sector,
            "sub_industry": sub,
            "source":       "Russell1000_Wikipedia",
            "updated":      today,
        })

    return components


FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "BRK-B", "LLY", "AVGO", "TSLA",
    "WMT", "JPM", "V", "UNH", "XOM", "ORCL",
    "MA", "COST", "HD", "PG", "NFLX", "JNJ",
    "BAC", "ABBV", "CRM", "CVX", "MRK", "TMUS",
    "AMD", "PEP", "KO", "ACN", "LIN", "MCD",
    "TMO", "CSCO", "WFC", "IBM", "GE", "AXP",
    "PM", "CAT", "MS", "GS", "INTU", "ISRG",
    "RTX", "AMGN", "DHR", "SPGI", "T", "NOW",
    "UBER", "BKNG", "PFE", "TJX", "HON", "LOW",
    "VRTX", "C", "SCHW", "CMCSA", "BLK", "BSX",
    "ADP", "UNP", "CB", "SYK", "DE", "GILD",
    "BA", "ANET", "BMY", "MU", "REGN", "MDT",
    "CI", "PLD", "ADI", "PANW", "ETN", "SO",
    "DUK", "MMC", "ZTS", "ICE", "LRCX", "KLAC",
    "APH", "MCO", "CME", "AON", "SHW", "SNPS",
    "CDNS", "WM", "EMR", "CTAS", "FCX", "ITW",
    "TDG", "CEG", "PSA", "WELL", "HCA", "PH",
    "NSC", "FI", "ECL", "COF", "USB", "MPC",
    "EOG", "OKE", "TT", "ROP", "CARR", "AJG",
    "ALL", "DHI", "FAST", "VRSK", "ODFL", "NEM",
    "BDX", "MNST", "CTVA", "PCAR", "AXON", "GEHC",
    "SPG", "NXPI", "KMB", "CL", "EXC", "CPRT",
    "PWR", "HLT", "AME", "GWW", "TRGP", "FANG",
    "MSCI", "PAYX", "AZO", "LHX", "RSG", "PCG",
    "XEL", "EA", "KEYS", "OTIS", "IR", "LEN",
    "DOW", "PPG", "ANSS", "IQV", "DECK", "ULTA",
]


def scrape_russell_1000() -> list[dict]:
    """Descarga Russell 1000 con múltiples fallbacks."""
    print("  Descargando Russell 1000 de Wikipedia...")

    # Método 1: requests directo
    try:
        html = _fetch_html(WIKI_URL)
        comps = _parse_table(html)
        if len(comps) >= 50:
            print(f"  ✓ {len(comps)} tickers (requests)")
            return comps
    except Exception as e:
        print(f"  Error método 1: {e}")

    # Método 2: Wikipedia API
    try:
        print("  Probando Wikipedia API...")
        r = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action":    "parse",
                "page":      "Russell_1000_Index",
                "prop":      "text",
                "format":    "json",
                "redirects": "true",
            },
            headers=HEADERS,
            timeout=30,
        )
        if r.status_code == 200:
            html  = r.json()["parse"]["text"]["*"]
            comps = _parse_table(html)
            if len(comps) >= 50:
                print(
                    f"  ✓ {len(comps)} tickers "
                    f"(Wikipedia API)"
                )
                return comps
    except Exception as e:
        print(f"  Error método 2: {e}")

    # Método 3: Fallback hardcoded
    print("  ⚠ Usando lista fallback")
    today = datetime.now().strftime("%Y-%m-%d")
    return [
        {
            "ticker":       t,
            "company":      "",
            "sector":       "",
            "sub_industry": "",
            "source":       "fallback",
            "updated":      today,
        }
        for t in FALLBACK_TICKERS
    ]


def save_locally(components: list[dict]) -> None:
    LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated":    datetime.now().isoformat(),
        "source":     WIKI_URL,
        "count":      len(components),
        "components": components,
        "tickers":    [c["ticker"] for c in components],
    }
    with open(LOCAL_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  ✓ Guardado local: {LOCAL_PATH}")


def push_to_github(components: list[dict]) -> bool:
    token = os.getenv("GITHUB_TOKEN")
    repo  = os.getenv("GITHUB_REPO")
    if not token or not repo:
        print("  ⚠ GITHUB_TOKEN/REPO no configurados")
        return False

    file_path = "data/universe/tickers.json"
    api_url   = (
        f"https://api.github.com/repos/{repo}"
        f"/contents/{file_path}"
    )
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    payload = {
        "updated":    datetime.now().isoformat(),
        "source":     WIKI_URL,
        "count":      len(components),
        "components": components,
        "tickers":    [c["ticker"] for c in components],
    }
    content_b64 = base64.b64encode(
        json.dumps(payload, indent=2, ensure_ascii=False)
        .encode("utf-8")
    ).decode()

    sha = None
    r   = requests.get(api_url, headers=headers, timeout=30)
    if r.status_code == 200:
        sha = r.json().get("sha")

    body: dict = {
        "message": (
            f"Update Russell 1000 "
            f"{datetime.now().strftime('%Y-%m-%d')}"
        ),
        "content": content_b64,
    }
    if sha:
        body["sha"] = sha

    r2 = requests.put(
        api_url, headers=headers, json=body, timeout=30
    )
    if r2.status_code in (200, 201):
        print(f"  ✓ GitHub actualizado")
        return True
    print(f"  Error GitHub: {r2.status_code}")
    return False


def update_universe() -> list[str]:
    print("\nActualizando universo Russell 1000...")
    components = scrape_russell_1000()
    save_locally(components)
    push_to_github(components)
    tickers = [c["ticker"] for c in components]
    print(f"  Universo final: {len(tickers)} tickers\n")
    return tickers


def load_universe() -> list[str]:
    if LOCAL_PATH.exists():
        try:
            data     = json.load(open(LOCAL_PATH))
            updated  = datetime.fromisoformat(
                data.get("updated", "2000-01-01")
            )
            age_days = (datetime.now() - updated).days
            tickers  = data.get("tickers", [])

            if tickers and age_days < 7:
                print(
                    f"  Universo local: {len(tickers)} "
                    f"tickers ({age_days}d)"
                )
                return tickers
        except Exception:
            pass

    return update_universe()
