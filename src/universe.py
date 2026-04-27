# src/universe.py
"""
Obtiene los tickers del Russell 1000 desde Wikipedia
y los sube automáticamente a GitHub.

Solución al 403: usa requests con User-Agent real
antes de pasar el HTML a pandas.
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

WIKI_URL   = (
    "https://en.wikipedia.org/wiki/"
    "Russell_1000_Index"
)
TABLE_ID   = "constituents"
LOCAL_PATH = Path("data/universe/tickers.json")

# Correcciones de símbolos conocidos
TICKER_FIXES = {
    "BRK.B": "BRK-B",
    "BF.B":  "BF-B",
    "BRK.A": "BRK-A",
}

# Headers que simulan un navegador real
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
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Cache-Control":   "max-age=0",
}


# ── Descarga HTML con requests ────────────────────────────────────────────────

def _fetch_html(url: str, retries: int = 3) -> str:
    """
    Descarga HTML usando requests con User-Agent real.
    Reintenta con backoff exponencial.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.text
            print(
                f"  HTTP {resp.status_code} "
                f"en intento {attempt + 1}"
            )
        except Exception as e:
            print(
                f"  Error intento {attempt + 1}: {e}"
            )

        wait = 2 ** attempt
        print(f"  Esperando {wait}s...")
        time.sleep(wait)

    raise Exception(
        f"No se pudo descargar {url} "
        f"tras {retries} intentos"
    )


# ── Parser de tabla ───────────────────────────────────────────────────────────

def _parse_table(html: str) -> list[dict]:
    """
    Parsea la tabla 'constituents' del HTML.
    Intenta por id primero, luego por contenido.
    """
    # Intentar con BeautifulSoup para más control
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        # Buscar tabla por id
        table = soup.find("table", {"id": TABLE_ID})

        if table is None:
            # Fallback: buscar tabla que contenga
            # columnas típicas del Russell 1000
            for t in soup.find_all("table"):
                headers = [
                    th.get_text(strip=True).lower()
                    for th in t.find_all("th")
                ]
                header_str = " ".join(headers)
                if (
                    "symbol" in header_str
                    or "ticker" in header_str
                ):
                    table = t
                    break

        if table is None:
            raise ValueError(
                "Tabla no encontrada en el HTML"
            )

        df = pd.read_html(
            io.StringIO(str(table)),
            flavor="lxml",
        )[0]
        return _dataframe_to_components(df)

    except ImportError:
        # Si no hay BeautifulSoup, usar pandas directo
        pass

    # Fallback: pandas sobre el HTML completo
    all_tables = pd.read_html(
        io.StringIO(html),
        attrs={"id": TABLE_ID},
        flavor="lxml",
    )
    if not all_tables:
        raise ValueError(
            "No se encontró tabla con "
            f"id='{TABLE_ID}'"
        )
    return _dataframe_to_components(all_tables[0])


def _dataframe_to_components(
    df: pd.DataFrame,
) -> list[dict]:
    """
    Convierte DataFrame de Wikipedia a lista de dicts.
    Detecta las columnas automáticamente.
    """
    print(f"  Columnas detectadas: {list(df.columns)}")

    # Mapear columnas
    col_map: dict[str, str] = {}
    for col in df.columns:
        cl = str(col).lower().strip()
        if "symbol" in cl or "ticker" in cl:
            col_map["symbol"] = col
        elif (
            "company" in cl
            or "security" in cl
            or "name" in cl
        ):
            if "company" not in col_map:
                col_map["company"] = col
        elif "sector" in cl and "sub" not in cl:
            col_map["sector"] = col
        elif "sub" in cl or "industry" in cl:
            col_map["sub_industry"] = col

    if "symbol" not in col_map:
        # Última opción: asumir que la segunda columna
        # es el símbolo (estructura típica Wikipedia)
        cols = list(df.columns)
        if len(cols) >= 2:
            col_map["symbol"]  = cols[1]
            col_map["company"] = cols[0]
            print(
                f"  ⚠ Symbol no encontrado, "
                f"asumiendo columna: {cols[1]}"
            )
        else:
            raise ValueError(
                f"Columna Symbol no encontrada. "
                f"Columnas disponibles: "
                f"{list(df.columns)}"
            )

    today      = datetime.now().strftime("%Y-%m-%d")
    components = []

    for _, row in df.iterrows():
        raw_ticker = str(
            row.get(col_map["symbol"], "")
        ).strip()

        if not raw_ticker or raw_ticker == "nan":
            continue

        # Aplicar correcciones conocidas
        ticker = TICKER_FIXES.get(
            raw_ticker, raw_ticker
        )

        # Ignorar filas de encabezado repetidas
        if ticker.lower() in (
            "symbol", "ticker", "nan"
        ):
            continue

        company = str(
            row.get(col_map.get("company", ""), "")
        ).strip()

        sector = str(
            row.get(col_map.get("sector", ""), "")
        ).strip()

        sub_industry = str(
            row.get(col_map.get("sub_industry", ""), "")
        ).strip()

        components.append({
            "ticker":       ticker,
            "company":      company,
            "sector":       sector,
            "sub_industry": sub_industry,
            "source":       "Russell1000_Wikipedia",
            "updated":      today,
        })

    return components


# ── Fallback: lista hardcoded de Russell 1000 ─────────────────────────────────

RUSSELL_1000_FALLBACK = [
    # Top 100 por capitalización como fallback mínimo
    # Se usa solo si Wikipedia no está disponible
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "GOOG",
    "META", "BRK-B", "LLY", "AVGO", "TSLA", "WMT",
    "JPM", "V", "UNH", "XOM", "ORCL", "MA", "COST",
    "HD", "PG", "NFLX", "JNJ", "BAC", "ABBV", "CRM",
    "CVX", "MRK", "TMUS", "AMD", "PEP", "KO", "ACN",
    "LIN", "MCD", "TMO", "CSCO", "WFC", "IBM", "GE",
    "AXP", "PM", "CAT", "MS", "GS", "INTU", "ISRG",
    "RTX", "AMGN", "DHR", "SPGI", "T", "NOW", "UBER",
    "BKNG", "PFE", "TJX", "HON", "LOW", "VRTX", "C",
    "SCHW", "CMCSA", "BLK", "BSX", "ADP", "UNP", "CB",
    "SYK", "DE", "GILD", "BA", "ANET", "BMY", "MU",
    "REGN", "MDT", "CI", "PLD", "ADI", "PANW", "ETN",
    "SO", "DUK", "MMC", "ZTS", "ICE", "LRCX", "KLAC",
    "APH", "MCO", "CME", "AON", "SHW", "SNPS", "CDNS",
    "WM", "EMR", "GEV", "CTAS", "FCX", "ITW", "TDG",
    "CEG", "PSA", "WELL", "HCA", "PH", "NSC", "FI",
    "ECL", "COF", "USB", "MPC", "EOG", "OKE", "TT",
    "ROP", "FICO", "CARR", "AJG", "ALL", "DHI", "FAST",
    "VRSK", "ODFL", "NEM", "BDX", "MNST", "CTVA",
    "PCAR", "AXON", "GEHC", "EW", "SPG", "NXPI",
    "KMB", "CL", "EXC", "ACGL", "CPRT", "PWR", "HLT",
    "MCHP", "MLM", "IDXX", "AME", "DXCM", "GWW",
    "TRGP", "FANG", "MSCI", "CSGP", "PAYX", "AZO",
    "LHX", "RSG", "PCG", "XEL", "EA", "KEYS", "MPWR",
    "WTW", "OTIS", "IR", "LEN", "TTWO", "DOW", "PPG",
    "ANSS", "IQV", "SBAC", "DECK", "ULTA", "WST",
    "RMD", "EPAM", "PODD", "TER", "ZBRA", "POOL",
    "MTCH", "TECH", "SWKS", "AKAM", "NLOK", "QRVO",
]


def _get_fallback_components() -> list[dict]:
    """
    Devuelve componentes mínimos desde lista hardcoded.
    Solo se usa si Wikipedia falla completamente.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    return [
        {
            "ticker":       t,
            "company":      "",
            "sector":       "",
            "sub_industry": "",
            "source":       "fallback_hardcoded",
            "updated":      today,
        }
        for t in RUSSELL_1000_FALLBACK
    ]


# ── Pipeline principal ────────────────────────────────────────────────────────

def scrape_russell_1000() -> list[dict]:
    """
    Descarga y parsea el Russell 1000 desde Wikipedia.
    Estrategia:
    1. requests + BeautifulSoup (método principal)
    2. requests + pandas read_html (fallback)
    3. Lista hardcoded (emergencia)
    """
    print("  Descargando Russell 1000 de Wikipedia...")

    # ── Método 1: requests con User-Agent real ─────────
    try:
        html       = _fetch_html(WIKI_URL)
        components = _parse_table(html)

        if len(components) >= 50:
            print(
                f"  ✓ {len(components)} tickers "
                f"extraídos (método requests)"
            )
            return components
        else:
            print(
                f"  Solo {len(components)} tickers, "
                f"reintentando..."
            )
    except Exception as e:
        print(f"  Error método 1: {e}")

    # ── Método 2: URL alternativa (sección directa) ────
    alt_url = (
        "https://en.wikipedia.org/wiki/"
        "Russell_1000_Index#Components"
    )
    try:
        print(f"  Probando URL alternativa...")
        html       = _fetch_html(alt_url)
        components = _parse_table(html)

        if len(components) >= 50:
            print(
                f"  ✓ {len(components)} tickers "
                f"(método URL alternativa)"
            )
            return components
    except Exception as e:
        print(f"  Error método 2: {e}")

    # ── Método 3: API de Wikipedia ─────────────────────
    try:
        print("  Probando Wikipedia API...")
        api_url = (
            "https://en.wikipedia.org/w/api.php"
        )
        params = {
            "action":      "parse",
            "page":        "Russell_1000_Index",
            "prop":        "text",
            "format":      "json",
            "redirects":   "true",
        }
        resp = requests.get(
            api_url,
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        if resp.status_code == 200:
            data    = resp.json()
            html    = data["parse"]["text"]["*"]
            components = _parse_table(html)

            if len(components) >= 50:
                print(
                    f"  ✓ {len(components)} tickers "
                    f"(Wikipedia API)"
                )
                return components
    except Exception as e:
        print(f"  Error método 3 (API): {e}")

    # ── Método 4: Lista hardcoded (emergencia) ─────────
    print(
        "  ⚠ Wikipedia no disponible. "
        "Usando lista hardcoded de emergencia."
    )
    components = _get_fallback_components()
    print(
        f"  Fallback: {len(components)} tickers"
    )
    return components


def save_locally(components: list[dict]) -> None:
    """Guarda el universo en data/universe/tickers.json"""
    LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "updated":    datetime.now().isoformat(),
        "source":     WIKI_URL,
        "count":      len(components),
        "components": components,
        "tickers":    [c["ticker"] for c in components],
    }

    with open(LOCAL_PATH, "w", encoding="utf-8") as f:
        json.dump(
            payload, f,
            indent=2, ensure_ascii=False,
        )

    print(f"  ✓ Guardado local: {LOCAL_PATH}")


def push_to_github(
    components: list[dict],
) -> bool:
    """
    Sube/actualiza data/universe/tickers.json en GitHub
    usando la API REST.
    Requiere GITHUB_TOKEN y GITHUB_REPO en .env
    """
    token = os.getenv("GITHUB_TOKEN")
    repo  = os.getenv("GITHUB_REPO")

    if not token or not repo:
        print(
            "  ⚠ GITHUB_TOKEN o GITHUB_REPO "
            "no configurados. "
            "Solo se guarda localmente."
        )
        return False

    file_path = "data/universe/tickers.json"
    api_url   = (
        f"https://api.github.com/repos/{repo}"
        f"/contents/{file_path}"
    )
    headers = {
        "Authorization": f"token {token}",
        "Accept": (
            "application/vnd.github.v3+json"
        ),
    }

    payload = {
        "updated":    datetime.now().isoformat(),
        "source":     WIKI_URL,
        "count":      len(components),
        "components": components,
        "tickers":    [c["ticker"] for c in components],
    }

    content_bytes = json.dumps(
        payload,
        indent=2,
        ensure_ascii=False,
    ).encode("utf-8")
    content_b64 = base64.b64encode(
        content_bytes
    ).decode()

    # Obtener SHA del archivo actual
    sha = None
    r   = requests.get(
        api_url, headers=headers, timeout=30
    )
    if r.status_code == 200:
        sha = r.json().get("sha")
        print(
            f"  Archivo GitHub existente, "
            f"SHA: {sha[:8]}..."
        )
    elif r.status_code == 404:
        print("  Archivo GitHub no existe, se creará.")
    else:
        print(
            f"  Error obteniendo SHA: "
            f"{r.status_code}"
        )
        return False

    body: dict = {
        "message": (
            f"Update Russell 1000 universe "
            f"{datetime.now().strftime('%Y-%m-%d')}"
        ),
        "content": content_b64,
    }
    if sha:
        body["sha"] = sha

    r2 = requests.put(
        api_url,
        headers=headers,
        json=body,
        timeout=30,
    )

    if r2.status_code in (200, 201):
        print(f"  ✓ GitHub actualizado: {file_path}")
        return True
    else:
        print(
            f"  Error GitHub: {r2.status_code} "
            f"{r2.text[:200]}"
        )
        return False


def update_universe() -> list[str]:
    """
    Pipeline completo:
    1. Scraping Wikipedia (con fallbacks)
    2. Guarda local
    3. Sube a GitHub
    Devuelve lista de tickers.
    """
    print("\nActualizando universo Russell 1000...")

    components = scrape_russell_1000()
    save_locally(components)
    push_to_github(components)

    tickers = [c["ticker"] for c in components]
    print(f"  Universo final: {len(tickers)} tickers\n")
    return tickers


def load_universe() -> list[str]:
    """
    Carga el universo local.
    Si no existe o tiene más de 7 días, lo actualiza.
    """
    if LOCAL_PATH.exists():
        try:
            data    = json.load(open(LOCAL_PATH))
            updated = datetime.fromisoformat(
                data.get("updated", "2000-01-01")
            )
            age_days = (datetime.now() - updated).days
            tickers  = data.get("tickers", [])

            if tickers and age_days < 7:
                print(
                    f"  Universo local OK: "
                    f"{len(tickers)} tickers "
                    f"(actualizado hace {age_days}d)"
                )
                return tickers
            else:
                print(
                    f"  Universo local de {age_days}d, "
                    f"actualizando..."
                )
        except Exception as e:
            print(
                f"  Error leyendo universo local: {e}"
            )

    return update_universe()
