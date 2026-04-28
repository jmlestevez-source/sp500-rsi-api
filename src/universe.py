# src/universe.py
"""
Obtiene los tickers del Russell 1000 desde Wikipedia.
Corrige símbolos para compatibilidad con Yahoo Finance.
Si Wikipedia falla, usa el archivo local anterior.
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

# Tickers a excluir (clases duplicadas o no cotizables)
EXCLUDE_TICKERS = {
    "BF.A",   "BF-A",
    "CWEN.A", "CWEN-A",
    "HEI.A",  "HEI-A",
    "LEN.B",  "LEN-B",
    "UHAL.B", "UHAL-B",
    "BRK.A",  "BRK-A",
    "FOX",
    "GOOG",
    "NWS",
}

# Correcciones específicas de símbolo
TICKER_FIXES = {
    "BRK.B": "BRK-B",
    "BF.B":  "BF-B",
}

# Fallback mínimo solo si no hay NADA guardado
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


# ── Utilidades ────────────────────────────────────────────────────────────────

def _fix_ticker(raw: str) -> str:
    """
    Corrige un ticker para Yahoo Finance.
    1. Aplica fixes específicos conocidos
    2. Reemplaza . por - (estándar Yahoo)
    3. Excluye tickers problemáticos
    Devuelve "" si debe excluirse.
    """
    raw = raw.strip().upper()

    if raw in TICKER_FIXES:
        return TICKER_FIXES[raw]

    if raw in EXCLUDE_TICKERS:
        return ""

    # Reemplazo genérico: punto → guion
    fixed = raw.replace(".", "-")

    if fixed in EXCLUDE_TICKERS:
        return ""

    return fixed


def _fetch_html(url: str, retries: int = 3) -> str:
    """Descarga HTML con User-Agent real."""
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
                f"intento {attempt + 1}"
            )
        except Exception as e:
            print(
                f"  Error intento {attempt + 1}: {e}"
            )
        time.sleep(2 ** attempt)

    raise Exception(
        f"No se pudo descargar {url} "
        f"tras {retries} intentos"
    )


def _parse_table(html: str) -> list[dict]:
    """Parsea la tabla de componentes del HTML."""
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

    # Fallback sin BeautifulSoup
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
            print(
                f"  ⚠ Asumiendo Symbol en "
                f"columna: {cols[1]}"
            )
        else:
            raise ValueError(
                f"No se encontró columna Symbol: "
                f"{list(df.columns)}"
            )

    today      = datetime.now().strftime("%Y-%m-%d")
    components = []
    seen       = set()

    for _, row in df.iterrows():
        raw = str(
            row.get(col_map["symbol"], "")
        ).strip()

        if not raw or raw.lower() in (
            "nan", "symbol", "ticker"
        ):
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
            row.get(
                col_map.get("sub_industry", ""), ""
            )
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


def _get_fallback_components() -> list[dict]:
    """Lista mínima de emergencia."""
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
        for t in FALLBACK_TICKERS
    ]


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_russell_1000() -> list[dict]:
    """
    Descarga Russell 1000 con múltiples métodos.
    Si Wikipedia falla, usa el archivo local anterior.
    Solo usa el fallback hardcoded si no hay nada.
    """
    print("  Descargando Russell 1000 de Wikipedia...")

    # Método 1: requests directo
    try:
        html  = _fetch_html(WIKI_URL)
        comps = _parse_table(html)
        if len(comps) >= 50:
            print(
                f"  ✓ {len(comps)} tickers "
                f"(requests directo)"
            )
            return comps
        print(
            f"  Solo {len(comps)} tickers, "
            f"reintentando..."
        )
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

    # Método 3: Usar archivo local anterior
    # (PRIORIDAD sobre fallback hardcoded)
    if LOCAL_PATH.exists():
        try:
            data    = json.load(open(LOCAL_PATH))
            tickers = data.get("tickers", [])
            comps   = data.get("components", [])

            if len(tickers) > 100:
                print(
                    f"  ⚠ Wikipedia no disponible. "
                    f"Usando archivo local anterior "
                    f"({len(tickers)} tickers)."
                )
                # Si no hay components, reconstruir
                if not comps:
                    today = datetime.now().strftime(
                        "%Y-%m-%d"
                    )
                    comps = [
                        {
                            "ticker":  t,
                            "company": "",
                            "sector":  "",
                            "source":  "local_cache",
                            "updated": today,
                        }
                        for t in tickers
                    ]
                return comps
        except Exception as e:
            print(
                f"  Error leyendo archivo local: {e}"
            )

    # Método 4: Fallback hardcoded (último recurso)
    print(
        "  ✗ Sin datos de Wikipedia ni archivo local. "
        "Usando fallback mínimo."
    )
    return _get_fallback_components()


# ── Guardar y publicar ────────────────────────────────────────────────────────

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

    print(
        f"  ✓ Guardado local: {LOCAL_PATH} "
        f"({len(components)} tickers)"
    )


def push_to_github(components: list[dict]) -> bool:
    """Sube tickers.json a GitHub via API REST."""
    token = os.getenv("GITHUB_TOKEN")
    repo  = os.getenv("GITHUB_REPO")

    if not token or not repo:
        print(
            "  ⚠ GITHUB_TOKEN o GITHUB_REPO "
            "no configurados."
        )
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
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8")
    ).decode()

    # Obtener SHA si el archivo ya existe
    sha = None
    r   = requests.get(
        api_url, headers=headers, timeout=30
    )
    if r.status_code == 200:
        sha = r.json().get("sha")
        print(f"  SHA existente: {sha[:8]}...")
    elif r.status_code == 404:
        print("  Archivo nuevo en GitHub.")
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
        print(
            f"  ✓ GitHub actualizado: {file_path}"
        )
        return True

    print(
        f"  Error GitHub: {r2.status_code} "
        f"{r2.text[:200]}"
    )
    return False


# ── API pública ───────────────────────────────────────────────────────────────

def update_universe() -> list[str]:
    """
    Pipeline completo:
    1. Scraping Wikipedia (con fallbacks)
    2. Guarda local
    3. Sube a GitHub
    Devuelve lista plana de tickers.
    """
    print("\nActualizando universo Russell 1000...")

    components = scrape_russell_1000()
    save_locally(components)
    push_to_github(components)

    tickers = [c["ticker"] for c in components]
    print(
        f"  Universo final: {len(tickers)} tickers\n"
    )
    return tickers


def load_universe() -> list[str]:
    """
    Carga el universo local.
    - Si no existe → lo descarga
    - Si tiene < 100 tickers → lo descarga (corrupto)
    - Si tiene > 7 días → lo actualiza
    - Si está OK → lo devuelve directamente
    """
    if LOCAL_PATH.exists():
        try:
            data     = json.load(open(LOCAL_PATH))
            tickers  = data.get("tickers", [])
            updated  = datetime.fromisoformat(
                data.get("updated", "2000-01-01")
            )
            age_days = (datetime.now() - updated).days

            # Archivo corrupto (fallback guardado)
            if len(tickers) < 100:
                print(
                    f"  ⚠ Universo local tiene solo "
                    f"{len(tickers)} tickers "
                    f"(posiblemente corrupto). "
                    f"Actualizando..."
                )
                return update_universe()

            # Archivo antiguo
            if age_days >= 7:
                print(
                    f"  Universo local de {age_days}d, "
                    f"actualizando..."
                )
                return update_universe()

            # Archivo OK
            print(
                f"  Universo local OK: "
                f"{len(tickers)} tickers "
                f"(actualizado hace {age_days}d)"
            )
            return tickers

        except Exception as e:
            print(
                f"  Error leyendo universo local: {e}. "
                f"Descargando..."
            )

    return update_universe()
