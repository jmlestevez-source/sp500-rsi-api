# src/universe.py

# (Elimina o comenta la lista FALLBACK_TICKERS si quieres,
# pero lo importante es el cambio en la función)

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
                print(f"  ✓ {len(comps)} tickers (API)")
                return comps
    except Exception as e:
        print(f"  Error método 2: {e}")

    # ── CAMBIO IMPORTANTE ──────────────────────────────
    # En lugar de usar fallback hardcoded, leer el archivo
    # existente para no perder los 1000 tickers
    if LOCAL_PATH.exists():
        try:
            data = json.load(open(LOCAL_PATH))
            tickers_local = data.get("tickers", [])
            if len(tickers_local) > 100:
                print(
                    "  ⚠ Wikipedia falló. "
                    "Usando archivo local anterior "
                    f"({len(tickers_local)} tickers)"
                )
                # Devolver los componentes guardados antes
                return data.get("components", [
                    {"ticker": t, "source": "local_cache"}
                    for t in tickers_local
                ])
        except Exception:
            pass

    # Solo si no hay nada guardado, usar fallback mínimo
    print(
        "  ✗ No se pudo descargar y no hay "
        "caché local. Usando fallback mínimo."
    )
    return _get_fallback_components() # Solo 20 tickers
