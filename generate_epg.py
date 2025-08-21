import os
import re
import gzip
import json
import asyncio
import aiohttp
import unicodedata
import lxml.etree as ET

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY no está definido en el entorno.")

CACHE_FILE = "tmdb_cache.json"

CANALES_USAR = {
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&.Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.Europa.Europa.mx",
    "Canal.Film.&.Arts.mx",
    "Canal.FX.(México).mx",
    "Canal.HBO.2.Latinoamérica.mx",
    "Canal.HBO.Family.Latinoamérica.mx",
    "Canal.HBO.(México).mx",
    "Canal.HBO.Mundi.mx",
    "Canal.HBO.Plus.mx",
    "Canal.HBO.Pop.mx",
    "Canal.HBO.Signature.Latinoamérica.mx",
    "Canal.Investigation.Discovery.(México).mx",
    "Canal.Lifetime.(México).mx",
    "Canal.MTV.00s.mx",
    "Canal.MTV.Hits.mx",
    "Canal.National.Geographic.(México).mx",
    "Canal.Pánico.mx",
    "Canal.Paramount.Channel.(México).mx",
    "Canal.Space.(México).mx",
    "Canal.Sony.(México).mx",
    "Canal.Star.Channel.(México).mx",
    "Canal.Studio.Universal.(México).mx",
    "Canal.TNT.(México).mx",
    "Canal.TNT.Series.(México).mx",
    "Canal.Universal.TV.(México).mx",
    "Canal.USA.Network.(México).mx",
    "Canal.Warner.TV.(México).mx",
}

# ----------------------
# UTILIDADES
# ----------------------
def normalizar_texto(texto):
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.strip()

def parse_episode_num(texto):
    if not texto:
        return None, None
    match = re.search(r"S(\d{1,2})E(\d{1,2})", texto, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(\d{1,2})[xE](\d{1,2})", texto)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(?:SPECIAL|ESPECIAL)[\s-]?(\d+)", texto)
    if match:
        return 0, int(match.group(1))
    return None, None

# ----------------------
# CACHÉ TMDB
# ----------------------
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        TMDB_CACHE = json.load(f)
else:
    TMDB_CACHE = {}

async def fetch_tmdb(session, url, params):
    try:
        async with session.get(url, params=params, timeout=15) as resp:
            if resp.status == 200:
                return await resp.json()
    except:
        return None
    return None

async def buscar_info(session, titulo, year=None, es_serie=False, temporada=None, episodio=None):
    key = f"{titulo}_{year}_{es_serie}_{temporada}_{episodio}"
    if key in TMDB_CACHE:
        return TMDB_CACHE[key]

    tipo = "tv" if es_serie else "movie"
    base_url = f"https://api.themoviedb.org/3/search/{tipo}"
    params = {"api_key": API_KEY, "query": titulo, "language": "es-MX"}
    if year:
        params["year"] = year

    data = await fetch_tmdb(session, base_url, params)
    results = data.get("results", []) if data else []

    if not results:
        params["language"] = "en-US"
        data = await fetch_tmdb(session, base_url, params)
        results = data.get("results", []) if data else []

    if not results:
        return None

    info = results[0]

    result = {
        "titulo": info.get("name") or info.get("title") or titulo,
        "anio": (info.get("first_air_date") or info.get("release_date") or "????")[:4],
        "descripcion": info.get("overview") or "Sin descripción."
    }

    if es_serie and temporada and episodio:
        ep_url = f"https://api.themoviedb.org/3/tv/{info['id']}/season/{temporada}/episode/{episodio}"
        ep_data = await fetch_tmdb(session, ep_url, {"api_key": API_KEY, "language": "es-MX"})
        if not ep_data:
            ep_data = await fetch_tmdb(session, ep_url, {"api_key": API_KEY, "language": "en-US"})
        if ep_data:
            epi_titulo = ep_data.get("name") or f"S{temporada:02d}E{episodio:02d}"
            epi_desc = ep_data.get("overview") or result["descripcion"]
            result["titulo"] = f"{result['titulo']} (S{int(temporada):02d}E{int(episodio):02d}) - {epi_titulo}"
            result["descripcion"] = epi_desc

    TMDB_CACHE[key] = result
    return result

async def guardar_cache():
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(TMDB_CACHE, f, ensure_ascii=False, indent=2)

# ----------------------
# PROCESAMIENTO EPG
# ----------------------
async def procesar_programa(session, elem):
    canal = elem.get("channel")
    if canal not in CANALES_USAR:
        return ET.tostring(elem, encoding="utf-8")

    title_el = elem.find("title")
    titulo = normalizar_texto(title_el.text) if title_el is not None else "Sin título"

    year_match = re.search(r"\((\d{4})\)", titulo)
    year = year_match.group(1) if year_match else None

    season, episode = parse_episode_num(titulo)
    es_serie = bool(season and episode)

    info = await buscar_info(session, titulo, year, es_serie, season, episode)
    if info:
        if title_el is not None:
            title_el.text = f"{info['titulo']} ({info['anio']})" if not es_serie else info['titulo']
        desc_el = elem.find("desc")
        if desc_el is None:
            desc_el = ET.SubElement(elem, "desc")
        desc_el.text = info["descripcion"]

    return ET.tostring(elem, encoding="utf-8")

async def procesar_epg(input_file, output_file):
    context = ET.iterparse(input_file, events=("end",), tag="programme")
    async with aiohttp.ClientSession() as session:
        with gzip.open(output_file, "wb") as f_out:
            f_out.write(b'<?xml version="1.0" encoding="utf-8"?>\n<tv>\n')

            batch = []
            for _, elem in context:
                batch.append(procesar_programa(session, elem))
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

                if len(batch) >= 50:
                    results = await asyncio.gather(*batch)
                    for r in results:
                        f_out.write(r)
                    batch = []

            if batch:
                results = await asyncio.gather(*batch)
                for r in results:
                    f_out.write(r)

            f_out.write(b"</tv>")
    await guardar_cache()

# ----------------------
# MAIN
# ----------------------
if __name__ == "__main__":
    input_file = "guide.xml"
    output_file = "guide_custom.xml.gz"
    asyncio.run(procesar_epg(input_file, output_file))
    print(f"✅ Guía generada y comprimida: {output_file}")
