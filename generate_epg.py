import os
import re
import gzip
import requests
import unicodedata
import lxml.etree as ET
import io
from bs4 import BeautifulSoup
import openai

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY no está definido en el entorno.")

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
EPG_FILE = "epg_original.xml"
OUTPUT_FILE = "guide_custom.xml"

NUEVAS_EPGS = [
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

EPG_FILES_TEMP = []

# ----------------------
# CANALES
# ----------------------
CANALES_USAR = {
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&amp;E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&amp;.Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.Europa.Europa.mx",
    "Canal.Film.&amp;.Arts.mx",
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
    # Canales internacionales
    "plex.tv.T2.plex",
    "TSN1.ca",
    "TSN2.ca",
    "TSN3.ca",
    "TSN4.ca",
    "Eurosport.2.es",
    "Eurosport.es",
    "M+.Deportes.2.es",
    "M+.Deportes.3.es",
    "M+.Deportes.4.es",
    "M+.Deportes.5.es",
    "M+.Deportes.6.es",
    "M+.Deportes.7.es",
    "M+.Deportes.es",
    "Movistar.Plus.es",
    "ABC.(WABC).New.York,.NY.us",
    "CBS.(WCBS).New.York,.NY.us",
    "FOX.(WNYW).New.York,.NY.us",
    "NBC.(WNBC).New.York,.NY.us",
    "ABC.(KABC).Los.Angeles,.CA.us",
    "NBC.(KNBC).Los.Angeles,.CA.us",
    "Bravo.USA.-.Eastern.Feed.us",
    "E!.Entertainment.USA.-.Eastern.Feed.us",
    "Hallmark.-.Eastern.Feed.us",
    "Hallmark.Mystery.Eastern.-.HD.us",
    "CW.(KFMB-TV2).San.Diego,.CA.us",
    "CNN.us",
    "The.Tennis.Channel.us",
    "HBO.-.Eastern.Feed.us",
    "HBO.Latino.(HBO.7).-.Eastern.us",
    "HBO.2.-.Eastern.Feed.us",
    "HBO.Comedy.HD.-.East.us",
    "HBO.Family.-.Eastern.Feed.us",
    "HBO.Signature.(HBO.3).-.Eastern.us",
    "HBO.Zone.HD.-.East.us",
    "Starz.Cinema.HD.-.Eastern.us",
    "Starz.Comedy.HD.-.Eastern.us",
    "Starz.-.Eastern.us",
    "Starz.Edge.-.Eastern.us",
    "Starz.Encore.Action.-.Eastern.us",
    "Starz.Encore.Black.-.Eastern.us",
    "Starz.Encore.Classic.-.Eastern.us",
    "Starz.Encore.-.Eastern.us",
    "Starz.Encore.Family.-.Eastern.us",
    "Starz.Encore.on.Demand.us",
    "Starz.Encore.-.Pacific.us",
    "Starz.Encore.Suspense.-.Eastern.us",
    "Starz.Encore.Westerns.-.Eastern.us",
    "Starz.In.Black.-.Eastern.us",
    "Starz.Kids.and.Family.-.Eastern.us",
    "Starz.On.Demand.us",
    "Starz.-.Pacific.us",
    "MoreMax..Eastern.us",
}

TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar 2: Escape de África",
    "H.Potter y la cámara secreta": "Harry Potter y la Cámara Secreta"
}

# ----------------------
# FUNCIONES AUXILIARES
# ----------------------
def normalizar_texto(texto):
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.strip()

def rellenar_descripcion(titulo, tipo="serie", temporada=None, episodio=None):
    if tipo == "pelicula":
        return f"Sinopsis no disponible para la película '{titulo}'."
    else:
        return f"Sinopsis no disponible para el episodio {temporada}-{episodio} de '{titulo}'."

def traducir_a_espanol(texto):
    if not texto:
        return ""
    return texto

# ----------------------
# BÚSQUEDAS EXTERNAS
# ----------------------
def buscar_google_snippet(titulo):
    try:
        query = "+".join(titulo.split())
        url = f"https://www.google.com/search?q={query}+site:imdb.com+OR+site:wikipedia.org"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        snippet = soup.select_one(".VwiC3b")
        if snippet:
            return snippet.text.strip()
    except Exception:
        return ""
    return ""

def buscar_tmdb(titulo, tipo="multi", lang="es-MX", year=None):
    titulo = TITULOS_MAP.get(titulo, titulo)
    url = f"https://api.themoviedb.org/3/search/{tipo}"
    params = {"api_key": API_KEY, "query": titulo, "language": lang}
    if tipo == "movie" and year:
        params["year"] = year
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0] if results else None
    except Exception:
        return None

def buscar_imdb_rotten(titulo):
    # Solo ejemplo, puedes expandir la búsqueda con scraping
    snippet = buscar_google_snippet(f"{titulo} site:imdb.com")
    if snippet:
        return snippet
    snippet_rt = buscar_google_snippet(f"{titulo} site:rottentomatoes.com")
    return snippet_rt

# ----------------------
# ChatGPT
# ----------------------
def chatgpt_disponible():
    if not OPENAI_API_KEY:
        return False
    try:
        resp = openai.usage.retrieve()  # verifica si hay cuota disponible
        return True
    except Exception:
        return False

def obtener_descripcion_chatgpt(titulo, tipo=None, temporada=None, episodio=None, anio=None, pistas=None):
    if not chatgpt_disponible():
        return None
    prompt = f"""
    Actúa como un experto en series y películas. 
    Completa la sinopsis de este contenido usando la información de pistas:
    Título: {titulo}
    Tipo: {tipo or 'desconocido'}
    Temporada: {temporada or ''}
    Episodio: {episodio or ''}
    Año: {anio or ''}
    Pistas: {pistas or {}}
    
    Si no estás seguro de algún dato, indica "INCIERTO".
    Devuelve solo la sinopsis en español.
    """
    try:
        resp = openai.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        texto = resp.choices[0].message.content.strip()
        if "INCIERTO" in texto.upper() or not texto:
            return None
        return texto
    except Exception:
        return None

# ----------------------
# Obtener descripción completa
# ----------------------
def obtener_descripcion_completa(titulo, tipo=None, temporada=None, episodio=None, anio=None, pistas=None):
    existing_desc = pistas.get("desc", "") if pistas else ""

    # Intentar ChatGPT primero solo si hay cuota
    overview = None
    if chatgpt_disponible():
        overview = obtener_descripcion_chatgpt(titulo, tipo, temporada, episodio, anio, pistas)

    # Google
    if not overview:
        overview = buscar_google_snippet(titulo)

    # TMDB
    if not overview:
        overview = ""
        if tipo == "serie" and temporada and episodio:
            search_res = buscar_tmdb(titulo, "tv")
            if search_res:
                tv_id = search_res.get("id")
                # Solo overview de temporada/episodio
                overview = search_res.get("overview", "")
        elif tipo == "pelicula":
            search_res = buscar_tmdb(titulo, "movie", year=anio)
            if search_res:
                overview = search_res.get("overview", "")

    # IMDb / Rotten
    if not overview:
        overview = buscar_imdb_rotten(titulo)

    # Placeholder final
    if not overview:
        overview = rellenar_descripcion(titulo, tipo, temporada, episodio)

    return traducir_a_espanol(overview)

# ----------------------
# Parse de episodio
# ----------------------
def parse_episode_num(ep_text):
    if not ep_text:
        return None, None
    ep_text = ep_text.strip().upper().replace(" ", "")
    patterns = [r"S(\d{1,2})E(\d{1,2})", r"(\d{1,2})x(\d{1,2})", r"(\d{1,2})[E](\d{1,2})"]
    for p in patterns:
        match = re.match(p, ep_text)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None

# ----------------------
# Descarga EPG
# ----------------------
if not os.path.exists(EPG_FILE):
    print("📥 Descargando guía original...")
    r = requests.get(EPG_URL, timeout=60)
    r.raise_for_status()
    with gzip.open(io.BytesIO(r.content), 'rb') as f_in:
        with open(EPG_FILE, 'wb') as f_out:
            f_out.write(f_in.read())
    print("✅ Guía original descargada.")

for idx, url in enumerate(NUEVAS_EPGS, start=1):
    temp_file = f"epg_nueva_{idx}.xml"
    if not os.path.exists(temp_file):
        print(f"📥 Descargando EPG desde {url}...")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with gzip.open(io.BytesIO(r.content), 'rb') as f_in:
            with open(temp_file, 'wb') as f_out:
                f_out.write(f_in.read())
        print(f"✅ EPG descargada: {temp_file}")
    EPG_FILES_TEMP.append(temp_file)

# ----------------------
# Procesar EPG
# ----------------------
def _get_desc_text(desc_el):
    if desc_el is None:
        return ""
    try:
        return "".join(desc_el.itertext()).strip()
    except Exception:
        return (desc_el.text or "").strip()

def procesar_epg(input_file, output_file, escribir_raiz=False):
    tree = ET.parse(input_file)
    root = tree.getroot()

    mode = "wb" if escribir_raiz else "ab"
    with open(output_file, mode) as f:
        if escribir_raiz:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>\n<tv>\n')

        for ch in root.findall("channel"):
            if ch.get("id") in CANALES_USAR:
                f.write(ET.tostring(ch, encoding="utf-8"))

        for elem in root.findall("programme"):
            canal = elem.get("channel")
            if canal not in CANALES_USAR:
                continue

            title_el = elem.find("title")
            sub_el = elem.find("sub-title")
            desc_el = elem.find("desc")
            date_el = elem.find("date")
            ep_el = elem.find("episode-num")
            ep_text = ep_el.text.strip() if ep_el is not None and ep_el.text else ""
            temporada, episodio = parse_episode_num(ep_text)
            categorias = [(c.text or "").lower() for c in elem.findall("category")]

            existing_desc = _get_desc_text(desc_el)
            es_serie = any("serie" in c for c in categorias) or (temporada is not None and episodio is not None)
            es_pelicula = not es_serie
            titulo_original = title_el.text.strip() if title_el is not None and title_el.text else "Sin título"
            titulo_norm = normalizar_texto(titulo_original)

            pistas = {"desc": existing_desc, "title": titulo_original, "categorias": categorias}

            # -------------------- SERIES --------------------
            if es_serie and temporada and episodio:
                nombre_ep = ep_text
                overview = obtener_descripcion_completa(titulo_norm, "serie", temporada, episodio, pistas=pistas)

                if sub_el is None or not (sub_el.text or "").strip():
                    if sub_el is None:
                        sub_el = ET.SubElement(elem, "sub-title")
                    sub_el.text = nombre_ep

                if desc_el is None:
                    desc_el = ET.SubElement(elem, "desc")
                desc_el.text = f"{nombre_ep}\n{overview}".strip()

                if title_el is None:
                    title_el = ET.SubElement(elem, "title")
                title_el.text = f'{titulo_original} (S{temporada:02d}E{episodio:02d})'

            # -------------------- PELÍCULAS --------------------
            elif es_pelicula:
                anio_epg = date_el.text.strip() if (date_el is not None and date_el.text) else ""
                overview = obtener_descripcion_completa(titulo_norm, "pelicula", anio=anio_epg, pistas=pistas)

                if (date_el is None or not (date_el.text or "").strip()) and overview:
                    anio = ""
                    search_res = buscar_tmdb(titulo_norm, "movie", year=anio_epg if anio_epg else None)
                    if search_res:
                        anio = (search_res.get("release_date") or "")[:4]
                        if date_el is None:
                            date_el = ET.SubElement(elem, "date")
                        date_el.text = anio

                if desc_el is None:
                    desc_el = ET.SubElement(elem, "desc")
                desc_el.text = overview if overview else rellenar_descripcion(titulo_original, "pelicula")

                if title_el is None:
                    title_el = ET.SubElement(elem, "title")
                title_el.text = f"{titulo_original} ({anio_epg})" if anio_epg else titulo_original

            f.write(ET.tostring(elem, encoding="utf-8"))

# ----------------------
# EJECUTAR
# ----------------------
procesar_epg(EPG_FILE, OUTPUT_FILE, escribir_raiz=True)

for temp_file in EPG_FILES_TEMP:
    procesar_epg(temp_file, OUTPUT_FILE, escribir_raiz=False)

with open(OUTPUT_FILE, "ab") as f:
    f.write(b"</tv>")

with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE + ".gz", "wb") as f_out:
    f_out.writelines(f_in)

print(f"✅ Guía generada: {OUTPUT_FILE} y {OUTPUT_FILE}.gz")
