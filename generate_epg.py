import os
import re
import gzip
import requests
import unicodedata
import lxml.etree as ET
import io
from bs4 import BeautifulSoup
import openai
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

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

# ----------------------
# ChatGPT
# ----------------------
def chatgpt_disponible():
    if not OPENAI_API_KEY:
        return False
    try:
        resp = openai.usage.retrieve()
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
    
    Devuelve solo la sinopsis en español.
    """
    try:
        resp = openai.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        texto = resp.choices[0].message.content.strip()
        if not texto:
            return None
        return texto
    except Exception:
        return None

def obtener_descripcion_completa_serie(titulo, temporada, episodio, pistas=None):
    episodios = {}  # Podrías integrar TMDB aquí como antes si quieres
    key = f"S{temporada:02d}E{episodio:02d}"
    overview = episodios.get(key)
    if overview:
        return traducir_a_espanol(overview)
    return obtener_descripcion_chatgpt(titulo, "serie", temporada, episodio, pistas=pistas) or rellenar_descripcion(titulo, "serie", temporada, episodio)

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
def descargar_epg(url, dest_file):
    if not os.path.exists(dest_file):
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with gzip.open(io.BytesIO(r.content), 'rb') as f_in:
            with open(dest_file, 'wb') as f_out:
                f_out.write(f_in.read())
    return dest_file

if not os.path.exists(EPG_FILE):
    descargar_epg(EPG_URL, EPG_FILE)

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(descargar_epg, url, f"epg_nueva_{idx}.xml") 
               for idx, url in enumerate(NUEVAS_EPGS, start=1)]
    for f in futures:
        EPG_FILES_TEMP.append(f.result())

# ----------------------
# Completar programa
# ----------------------
def completar_programa(elem, maratones_set=None):
    title_el = elem.find("title") or ET.SubElement(elem, "title")
    desc_el = elem.find("desc") or ET.SubElement(elem, "desc")
    sub_el = elem.find("sub-title") or ET.SubElement(elem, "sub-title")
    ep_el = elem.find("episode-num")
    ep_text = ep_el.text if ep_el is not None else ""
    temporada, episodio = parse_episode_num(ep_text)
    titulo_original = title_el.text.strip() if title_el.text else "Sin título"
    categorias = [c.text.lower() for c in elem.findall("category")]
    tipo = "serie" if any("serie" in c for c in categorias) or (temporada is not None) else "pelicula"

    if tipo == "serie" and temporada and episodio:
        overview = obtener_descripcion_completa_serie(titulo_original, temporada, episodio)
        title_el.text = titulo_original
        sub_el.text = f"S{temporada:02d}E{episodio:02d}"
        desc_el.text = overview
        if ep_el is not None:
            ep_el.text = sub_el.text
    elif tipo == "pelicula":
        anio_epg = elem.find("date").text if elem.find("date") is not None else ""
        overview = rellenar_descripcion(titulo_original, "pelicula")
        title_el.text = f"{titulo_original} ({anio_epg})" if anio_epg else titulo_original
        desc_el.text = overview
    else:
        overview = rellenar_descripcion(titulo_original, "serie")
        title_el.text = titulo_original
        desc_el.text = overview

    return elem, 0

# ----------------------
# PROCESAR EPG COMPLETO
# ----------------------
def procesar_epg(input_file, output_file):
    tree = ET.parse(input_file)
    root = tree.getroot()
    new_root = ET.Element("tv", attrib=root.attrib)

    # Copiar canales
    for channel in root.findall("channel"):
        new_root.append(channel)

    programas = root.findall("programme")
    for elem in programas:
        canal = elem.get("channel")
        if canal not in CANALES_USAR:
            continue
        elem_completo, _ = completar_programa(elem)
        new_root.append(elem_completo)

    new_tree = ET.ElementTree(new_root)
    new_tree.write(output_file, encoding="UTF-8", xml_declaration=True)

# ----------------------
# EJECUTAR
# ----------------------
procesar_epg(EPG_FILE, OUTPUT_FILE)
for temp_file in EPG_FILES_TEMP:
    procesar_epg(temp_file, OUTPUT_FILE)

# Comprimir
with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE + ".gz", "wb") as f_out:
    f_out.writelines(f_in)

print(f"✅ Guía generada: {OUTPUT_FILE} y {OUTPUT_FILE}.gz")
