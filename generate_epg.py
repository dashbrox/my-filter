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

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY no está definido en el entorno.")

# 8 API KEYS DE OPENAI
OPENAI_KEYS = [os.getenv(f"OPENAI_API_KEY_{i}") for i in range(1, 9)]
OPENAI_STATUS = [True] * len(OPENAI_KEYS)  # True = activa, False = agotada

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
# ROTACIÓN DE API KEYS OPENAI
# ----------------------
def obtener_api_disponible():
    for idx, activa in enumerate(OPENAI_STATUS):
        if activa and OPENAI_KEYS[idx]:
            openai.api_key = OPENAI_KEYS[idx]
            return True
    return False

def marcar_api_agotada():
    for idx, activa in enumerate(OPENAI_STATUS):
        if activa:
            OPENAI_STATUS[idx] = False
            break

# ----------------------
# ChatGPT
# ----------------------
def obtener_descripcion_chatgpt(titulo, tipo=None, temporada=None, episodio=None, anio=None, pistas=None):
    if not obtener_api_disponible():
        return None
    prompt = f"""
Eres un experto en series y películas. Completa la sinopsis usando toda la información disponible:
Título: {titulo}
Tipo: {tipo or 'desconocido'}
Temporada: {temporada or ''}
Episodio: {episodio or ''}
Año: {anio or ''}
Pistas: {pistas or {}}

Devuelve solo la sinopsis en español. Si está en otro idioma, tradúcela automáticamente al español.
"""
    try:
        resp = openai.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        texto = resp.choices[0].message.content.strip()
        return texto if texto else None
    except openai.errors.RateLimitError:
        marcar_api_agotada()
        return obtener_descripcion_chatgpt(titulo, tipo, temporada, episodio, anio, pistas)
    except Exception:
        return None

# ----------------------
# Función completar programa
# ----------------------
def completar_programa(elem):
    title_el = elem.find("title")
    if title_el is None:
        title_el = ET.SubElement(elem, "title")
    desc_el = elem.find("desc")
    if desc_el is None:
        desc_el = ET.SubElement(elem, "desc")
    sub_el = elem.find("sub-title")
    if sub_el is None:
        sub_el = ET.SubElement(elem, "sub-title")
    ep_el = elem.find("episode-num")
    ep_text = ep_el.text if ep_el is not None else ""
    temporada, episodio = parse_episode_num(ep_text)
    titulo_original = title_el.text.strip() if title_el.text else "Sin título"

    categorias = [c.text.lower() for c in elem.findall("category")]
    tipo = "serie" if any("serie" in c for c in categorias) or (temporada is not None) else "pelicula"

    if tipo == "serie" and temporada and episodio:
        if not desc_el.text:
            desc_el.text = f"Sinopsis del episodio {temporada}-{episodio} de {titulo_original}"
        if not sub_el.text:
            sub_el.text = f"S{temporada:02d}E{episodio:02d}"
        if ep_el is not None and not ep_el.text:
            ep_el.text = sub_el.text
    elif tipo == "pelicula":
        anio_epg = elem.find("date").text if elem.find("date") is not None else ""
        if not desc_el.text:
            desc_el.text = rellenar_descripcion(titulo_original, "pelicula")
        if not title_el.text:
            title_el.text = f"{titulo_original} ({anio_epg})" if anio_epg else titulo_original
    else:
        if not desc_el.text:
            desc_el.text = rellenar_descripcion(titulo_original, "serie")
        if not title_el.text:
            title_el.text = titulo_original

    return elem

# ----------------------
# Guardar guía final
# ----------------------
def guardar_guia():
    new_tree = ET.ElementTree(root_existente)
    new_tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    with gzip.open(f"{OUTPUT_FILE}.gz", "wb") as f_out:
        new_tree.write(f_out, encoding="utf-8", xml_declaration=True)
    print(f"✅ Guía guardada: {OUTPUT_FILE} y {OUTPUT_FILE}.gz")

# ----------------------
# Guardar y comprimir guía final
# ----------------------
new_tree = ET.ElementTree(root_existente)

# Guardar en XML plano
new_tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
print(f"✅ Archivo guardado: {OUTPUT_FILE}")

# Guardar versión comprimida
with gzip.open(f"{OUTPUT_FILE}.gz", "wb") as f_out:
    new_tree.write(f_out, encoding="utf-8", xml_declaration=True)
print(f"✅ Archivo comprimido guardado: {OUTPUT_FILE}.gz")
