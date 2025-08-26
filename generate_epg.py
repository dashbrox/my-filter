#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import gzip
import json
import time
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import unicodedata

print("🚀 Inicio del script ultimate Godzilla generate_epg.py")

# -------------------------
# LOGGING
LOG_FILE = Path("epg_log.txt")
def log(msg):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

# -------------------------
# CONFIGURACIÓN OpenAI
try:
    import openai
    OPENAI_API_KEYS = [os.getenv(f"OPENAI_API_KEY_{i}") for i in range(1, 9)]
    OPENAI_API_KEYS = [k for k in OPENAI_API_KEYS if k]
    OPENAI_MODELS = ["gpt-4o-mini", "gpt-4o", "gpt-3.5-turbo"]
    openai_index = 0

    def get_latest_model():
        try:
            client = openai.OpenAI(api_key=OPENAI_API_KEYS[0])
            models = client.models.list()
            ids = [m['id'] for m in models['data'] if m['id'].startswith("gpt-")]
            for preferred in ["gpt-5", "gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo"]:
                if preferred in ids:
                    return preferred
            return "gpt-3.5-turbo"
        except Exception as e:
            log(f"⚠️ Error detectando modelos OpenAI: {e}")
            return "gpt-3.5-turbo"

    OPENAI_MODELS[0] = get_latest_model()
    log(f"✅ OpenAI usando modelo: {OPENAI_MODELS[0]}")

except ImportError:
    OPENAI_API_KEYS = []
    OPENAI_MODELS = []
    log("⚠️ OpenAI no instalado, se ignorará.")

# -------------------------
# CONFIGURACIÓN TMDb
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = "https://api.themoviedb.org/3"
if not TMDB_API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY no está definido en el entorno.")

# -------------------------
# CONFIGURACIÓN OMDb
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
OMDB_BASE_URL = "http://www.omdbapi.com/"

# -------------------------
# Canales a incluir
CHANNELS = [
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.AE.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.Europa.Europa.mx",
    "Canal.Film.&Arts.mx",
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
]

# -------------------------
# URLs de EPG
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

# -------------------------
# Cache
CACHE_FILE = Path("epg_cache.json")
if CACHE_FILE.exists():
    CACHE = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
else:
    CACHE = {}

# Archivos finales
OUTPUT_FILE = Path("guide_custom.xml")
OUTPUT_FILE_GZ = Path("guide_custom.xml.gz")

# -------------------------
# Función de reintento con backoff
def retry(max_attempts=3, delay=2, backoff=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            current_delay = delay
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    log(f"⚠️ Error en {func.__name__}: {e} (intento {attempts}/{max_attempts})")
                    if attempts == max_attempts:
                        raise
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

# -------------------------
# Inferencia de tipo
def infer_type(title, desc):
    text = f"{title} {desc}".lower()
    if re.search(r"(S\d+ E\d+|season|episodio)", text):
        return "serie"
    if re.search(r"(documental|documentary|historia real)", text):
        return "documental"
    if "película" in text or "movie" in text or "film" in text:
        return "película"
    return None

# -------------------------
# Funciones auxiliares OpenAI, TMDb y OMDb
def get_openai_response(prompt):
    global openai_index
    if not OPENAI_API_KEYS:
        return None
    for _ in range(len(OPENAI_API_KEYS)):
        try:
            key = OPENAI_API_KEYS[openai_index]
            model = OPENAI_MODELS[0]
            client = openai.OpenAI(api_key=key)
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": "Eres un asistente de EPG."},
                          {"role": "user", "content": prompt}],
                temperature=0.7
            )
            content = response.choices[0].message.content.strip()
            if content:
                log(f"✅ OpenAI generó descripción para: {prompt[:30]}...")
                return content
        except Exception as e:
            log(f"⚠️ OpenAI falló con key {openai_index}: {e}")
            openai_index = (openai_index + 1) % len(OPENAI_API_KEYS)
    return None

@retry(max_attempts=3)
def get_tmdb_info(title, year=None):
    params = {"api_key": TMDB_API_KEY, "query": title, "language": "es-ES"}
    if year:
        params["year"] = year
    r = requests.get(f"{TMDB_BASE_URL}/search/movie", params=params, timeout=10)
    data = r.json()
    if data.get("results"):
        return data["results"][0]
    return None

@retry(max_attempts=3)
def get_omdb_info(title, year=None):
    params = {"apikey": OMDB_API_KEY, "t": title, "type": "movie"}
    if year:
        params["y"] = year
    r = requests.get(OMDB_BASE_URL, params=params, timeout=10)
    data = r.json()
    if data.get("Response") == "True":
        return data
    return None

@retry(max_attempts=3)
def fetch_epg(url):
    r = requests.get(url, timeout=15)
    with gzip.open(io.BytesIO(r.content)) as f:
        tree = ET.parse(f)
    return tree

# -------------------------
# Funciones del viejo para series y películas
def normalizar_texto(texto):
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.strip()

TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar 2: Escape de África",
    "H.Potter y la cámara secreta": "Harry Potter y la Cámara Secreta"
}

def buscar_tmdb(titulo, tipo="multi", lang="es-MX"):
    titulo = TITULOS_MAP.get(titulo, titulo)
    url = f"https://api.themoviedb.org/3/search/{tipo}"
    params = {"api_key": TMDB_API_KEY, "query": titulo, "language": lang}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0] if results else None
    except Exception:
        if lang != "en-US":
            return buscar_tmdb(titulo, tipo, "en-US")
    return None

def obtener_info_serie(tv_id, temporada, episodio, lang="es-MX"):
    url = f"{TMDB_BASE_URL}/tv/{tv_id}/season/{temporada}/episode/{episodio}"
    params = {"api_key": TMDB_API_KEY, "language": lang}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        if lang != "en-US":
            return obtener_info_serie(tv_id, temporada, episodio, "en-US")
    return {}

def parse_episode_num(ep_text):
    if not ep_text:
        return None, None
    ep_text = ep_text.strip().upper()
    match = re.match(r"S(\d{1,2})E(\d{1,2})", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(\d{1,2})[xE](\d{1,2})", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def procesar_programa_formato_viejo(elem):
    title_el = elem.find("title")
    titulo = title_el.text.strip() if title_el is not None else "Sin título"
    titulo_norm = normalizar_texto(titulo)

    category_el = elem.find("category")
    categoria = category_el.text.strip().lower() if category_el is not None else ""

    ep_el = elem.find("episode-num")
    ep_text = ep_el.text.strip() if ep_el is not None else ""
    temporada, episodio = parse_episode_num(ep_text)

    desc_el = elem.find("desc")
    date_el = elem.find("date")

    # --- SERIES ---
    if "serie" in categoria and temporada and episodio:
        sub_el = elem.find("sub-title")
        if sub_el is None:
            sub_el = ET.SubElement(elem, "sub-title")
            sub_el.text = ep_text

        if desc_el is None or not desc_el.text.strip():
            search_res = buscar_tmdb(titulo_norm, "tv")
            if search_res:
                tv_id = search_res.get("id")
                epi_info = obtener_info_serie(tv_id, temporada, episodio)
                nombre_ep = epi_info.get("name") or ep_text
                desc_text = f"{nombre_ep}\n{epi_info.get('overview') or ''}".strip()
                if desc_el is None:
                    desc_el = ET.SubElement(elem, "desc")
                desc_el.text = desc_text
                # CORRECCIÓN: el title de la serie NO incluye el nombre del episodio
                title_el.text = f"{titulo} (S{temporada:02d}E{episodio:02d})"

    # --- PELÍCULAS ---
    elif "pel" in categoria or "movie" in categoria:
        if (date_el is None or not date_el.text.strip()) or (desc_el is None or not desc_el.text.strip()):
            search_res = buscar_tmdb(titulo_norm, "movie")
            if search_res:
                anio = (search_res.get("release_date") or "????")[:4]
                overview = search_res.get("overview") or ""

                if date_el is None or not date_el.text.strip():
                    if date_el is None:
                        date_el = ET.SubElement(elem, "date")
                    date_el.text = anio

                if desc_el is None or not desc_el.text.strip():
                    if desc_el is None:
                        desc_el = ET.SubElement(elem, "desc")
                    desc_el.text = overview

                if f"({anio})" not in titulo:
                    title_el.text = f"{titulo} ({anio})"

    return elem

# -------------------------
# Función principal de procesamiento
def process_programme(prog):
    title = prog.findtext("title")
    if not title:
        return prog
    desc_elem = prog.find("desc")
    desc = desc_elem.text if desc_elem is not None else ""
    categories_elem = prog.findall("category")
    existing_categories = [c.text for c in categories_elem if c.text]

    if not existing_categories:
        tipo = infer_type(title, desc)
        if tipo:
            ET.SubElement(prog, "category").text = tipo

    key = title
    if key in CACHE:
        if desc_elem is None:
            ET.SubElement(prog, "desc").text = CACHE[key]["desc"]
        return prog

    new_desc = None
    if not desc:
        new_desc = get_openai_response(title)
        if not new_desc:
            info_tmdb = get_tmdb_info(title)
            if info_tmdb:
                new_desc = info_tmdb.get("overview", "")
        if not new_desc:
            info_omdb = get_omdb_info(title)
            if info_omdb:
                new_desc = info_omdb.get("Plot", "")
        if not new_desc:
            new_desc = "Sin descripción disponible"

        if desc_elem is None:
            ET.SubElement(prog, "desc").text = new_desc
        else:
            desc_elem.text = new_desc

    CACHE[key] = {"desc": new_desc if new_desc else desc}
    return prog

# -------------------------
# Proceso principal
def main():
    all_programmes = []
    channels = {}

    for url in EPG_URLS:
        try:
            tree = fetch_epg(url)
            root = tree.getroot()
            for elem in root:
                if elem.tag == "programme" and elem.attrib.get("channel") in CHANNELS:
                    all_programmes.append(elem)
                elif elem.tag == "channel":
                    channels[elem.attrib.get("id")] = elem
        except Exception as e:
            log(f"⚠️ Error descargando {url}: {e}")

    # Procesar programas en paralelo
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_programme, prog): prog for prog in all_programmes}
        results = []
        for future in as_completed(futures):
            prog = future.result()
            prog = procesar_programa_formato_viejo(prog)  # <-- aplica formato antiguo
            results.append(prog)

    # Crear XML final
    tv = ET.Element("tv")
    for ch in channels.values():
        tv.append(ch)
    for prog in results:
        tv.append(prog)

    # Guardar XML y comprimido
    tree_out = ET.ElementTree(tv)
    tree_out.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE_GZ, "wb") as f_out:
        f_out.writelines(f_in)

    # Guardar cache
    CACHE_FILE.write_text(json.dumps(CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"✅ Guía generada en {OUTPUT_FILE} y comprimida en {OUTPUT_FILE_GZ}")

if __name__ == "__main__":
    main()
