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

print("🚀 Inicio del script generate_epg.py")

# -------------------------
# CONFIGURACIÓN OpenAI (opcional)
# -------------------------
try:
    import openai
    OPENAI_API_KEYS = [os.getenv(f"OPENAI_API_KEY_{i}") for i in range(1, 9)]
    OPENAI_API_KEYS = [k for k in OPENAI_API_KEYS if k]
    OPENAI_MODEL = "gpt-4o-mini"
    openai_index = 0
except ImportError:
    OPENAI_API_KEYS = []
    print("⚠️ OpenAI no instalado, se ignorará.")

# -------------------------
# CONFIGURACIÓN TMDb
# -------------------------
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = "https://api.themoviedb.org/3"

# -------------------------
# CONFIGURACIÓN OMDb
# -------------------------
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

# URLs de EPG
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

# Cache
CACHE_FILE = Path("epg_cache.json")
if CACHE_FILE.exists():
    CACHE = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
else:
    CACHE = {}

# Archivo final
OUTPUT_FILE = Path("guide_custom.xml")
OUTPUT_FILE_GZ = Path("guide_custom.xml.gz")

# -------------------------
# Función estricta de inferencia de tipo
# -------------------------
def infer_type(title, desc):
    """
    Retorna 'serie', 'documental' o 'película' solo si hay pistas claras.
    Retorna None si no se puede determinar con certeza.
    """
    text = f"{title} {desc}".lower()

    # Detectar serie
    if re.search(r"(S\d+ E\d+|season|episodio)", text):
        return "serie"

    # Detectar documental
    if re.search(r"(documental|documentary|historia real)", text):
        return "documental"

    # Detectar película explícita
    if "película" in text or "movie" in text or "film" in text:
        return "película"

    # Sin evidencia suficiente
    return None

# -------------------------
# Funciones auxiliares
# -------------------------
def get_openai_response(prompt):
    """Opcional: solo se usa si funciona, si falla se ignora."""
    if not OPENAI_API_KEYS:
        return None
    global openai_index
    try:
        key = OPENAI_API_KEYS[openai_index]
        client = openai.OpenAI(api_key=key)
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": "Eres un asistente de EPG."},
                      {"role": "user", "content": prompt}],
            temperature=0.7
        )
        return response.choices[0].message.content.strip()
    except Exception:
        print("❌ OpenAI falló o se negó a trabajar, seguimos con TMDB/OMDB y analizador de pistas.")
        openai_index = (openai_index + 1) % len(OPENAI_API_KEYS)
        return None

def get_tmdb_info(title, year=None):
    try:
        params = {"api_key": TMDB_API_KEY, "query": title, "language": "es-ES"}
        if year:
            params["year"] = year
        r = requests.get(f"{TMDB_BASE_URL}/search/movie", params=params, timeout=10)
        data = r.json()
        if data.get("results"):
            return data["results"][0]
    except:
        pass
    return None

def get_omdb_info(title, year=None):
    try:
        params = {"apikey": OMDB_API_KEY, "t": title, "type": "movie"}
        if year:
            params["y"] = year
        r = requests.get(OMDB_BASE_URL, params=params, timeout=10)
        data = r.json()
        if data.get("Response") == "True":
            return data
    except:
        pass
    return None

def fetch_epg(url):
    r = requests.get(url, timeout=15)
    with gzip.open(io.BytesIO(r.content)) as f:
        tree = ET.parse(f)
    return tree

def process_programme(prog):
    title = prog.findtext("title")
    if not title:
        return prog

    desc_elem = prog.find("desc")
    desc = desc_elem.text if desc_elem is not None else ""
    categories_elem = prog.findall("category")
    existing_categories = [c.text for c in categories_elem if c.text]

    # Inferir tipo solo si no existe categoría
    if not existing_categories:
        tipo = infer_type(title, desc)
        if tipo:
            ET.SubElement(prog, "category").text = tipo
        else:
            print(f"⚠️ No se pudo determinar categoría para: '{title}'")

    # Obtener descripción con TMDB/OMDB si está vacía
    key = f"{title}"
    if key in CACHE:
        if desc_elem is None:
            ET.SubElement(prog, "desc").text = CACHE[key]["desc"]
        return prog

    if not desc:
        # Intentar OpenAI primero (opcional)
        new_desc = get_openai_response(f"Escribe una sinopsis corta en español para: {title}")
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

    CACHE[key] = {"desc": desc}
    return prog

# -------------------------
# Proceso principal paralelo
# -------------------------
def main():
    all_elements = []

    for url in EPG_URLS:
        try:
            tree = fetch_epg(url)
            root = tree.getroot()
            programmes = [elem for elem in root if elem.tag == "programme" and elem.attrib.get("channel") in CHANNELS]
            others = [elem for elem in root if elem.tag != "programme"]

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(process_programme, prog): prog for prog in programmes}
                for future in as_completed(futures):
                    all_elements.append(future.result())

            all_elements.extend(others)
        except Exception as e:
            print(f"⚠️ Error descargando {url}: {e}")

    tv = ET.Element("tv")
    for elem in all_elements:
        tv.append(elem)

    tree_out = ET.ElementTree(tv)
    tree_out.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE_GZ, "wb") as f_out:
        f_out.writelines(f_in)

    CACHE_FILE.write_text(json.dumps(CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Guía generada en {OUTPUT_FILE} y comprimida en {OUTPUT_FILE_GZ}")

if __name__ == "__main__":
    main()
