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
import openai
import re

# -------------------------
# CONFIGURACIÓN
# -------------------------

# OpenAI: 8 claves posibles
OPENAI_API_KEYS = [
    os.getenv(f"OPENAI_API_KEY{i}") for i in range(1, 9)
]
openai_index = 0

# TMDb
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_BASE_URL = "https://api.themoviedb.org/3"

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
# FUNCIONES
# -------------------------

def get_openai_response(prompt):
    global openai_index
    retries = 0
    while retries < len(OPENAI_API_KEYS):
        try:
            openai.api_key = OPENAI_API_KEYS[openai_index]
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error OpenAI con key {openai_index}: {e}")
            openai_index = (openai_index + 1) % len(OPENAI_API_KEYS)
            retries += 1
            time.sleep(1)
    return None

def get_tmdb_info(title, year=None):
    params = {"api_key": TMDB_API_KEY, "query": title, "language": "es-ES"}
    if year:
        params["year"] = year
    r = requests.get(f"{TMDB_BASE_URL}/search/movie", params=params)
    data = r.json()
    if data.get("results"):
        return data["results"][0]
    return None

def fetch_epg(url):
    r = requests.get(url, timeout=15)
    with gzip.open(io.BytesIO(r.content)) as f:
        tree = ET.parse(f)
    return tree

def process_programme(prog):
    title = prog.findtext("title")
    if not title:
        return

    desc_elem = prog.find("desc")
    desc = desc_elem.text if desc_elem is not None else ""

    # Determinar tipo: serie o película
    is_series = False
    category_elems = prog.findall("category")
    for cat in category_elems:
        if cat.text and "Serie" in cat.text:
            is_series = True
            break

    key = f"{title}"
    if key in CACHE:
        if desc_elem is None:
            ET.SubElement(prog, "desc").text = CACHE[key]["desc"]
        return

    # Si ya tiene descripción
    if desc:
        if is_series:
            lines = desc.strip().split("\n")
            if len(lines) == 1 or not re.match(r"^S\d+ E\d+", lines[0]):
                # Extraer episodio si está en el título: "Serie (S1 E2)"
                m = re.search(r"\((S\d+ E\d+)\)", title)
                episode_title = m.group(1) if m else "Episodio"
                desc = f"{episode_title}\n{desc}"
                if desc_elem is not None:
                    desc_elem.text = desc
        CACHE[key] = {"desc": desc}
        return

    # No hay descripción: generar con OpenAI
    prompt = f"Escribe una sinopsis corta en español para: {title}"
    new_desc = get_openai_response(prompt)

    # Si OpenAI falla, usar TMDb
    if not new_desc:
        info = get_tmdb_info(title)
        if info:
            new_desc = info.get("overview", "")

    # Para series, poner título del episodio como primera línea
    if is_series:
        m = re.search(r"\((S\d+ E\d+)\)", title)
        episode_title = m.group(1) if m else "Episodio"
        new_desc = f"{episode_title}\n{new_desc}"

    # Asignar descripción
    if new_desc:
        if desc_elem is None:
            ET.SubElement(prog, "desc").text = new_desc
        else:
            desc_elem.text = new_desc
        CACHE[key] = {"desc": new_desc}

# -------------------------
# PROCESO PRINCIPAL
# -------------------------

def main():
    all_elements = []

    for url in EPG_URLS:
        print(f"Descargando {url} ...")
        tree = fetch_epg(url)
        root = tree.getroot()
        for elem in root:
            if elem.tag == "programme":
                channel = elem.attrib.get("channel")
                if channel in CHANNELS:
                    try:
                        process_programme(elem)
                        all_elements.append(elem)
                    except Exception as e:
                        print(f"Programa ignorado por error: {e}")
            else:
                # Mantener <channel> u otros elementos tal cual
                all_elements.append(elem)

    # Crear XML final
    tv = ET.Element("tv")
    for elem in all_elements:
        tv.append(elem)

    tree_out = ET.ElementTree(tv)
    tree_out.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    # Comprimir guía final
    with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE_GZ, "wb") as f_out:
        f_out.writelines(f_in)

    # Guardar cache
    CACHE_FILE.write_text(json.dumps(CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Guía generada en {OUTPUT_FILE} y comprimida en {OUTPUT_FILE_GZ}")

if __name__ == "__main__":
    main()
