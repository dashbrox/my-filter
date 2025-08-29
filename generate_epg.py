#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import xml.etree.ElementTree as ET
import requests
import json
import io
from pathlib import Path
from urllib.parse import quote

# ================= CONFIG =================
CHANNELS = [
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
]

EPG_SOURCES = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz"
]

OMDB_API_KEY = os.getenv("OMDB_API_KEY")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# ================= ARCHIVOS =================
LIBRARY_FILE = Path("library.json")
OUTPUT_FILE = Path("guide_custom.xml")

# ================= UTILIDADES =================
def download_and_parse(url):
    print(f"[INFO] Descargando y descomprimiendo: {url}")
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as f:
        tree = ET.parse(f)
    return tree

def load_library():
    if LIBRARY_FILE.exists():
        print(f"[INFO] Cargando biblioteca existente: {LIBRARY_FILE}")
        return json.loads(LIBRARY_FILE.read_text(encoding="utf-8"))
    return {}

def save_library(lib):
    LIBRARY_FILE.write_text(json.dumps(lib, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] Biblioteca guardada: {LIBRARY_FILE}")

def normalize_episode(ep_str):
    ep_str = ep_str.lower().replace(" ", "")
    if ep_str.startswith("s") and ep_str[1:].isdigit():
        ep_num = int(ep_str[1:])
        season = (ep_num - 1) // 22 + 1
        episode = (ep_num - 1) % 22 + 1
        return f"S{season}E{episode}"
    elif ep_str.startswith("e") and ep_str[1:].isdigit():
        return f"E{int(ep_str[1:]):02}"
    return ep_str.upper()

# ================= API CONSULTAS =================
def query_omdb(title, year=None):
    if not OMDB_API_KEY:
        return None
    url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={quote(title)}"
    if year:
        url += f"&y={year}"
    try:
        resp = requests.get(url, timeout=5).json()
        if resp.get("Response") == "True":
            return resp
    except Exception:
        return None
    return None

def query_tmdb(title, year=None):
    if not TMDB_API_KEY:
        return None
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(title)}"
    if year:
        url += f"&year={year}"
    try:
        resp = requests.get(url, timeout=5).json()
        results = resp.get("results", [])
        return results[0] if results else None
    except Exception:
        return None

# ================= PROCESAMIENTO =================
def enrich_program(title, subtitle, desc, year=None, season=None, episode=None):
    key = f"{title.strip().lower()}_{season or ''}_{episode or ''}"
    if key in library:
        return library[key]["title"], library[key]["subtitle"], library[key]["desc"]

    enriched_desc = desc

    # 1️⃣ TMDb
    tmdb_info = query_tmdb(title, year)
    if tmdb_info:
        if not enriched_desc and tmdb_info.get("overview"):
            enriched_desc = tmdb_info["overview"]

    # 2️⃣ OMDb
    omdb_info = query_omdb(title, year)
    if omdb_info and not enriched_desc:
        enriched_desc = omdb_info.get("Plot", "")

    # Validación: si no es confiable, mantenemos el original
    if not enriched_desc or len(enriched_desc.strip()) < 20:
        enriched_desc = desc

    library[key] = {"title": title, "subtitle": subtitle, "desc": enriched_desc}
    return title, subtitle, enriched_desc

# ================= MAIN =================
library = load_library()
root = ET.Element("tv", attrib={"generator-info-name": "my-filter"})

for url in EPG_SOURCES:
    try:
        tree = download_and_parse(url)
    except Exception as e:
        print(f"[ERROR] No se pudo procesar {url}: {e}")
        continue

    # --- Procesar canales ---
    for channel in tree.findall("channel"):
        chan_id = channel.attrib.get("id")
        if chan_id in CHANNELS:
            # copiar canal completo con todos sus hijos
            root.append(channel)
            print(f"[INFO] Agregando canal: {chan_id}")

    # --- Procesar programas ---
    for prog in tree.findall("programme"):
        chan_id = prog.attrib.get("channel")
        if chan_id not in CHANNELS:
            continue

        title_el = prog.find("title")
        sub_el = prog.find("sub-title")
        desc_el = prog.find("desc")

        title_text = title_el.text if title_el is not None else ""
        sub_text = sub_el.text if sub_el is not None else ""
        desc_text = desc_el.text if desc_el is not None else ""

        ep_el = prog.find("episode-num")
        season, episode = None, None
        if ep_el is not None and ep_el.text:
            normalized = normalize_episode(ep_el.text)
            if "S" in normalized and "E" in normalized:
                season = normalized.split("E")[0][1:]
                episode = normalized.split("E")[1]

        title_text, sub_text, desc_text = enrich_program(title_text, sub_text, desc_text, season=season, episode=episode)

        if sub_el is None:
            sub_el = ET.SubElement(prog, "sub-title")
        sub_el.text = sub_text

        if desc_el is None:
            desc_el = ET.SubElement(prog, "desc")
        desc_el.text = desc_text

        root.append(prog)
        print(f"[OK] Procesado: {title_text} S{season}E{episode}")

# Guardar biblioteca y XML final
save_library(library)

# salida XML bonita
import xml.dom.minidom as minidom
xml_str = ET.tostring(root, encoding="utf-8")
pretty_xml = minidom.parseString(xml_str).toprettyxml(indent="  ")
OUTPUT_FILE.write_text(pretty_xml, encoding="utf-8")

print(f"[FIN] Guía final generada en {OUTPUT_FILE}")
