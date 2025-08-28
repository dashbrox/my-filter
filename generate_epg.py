#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import xml.etree.ElementTree as ET
import requests
import json
from pathlib import Path
from urllib.parse import quote
from itertools import cycle
import time
from openai import OpenAI

# ================= CONFIG =================
CHANNELS = [
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

# ================= OPENAI CONFIG =================
OPENAI_KEYS = [
    os.getenv("OPENAI_API_KEY"),
    os.getenv("OPENAI_API_KEY_1"),
    os.getenv("OPENAI_API_KEY_2"),
    os.getenv("OPENAI_API_KEY_3"),
    os.getenv("OPENAI_API_KEY_4"),
    os.getenv("OPENAI_API_KEY_5"),
    os.getenv("OPENAI_API_KEY_6"),
    os.getenv("OPENAI_API_KEY_7"),
    os.getenv("OPENAI_API_KEY_8"),
]

# Filtrar keys None
OPENAI_KEYS = [k for k in OPENAI_KEYS if k]

if not OPENAI_KEYS:
    raise RuntimeError("No se encontraron claves de OpenAI válidas en las variables de entorno")

key_cycle = cycle(OPENAI_KEYS)
OPENAI_MODEL = "gpt-5-mini"

# ================= ARCHIVOS =================
LIBRARY_FILE = Path("library.json")
OUTPUT_FILE = Path("guide_custom.xml")

# ================= UTILIDADES =================
def download_and_parse(url):
    print(f"[INFO] Descargando y descomprimiendo: {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with gzip.GzipFile(fileobj=resp.raw) as f:
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
    url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={quote(title)}"
    if year:
        url += f"&y={year}"
    resp = requests.get(url).json()
    if resp.get("Response") == "True":
        return resp
    return None

def query_tmdb(title, year=None):
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(title)}"
    if year:
        url += f"&year={year}"
    resp = requests.get(url).json()
    results = resp.get("results", [])
    return results[0] if results else None

def query_openai(prompt, key_cycle):
    max_retries = len(OPENAI_KEYS)
    for _ in range(max_retries):
        key = next(key_cycle)
        try:
            client = OpenAI(api_key=key)
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=500
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            print(f"[WARN] OpenAI key falló, cambiando a siguiente: {e}")
            time.sleep(1)
    return ""

# ================= PROCESAMIENTO =================
def enrich_program(title, subtitle, desc, year=None, season=None, episode=None):
    key = f"{title}_{season}_{episode}"
    if key in library:
        print(f"[INFO] Reutilizando datos existentes para {title} S{season}E{episode}")
        return library[key]["title"], library[key]["subtitle"], library[key]["desc"]

    print(f"[INFO] Enriqueciendo: {title} S{season}E{episode}")

    tmdb_info = query_tmdb(title, year)
    if tmdb_info:
        if "release_date" in tmdb_info:
            year = tmdb_info["release_date"][:4]
        if not desc:
            desc = tmdb_info.get("overview", "")

    omdb_info = query_omdb(title, year)
    if omdb_info and not desc:
        desc = omdb_info.get("Plot", "")

    prompt = f"""
    Mejora esta sinopsis para el programa:
    Title: {title}
    Subtitle: {subtitle}
    Description: {desc}
    Season: {season}
    Episode: {episode}
    Español, precisa y basada en la información existente, sin inventar datos.
    """
    desc = query_openai(prompt, key_cycle)

    library[key] = {"title": title, "subtitle": subtitle, "desc": desc}
    return title, subtitle, desc

# ================= MAIN =================
library = load_library()
root = ET.Element("tv", attrib={"generator-info-name": "my-filter"})

for url in EPG_SOURCES:
    try:
        tree = download_and_parse(url)
    except Exception as e:
        print(f"[ERROR] No se pudo procesar {url}: {e}")
        continue

    for channel in tree.findall("channel"):
        chan_id = channel.attrib.get("id")
        if chan_id in CHANNELS:
            print(f"[INFO] Agregando canal: {chan_id}")
            root.append(channel)

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
tree_final = ET.ElementTree(root)
tree_final.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
print(f"[FIN] Guía final generada en {OUTPUT_FILE}")
