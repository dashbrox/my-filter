#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import json
import requests
import xml.etree.ElementTree as ET
import urllib.parse
from datetime import datetime, timedelta
from difflib import SequenceMatcher

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

INPUT_URL = "http://m3u4u.com/xml/w16vy5vmkrbxp889n39p"
OUTPUT_FILE = "guide_custom.xml"
CACHE_FILE = "epg_cache.json"
CACHE_DAYS = 7
REQUEST_TIMEOUT = 10  # segundos

# --------------------------
# Helpers
# --------------------------

def log(msg, level="INFO"):
    print(f"[{level}] {msg}")

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

def fetch_xml(url):
    log(f"Descargando guía desde {url} ...")
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    content = r.content
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    log("Guía descargada correctamente")
    return ET.ElementTree(ET.fromstring(content))

def tmdb_search(title, type_hint="movie"):
    query = urllib.parse.quote(title)
    url = f"https://api.themoviedb.org/3/search/{type_hint}?api_key={TMDB_API_KEY}&query={query}&language=es-ES"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.ok:
            data = r.json()
            if data.get("results"):
                log(f"TMDB encontró {type_hint}: {data['results'][0].get('name') or data['results'][0].get('title')}", "DEBUG")
                return data["results"][0]
    except Exception as e:
        log(f"Error TMDB search: {e}", "WARN")
    return None

def omdb_search(title):
    url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={urllib.parse.quote(title)}"
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT)
        if r.ok:
            data = r.json()
            if data.get("Response") == "True":
                log(f"OMDb encontró película: {data.get('Title')}", "DEBUG")
                return data
    except Exception as e:
        log(f"Error OMDb search: {e}", "WARN")
    return None

# --------------------------
# Enriquecimiento
# --------------------------

def enrich_programme(prog, cache):
    title_el = prog.find("title")
    desc_el = prog.find("desc")
    if title_el is None:
        title_el = ET.SubElement(prog, "title")
        title_el.text = "Título desconocido"
    title = title_el.text or ""
    desc = desc_el.text or ""

    # Usar cache si existe y no ha expirado
    prog_id = title.lower() + (desc[:50].lower() if desc else "")
    now = datetime.now().timestamp()
    if prog_id in cache and now - cache[prog_id]["timestamp"] < CACHE_DAYS * 86400:
        title_el.text = cache[prog_id]["title"]
        if desc_el is not None:
            desc_el.text = cache[prog_id]["desc"]
        return True

    enriched = False

    # Intentar como serie
    info = tmdb_search(title, "tv")
    if info:
        show_id = info["id"]
        show_name = info["name"]
        year = info.get("first_air_date", "")[:4]

        try:
            url_show = f"https://api.themoviedb.org/3/tv/{show_id}?api_key={TMDB_API_KEY}&language=es-ES"
            show_data = requests.get(url_show, timeout=REQUEST_TIMEOUT).json()
            season_num, ep_num, ep_name = None, None, None

            for season in range(1, show_data.get("number_of_seasons", 0) + 1):
                url_season = f"https://api.themoviedb.org/3/tv/{show_id}/season/{season}?api_key={TMDB_API_KEY}&language=es-ES"
                season_data = requests.get(url_season, timeout=REQUEST_TIMEOUT).json()
                for ep in season_data.get("episodes", []):
                    if desc and similar(desc.lower(), ep.get("overview", "").lower()) > 0.6:
                        season_num = season
                        ep_num = ep.get("episode_number")
                        ep_name = ep.get("name")
                        break
                if ep_name:
                    break

            if season_num and ep_num:
                title_el.text = f"{show_name} (S{season_num} E{ep_num})"
                if desc_el is not None and ep_name:
                    desc_el.text = f"“{ep_name}”\n{desc}"
            else:
                title_el.text = f"{show_name} ({year})" if year else show_name

            enriched = True

        except Exception as e:
            log(f"Error al enriquecer serie {title}: {e}", "WARN")
            title_el.text = f"{show_name} ({year})" if year else show_name
            enriched = True

    # Intentar como película si no se enriqueció
    if not enriched:
        info = tmdb_search(title, "movie")
        if not info:
            info = omdb_search(title)
        if info:
            year = info.get("release_date", "")[:4] or info.get("Year", "")
            movie_name = info.get("title") or info.get("Title") or title
            title_el.text = f"{movie_name} ({year})" if year else movie_name
            enriched = True

    # Guardar en cache
    cache[prog_id] = {
        "title": title_el.text,
        "desc": desc_el.text or "",
        "timestamp": now
    }

    return enriched

# --------------------------
# Main
# --------------------------

def main():
    log("Iniciando script...")
    cache = load_cache()
    tree = fetch_xml(INPUT_URL)
    root = tree.getroot()
    programmes = root.findall("programme")
    log(f"Se encontraron {len(programmes)} programas en la guía original")

    enriched_count = 0
    for i, prog in enumerate(programmes, 1):
        log(f"Procesando programa {i} ...", "DEBUG")
        if enrich_programme(prog, cache):
            enriched_count += 1

    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    save_cache(cache)
    log(f"Guía enriquecida guardada en {OUTPUT_FILE} ({enriched_count} programas enriquecidos)")

if __name__ == "__main__":
    main()
