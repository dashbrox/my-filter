#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import requests
import xml.etree.ElementTree as ET

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

INPUT_URL = "http://m3u4u.com/xml/w16vy5vmkrbxp889n39p"
OUTPUT_FILE = "guide_custom.xml"


def fetch_xml(url):
    print(f"[INFO] Descargando guía desde {url} ...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    content = r.content
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return ET.ElementTree(ET.fromstring(content))


def tmdb_search(title, type_hint="movie"):
    import urllib.parse
    query = urllib.parse.quote(title)
    url = f"https://api.themoviedb.org/3/search/{type_hint}?api_key={TMDB_API_KEY}&query={query}&language=es-ES"
    r = requests.get(url, timeout=15)
    if r.ok:
        data = r.json()
        if data.get("results"):
            return data["results"][0]
    return None


def omdb_search(title):
    url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={title}"
    r = requests.get(url, timeout=15)
    if r.ok:
        data = r.json()
        if data.get("Response") == "True":
            return data
    return None


def enrich_programme(prog):
    title_el = prog.find("title")
    desc_el = prog.find("desc")

    if title_el is None:
        return

    title = title_el.text or ""
    desc = desc_el.text if desc_el is not None else ""

    # --- primero intentamos como TV ---
    info = tmdb_search(title, "tv")
    if info:
        show_id = info["id"]
        show_name = info["name"]
        year = info.get("first_air_date", "")[:4]

        # buscar episodios de la serie
        url = f"https://api.themoviedb.org/3/tv/{show_id}?api_key={TMDB_API_KEY}&language=es-ES"
        show_data = requests.get(url, timeout=15).json()

        season_num, ep_num, ep_name = None, None, None

        # intentar encontrar temporada/episodio dentro de overview
        for season in range(1, show_data.get("number_of_seasons", 0) + 1):
            url_season = f"https://api.themoviedb.org/3/tv/{show_id}/season/{season}?api_key={TMDB_API_KEY}&language=es-ES"
            season_data = requests.get(url_season, timeout=15).json()
            for ep in season_data.get("episodes", []):
                if desc[:40].lower() in (ep.get("overview", "").lower()):
                    season_num = season
                    ep_num = ep.get("episode_number")
                    ep_name = ep.get("name")
                    break
            if ep_name:
                break

        # --- armar título final ---
        if season_num and ep_num:
            title_el.text = f"{show_name} (S{season_num} E{ep_num})"
            if desc_el is not None and ep_name:
                desc_el.text = f"“{ep_name}”\n{desc}"
        else:
            # fallback solo con año
            title_el.text = f"{show_name} ({year})"

        return

    # --- si no es serie, intentar película ---
    info = tmdb_search(title, "movie")
    if not info:
        info = omdb_search(title)

    if info:
        year = info.get("release_date", "")[:4] or info.get("Year", "")
        movie_name = info.get("title") or info.get("Title") or title
        if year:
            title_el.text = f"{movie_name} ({year})"
        else:
            title_el.text = movie_name


def main():
    tree = fetch_xml(INPUT_URL)
    root = tree.getroot()

    for prog in root.findall("programme"):
        enrich_programme(prog)

    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    print(f"[INFO] Guía enriquecida guardada en {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
