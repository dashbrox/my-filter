#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml filtrando canales específicos y enriqueciendo metadatos
con TMDB (series/películas, sinopsis en español, título de episodio, etc.)
y OMDb como respaldo para películas. 
Solo TMDB y OMDb, sin Google KG.

Principios:
- NUNCA modificamos <channel> (id, display-name, icon, url).
- No inventamos S/E ni años.
- Añadimos sinopsis solo si es confiable (TMDB, OMDb).
- Primera comprobación: TMDB, luego OMDb.

Requerido en entorno:
- TMDB_API_KEY
- OMDB_API_KEY  (recomendado)

Salida:
- guide_custom.xml
"""

from __future__ import annotations
import os
import re
import io
import gzip
import json
import time
import hashlib
import html
import unicodedata
import threading
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any
import requests
import xml.etree.ElementTree as ET

# ---------------------------
# CONFIG / KEYS
# ---------------------------
GUIDE_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

CHANNEL_IDS_RAW = [
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

OUT_FILE = "guide_custom.xml"
TMDB_KEY = os.getenv("TMDB_API_KEY", "").strip()
OMDB_KEY = os.getenv("OMDB_API_KEY", "").strip()

HEADERS = {
    "User-Agent": "EPG-Builder/1.0 (+https://github.com/dashbrox/my-filter)",
    "Accept-Language": "es-ES,es;q=0.9",
}

CACHE_PATH = ".epg_cache_tmdb_omdb.json"
CACHE: Dict[str, Any] = {}

# ---------------------------
# LOGGING
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ---------------------------
# UTILITIES & CACHE
# ---------------------------
def load_cache():
    global CACHE
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                CACHE = json.load(f)
        except Exception:
            CACHE = {}

def save_cache():
    try:
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(CACHE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def cache_get(ns: str, key: str):
    return CACHE.get(ns, {}).get(key)

def cache_set(ns: str, key: str, value: Any):
    CACHE.setdefault(ns, {})[key] = value

def normalize_id(ch_id: str) -> str:
    if ch_id is None:
        return ""
    s = html.unescape(ch_id).strip()
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    s = re.sub(r"\s+", " ", s)
    return s.lower()

FILTER_SET = {normalize_id(x) for x in CHANNEL_IDS_RAW}

def http_get(url: str, params: dict = None, timeout: int = 30, retries: int = 3):
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(0.8 * (attempt+1))
    raise last_exc

def parse_gz_xml(url: str) -> ET.Element:
    r = http_get(url, timeout=60)
    buf = io.BytesIO(r.content)
    with gzip.open(buf, "rb") as gz:
        xml_bytes = gz.read()
    return ET.fromstring(xml_bytes)

def xmltv_datetime_parse(dt_raw: str) -> datetime:
    m = re.match(r"(\d{14})(?:\s*([+-]\d{4}))?", dt_raw)
    if not m:
        raise ValueError(f"Invalid xmltv datetime: {dt_raw}")
    base = m.group(1)
    tzpart = m.group(2)
    dt = datetime.strptime(base, "%Y%m%d%H%M%S")
    if tzpart:
        sign = 1 if tzpart[0] == '+' else -1
        hours = int(tzpart[1:3])
        mins = int(tzpart[3:5])
        offset = timedelta(hours=hours, minutes=mins) * sign
        return (dt - offset).replace(tzinfo=timezone.utc)
    return dt.replace(tzinfo=timezone.utc)

def duration_minutes_from_prog(prog: ET.Element) -> Optional[int]:
    try:
        start = prog.attrib.get("start","")
        stop = prog.attrib.get("stop","")
        if not start or not stop:
            return None
        s_dt = xmltv_datetime_parse(start)
        e_dt = xmltv_datetime_parse(stop)
        delta = e_dt - s_dt
        return int(delta.total_seconds() // 60)
    except Exception:
        return None

def safe_text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""

def set_or_create(parent: ET.Element, tag: str, text: str, lang: str = "es", overwrite=True) -> ET.Element:
    el = parent.find(tag)
    if el is None:
        el = ET.SubElement(parent, tag, {"lang": lang} if tag in ("title","sub-title","desc") else {})
    if overwrite or not (el.text and el.text.strip()):
        el.text = text
    return el

# ---------------------------
# EXTERNAL LOOKUPS (TMDB, OMDb)
# ---------------------------
def tmdb_search_movie(query: str) -> Optional[Dict[str,Any]]:
    key = f"tmdb_movie:{query}".lower()
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get("https://api.themoviedb.org/3/search/movie",
                     {"api_key": TMDB_KEY, "query": query, "language": "es", "include_adult":"false"})
        data = r.json()
        res = data.get("results", [None])[0]
    except Exception:
        res = None
    cache_set("tmdb", key, res)
    return res

def tmdb_search_tv(query: str) -> Optional[Dict[str,Any]]:
    key = f"tmdb_tv:{query}".lower()
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get("https://api.themoviedb.org/3/search/tv",
                     {"api_key": TMDB_KEY, "query": query, "language": "es", "include_adult":"false"})
        data = r.json()
        res = data.get("results", [None])[0]
    except Exception:
        res = None
    cache_set("tmdb", key, res)
    return res

def tmdb_get_tv(tv_id: int) -> Optional[Dict[str,Any]]:
    key = f"tv_id:{tv_id}"
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}", {"api_key": TMDB_KEY, "language":"es"})
        res = r.json()
    except Exception:
        res = None
    cache_set("tmdb", key, res)
    return res

def tmdb_get_season(tv_id:int, season:int) -> Optional[Dict[str,Any]]:
    key = f"tmdb_season:{tv_id}:{season}"
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}",
                     {"api_key": TMDB_KEY, "language":"es"})
        res = r.json()
    except Exception:
        res = None
    cache_set("tmdb", key, res)
    return res

def omdb_lookup_title(query: str) -> Optional[Dict[str,Any]]:
    if not OMDB_KEY:
        return None
    key = f"omdb:{query}".lower()
    cached = cache_get("omdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get("http://www.omdbapi.com/", {"t": query, "apikey": OMDB_KEY, "r":"json"})
        data = r.json()
        res = data if data.get("Response","False") == "True" else None
    except Exception:
        res = None
    cache_set("omdb", key, res)
    return res

# ---------------------------
# Episode parsing
# ---------------------------
def convert_abs_to_season_episode(tv_id: int, abs_ep: int) -> Optional[Tuple[int,int]]:
    tv_data = tmdb_get_tv(tv_id)
    if not tv_data:
        return None
    for season in tv_data.get("seasons", []):
        s_num = season.get("season_number")
        if s_num == 0:  # specials
            continue
        s_detail = tmdb_get_season(tv_id, s_num)
        eps = s_detail.get("episodes", [])
        for idx, ep in enumerate(eps, start=1):
            if ep.get("episode_number") == abs_ep:
                return (s_num, idx)
    return None

# ---------------------------
# PROCESSING PROGRAMS
# ---------------------------
def process_programme(prog: ET.Element):
    title_el = prog.find("title")
    title_raw = safe_text(title_el)
    if not title_raw:
        return
    desc_el = prog.find("desc")
    desc_raw = safe_text(desc_el)

    # Decide si serie o película
    is_series = bool(prog.find("episode-num"))
    is_movie = not is_series

    # -----------------
    # SERIES
    # -----------------
    if is_series and TMDB_KEY:
        # extraer S/E
        ep_el = prog.find("episode-num")
        ep_text = safe_text(ep_el)
        s_num, e_num = None, None
        m = re.match(r"(?i)s(\d+)[ex](\d+)", ep_text)
        if m:
            s_num, e_num = int(m.group(1)), int(m.group(2))
        elif ep_text.isdigit():
            # podría ser episodio absoluto
            tv_data = tmdb_search_tv(title_raw)
            if tv_data:
                s_e = convert_abs_to_season_episode(tv_data.get("id"), int(ep_text))
                if s_e:
                    s_num, e_num = s_e
        if s_num and e_num:
            set_or_create(prog, "episode-num", f"S{s_num:02d}E{e_num:02d}", overwrite=True)
        # obtener sinopsis
        tv_data = tmdb_search_tv(title_raw)
        if tv_data:
            full_tv = tmdb_get_tv(tv_data.get("id"))
            if full_tv:
                set_or_create(prog, "sub-title", full_tv.get("name",""))
                set_or_create(prog, "desc", full_tv.get("overview",""))

    # -----------------
    # PELÍCULAS
    # -----------------
    if is_movie and TMDB_KEY:
        movie_data = tmdb_search_movie(title_raw)
        if movie_data:
            year = movie_data.get("release_date","")[:4] or ""
            set_or_create(prog, "title", f"{title_raw} ({year})")
            set_or_create(prog, "desc", movie_data.get("overview",""))
        elif OMDB_KEY:
            movie_data = omdb_lookup_title(title_raw)
            if movie_data:
                year = movie_data.get("Year","")
                set_or_create(prog, "title", f"{title_raw} ({year})")
                set_or_create(prog, "desc", movie_data.get("Plot",""))

# ---------------------------
# MAIN
# ---------------------------
def main():
    load_cache()
    for url in GUIDE_URLS:
        logging.info(f"Procesando guía {url}")
        try:
            root = parse_gz_xml(url)
        except Exception as e:
            logging.warning(f"No se pudo abrir {url}: {e}")
            continue
        progs = root.findall("programme")
        filtered = [p for p in progs if normalize_id(p.attrib.get("channel")) in FILTER_SET]
        logging.info(f"Total programas: {len(progs)}, Filtrados: {len(filtered)}")
        for i, prog in enumerate(filtered, start=1):
            process_programme(prog)
            if i % 50 == 0:
                logging.info(f"Procesados {i}/{len(filtered)} programas")
        tree = ET.ElementTree(root)
        tree.write(OUT_FILE, encoding="utf-8", xml_declaration=True)
        logging.info(f"Guardado {OUT_FILE}")
    save_cache()
    logging.info("Cache guardada.")

if __name__ == "__main__":
    main()
