#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml filtrando canales específicos y enriqueciendo metadatos
con TMDB (series/películas, sinopsis en español) y OMDb como respaldo para películas.
Opcional: usa GPT para mejorar sinopsis si hay claves OpenAI disponibles.

Principios:
- NUNCA modificamos <channel> (id, display-name, icon, url).
- No inventamos S/E ni años.
- Añadimos sinopsis solo si es confiable (TMDB, OMDb, GPT como mejora opcional).
- Primera comprobación: TMDB/OMDb; GPT solo mejora si hay clave disponible.

Requerido en entorno:
- TMDB_API_KEY
- OMDB_API_KEY  (recomendado)
- OPENAI_API_KEY(s) (opcional; para mejorar sinopsis)

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
    # (mantén todos los canales que ya tenías, no borrar)
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

OPENAI_KEYS = [
    os.getenv("OPENAI_API_KEY","").strip(),
    os.getenv("OPENAI_API_KEY_1","").strip(),
    os.getenv("OPENAI_API_KEY_2","").strip(),
    os.getenv("OPENAI_API_KEY_3","").strip(),
    os.getenv("OPENAI_API_KEY_4","").strip(),
    os.getenv("OPENAI_API_KEY_5","").strip(),
    os.getenv("OPENAI_API_KEY_6","").strip(),
    os.getenv("OPENAI_API_KEY_7","").strip(),
    os.getenv("OPENAI_API_KEY_8","").strip(),
]
OPENAI_KEYS = [k for k in OPENAI_KEYS if k]
_openai_key_index = 0

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
# TMDB / OMDb lookups
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

def omdb_lookup(title: str, year: Optional[int] = None, type_: str="movie") -> Optional[Dict[str,Any]]:
    key = f"omdb:{title}:{year}:{type_}".lower()
    cached = cache_get("omdb", key)
    if cached is not None:
        return cached
    try:
        params = {"apikey": OMDB_KEY, "t": title, "type": type_}
        if year:
            params["y"] = str(year)
        r = http_get("http://www.omdbapi.com/", params=params)
        data = r.json()
        res = data if data.get("Response","False")=="True" else None
    except Exception:
        res = None
    cache_set("omdb", key, res)
    return res

# ---------------------------
# GPT SYNOPSIS
# ---------------------------

def rotate_openai_key():
    global _openai_key_index
    if not OPENAI_KEYS:
        return None
    key = OPENAI_KEYS[_openai_key_index]
    _openai_key_index = (_openai_key_index + 1) % len(OPENAI_KEYS)
    return key

def gpt_enhance_synopsis(prompt: str, existing_desc: str = "") -> str:
    import openai
    retries = 3
    delay = 1.0
    for attempt in range(retries):
        key = rotate_openai_key()
        if not key:
            logging.warning("No hay claves OpenAI disponibles. Se mantiene descripción existente.")
            return existing_desc
        try:
            openai.api_key = key
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role":"system","content":"Eres un asistente que resume y mejora sinopsis de series y películas."},
                    {"role":"user","content":prompt}
                ],
                temperature=0.7,
                max_tokens=300
            )
            text = response.choices[0].message.content.strip()
            return text
        except openai.error.RateLimitError as e:
            logging.warning("Clave OpenAI saturada, rotando a siguiente... intento %d/%d", attempt+1, retries)
            time.sleep(delay)
            delay *= 2
        except Exception as e:
            logging.warning("Error GPT: %s", e)
            return existing_desc
    logging.warning("No se pudo generar sinopsis con GPT, se mantiene la existente.")
    return existing_desc

# ---------------------------
# EPISODE PARSING
# ---------------------------

def parse_episode_number(elem: ET.Element) -> Optional[Tuple[int,int]]:
    """
    Intenta detectar S/E en:
    <episode-num system="onscreen">S7 E8</episode-num>
    """
    en = elem.find("episode-num")
    if en is not None and en.text:
        m = re.search(r"S(\d+)\s*E(\d+)", en.text, re.IGNORECASE)
        if m:
            return int(m.group(1)), int(m.group(2))
    return None

# ---------------------------
# MAIN PROCESS
# ---------------------------

def process_programme(prog: ET.Element):
    title_el = prog.find("title")
    sub_el = prog.find("sub-title")
    desc_el = prog.find("desc")
    category_el = prog.find("category")
    title_text = safe_text(title_el)
    sub_text = safe_text(sub_el)
    desc_text = safe_text(desc_el)
    category_text = safe_text(category_el).lower()
    season_episode = parse_episode_number(prog)

    # Solo intentamos mejorar si hay TMDB/OMDb
    enhanced_desc = desc_text
    if "serie" in category_text and season_episode:
        tv_data = tmdb_search_tv(title_text)
        if tv_data:
            enhanced_desc = tv_data.get("overview") or enhanced_desc
            set_or_create(prog, "sub-title", tv_data.get("name",""))
    elif "película" in category_text or "movie" in category_text:
        movie_data = tmdb_search_movie(title_text)
        if movie_data:
            enhanced_desc = movie_data.get("overview") or enhanced_desc
        else:
            omdb_data = omdb_lookup(title_text)
            if omdb_data:
                enhanced_desc = omdb_data.get("Plot") or enhanced_desc

    # GPT mejora opcional
    if OPENAI_KEYS:
        prompt = f"Mejora esta sinopsis para '{title_text}': {enhanced_desc}"
        enhanced_desc = gpt_enhance_synopsis(prompt, existing_desc=enhanced_desc)

    set_or_create(prog, "desc", enhanced_desc)

def main():
    load_cache()
    # Descargar y combinar todos los XML
    programmes = []
    for url in GUIDE_URLS:
        logging.info("Descargando y parseando: %s", url)
        root = parse_gz_xml(url)
        for prog in root.findall("programme"):
            ch_id = normalize_id(prog.attrib.get("channel",""))
            if ch_id in FILTER_SET:
                programmes.append(prog)
    logging.info("Programas filtrados: %d", len(programmes))

    # Procesar programas
    for idx, prog in enumerate(programmes,1):
        process_programme(prog)
        if idx % 50 == 0:
            logging.info("Procesados %d programas...", idx)

    # Guardar resultado
    tv = ET.Element("tv")
    for prog in programmes:
        tv.append(prog)
    tree = ET.ElementTree(tv)
    tree.write(OUT_FILE, encoding="utf-8", xml_declaration=True)
    logging.info("Archivo generado: %s", OUT_FILE)
    save_cache()

if __name__ == "__main__":
    main()
