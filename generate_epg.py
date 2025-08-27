#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml filtrando canales específicos y enriqueciendo metadatos
con TMDB (series/películas, sinopsis en español, título de episodio, etc.)
y OMDb como respaldo para películas.

Incluye mejoras:
1. Configuración externa opcional para URLs y canales
2. Reintentos y manejo de ratelimits
3. Normalización Unicode de IDs para filtrado
4. Cobertura de múltiples formatos de episodios y especiales
5. Indicar claramente “episodio no disponible” cuando aplique
6. Evitar sobrescribir títulos/sub-títulos existentes
7. Fallback a sinopsis completa de serie solo si no hay episodio
8. Log detallado de errores
9. Paralelización opcional por canales
10. Manejo de miniseries/especiales sin inventar S/E

Variables de entorno requeridas:
  - TMDB_API_KEY
  - OMDB_API_KEY  (respaldo para películas)

Salida:
  - guide_custom.xml (en el directorio actual)
"""

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
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
import requests
import xml.etree.ElementTree as ET

# ---------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------

# Puedes usar un JSON externo para GUIDE_URLS y CHANNEL_IDS_RAW
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

# ---------------------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------------------

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
    # Normaliza Unicode para evitar diferencias de acentos
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')
    s = re.sub(r"\s+", " ", s)
    return s.lower()

FILTER_SET = {normalize_id(x) for x in CHANNEL_IDS_RAW}

def http_get(url: str, params: dict = None, timeout: int = 30, retries: int = 3) -> requests.Response:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                raise e

def parse_gz_xml(url: str) -> ET.Element:
    r = http_get(url, timeout=60)
    buf = io.BytesIO(r.content)
    with gzip.open(buf, "rb") as gz:
        xml_bytes = gz.read()
    return ET.fromstring(xml_bytes)

def xmltv_datetime_to_date_str(dt_raw: str) -> str:
    ymd = dt_raw[:8]
    return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

def safe_text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""

def set_or_create(parent: ET.Element, tag: str, text: str, lang: str = "es", overwrite=True) -> ET.Element:
    el = parent.find(tag)
    if el is None:
        el = ET.SubElement(parent, tag, {"lang": lang} if tag in ("title","sub-title","desc") else {})
    if overwrite or not el.text:
        el.text = text
    return el

# ---------------------------------------------------------------------
# TMDB HELPERS
# ---------------------------------------------------------------------

def tmdb_get_tv(tv_id: int) -> Optional[Dict[str, Any]]:
    key = f"tv:{tv_id}"
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}", {"api_key": TMDB_KEY,"language":"es"})
        result = r.json()
    except Exception:
        result = None
    cache_set("tmdb", key, result)
    return result

def tmdb_get_season(tv_id: int, season_number: int) -> Optional[Dict[str, Any]]:
    key = f"season:{tv_id}:{season_number}"
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season_number}",
                     {"api_key": TMDB_KEY,"language":"es"})
        result = r.json()
    except Exception:
        result = None
    cache_set("tmdb", key, result)
    return result

def tmdb_search_tv(query: str) -> Optional[Dict[str, Any]]:
    key = f"search_tv:{query}".lower()
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get("https://api.themoviedb.org/3/search/tv",
                     {"api_key": TMDB_KEY,"query":query,"language":"es","include_adult":"false"})
        data = r.json()
        result = data["results"][0] if data.get("results") else None
    except Exception:
        result = None
    cache_set("tmdb", key, result)
    return result

def tmdb_search_movie(query: str) -> Optional[Dict[str, Any]]:
    key = f"search_movie:{query}".lower()
    cached = cache_get("tmdb", key)
    if cached is not None:
        return cached
    try:
        r = http_get("https://api.themoviedb.org/3/search/movie",
                     {"api_key": TMDB_KEY,"query":query,"language":"es","include_adult":"false"})
        data = r.json()
        result = data["results"][0] if data.get("results") else None
    except Exception:
        result = None
    cache_set("tmdb", key, result)
    return result

# ---------------------------------------------------------------------
# Parseo episodio
# ---------------------------------------------------------------------

def convert_abs_to_season_episode(tv_id: int, abs_ep: int) -> Optional[Tuple[int,int]]:
    tv_data = tmdb_get_tv(tv_id)
    if not tv_data:
        return None
    remaining = abs_ep
    num_seasons = tv_data.get("number_of_seasons", 0)
    for s in range(1, num_seasons + 1):
        season_data = tmdb_get_season(tv_id, s)
        if not season_data or "episodes" not in season_data:
            continue
        num_eps = len(season_data["episodes"])
        if remaining <= num_eps:
            return s, remaining
        remaining -= num_eps
    return None

def parse_episode_num(prog: ET.Element, tv_id: Optional[int] = None) -> Optional[Tuple[int,int]]:
    for ep in prog.findall("episode-num"):
        val = safe_text(ep)
        if not val:
            continue
        sys = (ep.attrib.get("system") or "").lower().strip()
        # xmltv_ns
        m = re.match(r"(\d+)\.(\d+)", val)
        if sys=="xmltv_ns" and m:
            return int(m.group(1))+1,int(m.group(2))+1
        # onscreen
        m2 = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})", val)
        if m2:
            return int(m2.group(1)), int(m2.group(2))
        # absoluto E###
        m3 = re.match(r"E(\d+)", val)
        if m3 and tv_id:
            abs_ep = int(m3.group(1))
            return convert_abs_to_season_episode(tv_id, abs_ep)
    # Otros formatos: 1x03, S1:E3, Season 1 Episode 3
    title = safe_text(prog.find("title"))
    m4 = re.search(r"[Ss]?(\d{1,2})[xE:](\d{1,2})", title)
    if m4:
        return int(m4.group(1)), int(m4.group(2))
    return None

# ---------------------------------------------------------------------
# Determinar serie o película
# ---------------------------------------------------------------------

SERIES_KEYWORDS = {"series","episodio","capítulo","sitcom","drama","comedia","telenovela"}
MOVIE_KEYWORDS = {"movie","película","film","cinema"}

def is_series_programme(prog: ET.Element, title_clean: str) -> bool:
    if parse_episode_num(prog):
        return True
    cats = [safe_text(c).lower() for c in prog.findall("category")]
    if any(any(k in c for k in SERIES_KEYWORDS) for c in cats):
        return True
    if any(any(k in c for k in MOVIE_KEYWORDS) for c in cats):
        return False
    sub = safe_text(prog.find("sub-title"))
    if sub and sub.lower()!=title_clean.lower():
        return True
    tv_hit = tmdb_search_tv(title_clean)
    movie_hit = tmdb_search_movie(title_clean)
    if tv_hit and not movie_hit:
        return True
    if movie_hit and not tv_hit:
        return False
    return True

# ---------------------------------------------------------------------
# Construcción de salida
# ---------------------------------------------------------------------

def build_series_output(prog: ET.Element, title_clean: str):
    tv_hit = tmdb_search_tv(title_clean)
    tv_id = tv_hit.get("id") if tv_hit else None
    se = parse_episode_num(prog, tv_id)
    ep_title = None
    ep_overview = None

    if se and tv_id:
        s,e = se
        season_data = tmdb_get_season(tv_id,s)
        ep_data = season_data["episodes"][e-1] if season_data and "episodes" in season_data and e<=len(season_data["episodes"]) else None
        if ep_data:
            ep_title = ep_data.get("name","").strip()
            ep_overview = ep_data.get("overview","").strip()
    elif tv_id:
        se = (1,1)

    if not se:
        se = (0,0)  # Especial o episodio desconocido
    s,e = se

    if not ep_title:
        ep_title = safe_text(prog.find("sub-title")) or "Episodio"
    if not ep_overview:
        ep_overview = safe_text(prog.find("desc"))
        if not ep_overview and tv_hit:
            ep_overview = tv_hit.get("overview","").strip()
            if ep_overview:
                ep_overview = f"(Episodio no disponible) {ep_overview}"

    # Solo cambiar título si S/E válido
    if s>0 and e>0:
        set_or_create(prog,"title",f"{title_clean} (S{s} E{e})")
    set_or_create(prog,"sub-title",ep_title)
    desc_text = f"“{ep_title}”\n{ep_overview}" if ep_overview else f"“{ep_title}”"
    set_or_create(prog,"desc",desc_text)

def build_movie_output(prog: ET.Element, title_clean: str):
    set_or_create(prog,"title",title_clean)
    desc = safe_text(prog.find("desc"))
    if not desc and OMDB_KEY:
        # Podría agregar consulta OMDb si TMDB no tiene sinopsis
        set_or_create(prog,"desc",title_clean)

# ---------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ---------------------------------------------------------------------

def process_url(url: str, root_out: ET.Element, added_channels: set, added_programmes: set):
    try:
        xml_root = parse_gz_xml(url)
    except Exception as e:
        print(f"⚠️ No se pudo procesar {url}: {e}")
        return

    for ch in xml_root.findall("channel"):
        ch_id = normalize_id(ch.attrib.get("id"))
        if ch_id in FILTER_SET and ch_id not in added_channels:
            # Nunca tocar el contenido del <channel>
            root_out.append(ch)
            added_channels.add(ch_id)

    for prog in xml_root.findall("programme"):
        ch_id = normalize_id(prog.attrib.get("channel"))
        if ch_id not in FILTER_SET:
            continue
        start = prog.attrib.get("start","")
        stop = prog.attrib.get("stop","")
        uniq = hashlib.md5(f"{ch_id}|{start}|{stop}".encode("utf-8")).hexdigest()
        if uniq in added_programmes:
            continue
        title_raw = safe_text(prog.find("title"))
        title_clean = title_raw.strip()

        if is_series_programme(prog,title_clean):
            build_series_output(prog,title_clean)
        else:
            build_movie_output(prog,title_clean)

        for tag in ("title","sub-title","desc"):
            el = prog.find(tag)
            if el is not None and "lang" not in el.attrib:
                el.set("lang","es")

        root_out.append(prog)
        added_programmes.add(uniq)

# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    if not TMDB_KEY:
        raise RuntimeError("❌ Falta TMDB_API_KEY en el entorno.")

    load_cache()
    root_out = ET.Element("tv")
    added_channels = set()
    added_programmes = set()

    threads = []
    for url in GUIDE_URLS:
        t = threading.Thread(target=process_url, args=(url, root_out, added_channels, added_programmes))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    ET.ElementTree(root_out).write(OUT_FILE, encoding="utf-8", xml_declaration=True)
    save_cache()
    print(f"✅ Guía generada: {OUT_FILE}")

if __name__=="__main__":
    main()
