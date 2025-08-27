#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml filtrando canales específicos y enriqueciendo metadatos
con TMDB (series/películas, sinopsis en español, título de episodio, etc.)
y OMDb como respaldo para películas. Usa Google Knowledge Graph (opcional)
como primera comprobación si GOOGLE_API_KEY está en el entorno.

Principios:
- NUNCA modificamos <channel> (id, display-name, icon, url).
- No inventamos S/E ni años.
- Añadimos sinopsis solo si es confiable (TMDB, OMDb, KG).
- Primera comprobación: Google (Knowledge Graph), si está disponible.

Requerido en entorno:
- TMDB_API_KEY
- OMDB_API_KEY  (recomendado)
- GOOGLE_API_KEY (opcional; para Knowledge Graph)

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
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "").strip()

HEADERS = {
    "User-Agent": "EPG-Builder/1.0 (+https://github.com/dashbrox/my-filter)",
    "Accept-Language": "es-ES,es;q=0.9",
}

CACHE_PATH = ".epg_cache_tmdb_omdb_kg.json"
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
    # Format like: YYYYMMDDHHMMSS +/-ZZZZ
    # We'll parse naive then apply offset if present.
    m = re.match(r"(\d{14})(?:\s*([+-]\d{4}))?", dt_raw)
    if not m:
        raise ValueError(f"Invalid xmltv datetime: {dt_raw}")
    base = m.group(1)
    tzpart = m.group(2)
    dt = datetime.strptime(base, "%Y%m%d%H%M%S")
    if tzpart:
        # tz like -0500
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
# EXTERNAL LOOKUPS (TMDB, OMDb, Google KG)
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

def google_kg_lookup(query: str) -> Optional[Dict[str,Any]]:
    """
    Uses Google Knowledge Graph Search API if GOOGLE_API_KEY is set.
    Returns top result (type hints).
    """
    if not GOOGLE_API_KEY:
        return None
    key = f"kg:{query}".lower()
    cached = cache_get("kg", key)
    if cached is not None:
        return cached
    try:
        r = http_get("https://kgsearch.googleapis.com/v1/entities:search",
                     {"query": query, "key": GOOGLE_API_KEY, "limit":1, "languages":"es,en"})
        data = r.json()
        item = data.get("itemListElement",[None])[0]
        if item:
            res = item.get("result")
        else:
            res = None
    except Exception:
        res = None
    cache_set("kg", key, res)
    return res

# ---------------------------
# Channel rules (preferences)
# ---------------------------

# If a channel matches these regexes, apply the rule:
# value True = prefer movie by default, False = prefer series by default
CHANNEL_PREFERENCES = {
    r"\bhbo\b": True,
    r"\bcinemax\b": True,
    r"\bmax\b": True,         # HBO/Max - prefer movies unless explicit episode information
    r"\bparamount\b": True,
    r"\btnt\b": True,
    r"\bspace\b": True,
    r"\btelecine\b": True,
    r"\bmovistar\b": True,
    r"\bdisney\." : False,    # Disney Channel tends to be shows; but be conservative
    r"\bnetflix\b": False,    # Netflix often shows episodes in guides
    r"\bprime video\b": False,
    # add other rules as needed
}

def channel_prefers_movie(channel_id: str) -> Optional[bool]:
    low = channel_id.lower()
    for patt, val in CHANNEL_PREFERENCES.items():
        if re.search(patt, low):
            return val
    return None

# ---------------------------
# Episode parsing (robust)
# ---------------------------

def convert_abs_to_season_episode(tv_id: int, abs_ep: int) -> Optional[Tuple[int,int]]:
    tv_data = tmdb_get_tv(tv_id)
    if not tv_data:
        return None
    remaining = abs_ep
    num_seasons = tv_data.get("number_of_seasons", 0)
    for s in range(1, num_seasons+1):
        season_data = tmdb_get_season(tv_id, s)
        if not season_data or "episodes" not in season_data:
            continue
        num_eps = len(season_data["episodes"])
        if remaining <= num_eps:
            return s, remaining
        remaining -= num_eps
    return None

def parse_episode_num(prog: ET.Element, tv_id: Optional[int]=None) -> Optional[Tuple[int,int]]:
    # Check <episode-num> entries
    for ep in prog.findall("episode-num"):
        val = safe_text(ep)
        if not val:
            continue
        sys = (ep.attrib.get("system") or "").lower().strip()
        m = re.match(r"(\d+)\.(\d+)", val)
        if sys == "xmltv_ns" and m:
            return int(m.group(1))+1, int(m.group(2))+1
        m2 = re.search(r"[Ss](\d{1,2})[Ee](\d{1,2})", val)
        if m2:
            return int(m2.group(1)), int(m2.group(2))
        m3 = re.match(r"E(\d+)", val)
        if m3 and tv_id:
            return convert_abs_to_season_episode(tv_id, int(m3.group(1)))
    # Try title formats: S01E02, 1x03, Season 1 Episode 3, etc
    title = safe_text(prog.find("title"))
    if title:
        m4 = re.search(r"[Ss]?(\d{1,2})[xXeE:](\d{1,2})", title)
        if m4:
            return int(m4.group(1)), int(m4.group(2))
        m5 = re.search(r"season\s*(\d{1,2}).*episode\s*(\d{1,3})", title.lower())
        if m5:
            return int(m5.group(1)), int(m5.group(2))
    return None

# ---------------------------
# Decision: is_series_programme (full layered logic)
# ---------------------------

def is_series_programme(prog: ET.Element, title_clean: str) -> bool:
    """
    Return True if we believe programme is a series episode, False if movie or unknown.
    Decision order:
      1) explicit episode-num formats in programme (strong)
      2) channel preferences (HBO -> prefer movie, Netflix -> prefer series)
      3) title/subtitle/category hints (episodio, temporada, S01E01, etc.)
      4) duration heuristic (minutes)
      5) TMDB search (compare results)
      6) OMDb lookup (Type field)
      7) Google Knowledge Graph (type)
      8) popularity comparison (vote_count) as tie-break
      9) fallback: prefer MOVIE (we changed default away from series)
    """
    title_l = title_clean.lower()
    sub = safe_text(prog.find("sub-title"))
    cats = [safe_text(c).lower() for c in prog.findall("category")]

    # 1) explicit episode numbers in XML
    if parse_episode_num(prog):
        return True

    # 2) Channel rule
    ch_raw = prog.attrib.get("channel","")
    ch_norm = normalize_id(ch_raw)
    ch_pref = channel_prefers_movie(ch_norm)
    # If channel says prefer movie, and there are no episode hints -> movie
    if ch_pref is True:
        # If explicit episode hints in sub/title or categories -> series
        if sub and re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", sub.lower()):
            return True
        if re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", title_l):
            return True
        # otherwise prefer movie
        return False
    if ch_pref is False:
        # If channel prefers series: require fewer signals to pick series
        if re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", title_l):
            return True
        # else continue to deeper checks

    # 3) Title/subtitle/category hints
    if sub and sub.lower() != title_clean.lower() and re.search(r"(episodio|cap[ií]tulo|temporada|ep\.)", sub.lower()):
        return True
    if any(any(k in c for k in ("episodio","capítulo","temporada","capitulo")) for c in cats):
        return True

    # 4) duration heuristic
    duration = duration_minutes_from_prog(prog)
    if duration is not None:
        if duration >= 85:  # typical movie
            return False
        if 20 <= duration <= 75:
            # typical episode length
            return True

    # 5) TMDB lookup
    tv_hit = tmdb_search_tv(title_clean)
    movie_hit = tmdb_search_movie(title_clean)

    # If only one hit -> take it
    if tv_hit and not movie_hit:
        return True
    if movie_hit and not tv_hit:
        return False

    # 6) OMDb fallback (type field explicit)
    omdb = omdb_lookup_title(title_clean)
    if omdb:
        typ = omdb.get("Type","").lower()
        if typ == "movie":
            return False
        if typ in ("series","episode"):
            return True

    # 7) Google Knowledge Graph (if available)
    kg = google_kg_lookup(title_clean)
    if kg:
        kg_types = kg.get("@type") or kg.get("type") or kg.get("description")
        # normalize
        kg_str = json.dumps(kg_types).lower() if kg_types else ""
        if "movie" in kg_str or "film" in kg_str:
            return False
        if "tvseries" in kg_str or "tv series" in kg_str or "television" in kg_str:
            return True

    # 8) If both TMDB tv and movie exist -> use popularity comparison
    if tv_hit and movie_hit:
        movie_votes = movie_hit.get("vote_count", 0) or 0
        tv_votes = tv_hit.get("vote_count", 0) or 0
        # if movie vastly more popular -> movie
        if movie_votes >= max(50, tv_votes * 3):
            return False
        # conversely if tv vastly more popular -> series
        if tv_votes >= max(50, movie_votes * 3):
            return True
        # else uncertain - fallthrough

    # 9) FINAL fallback: prefer MOVIE (do not invent series S/E)
    return False

# ---------------------------
# Build outputs (series/movie)
# ---------------------------

def build_series_output(prog: ET.Element, title_clean: str):
    """
    Only set SxEy if confirmed/parsed. If not, do NOT invent S/E.
    Add episode title and description only if confident.
    If episode not found, add series overview (TMDB) as extra with a marker.
    """
    tv_hit = tmdb_search_tv(title_clean)
    tv_id = tv_hit.get("id") if tv_hit else None
    se = parse_episode_num(prog, tv_id)
    ep_title = None
    ep_overview = None

    if se and tv_id:
        s,e = se
        season_data = tmdb_get_season(tv_id, s)
        if season_data and "episodes" in season_data and e <= len(season_data["episodes"]):
            ep_data = season_data["episodes"][e-1]
            ep_title = ep_data.get("name","").strip()
            ep_overview = ep_data.get("overview","").strip()

    # If no episode found, DO NOT invent (no S/E). But we can add series overview as extra.
    if se and ep_title:
        # overwrite title only when S/E confirmed
        set_or_create(prog, "title", f"{title_clean} (S{se[0]} E{se[1]})", overwrite=True)
        set_or_create(prog, "sub-title", ep_title, overwrite=True)
    else:
        # keep original title untouched (do not append S/E)
        set_or_create(prog, "title", title_clean, overwrite=False)
        sub_orig = safe_text(prog.find("sub-title"))
        if sub_orig:
            set_or_create(prog, "sub-title", sub_orig, overwrite=False)

    # description: prefer episode overview; else series overview from TMDB (marked)
    if ep_overview:
        desc_text = f"“{ep_title}”\n{ep_overview}"
        set_or_create(prog, "desc", desc_text, overwrite=True)
    else:
        if tv_hit:
            series_overview = tv_hit.get("overview","").strip()
            if series_overview:
                marked = f"(Sin episodio disponible) {series_overview}"
                set_or_create(prog, "desc", marked, overwrite=False)

def build_movie_output(prog: ET.Element, title_clean: str):
    """
    For movies: set title (preserve if exists), set desc only if OMDb/TMDB provide reliable overview.
    """
    # keep title if exists, else set
    set_or_create(prog, "title", title_clean, overwrite=False)

    # Prefer TMDB movie overview if available
    m_hit = tmdb_search_movie(title_clean)
    if m_hit:
        overview = m_hit.get("overview","").strip()
        if overview:
            set_or_create(prog, "desc", overview, overwrite=True)
            return

    # Fallback to OMDb plot if exists
    om = omdb_lookup_title(title_clean)
    if om and om.get("Plot") and om.get("Plot") != "N/A":
        set_or_create(prog, "desc", om.get("Plot"), overwrite=True)
        return

    # If no reliable synopsis, DO NOT invent a description (leave existing desc)
    # but if desc missing and we have nothing, we leave it blank (policy: no invented desc)

# ---------------------------
# Processing pipeline
# ---------------------------

def process_url(url: str, root_out: ET.Element, added_channels: set, added_programmes: set):
    try:
        xml_root = parse_gz_xml(url)
    except Exception as e:
        logging.warning("No se pudo procesar %s: %s", url, e)
        return

    # Append channels (unaltered) for those in FILTER_SET
    for ch in xml_root.findall("channel"):
        ch_id = normalize_id(ch.attrib.get("id"))
        if ch_id in FILTER_SET and ch_id not in added_channels:
            root_out.append(ch)   # DO NOT modify channel content
            added_channels.add(ch_id)

    # Programmes
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

        try:
            if is_series_programme(prog, title_clean):
                build_series_output(prog, title_clean)
            else:
                build_movie_output(prog, title_clean)
        except Exception as e:
            logging.exception("Error procesando programa '%s' en canal '%s': %s", title_clean, ch_id, e)
            # On error, keep original programme as-is (do not drop it)

        # Ensure lang attribute on tags we may have modified
        for tag in ("title","sub-title","desc"):
            el = prog.find(tag)
            if el is not None and "lang" not in el.attrib:
                el.set("lang","es")

        root_out.append(prog)
        added_programmes.add(uniq)

# ---------------------------
# MAIN
# ---------------------------

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
        t.daemon = True
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    # write file
    ET.ElementTree(root_out).write(OUT_FILE, encoding="utf-8", xml_declaration=True)
    save_cache()
    logging.info("Guía generada: %s", OUT_FILE)

if __name__ == "__main__":
    main()
