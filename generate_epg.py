#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml filtrando canales específicos y enriqueciendo metadatos
con TMDB y OMDb. Usa GPT-5-mini solo para mejorar sinopsis cuando no haya datos fiables.
"""

from __future__ import annotations
import os, re, io, gzip, json, time, hashlib, html, unicodedata, threading, logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any
import requests
import xml.etree.ElementTree as ET
import itertools
import openai

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
OPENAI_KEYS = [k for k in OPENAI_KEYS if k]
key_cycle = itertools.cycle(OPENAI_KEYS)

HEADERS = {
    "User-Agent": "EPG-Builder/1.0",
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
# OpenAI GPT-5-mini synopsis enhancement
# ---------------------------
def get_next_openai_key() -> str:
    return next(key_cycle)

def gpt_enhance_synopsis(title: str, existing_desc: str = "") -> str:
    if not OPENAI_KEYS:
        return existing_desc
    key = get_next_openai_key()
    openai.api_key = key
    logging.info(f"GPT mejora: '{title}' con clave {key[:6]}...")
    prompt = f"""
    Tienes información parcial:
    Título: {title}
    Sinopsis actual: {existing_desc if existing_desc else '(vacía)'}
    Mejora la sinopsis de forma clara y breve. No inventes hechos.
    """
    try:
        response = openai.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        enhanced = response.choices[0].message.content.strip()
        logging.info(f"Sinopsis mejorada GPT: {enhanced[:60]}...")
        return enhanced
    except Exception as e:
        logging.warning(f"No se pudo mejorar sinopsis GPT: {e}")
        return existing_desc

# ---------------------------
# Episode parsing
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
# Series / Movie decision
# ---------------------------
CHANNEL_PREFERENCES = {
    r"\bhbo\b": True,
    r"\bcinemax\b": True,
    r"\bmax\b": True,
    r"\bparamount\b": True,
    r"\btnt\b": True,
    r"\bspace\b": True,
    r"\bdisney\." : False,
    r"\bnetflix\b": False,
    r"\bprime video\b": False,
}

def channel_prefers_movie(channel_id: str) -> Optional[bool]:
    low = channel_id.lower()
    for patt, val in CHANNEL_PREFERENCES.items():
        if re.search(patt, low):
            return val
    return None

def is_series_programme(prog: ET.Element, title_clean: str) -> bool:
    title_l = title_clean.lower()
    sub = safe_text(prog.find("sub-title"))
    cats = [safe_text(c).lower() for c in prog.findall("category")]

    if parse_episode_num(prog):
        return True

    ch_raw = prog.attrib.get("channel","")
    ch_norm = normalize_id(ch_raw)
    ch_pref = channel_prefers_movie(ch_norm)
    if ch_pref is True:
        if sub and re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", sub.lower()):
            return True
        if re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", title_l):
            return True
        return False
    if ch_pref is False:
        if re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", sub.lower() + " " + title_l):
            return True
        return False

    if any(re.search(r"(episodio|cap[ií]tulo|temporada|s\d+e\d+|t\d+e\d+)", c) for c in cats):
        return True

    return False

# ---------------------------
# Build output XML
# ---------------------------
def build_guide_output(root: ET.Element) -> ET.Element:
    out_root = ET.Element("tv")
    for ch in root.findall("channel"):
        ch_id = normalize_id(ch.attrib.get("id",""))
        if ch_id not in FILTER_SET:
            continue
        out_root.append(ch)

    for prog in root.findall("programme"):
        ch_id = normalize_id(prog.attrib.get("channel",""))
        if ch_id not in FILTER_SET:
            continue

        title_el = prog.find("title")
        title_clean = safe_text(title_el)
        if not title_clean:
            continue

        if is_series_programme(prog, title_clean):
            # --- SERIES ---
            ep_tuple = parse_episode_num(prog)
            season_num, ep_num = ep_tuple if ep_tuple else (None, None)

            tmdb_hit = tmdb_search_tv(title_clean)
            tv_overview = tmdb_hit.get("overview","") if tmdb_hit else ""
            if not tv_overview:
                om_hit = omdb_lookup_title(title_clean)
                tv_overview = om_hit.get("Plot","") if om_hit else ""
            # Mejorar con GPT solo si no hay TMDB/OMDb confiable
            tv_overview = gpt_enhance_synopsis(title_clean, tv_overview) if not tv_overview else tv_overview

            set_or_create(prog, "sub-title", title_clean)
            set_or_create(prog, "desc", tv_overview)

        else:
            # --- MOVIES ---
            tmdb_hit = tmdb_search_movie(title_clean)
            om_hit = omdb_lookup_title(title_clean)
            desc_text = tmdb_hit.get("overview","") if tmdb_hit else ""
            if not desc_text and om_hit:
                desc_text = om_hit.get("Plot","")
            if not desc_text:
                desc_text = gpt_enhance_synopsis(title_clean)
            set_or_create(prog, "desc", desc_text)

        out_root.append(prog)
        logging.info(f"Procesado: {title_clean}")

    return out_root

# ---------------------------
# MAIN
# ---------------------------
def main():
    load_cache()
    final_root = ET.Element("tv")
    for url in GUIDE_URLS:
        try:
            src_root = parse_gz_xml(url)
            guide_out = build_guide_output(src_root)
            for el in guide_out:
                final_root.append(el)
        except Exception as e:
            logging.warning(f"Error procesando {url}: {e}")

    save_cache()
    tree = ET.ElementTree(final_root)
    tree.write(OUT_FILE, encoding="utf-8", xml_declaration=True)
    logging.info(f"Archivo generado: {OUT_FILE}")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
