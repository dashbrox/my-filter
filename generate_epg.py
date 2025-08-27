#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml filtrando canales específicos y enriqueciendo metadatos
con TMDB (series/películas, sinopsis en español, título de episodio, etc.)
y OMDb como respaldo para películas.

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
import math
import html
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any, List
import requests
import xml.etree.ElementTree as ET

# ---------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------

GUIDE_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

# Lista EXACTA que proporcionaste (algunas traen &amp;). Normalizaremos al comparar.
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
    # Internacionales
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

# Cache en memoria (y opcional disco)
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
    # Decodifica entidades HTML y normaliza espacios
    s = html.unescape(ch_id).strip()
    # Quita dobles espacios si los hubiera
    s = re.sub(r"\s+", " ", s)
    return s

FILTER_SET = {normalize_id(x) for x in CHANNEL_IDS_RAW}

def http_get(url: str, params: dict = None, timeout: int = 30) -> requests.Response:
    r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r

def parse_gz_xml(url: str) -> ET.Element:
    r = http_get(url, timeout=60)
    buf = io.BytesIO(r.content)
    with gzip.open(buf, "rb") as gz:
        xml_bytes = gz.read()
    return ET.fromstring(xml_bytes)

def xmltv_datetime_to_date_str(dt_raw: str) -> str:
    """
    Convierte 'YYYYMMDDHHMMSS +0000' o 'YYYYMMDDHHMMSS' a 'YYYY-MM-DD' (fecha local del atributo).
    """
    # Solo usamos los primeros 8 dígitos (fecha del atributo tal cual)
    ymd = dt_raw[:8]
    return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

def safe_text(el: Optional[ET.Element]) -> str:
    return (el.text or "").strip() if el is not None else ""

def set_or_create(parent: ET.Element, tag: str, text: str, lang: str = "es") -> ET.Element:
    el = parent.find(tag)
    if el is None:
        el = ET.SubElement(parent, tag, {"lang": lang} if tag in ("title", "sub-title", "desc") else {})
    el.text = text
    return el

def looks_english(text: str) -> bool:
    # Heurística simple para detectar inglés (no crítico porque pedimos a TMDB en español)
    text = text.strip()
    if not text:
        return False
    common = sum(text.lower().count(w) for w in [" the ", " and ", " of ", " in ", " to "])
    return common >= 1

# ---------------------------------------------------------------------
# PARSEO DE episode-num
# ---------------------------------------------------------------------

def parse_episode_num(prog: ET.Element) -> Optional[Tuple[int, int]]:
    """
    Intenta obtener (season, episode) a partir de <episode-num>.
    Soporta:
      - system="xmltv_ns"   -> '0.14.0/1' (base 0) => S1 E15
      - system="onscreen"   -> 'S02E07' etc.
    """
    for ep in prog.findall("episode-num"):
        sys = (ep.attrib.get("system") or "").lower().strip()
        val = safe_text(ep)
        if not val:
            continue

        if sys == "xmltv_ns":
            # formato: season . episode . [part] [/ total]
            m = re.match(r"(\d+)\.(\d+)(?:\.\d+)?", val)
            if m:
                s = int(m.group(1)) + 1
                e = int(m.group(2)) + 1
                return s, e

        # onscreen o vacío: buscar SxxEyy
        m2 = re.search(r"[Ss](\d{1,2})\s*[Ee](\d{1,2})", val)
        if m2:
            return int(m2.group(1)), int(m2.group(2))

    # A veces ya viene en el título
    title = safe_text(prog.find("title"))
    m3 = re.search(r"[Ss](\d{1,2})\s*[Ee](\d{1,2})", title)
    if m3:
        return int(m3.group(1)), int(m3.group(2))

    return None

# ---------------------------------------------------------------------
# TMDB / OMDb HELPERS (con caché)
# ---------------------------------------------------------------------

def tmdb_search_tv(query: str) -> Optional[Dict[str, Any]]:
    if not TMDB_KEY: 
        return None
    key = f"{query}".lower()
    cached = cache_get("tmdb_search_tv", key)
    if cached is not None:
        return cached

    params = {
        "api_key": TMDB_KEY,
        "query": query,
        "language": "es",
        "include_adult": "false",
    }
    try:
        data = http_get("https://api.themoviedb.org/3/search/tv", params).json()
        result = data["results"][0] if data.get("results") else None
    except Exception:
        result = None

    cache_set("tmdb_search_tv", key, result)
    return result

def tmdb_get_tv(tv_id: int) -> Optional[Dict[str, Any]]:
    if not TMDB_KEY:
        return None
    key = f"{tv_id}"
    cached = cache_get("tmdb_get_tv", key)
    if cached is not None:
        return cached
    params = {"api_key": TMDB_KEY, "language": "es"}
    try:
        result = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}", params).json()
    except Exception:
        result = None
    cache_set("tmdb_get_tv", key, result)
    return result

def tmdb_get_season(tv_id: int, season_number: int) -> Optional[Dict[str, Any]]:
    if not TMDB_KEY:
        return None
    key = f"{tv_id}:{season_number}"
    cached = cache_get("tmdb_get_season", key)
    if cached is not None:
        return cached
    params = {"api_key": TMDB_KEY, "language": "es"}
    try:
        result = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season_number}", params).json()
    except Exception:
        result = None
    cache_set("tmdb_get_season", key, result)
    return result

def tmdb_get_episode(tv_id: int, season: int, episode: int) -> Optional[Dict[str, Any]]:
    if not TMDB_KEY:
        return None
    key = f"{tv_id}:{season}:{episode}"
    cached = cache_get("tmdb_get_episode", key)
    if cached is not None:
        return cached
    params = {"api_key": TMDB_KEY, "language": "es"}
    try:
        result = http_get(f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}", params).json()
    except Exception:
        result = None
    cache_set("tmdb_get_episode", key, result)
    return result

def tmdb_find_episode_by_airdate(tv_id: int, airdate: str) -> Optional[Tuple[int, int, Dict[str, Any]]]:
    """
    Busca un episodio por fecha de emisión (YYYY-MM-DD), iterando temporadas.
    Devuelve (season, episode, episode_data) o None.
    """
    tv = tmdb_get_tv(tv_id)
    if not tv:
        return None
    num_seasons = int(tv.get("number_of_seasons") or 0)
    # Buscar fecha exacta y +/- 1 día por seguridad
    candidates = {airdate}
    try:
        d = datetime.strptime(airdate, "%Y-%m-%d")
        candidates.add((d - timedelta(days=1)).strftime("%Y-%m-%d"))
        candidates.add((d + timedelta(days=1)).strftime("%Y-%m-%d"))
    except Exception:
        pass

    for s in range(1, num_seasons + 1):
        season = tmdb_get_season(tv_id, s)
        if not season or not season.get("episodes"):
            continue
        for ep in season["episodes"]:
            ad = (ep.get("air_date") or "").strip()
            if ad in candidates:
                ep_num = int(ep.get("episode_number") or 0)
                if ep_num > 0:
                    # Asegurar data completa del episodio:
                    full = tmdb_get_episode(tv_id, s, ep_num) or ep
                    return s, ep_num, full
    return None

def tmdb_search_movie(query: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
    if not TMDB_KEY:
        return None
    key = f"{query}|{year}"
    cached = cache_get("tmdb_search_movie", key)
    if cached is not None:
        return cached
    params = {
        "api_key": TMDB_KEY,
        "query": query,
        "language": "es",
        "include_adult": "false",
    }
    if year:
        params["year"] = year
    try:
        data = http_get("https://api.themoviedb.org/3/search/movie", params).json()
        result = data["results"][0] if data.get("results") else None
    except Exception:
        result = None
    cache_set("tmdb_search_movie", key, result)
    return result

def tmdb_get_movie(movie_id: int) -> Optional[Dict[str, Any]]:
    if not TMDB_KEY:
        return None
    key = f"{movie_id}"
    cached = cache_get("tmdb_get_movie", key)
    if cached is not None:
        return cached
    params = {"api_key": TMDB_KEY, "language": "es"}
    try:
        result = http_get(f"https://api.themoviedb.org/3/movie/{movie_id}", params).json()
    except Exception:
        result = None
    cache_set("tmdb_get_movie", key, result)
    return result

def omdb_get_title(title: str, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
    if not OMDB_KEY:
        return None
    key = f"{title}|{year}"
    cached = cache_get("omdb_title", key)
    if cached is not None:
        return cached
    params = {"apikey": OMDB_KEY, "t": title}
    if year:
        params["y"] = str(year)
    try:
        data = http_get("http://www.omdbapi.com/", params).json()
        if data.get("Response") == "True":
            cache_set("omdb_title", key, data)
            return data
    except Exception:
        pass
    cache_set("omdb_title", key, None)
    return None

# ---------------------------------------------------------------------
# CLASIFICACIÓN CONTENIDO
# ---------------------------------------------------------------------

MOVIE_KEYWORDS = {"movie", "película", "film", "cinema"}
SERIES_KEYWORDS = {"series", "episodio", "capítulo", "sitcom", "drama", "comedia", "telenovela"}

def is_series_programme(prog: ET.Element, title_clean: str) -> bool:
    # 1) Si hay episode-num => serie
    if parse_episode_num(prog):
        return True
    # 2) Categorías
    cats = [safe_text(c).lower() for c in prog.findall("category")]
    if any(any(k in c for k in SERIES_KEYWORDS) for c in cats):
        return True
    if any(any(k in c for k in MOVIE_KEYWORDS) for c in cats):
        return False
    # 3) Heurística por sub-title
    sub = safe_text(prog.find("sub-title"))
    if sub and sub.lower() != title_clean.lower():
        return True
    # 4) Consultita ligera a TMDB para decidir (último recurso, cacheado)
    tv_hit = tmdb_search_tv(title_clean)
    movie_hit = tmdb_search_movie(title_clean)
    if tv_hit and not movie_hit:
        return True
    if movie_hit and not tv_hit:
        return False
    # Si empate, inclinarse por serie para canales de series comunes
    return True

# ---------------------------------------------------------------------
# PROCESAMIENTO DE PROGRAMAS
# ---------------------------------------------------------------------

def build_series_output(prog: ET.Element, title_clean: str, airdate: str):
    """
    Construye título con (Sx Ey), sub-title y desc con primera línea “sub-title”.
    """
    # 1) Obtener season/episode de <episode-num> o por fecha en TMDB
    se = parse_episode_num(prog)
    ep_title = None
    ep_overview = None

    tv_hit = tmdb_search_tv(title_clean)
    tv_id = tv_hit.get("id") if tv_hit else None

    if se and tv_id:
        s, e = se
        ep_data = tmdb_get_episode(tv_id, s, e)
        if ep_data:
            ep_title = (ep_data.get("name") or "").strip()
            ep_overview = (ep_data.get("overview") or "").strip()
    elif tv_id:
        found = tmdb_find_episode_by_airdate(tv_id, airdate)
        if found:
            s, e, ep_data = found
            se = (s, e)
            ep_title = (ep_data.get("name") or "").strip()
            ep_overview = (ep_data.get("overview") or "").strip()

    # 2) Si aún no tenemos S/E, poner 0 como placeholder (evitar dejar solo el año de la serie)
    if not se:
        se = (1, 1)  # mejor que nada; pero intentamos no llegar aquí

    s, e = se
    # 3) Episodio: si no conseguimos título/overview, intentar usar lo que venga en el EPG
    if not ep_title:
        ep_title = safe_text(prog.find("sub-title")) or "Episodio"
    if not ep_overview:
        ep_overview = safe_text(prog.find("desc"))

    # 4) Actualizar nodos
    set_or_create(prog, "title", f"{title_clean} (S{s} E{e})")
    set_or_create(prog, "sub-title", ep_title)

    # Descripción: primera línea con el título del episodio entre comillas
    desc_body = ep_overview.strip()
    if desc_body:
        # Evitar duplicar si ya empieza con el sub-título
        first_line = f"“{ep_title}”"
        if not desc_body.lower().startswith(ep_title.lower()):
            desc_text = f"{first_line}\n{desc_body}"
        else:
            desc_text = desc_body  # ya incluye el título
    else:
        desc_text = f"“{ep_title}”"

    set_or_create(prog, "desc", desc_text)

def build_movie_output(prog: ET.Element, title_clean: str, airdate: str):
    """
    Formatea películas: 'Título (Año)' y sinopsis en español.
    """
    # Priorizar TMDB (español)
    year_hint = None
    # Si el título trae (YYYY), úsalo
    m = re.search(r"\((\d{4})\)$", title_clean)
    if m:
        year_hint = int(m.group(1))
        title_base = title_clean[: m.start()].strip()
    else:
        title_base = title_clean

    tmdb_hit = tmdb_search_movie(title_base, year_hint)
    overview = ""
    year = None
    movie_title = title_base

    if tmdb_hit:
        movie_title = (tmdb_hit.get("title") or title_base).strip()
        detail = tmdb_get_movie(tmdb_hit.get("id"))
        if detail:
            overview = (detail.get("overview") or "").strip()
            rd = (detail.get("release_date") or "").strip()
            if rd and re.match(r"^\d{4}-\d{2}-\d{2}$", rd):
                year = int(rd[:4])

    # Respaldo OMDb si falta algo
    if (not overview or not year) and OMDB_KEY:
        om = omdb_get_title(title_base, year)
        if om:
            if not year:
                try:
                    year = int((om.get("Year") or "")[:4])
                except Exception:
                    pass
            if not overview:
                overview = (om.get("Plot") or "").strip()

    # Actualizar título
    title_final = f"{movie_title} ({year})" if year else movie_title
    set_or_create(prog, "title", title_final)

    # Descripción
    desc_el = prog.find("desc")
    current = safe_text(desc_el)
    if overview:
        set_or_create(prog, "desc", overview)
    elif current:
        # mantener la existente
        set_or_create(prog, "desc", current)

# ---------------------------------------------------------------------
# PIPELINE PRINCIPAL
# ---------------------------------------------------------------------

def main():
    if not TMDB_KEY:
        raise RuntimeError("❌ Falta TMDB_API_KEY en el entorno.")
    # OMDB es opcional (respaldo); no detenemos si falta.

    load_cache()

    root_out = ET.Element("tv")
    added_channels: set[str] = set()
    added_programmes: set[str] = set()  # clave = hash(channel|start|stop)

    for url in GUIDE_URLS:
        print(f"↓ Descargando {url}")
        try:
            xml_root = parse_gz_xml(url)
        except Exception as e:
            print(f"⚠️ No se pudo procesar {url}: {e}")
            continue

        # Canales
        for ch in xml_root.findall("channel"):
            ch_id = normalize_id(ch.attrib.get("id"))
            if ch_id in FILTER_SET and ch_id not in added_channels:
                # Copiar el elemento canal
                root_out.append(ch)
                added_channels.add(ch_id)

        # Programas
        for prog in xml_root.findall("programme"):
            ch_id = normalize_id(prog.attrib.get("channel"))
            if ch_id not in FILTER_SET:
                continue

            start = prog.attrib.get("start", "")
            stop = prog.attrib.get("stop", "")
            uniq = hashlib.md5(f"{ch_id}|{start}|{stop}".encode("utf-8")).hexdigest()
            if uniq in added_programmes:
                continue

            title_raw = safe_text(prog.find("title"))
            title_clean = title_raw.strip()

            # Fecha de emisión (para buscar episodio por air_date)
            airdate = xmltv_datetime_to_date_str(start)

            # Decidir serie/película
            if is_series_programme(prog, title_clean):
                build_series_output(prog, title_clean, airdate)
            else:
                build_movie_output(prog, title_clean, airdate)

            # Asegurar lang="es" en title/desc/sub-title si existen
            for tag in ("title", "sub-title", "desc"):
                el = prog.find(tag)
                if el is not None:
                    if "lang" not in el.attrib:
                        el.set("lang", "es")

            root_out.append(prog)
            added_programmes.add(uniq)

        # Pequeña pausa para no saturar las APIs si hay muchas guías
        time.sleep(0.3)

    # Guardar salida
    tree = ET.ElementTree(root_out)
    tree.write(OUT_FILE, encoding="utf-8", xml_declaration=True)
    save_cache()
    print(f"✅ Guía generada: {OUT_FILE}")


if __name__ == "__main__":
    main()
