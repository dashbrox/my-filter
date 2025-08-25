#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, io, gzip, json, time, requests, unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from openai import OpenAI

# -------------------------
# Configuración
# -------------------------
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OPENAI_API_KEYS = [os.getenv(f"OPENAI_API_KEY_{i}") for i in range(1, 9)]
OPENAI_API_KEYS = [k for k in OPENAI_API_KEYS if k]

if not TMDB_API_KEY or not OPENAI_API_KEYS:
    raise RuntimeError("❌ Falta TMDB_API_KEY o OPENAI_API_KEY_X")

TMDB_BASE = "https://api.themoviedb.org/3"

CHANNELS = [
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.AE.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.Europa.Europa.mx",
    "Canal.Film.&Arts.mx",
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

EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

CACHE_FILE = Path("epg_cache.json")
OUTPUT_FILE = Path("guide_custom.xml")
OUTPUT_FILE_GZ = Path("guide_custom.xml.gz")

if CACHE_FILE.exists():
    try:
        CACHE = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except:
        CACHE = {}
else:
    CACHE = {}

# -------------------------
# Funciones
# -------------------------

def normalize_title(title):
    return unicodedata.normalize('NFC', title).strip().lower()

def get_openai_response(prompt, max_retries=3):
    for i, key in enumerate(OPENAI_API_KEYS):
        for attempt in range(max_retries):
            try:
                client = OpenAI(api_key=key)
                resp = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role":"user","content":prompt}],
                    temperature=0.7
                )
                return resp.choices[0].message.content.strip()
            except Exception as e:
                print(f"⚠️ OpenAI key {i+1} attempt {attempt+1} error: {e}")
                time.sleep(2**attempt)
    return None

def get_tmdb_info(title, is_series=False):
    try:
        r = requests.get(f"{TMDB_BASE}/search/{'tv' if is_series else 'movie'}",
                         params={"api_key": TMDB_API_KEY, "query": title, "language": "es-ES"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("results"):
            return data["results"][0]
    except Exception as e:
        print(f"⚠️ TMDb error '{title}': {e}")
    return None

def fetch_epg(url, retries=3):
    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            with gzip.open(io.BytesIO(r.content)) as f:
                return ET.parse(f)
        except Exception as e:
            print(f"⚠️ EPG fetch {url} attempt {attempt} error: {e}")
            time.sleep(2)
    return None

def infer_missing_info(title, category, existing_desc=""):
    prompt = f"Basado en el título '{title}' y categoría '{category}', completa información faltante usando pistas del contexto existente: '{existing_desc}'"
    return get_openai_response(prompt) or existing_desc or "Información no disponible"

def process_programme(prog):
    title_elem = prog.find("title")
    if title_elem is None or not title_elem.text:
        return
    title = title_elem.text.strip()
    categories = [c.text for c in prog.findall("category") if c.text]
    is_series = any("Serie" in c for c in categories)
    start_time = prog.attrib.get("start","")
    channel = prog.attrib.get("channel","")
    key = f"{channel}_{normalize_title(title)}_{start_time}"

    if key in CACHE:
        return

    info = get_tmdb_info(title, is_series)
    overview = info.get("overview","") if info else ""

    # Traducción si está en inglés
    if overview and re.search(r"[a-zA-Z]", overview) and not re.search(r"[áéíóúñÁÉÍÓÚ]", overview):
        overview = get_openai_response(f"Traduce al español: {overview}") or overview

    # Inferir datos faltantes usando pistas
    if is_series:
        season_num = info.get("season_number",1) if info else 1
        episode_num = info.get("episode_number",1) if info else 1
        episode_title = info.get("name","Episodio") if info else "Episodio"
        if not overview:
            overview = infer_missing_info(title, "Serie", f"{episode_title}")
        title_elem.text = f"{title} (S{season_num} E{episode_num})"
        ET.SubElement(prog, "sub-title", {"lang":"es"}).text = episode_title
        ET.SubElement(prog, "episode-num", {"system":"onscreen"}).text = f"S{season_num}E{episode_num}"
        desc_elem = prog.find("desc")
        desc_text = f"{episode_title}\n{overview}" if overview else episode_title
        if desc_elem is None:
            ET.SubElement(prog,"desc",{"lang":"es"}).text = desc_text
        else:
            desc_elem.text = desc_text
    else:
        year = info.get("release_date","")[:4] if info else ""
        if not year:
            year = infer_missing_info(title, "Película")
        if year:
            title_elem.text = f"{title} ({year})"
        desc_elem = prog.find("desc")
        if desc_elem is None:
            ET.SubElement(prog,"desc",{"lang":"es"}).text = overview or infer_missing_info(title,"Película")
        else:
            if not desc_elem.text or desc_elem.text.strip()=="":
                desc_elem.text = overview or infer_missing_info(title,"Película")

    CACHE[key] = True

# -------------------------
# Main
# -------------------------

def main():
    all_channels = []
    all_programmes = []

    for url in EPG_URLS:
        print(f"📥 Descargando {url} ...")
        tree = fetch_epg(url)
        if not tree:
            continue
        root = tree.getroot()
        for elem in root:
            if elem.tag == "programme" and elem.attrib.get("channel") in CHANNELS:
                try:
                    process_programme(elem)
                    all_programmes.append(elem)
                except Exception as e:
                    print(f"⚠️ Ignorado por error: {e}")
            elif elem.tag == "channel":
                all_channels.append(elem)

    tv = ET.Element("tv")
    for ch in all_channels:
        tv.append(ch)
    for prog in all_programmes:
        tv.append(prog)

    tree_out = ET.ElementTree(tv)
    tree_out.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    if OUTPUT_FILE.exists() and OUTPUT_FILE.stat().st_size > 0:
        with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE_GZ, "wb") as f_out:
            f_out.writelines(f_in)

    CACHE_FILE.write_text(json.dumps(CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Guía generada en {OUTPUT_FILE} y comprimida en {OUTPUT_FILE_GZ}")

if __name__=="__main__":
    main()
