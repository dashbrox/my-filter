
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import gzip
import io
import json
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from openai import OpenAI

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY or not OPENAI_KEY:
    raise RuntimeError("❌ Falta TMDB_API_KEY u OPENAI_API_KEY en el entorno.")

TMDB_BASE = "https://api.themoviedb.org/3"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
client = OpenAI(api_key=OPENAI_KEY)

CACHE_FILE = Path("epg_cache.json")
if CACHE_FILE.exists():
    CACHE = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
else:
    CACHE = {}

OUTPUT_FILE = Path("guide_custom.xml")
OUTPUT_FILE_GZ = Path("guide_custom.xml.gz")

# ----------------------
# Canales a incluir
# ----------------------
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

# ----------------------
# URLs de EPG
# ----------------------
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

# ----------------------
# FUNCIONES
# ----------------------

def tmdb_search(title, is_series=False):
    endpoint = f"{TMDB_BASE}/search/{'tv' if is_series else 'movie'}"
    r = requests.get(endpoint, params={"query": title, "language": "es-ES"}, headers=HEADERS)
    if r.status_code == 200 and r.json()["results"]:
        return r.json()["results"][0]
    return None


def translate_to_spanish(text):
    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=f"Traduce al español sin cambiar el sentido: {text}"
    )
    return resp.output[0].content[0].text.strip()


def process_movie(prog, title):
    info = tmdb_search(title, is_series=False)
    if info:
        year = info.get("release_date", "")[:4]
        if year and year not in title:
            prog.find("title").text = f"{title} ({year})"
        overview = info.get("overview", "").strip()
        if overview:
            if re.search(r"[a-zA-Z]", overview) and not re.search(r"[áéíóúñÁÉÍÓÚ]", overview):
                overview = translate_to_spanish(overview)
            desc = prog.find("desc")
            if desc is None:
                ET.SubElement(prog, "desc", {"lang": "es"}).text = overview
            else:
                if not desc.text or desc.text.strip() == "":
                    desc.text = overview


def process_series(prog, title):
    info = tmdb_search(title, is_series=True)
    if not info:
        return

    show_name = info.get("name", title)
    season_num = 1
    episode_num = 1
    episode_title = info.get("original_name", "Episodio")

    prog.find("title").text = f"{show_name} (S{season_num} E{episode_num})"
    ET.SubElement(prog, "sub-title", {"lang": "es"}).text = episode_title
    ET.SubElement(prog, "episode-num", {"system": "onscreen"}).text = f"S{season_num}E{episode_num}"

    overview = info.get("overview", "").strip()
    if overview:
        if re.search(r"[a-zA-Z]", overview) and not re.search(r"[áéíóúñÁÉÍÓÚ]", overview):
            overview = translate_to_spanish(overview)
        new_desc = f"{episode_title}\n{overview}"
        desc = prog.find("desc")
        if desc is None:
            ET.SubElement(prog, "desc", {"lang": "es"}).text = new_desc
        else:
            desc.text = new_desc


def process_programme(prog):
    title_elem = prog.find("title")
    if title_elem is None:
        return
    title = title_elem.text.strip()
    categories = [c.text for c in prog.findall("category") if c.text]
    is_series = any("Serie" in c for c in categories)

    key = f"{title}_{'serie' if is_series else 'movie'}"
    if key in CACHE:
        return

    if is_series:
        process_series(prog, title)
    else:
        process_movie(prog, title)

    CACHE[key] = True


def fetch_and_merge_epgs():
    all_programmes = []
    for url in EPG_URLS:
        r = requests.get(url, timeout=15)
        with gzip.open(io.BytesIO(r.content)) as f:
            tree = ET.parse(f)
        root = tree.getroot()
        for elem in root:
            if elem.tag == "programme" and elem.attrib.get("channel") in CHANNELS:
                process_programme(elem)
                all_programmes.append(elem)
            elif elem.tag != "programme":
                all_programmes.append(elem)
    return all_programmes


def main():
    all_elements = fetch_and_merge_epgs()
    tv = ET.Element("tv")
    for elem in all_elements:
        tv.append(elem)

    tree_out = ET.ElementTree(tv)
    tree_out.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE_GZ, "wb") as f_out:
        f_out.writelines(f_in)

    CACHE_FILE.write_text(json.dumps(CACHE, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Guía generada en {OUTPUT_FILE} y comprimida en {OUTPUT_FILE_GZ}")


if __name__ == "__main__":
    main()
