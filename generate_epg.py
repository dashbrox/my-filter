#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Genera guide_custom.xml.gz filtrando canales específicos y enriqueciendo metadatos
con TMDB (series/películas, sinopsis en español) y OMDb como respaldo.
Formato:
- Series: "Título (Sx Ey)" + subtítulo en primera línea + sinopsis
- Películas: "Título (Año)" + sinopsis
"""

import os
import gzip
import json
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote
from deep_translator import GoogleTranslator  # pip install deep-translator

# ================= CONFIG =================
CHANNELS = [
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&.Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.Europa.Europa.mx",
    "Canal.Film.&.Arts.mx",
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

EPG_SOURCES = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz"
]

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "")

LIBRARY_FILE = Path("library.json")

# ================= HELPERS =================
def load_library():
    if LIBRARY_FILE.exists():
        return json.loads(LIBRARY_FILE.read_text(encoding="utf-8"))
    return {}

def save_library(library):
    LIBRARY_FILE.write_text(json.dumps(library, ensure_ascii=False, indent=2), encoding="utf-8")

def download_and_parse(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    content = gzip.decompress(r.content) if url.endswith(".gz") else r.content
    return ET.ElementTree(ET.fromstring(content))

def normalize_episode(ep_text):
    ep_text = ep_text.strip().lower()
    if ep_text.startswith("s") and "e" in ep_text:
        return ep_text.upper()
    if "." in ep_text:
        parts = ep_text.split(".")
        if len(parts) >= 2:
            return f"S{parts[0]}E{parts[1]}"
    return ep_text.upper()

def query_tmdb(title, year=None):
    if not TMDB_API_KEY:
        return None
    try:
        url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={quote(title)}"
        if year:
            url += f"&year={year}"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("results"):
            return data["results"][0]
    except Exception:
        return None
    return None

def query_omdb(title, year=None):
    if not OMDB_API_KEY:
        return None
    try:
        url = f"http://www.omdbapi.com/?apikey={OMDB_API_KEY}&t={quote(title)}"
        if year:
            url += f"&y={year}"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None
    return None

def translate_to_spanish(text):
    try:
        if not text:
            return text
        if any(word in text.lower() for word in ["the", "and", "of", "with", "is", "in", "on"]):
            return GoogleTranslator(source="en", target="es").translate(text)
    except Exception:
        return text
    return text

# ================= ENRICH =================
def enrich_program(title, subtitle, desc, year=None, season=None, episode=None, library=None):
    key = f"{title.strip().lower()}_{season or ''}_{episode or ''}"
    if key in library:
        return library[key]["title"], library[key]["subtitle"], library[key]["desc"]

    enriched_desc = desc

    # 1️⃣ Intentar TMDb
    tmdb_info = query_tmdb(title, year)
    if tmdb_info:
        if not enriched_desc and tmdb_info.get("overview"):
            enriched_desc = tmdb_info["overview"]

    # 2️⃣ Respaldar con OMDb
    omdb_info = query_omdb(title, year)
    if omdb_info and not enriched_desc:
        enriched_desc = omdb_info.get("Plot", "")

    # Traducción si está en inglés
    enriched_desc = translate_to_spanish(enriched_desc)

    # Validación (mínimo 20 caracteres)
    if not enriched_desc or len(enriched_desc.strip()) < 20:
        enriched_desc = desc

    # 📌 Formateo final
    if season and episode:  # Serie
        final_title = f"{title} (S{season} E{episode})"
        final_subtitle = f"“{subtitle}”" if subtitle else ""
        final_desc = f"{final_subtitle}\n{enriched_desc}" if final_subtitle else enriched_desc
    elif year:  # Película
        final_title = f"{title} ({year})"
        final_subtitle = subtitle
        final_desc = enriched_desc
    else:  # Genérico
        final_title = title
        final_subtitle = subtitle
        final_desc = enriched_desc

    # Guardar solo si se enriqueció
    if enriched_desc != desc:
        library[key] = {"title": final_title, "subtitle": final_subtitle, "desc": final_desc}

    return final_title, final_subtitle, final_desc

# ================= MAIN =================
def main():
    library = load_library()
    root = ET.Element("tv", attrib={"generator-info-name": "my-filter"})
    channels_added = set()

    for url in EPG_SOURCES:
        try:
            tree = download_and_parse(url)
        except Exception as e:
            print(f"[ERROR] No se pudo procesar {url}: {e}")
            continue

        # --- Procesar canales ---
        for channel in tree.findall("channel"):
            chan_id = channel.attrib.get("id")
            if chan_id in CHANNELS and chan_id not in channels_added:
                root.append(channel)
                channels_added.add(chan_id)
                print(f"[INFO] Agregando canal: {chan_id}")

        # --- Procesar programas ---
        for prog in tree.findall("programme"):
            chan_id = prog.attrib.get("channel")
            if chan_id not in CHANNELS:
                continue

            title_el = prog.find("title")
            sub_el = prog.find("sub-title")
            desc_el = prog.find("desc")

            title_text = title_el.text if title_el is not None else ""
            sub_text = sub_el.text if sub_el is not None else ""
            desc_text = desc_el.text if desc_el is not None else ""

            ep_el = prog.find("episode-num")
            season, episode = None, None
            if ep_el is not None and ep_el.text:
                normalized = normalize_episode(ep_el.text)
                if "S" in normalized and "E" in normalized:
                    season = normalized.split("E")[0][1:]
                    episode = normalized.split("E")[1]

            year = None
            if prog.find("date") is not None and prog.find("date").text:
                year = prog.find("date").text.strip()

            title_text, sub_text, desc_text = enrich_program(
                title_text, sub_text, desc_text, year=year, season=season, episode=episode, library=library
            )

            if title_el is None:
                title_el = ET.SubElement(prog, "title")
            title_el.text = title_text

            if sub_el is None:
                sub_el = ET.SubElement(prog, "sub-title")
            sub_el.text = sub_text

            if desc_el is None:
                desc_el = ET.SubElement(prog, "desc")
            desc_el.text = desc_text

            root.append(prog)
            print(f"[OK] Procesado: {title_text}")

    # --- Asegurar todos los canales ---
    for chan_id in CHANNELS:
        if chan_id not in channels_added:
            ch = ET.SubElement(root, "channel", id=chan_id)
            ET.SubElement(ch, "display-name", lang="es").text = chan_id
            ET.SubElement(ch, "icon", src="")
            ET.SubElement(ch, "url").text = ""
            print(f"[INFO] Canal faltante agregado: {chan_id}")

    # Guardar resultados
    save_library(library)
    xml_str = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    with gzip.open("guide_custom.xml.gz", "wb") as f:
        f.write(xml_str)

    print("[DONE] guide_custom.xml.gz generado con éxito")

if __name__ == "__main__":
    main()
