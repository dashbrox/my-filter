import gzip
import requests
import lxml.etree as ET
import os
import re

# API TMDB
API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/search/multi"

# Descargar EPG base
EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
print("📥 Descargando EPG base...")
r = requests.get(EPG_URL, timeout=60)
with open("epg_original.xml.gz", "wb") as f:
    f.write(r.content)

# Abrir archivo comprimido
with gzip.open("epg_original.xml.gz", "rb") as f:
    tree = ET.parse(f)

root = tree.getroot()

def buscar_tmdb(titulo):
    params = {"api_key": API_KEY, "query": titulo, "language": "es"}
    r = requests.get(BASE_URL, params=params)
    if r.status_code == 200 and r.json().get("results"):
        return r.json()["results"][0]
    return None

for programme in root.findall("programme"):
    title_elem = programme.find("title")
    if title_elem is None or not title_elem.text:
        continue

    title = title_elem.text

    # Subtítulo: temporada/episodio
    sub_elem = programme.find("sub-title")
    if sub_elem is None:
        match = re.search(r"S(\d+)E(\d+)", title, re.IGNORECASE)
        if match:
            sub_elem = ET.Element("sub-title")
            sub_elem.text = match.group(0)
            programme.append(sub_elem)

    # Descripción
    if programme.find("desc") is None:
        result = buscar_tmdb(title)
        if result and result.get("overview"):
            desc = ET.Element("desc", lang="es")
            desc.text = result["overview"]
            programme.append(desc)

        # Fecha
        if programme.find("date") is None:
            release_date = result.get("release_date") or result.get("first_air_date")
            if release_date:
                date_elem = ET.Element("date")
                date_elem.text = release_date.split("-")[0]
                programme.append(date_elem)

        # Categoría
        if programme.find("category") is None:
            cat_elem = ET.Element("category", lang="es")
            if result.get("media_type"):
                cat_elem.text = "Película" if result["media_type"] == "movie" else "Serie"
            programme.append(cat_elem)

tree.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
print("✅ guide_custom.xml generado correctamente")
