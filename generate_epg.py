import gzip
import requests
import lxml.etree as ET
import os
import re
import sys

# API TMDB
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    print("❌ TMDB_API_KEY no está definido como secreto en GitHub")
    sys.exit(1)

BASE_URL = "https://api.themoviedb.org/3/search/multi"

# Canales a procesar
CANALES_USAR = [
    "Canal.HBO.2.Latinoamérica.mx",
    "Canal.HBO.Family.Latinoamérica.mx",
    "Canal.HBO.(México).mx",
    "Canal.HBO.Mundi.mx",
    "Canal.HBO.Plus.mx",
    "Canal.HBO.Pop.mx",
    "Canal.HBO.Signature.Latinoamérica.mx"
]

# Descargar EPG base
EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
print("📥 Descargando EPG base...")
try:
    r = requests.get(EPG_URL, timeout=60)
    r.raise_for_status()
except requests.RequestException as e:
    print(f"❌ Error al descargar la guía: {e}")
    sys.exit(1)

with open("epg_original.xml.gz", "wb") as f:
    f.write(r.content)

# Abrir archivo comprimido y parsear XML
try:
    with gzip.open("epg_original.xml.gz", "rb") as f:
        tree = ET.parse(f)
except (ET.XMLSyntaxError, OSError) as e:
    print(f"❌ Error al parsear XML: {e}")
    sys.exit(1)

root = tree.getroot()
if root is None:
    print("❌ XML vacío")
    sys.exit(1)

# Función para buscar datos en TMDB
def buscar_tmdb(titulo):
    try:
        params = {"api_key": API_KEY, "query": titulo, "language": "es"}
        r = requests.get(BASE_URL, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results")
        if results:
            return results[0]
    except requests.RequestException:
        pass
    return None

# Recorrer todos los programas
for programme in root.findall("programme"):
    channel = programme.get("channel", "")
    if channel not in CANALES_USAR:
        continue  # Ignorar canales que no necesitamos

    title_elem = programme.find("title")
    if title_elem is None or not title_elem.text:
        continue

    title = title_elem.text.strip()
    print(f"Procesando: {title} ({channel})")

    # Subtítulo: temporada/episodio
    sub_elem = programme.find("sub-title")
    if sub_elem is None:
        match = re.search(r"S(\d+)E(\d+)", title, re.IGNORECASE)
        if match:
            sub_elem = ET.Element("sub-title")
            sub_elem.text = match.group(0)
            programme.append(sub_elem)

    # Descripción y datos desde TMDB
    if programme.find("desc") is None or programme.find("date") is None or programme.find("category") is None:
        result = buscar_tmdb(title)
        if result:
            # Sinopsis
            if programme.find("desc") is None and result.get("overview"):
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
                media_type = result.get("media_type")
                if media_type:
                    cat_elem.text = "Película" if media_type == "movie" else "Serie"
                programme.append(cat_elem)
        else:
            print(f"⚠️ No encontrado en TMDB: {title}")

# Guardar XML final
try:
    tree.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado correctamente")
except Exception as e:
    print(f"❌ Error al guardar XML: {e}")
    sys.exit(1)
