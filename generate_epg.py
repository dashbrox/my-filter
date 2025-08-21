import gzip
import requests
import lxml.etree as ET
import os
import re
import sys
import unicodedata

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    print("❌ TMDB_API_KEY no está definido como secreto en GitHub")
    sys.exit(1)

BASE_URL_SEARCH = "https://api.themoviedb.org/3/search/multi"
BASE_URL_TV_EP = "https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}"

# Lista de canales a procesar
CANALES_USAR = [
    "Canal.Azteca.Uno.mx",
    "Canal.HBO.Signature.Latinoamérica.mx",
    # Agrega los demás canales aquí
]

# Mapa de títulos especiales
TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar: Escape 2 Africa",
    "H.Potter y la cámara secreta": "Harry Potter and the Chamber of Secrets"
}

EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"

# ----------------------
# FUNCIONES
# ----------------------
def buscar_tmdb(titulo):
    """Buscar serie o película en TMDB"""
    try:
        params = {"api_key": API_KEY, "query": titulo, "language": "es"}
        r = requests.get(BASE_URL_SEARCH, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results")
        if results:
            return results[0]
    except requests.RequestException:
        pass
    return None

def buscar_episodio(tv_id, season, episode):
    """Obtener info de episodio específico"""
    try:
        url = BASE_URL_TV_EP.format(tv_id=tv_id, season=season, episode=episode)
        params = {"api_key": API_KEY, "language": "es"}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None

def normalizar_titulo(titulo):
    titulo = TITULOS_MAP.get(titulo, titulo)
    return unicodedata.normalize('NFKD', titulo).encode('ascii', 'ignore').decode()

def parse_episode_num(ep_text):
    """Convierte S122 o S01E02 a temporada y episodio"""
    if not ep_text:
        return None, None
    # Formato pegado S122 -> S1 E22
    m = re.match(r"S(\d{1,2})(\d{2})$", ep_text)
    if m:
        return int(m.group(1)), int(m.group(2))
    # Formato normal S01E02
    m = re.search(r"S(\d+)E(\d+)", ep_text, re.IGNORECASE)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None

# ----------------------
# DESCARGAR EPG
# ----------------------
print("📥 Descargando EPG base...")
try:
    r = requests.get(EPG_URL, timeout=60)
    r.raise_for_status()
except requests.RequestException as e:
    print(f"❌ Error al descargar la guía: {e}")
    sys.exit(1)

with open("epg_original.xml.gz", "wb") as f:
    f.write(r.content)

# ----------------------
# PARSEAR XML
# ----------------------
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

# ----------------------
# PROCESAR PROGRAMAS
# ----------------------
for programme in root.findall("programme"):
    channel = programme.get("channel", "")
    if channel not in CANALES_USAR:
        continue

    title_elem = programme.find("title")
    if title_elem is None or not title_elem.text:
        continue

    title_original = title_elem.text.strip()
    title_to_search = normalizar_titulo(title_original)

    ep_num_elem = programme.find("episode-num")
    se_text = ep_num_elem.text.strip() if ep_num_elem is not None and ep_num_elem.text else ""
    season_num, episode_num = parse_episode_num(se_text)

    # Sub-title solo si falta
    sub_elem = programme.find("sub-title")
    if sub_elem is None and se_text:
        sub_elem = ET.Element("sub-title")
        sub_elem.text = se_text
        programme.append(sub_elem)

    desc_elem = programme.find("desc")
    date_elem = programme.find("date")
    category_elem = programme.find("category")

    # Solo rellenar campos faltantes
    if desc_elem is None or date_elem is None or (season_num and episode_num and (sub_elem is None or not sub_elem.text)):
        result = buscar_tmdb(title_to_search)
        if result:
            media_type = result.get("media_type")
            if media_type == "movie":
                # Películas
                if date_elem is None and result.get("release_date"):
                    year = result["release_date"].split("-")[0]
                    date_elem = ET.Element("date")
                    date_elem.text = year
                    programme.append(date_elem)
                    if "(" not in title_original and year:
                        title_elem.text = f"{title_original} ({year})"
                if desc_elem is None and result.get("overview"):
                    desc_elem = ET.Element("desc", lang="es")
                    desc_elem.text = result["overview"]
                    programme.append(desc_elem)
            elif media_type == "tv":
                # Series
                tv_id = result.get("id")
                ep_info = None
                if season_num and episode_num:
                    ep_info = buscar_episodio(tv_id, season_num, episode_num)

                episode_name = se_text
                episode_desc = ""
                if ep_info:
                    if ep_info.get("name"):
                        episode_name = ep_info["name"]
                    if ep_info.get("overview"):
                        episode_desc = ep_info["overview"]

                # Solo completar si falta
                if desc_elem is None:
                    desc_text = f"\"{episode_name}\"\n{episode_desc}" if episode_desc else f"\"{episode_name}\""
                    desc_elem = ET.Element("desc", lang="es")
                    desc_elem.text = desc_text
                    programme.append(desc_elem)
                if sub_elem is None:
                    sub_elem = ET.Element("sub-title")
                    sub_elem.text = se_text
                    programme.append(sub_elem)

    # Eliminar fields no deseados
    for tag in ["credits", "rating", "star-rating"]:
        elem = programme.find(tag)
        if elem is not None:
            programme.remove(elem)

# ----------------------
# GUARDAR XML FINAL
# ----------------------
try:
    tree.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado correctamente")
except Exception as e:
    print(f"❌ Error al guardar XML: {e}")
    sys.exit(1)
