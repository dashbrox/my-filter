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

CANALES_USAR = [
    # ... tu lista completa de canales
]

TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar: Escape 2 Africa",
    "H.Potter y la cámara secreta": "Harry Potter and the Chamber of Secrets"
}

EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"

# ----------------------
# FUNCIONES
# ----------------------
def buscar_tmdb(titulo, lang="es"):
    """Buscar serie o película en TMDB con fallback a inglés"""
    try:
        params = {"api_key": API_KEY, "query": titulo, "language": lang}
        r = requests.get(BASE_URL_SEARCH, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results")
        if results:
            return results[0]
    except requests.RequestException:
        return None
    return None

def buscar_episodio(tv_id, season, episode, lang="es"):
    """Obtener info de episodio específico con fallback a inglés"""
    try:
        url = BASE_URL_TV_EP.format(tv_id=tv_id, season=season, episode=episode)
        params = {"api_key": API_KEY, "language": lang}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None
    return None

def normalizar_titulo(titulo):
    titulo = TITULOS_MAP.get(titulo, titulo)
    return unicodedata.normalize('NFKD', titulo).encode('ascii', 'ignore').decode()

def parse_episode_num(ep_text):
    if not ep_text:
        return None, None
    ep_text = ep_text.strip().upper()
    match = re.match(r"S(\d{1,2})E(\d{1,2})", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(?:T|S)?(\d+)[xE](\d+)", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(SPECIAL|ESPECIAL)[\s-]?(\d+)", ep_text)
    if match:
        return 0, int(match.group(2))
    return None, None

def safe_get(result, key):
    """Extrae valor de TMDB o None"""
    if not result:
        return None
    return result.get(key)

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

    category = programme.get("category", "").lower()
    title_elem = programme.find("title")
    if not title_elem or not title_elem.text:
        continue

    title_original = title_elem.text.strip()
    title_to_search = normalizar_titulo(title_original)

    ep_num_elem = programme.find("episode-num")
    se_text = ep_num_elem.text.strip() if ep_num_elem is not None and ep_num_elem.text else ""
    season_num, episode_num = parse_episode_num(se_text)

    sub_elem = programme.find("sub-title")
    desc_elem = programme.find("desc")
    date_elem = programme.find("date")

    # -------------------
    # SERIES
    # -------------------
    if category == "series":
        if not sub_elem:
            sub_elem = ET.Element("sub-title")
            sub_elem.text = se_text if se_text else ""
            programme.append(sub_elem)

        episode_name = None
        episode_desc = None

        if season_num and episode_num:
            try:
                result = buscar_tmdb(title_to_search)
                if result and result.get("media_type") == "tv":
                    tv_id = safe_get(result, "id")
                    ep_info = buscar_episodio(tv_id, season_num, episode_num)
                    if not ep_info:
                        ep_info = buscar_episodio(tv_id, season_num, episode_num, lang="en")
                    if ep_info:
                        episode_name = safe_get(ep_info, "name")
                        episode_desc = safe_get(ep_info, "overview")
            except Exception:
                pass

        desc_text = ""
        if episode_name:
            desc_text += f"\"{episode_name}\"\n"
        if episode_desc:
            desc_text += episode_desc
        if not desc_text:
            desc_text = se_text if se_text else "Episodio sin información"

        if not desc_elem:
            desc_elem = ET.Element("desc", lang="es")
            desc_elem.text = desc_text
            programme.append(desc_elem)
        else:
            desc_elem.text = desc_text

    # -------------------
    # PELÍCULAS
    # -------------------
    elif category == "movie":
        year = None
        overview = None
        try:
            result = buscar_tmdb(title_to_search)
            if result and result.get("media_type") == "movie":
                release_date = safe_get(result, "release_date") or ""
                overview = safe_get(result, "overview")
                if not release_date or not overview:
                    result_en = buscar_tmdb(title_to_search, lang="en")
                    if result_en:
                        if not release_date:
                            release_date = safe_get(result_en, "release_date") or ""
                        if not overview:
                            overview = safe_get(result_en, "overview")
                year = release_date.split("-")[0] if release_date else None
        except Exception:
            pass

        if not year:
            year = "0000"
        if not overview:
            overview = "Sin descripción disponible"

        if not date_elem:
            date_elem = ET.Element("date")
            date_elem.text = year
            programme.append(date_elem)

        if not re.search(r"\(\d{4}\)", title_original):
            title_elem.text = f"{title_original} ({year})"

        if not desc_elem:
            desc_elem = ET.Element("desc", lang="es")
            desc_elem.text = overview
            programme.append(desc_elem)
        else:
            desc_elem.text = overview

    # -------------------
    # TALKSHOW
    # -------------------
    elif category == "talkshow":
        overview = None
        try:
            result = buscar_tmdb(title_to_search)
            overview = safe_get(result, "overview")
            if not overview:
                result_en = buscar_tmdb(title_to_search, lang="en")
                overview = safe_get(result_en, "overview") if result_en else None
        except Exception:
            pass
        if not overview:
            overview = "Sin descripción disponible"

        if not desc_elem:
            desc_elem = ET.Element("desc", lang="es")
            desc_elem.text = overview
            programme.append(desc_elem)
        else:
            desc_elem.text = overview

    # -------------------
    # ELIMINAR CAMPOS INNECESARIOS
    # -------------------
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
