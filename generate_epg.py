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
    "Canal.HBO.2.Latinoamérica.mx",
    "Canal.HBO.Family.Latinoamérica.mx",
    "Canal.HBO.(México).mx",
    "Canal.HBO.Mundi.mx",
    "Canal.HBO.Plus.mx",
    "Canal.HBO.Pop.mx",
    "Canal.HBO.Signature.Latinoamérica.mx"
]

TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar: Escape 2 Africa",
    "H.Potter y la cámara secreta": "Harry Potter and the Chamber of Secrets",
    "Los Pingüinos deMadagascar": "Penguins of Madagascar"
}

EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"

# ----------------------
# FUNCIONES
# ----------------------
def buscar_tmdb(titulo, is_tv=False, is_movie=False):
    """Buscar serie o película en TMDB"""
    try:
        params = {"api_key": API_KEY, "query": titulo, "language": "es"}
        r = requests.get(BASE_URL_SEARCH, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None

        # Filtrar según lo que sabemos
        if is_tv:
            results = [res for res in results if res.get("media_type") == "tv"]
        elif is_movie:
            results = [res for res in results if res.get("media_type") == "movie"]

        return results[0] if results else None
    except requests.RequestException:
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
    return TITULOS_MAP.get(titulo, titulo)

def parse_episode_num(ep_text):
    """Extraer temporada y episodio (S01E02, S1 E2, 1x02)"""
    match = re.search(r"(?:S\s?(\d+)[xE]\s?(\d+))", ep_text, re.IGNORECASE)
    if not match:
        match = re.search(r"(\d+)x(\d+)", ep_text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def is_english_title(titulo):
    """Heurística simple para detectar si el título parece inglés"""
    # Si contiene palabras comunes del español → asumimos que es español
    if re.search(r"\b(el|la|los|las|de|y|en|un|una)\b", titulo, re.IGNORECASE):
        return False
    # Si contiene solo caracteres ascii y ninguna palabra española → inglés
    return True

def needs_correction(titulo):
    """Detectar si el título necesita corrección"""
    if titulo in TITULOS_MAP:
        return True
    if re.search(r"[a-z][A-Z]", titulo):  # palabras pegadas
        return True
    return False

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

    # --- EXTRAER EPISODIO ---
    ep_num_elem = programme.find("episode-num")
    se_text = ep_num_elem.text.strip() if ep_num_elem is not None and ep_num_elem.text else ""
    season_num, episode_num = parse_episode_num(se_text)

    # Decidir si buscar en TV o Movie
    is_tv = season_num is not None and episode_num is not None
    is_movie = not is_tv

    result = buscar_tmdb(title_to_search, is_tv=is_tv, is_movie=is_movie)
    if not result:
        continue

    media_type = result.get("media_type")

    # --- Películas ---
    if media_type == "movie" and is_movie:
        tmdb_title = result.get("original_title", result.get("title"))
        release_date = result.get("release_date") or ""
        year = release_date.split("-")[0] if release_date else ""

        # Si la guía está en inglés, respetamos el título original
        if is_english_title(title_original):
            title_clean = title_original
        else:
            if needs_correction(title_original):
                title_clean = TITULOS_MAP.get(title_original, tmdb_title)
            else:
                title_clean = title_original

        title_elem.text = f"{title_clean} ({year})" if year else title_clean

        # Descripción
        if programme.find("desc") is None and result.get("overview"):
            desc = ET.Element("desc", lang="es")
            desc.text = result["overview"]
            programme.append(desc)

        # Año
        if programme.find("date") is None and year:
            date_elem = ET.Element("date")
            date_elem.text = year
            programme.append(date_elem)

        # Categoría
        if programme.find("category") is None:
            cat_elem = ET.Element("category", lang="es")
            cat_elem.text = "Película"
            programme.append(cat_elem)

    # --- Series ---
    elif media_type == "tv" and is_tv:
        tv_id = result.get("id")
        ep_info = buscar_episodio(tv_id, season_num, episode_num) if tv_id else None

        # Decidir título base
        if is_english_title(title_original):
            title_clean = title_original
        else:
            if needs_correction(title_original):
                title_clean = TITULOS_MAP.get(title_original, result.get("original_name", result.get("name")))
            else:
                title_clean = title_original

        title_elem.text = f"{title_clean} (S{season_num:02d}E{episode_num:02d})" if se_text else title_clean

        # Subtítulo y descripción
        episode_name = ep_info.get("name") if ep_info and ep_info.get("name") else se_text
        episode_desc = ep_info.get("overview") if ep_info and ep_info.get("overview") else ""

        sub_elem = programme.find("sub-title")
        if sub_elem is None:
            sub_elem = ET.Element("sub-title", lang="es")
            programme.append(sub_elem)
        sub_elem.text = episode_name

        if programme.find("desc") is None:
            desc = ET.Element("desc", lang="es")
            desc.text = f"\"{episode_name}\"\n{episode_desc}" if episode_desc else f"\"{episode_name}\""
            programme.append(desc)

        # Categoría
        if programme.find("category") is None:
            cat_elem = ET.Element("category", lang="es")
            cat_elem.text = "Serie"
            programme.append(cat_elem)

# ----------------------
# GUARDAR XML FINAL
# ----------------------
try:
    tree.write("guide_custom.xml", encoding="utf-8", xml_declaration=True, pretty_print=True)
    print("✅ guide_custom.xml generado correctamente")
except Exception as e:
    print(f"❌ Error al guardar XML: {e}")
    sys.exit(1)
