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
    titulo_normalized = unicodedata.normalize('NFKD', titulo).encode('ascii', 'ignore').decode()
    return titulo_normalized

def parse_episode_num(ep_text):
    """Extraer temporada y episodio de formato S01 E02"""
    match = re.search(r"S(\d+)E(\d+)", ep_text, re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
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
    print(f"Procesando: {title_original} ({channel}) → Buscando como: {title_to_search}")

    # --- EXTRAER EPISODIO ---
    ep_num_elem = programme.find("episode-num")
    se_text = ep_num_elem.text.strip() if ep_num_elem is not None and ep_num_elem.text else ""
    season_num, episode_num = parse_episode_num(se_text)

    # --- ACTUALIZAR TÍTULO ORIGINAL CON EPISODIO ---
    title_elem.text = f"{title_original} ({se_text})" if se_text else title_original

    # --- SUBTÍTULO Fallback ---
    sub_elem = programme.find("sub-title")
    if sub_elem is None and se_text:
        sub_elem = ET.Element("sub-title")
        sub_elem.text = se_text
        programme.append(sub_elem)

    # --- BUSCAR TMDB PARA COMPLETAR INFO ---
    result = buscar_tmdb(title_to_search)
    if result:
        media_type = result.get("media_type")
        # --- Películas ---
        if media_type == "movie":
            release_date = result.get("release_date") or ""
            year = release_date.split("-")[0] if release_date else ""
            title_elem.text = f"{result['title']} ({year})" if year else result['title']
            # Descripción
            if programme.find("desc") is None and result.get("overview"):
                desc = ET.Element("desc", lang="es")
                desc.text = result["overview"]
                programme.append(desc)
            # Fecha
            if programme.find("date") is None and release_date:
                date_elem = ET.Element("date")
                date_elem.text = year
                programme.append(date_elem)
        # --- Series ---
        elif media_type == "tv":
            tv_id = result.get("id")
            ep_info = None
            if season_num and episode_num:
                ep_info = buscar_episodio(tv_id, season_num, episode_num)
            # Nombre episodio y sinopsis
            episode_name = se_text
            episode_desc = ""
            if ep_info:
                if ep_info.get("name"):
                    episode_name = ep_info["name"]
                if ep_info.get("overview"):
                    episode_desc = ep_info["overview"]
            # Actualizar título con episodio
            title_elem.text = f"{result['name']} ({se_text})" if se_text else result['name']
            # Actualizar descripción
            if programme.find("desc") is None:
                desc_text = f"\"{episode_name}\"\n{episode_desc}" if episode_desc else f"\"{episode_name}\""
                desc = ET.Element("desc", lang="es")
                desc.text = desc_text
                programme.append(desc)
            # Categoría
            existing_cat = programme.find("category")
            if existing_cat is None:
                cat_elem = ET.Element("category", lang="es")
                cat_elem.text = "Serie"
                programme.append(cat_elem)

# ----------------------
# GUARDAR XML FINAL
# ----------------------
try:
    tree.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado correctamente")
except Exception as e:
    print(f"❌ Error al guardar XML: {e}")
    sys.exit(1)
