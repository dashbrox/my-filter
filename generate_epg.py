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
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&amp;E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinema.mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&amp;.Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mxCanal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.E!.Entertainment.Television.(México).mx",
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
    "Canal.Sony.(México).mxCanal.Space.(México).mx",
    "Canal.Star.Channel.(México).mx",
    "Canal.Studio.Universal.(México).mx",
    "Canal.TNT.(México).mx",
    "Canal.TNT.Series.(México).mx",
    "Canal.Universal.TV.(México).mx",
    "Canal.USA.Network.(México).mx",
    "Canal.Warner.TV.(México).mx"
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
        pass
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

def normalizar_titulo(titulo):
    titulo = TITULOS_MAP.get(titulo, titulo)
    return unicodedata.normalize('NFKD', titulo).encode('ascii', 'ignore').decode()

def parse_episode_num(ep_text):
    if not ep_text:
        return None, None
    ep_text = ep_text.strip().upper()
    match = re.match(r"S(\d{1,2})(\d{2})$", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(?:T|S)?(\d+)[xE](\d+)", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(SPECIAL|ESPECIAL)[\s-]?(\d+)", ep_text)
    if match:
        return 0, int(match.group(2))
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

    category = programme.get("category", "").lower()
    title_elem = programme.find("title")
    if title_elem is None or not title_elem.text:
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
        if sub_elem is None and se_text:
            sub_elem = ET.Element("sub-title")
            sub_elem.text = se_text
            programme.append(sub_elem)

        if desc_elem is None and season_num is not None and episode_num is not None:
            result = buscar_tmdb(title_to_search)
            if result and result.get("media_type") == "tv":
                tv_id = result.get("id")
                ep_info = buscar_episodio(tv_id, season_num, episode_num)
                if not ep_info:
                    ep_info = buscar_episodio(tv_id, season_num, episode_num, lang="en")
                episode_name = ep_info.get("name") if ep_info and ep_info.get("name") else None
                episode_desc = ep_info.get("overview") if ep_info and ep_info.get("overview") else None

                if episode_name or episode_desc:
                    desc_text = ""
                    if episode_name:
                        desc_text += f"\"{episode_name}\"\n"
                    if episode_desc:
                        desc_text += episode_desc
                    desc_elem = ET.Element("desc", lang="es")
                    desc_elem.text = desc_text
                    programme.append(desc_elem)
                elif se_text:  # fallback: solo número de episodio
                    desc_elem = ET.Element("desc", lang="es")
                    desc_elem.text = se_text
                    programme.append(desc_elem)

    # -------------------
    # PELÍCULAS
    # -------------------
    elif category == "movie":
        result = buscar_tmdb(title_to_search)
        if result and result.get("media_type") == "movie":
            release_date = result.get("release_date") or ""
            if not release_date:
                result_en = buscar_tmdb(title_to_search, lang="en")
                release_date = result_en.get("release_date") if result_en else ""
            year = release_date.split("-")[0] if release_date else ""
            if date_elem is None and year:
                date_elem = ET.Element("date")
                date_elem.text = year
                programme.append(date_elem)
            overview = result.get("overview")
            if not overview:
                result_en = buscar_tmdb(title_to_search, lang="en")
                overview = result_en.get("overview") if result_en else None
            if desc_elem is None and overview:
                desc_elem = ET.Element("desc", lang="es")
                desc_elem.text = overview
                programme.append(desc_elem)
            if year and (not re.search(r"\(\d{4}\)", title_original)):
                title_elem.text = f"{title_original} ({year})"

    # -------------------
    # TALKSHOW
    # -------------------
    elif category == "talkshow":
        if desc_elem is None:
            result = buscar_tmdb(title_to_search)
            overview = None
            if result:
                overview = result.get("overview")
                if not overview:
                    result_en = buscar_tmdb(title_to_search, lang="en")
                    overview = result_en.get("overview") if result_en else None
            if overview:
                desc_elem = ET.Element("desc", lang="es")
                desc_elem.text = overview
                programme.append(desc_elem)

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
