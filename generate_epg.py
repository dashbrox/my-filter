import os
import gzip
import shutil
import requests
import xml.etree.ElementTree as ET
import re

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    print("❌ TMDB_API_KEY no está definido")
    exit(1)

EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
GZ_FILE = "guide_original.xml.gz"
XML_FILE = "guide_original.xml"
OUTPUT_FILE = "guide_custom.xml"

# Lista de canales
CHANNEL_WHITELIST = {
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinema.mx",
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
    "Canal.Warner.TV.(México).mx"
}

# Map de títulos abreviados o pegados
TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar: Escape 2 Africa",
    "H.Potter y la cámara secreta": "Harry Potter and the Chamber of Secrets",
    "Los Pingüinos deMadagascar": "Penguins of Madagascar"
}

# ----------------------
# FUNCIONES
# ----------------------

def descargar_y_descomprimir():
    if not os.path.exists(GZ_FILE):
        print(f"📥 Descargando {EPG_URL} ...")
        r = requests.get(EPG_URL, timeout=60)
        r.raise_for_status()
        with open(GZ_FILE, "wb") as f:
            f.write(r.content)
        print("✅ Descargado")
    with gzip.open(GZ_FILE, "rb") as f_in:
        with open(XML_FILE, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    print("✅ Descomprimido:", XML_FILE)

def parse_episode_num(ep_text):
    match = re.search(r"S?(\d+)[xE ](\d+)", ep_text or "", re.IGNORECASE)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

def titulo_esta_en_ingles(titulo):
    palabras_en = {"the","of","and","season","episode","idol","game","thrones"}
    return any(p.lower() in titulo.lower() for p in palabras_en)

def needs_correction(titulo):
    if titulo in TITULOS_MAP:
        return True
    if re.search(r"[a-z][A-Z]", titulo):  # pegadas
        return True
    if re.search(r"[A-Z]\.[A-Z]", titulo):  # abreviaciones tipo H. Potter
        return True
    return False

def buscar_tmdb(titulo, is_tv=False, is_movie=False):
    try:
        params = {"api_key": API_KEY, "query": titulo, "language": "es"}
        r = requests.get("https://api.themoviedb.org/3/search/multi", params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None
        if is_tv:
            results = [res for res in results if res.get("media_type")=="tv"]
        elif is_movie:
            results = [res for res in results if res.get("media_type")=="movie"]
        return results[0] if results else None
    except requests.RequestException:
        return None

def procesar_programa(prog):
    channel = prog.attrib.get("channel")
    if channel not in CHANNEL_WHITELIST:
        return None

    title_elem = prog.find("title")
    if title_elem is None or not title_elem.text:
        return None
    original_title = title_elem.text.strip()

    ep_elem = prog.find("episode-num")
    is_tv = ep_elem is not None and re.search(r"S\d+E\d+", ep_elem.text or "", re.IGNORECASE)

    # Solo buscar TMDB si hace falta
    usar_tmdb = (needs_correction(original_title) or
                 (ep_elem is not None and (prog.find("desc") is None or prog.find("title") is None)))

    tmdb_data = buscar_tmdb(original_title, is_tv=is_tv, is_movie=not is_tv) if usar_tmdb else None

    # Título: solo actualizar si original vacío o necesita corrección
    if tmdb_data and (needs_correction(original_title) or not original_title):
        tmdb_title = tmdb_data.get("name") or tmdb_data.get("title")
        if titulo_esta_en_ingles(original_title):
            final_title = original_title
        else:
            final_title = tmdb_title
        title_elem.text = final_title

    # Episodio SxxExx
    if is_tv and ep_elem is not None:
        season_num, episode_num = parse_episode_num(ep_elem.text)
        if season_num and episode_num:
            title_elem.text = f"{title_elem.text} (S{season_num:02d}E{episode_num:02d})"

    # Sub-title: solo agregar si no existe y es serie
    if is_tv and ep_elem is not None:
        sub_elem = prog.find("sub-title")
        if sub_elem is None:
            sub_elem = ET.Element("sub-title")
            sub_elem.text = ep_elem.text
            prog.append(sub_elem)

    # Desc: solo si falta
    desc_elem = prog.find("desc")
    if (desc_elem is None or not desc_elem.text) and tmdb_data:
        desc_elem = prog.find("desc")
        if desc_elem is None:
            desc_elem = ET.Element("desc", {"lang":"es"})
            prog.append(desc_elem)
        overview = tmdb_data.get("overview","")
        if is_tv and ep_elem is not None:
            # Intentar obtener sinopsis de episodio
            tv_id = tmdb_data.get("id")
            ep_info = buscar_tmdb_episodio(tv_id, season_num, episode_num)
            ep_name = ep_info.get("name") if ep_info else ep_elem.text
            ep_overview = ep_info.get("overview") if ep_info else ""
            desc_elem.text = f"\"{ep_name}\"\n{ep_overview}" if ep_overview else f"\"{ep_name}\""
        else:
            desc_elem.text = overview

    # Fecha: solo si falta y es película
    date_elem = prog.find("date")
    if date_elem is None and tmdb_data and tmdb_data.get("media_type")=="movie":
        release_date = tmdb_data.get("release_date") or ""
        year = release_date.split("-")[0] if release_date else ""
        if year:
            date_elem = ET.Element("date")
            date_elem.text = year
            prog.append(date_elem)

    # Categoría: solo si falta
    cat_elem = prog.find("category")
    if cat_elem is None:
        cat_elem = ET.Element("category", {"lang":"es"})
        cat_elem.text = "Serie" if is_tv else "Película"
        prog.append(cat_elem)

    return prog

def buscar_tmdb_episodio(tv_id, season, episode):
    if not tv_id or not season or not episode:
        return None
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season}/episode/{episode}"
    try:
        r = requests.get(url, params={"api_key": API_KEY, "language":"es"}, timeout=10)
        r.raise_for_status()
        return r.json()
    except:
        return None

def procesar_xml(input_file, output_file):
    tree = ET.parse(input_file)
    root = tree.getroot()
    for prog in root.findall("programme"):
        procesar_programa(prog)
    tree.write(output_file, encoding="utf-8", xml_declaration=True)
    print("✅ Generado:", output_file)

# ----------------------
# MAIN
# ----------------------
if __name__ == "__main__":
    descargar_y_descomprimir()
    procesar_xml(XML_FILE, OUTPUT_FILE)
