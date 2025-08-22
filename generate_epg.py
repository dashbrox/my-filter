import os
import re
import gzip
import requests
import unicodedata
import lxml.etree as ET
import io

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY no está definido en el entorno.")

EPG_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
EPG_FILE = "epg_original.xml"
OUTPUT_FILE = "guide_custom.xml"

NUEVAS_EPGS = [
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

EPG_FILES_TEMP = []

CANALES_USAR = {
    # Lista de canales como estaba antes (omitida aquí por brevedad, usar tu lista original)
}

TITULOS_MAP = {
    "Madagascar 2Escape de África": "Madagascar 2: Escape de África",
    "H.Potter y la cámara secreta": "Harry Potter y la Cámara Secreta"
}

# ----------------------
# FUNCIONES AUXILIARES
# ----------------------
def normalizar_texto(texto):
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.strip()

def rellenar_descripcion(titulo, tipo="serie", temporada=None, episodio=None):
    if tipo == "pelicula":
        return f"Sinopsis no disponible para la película '{titulo}'."
    else:
        return f"Sinopsis no disponible para el episodio {temporada}-{episodio} de '{titulo}'."

def traducir_a_espanol(texto):
    if not texto:
        return ""
    return texto

# ----------------------
# BUSQUEDAS TMDB
# ----------------------
def buscar_tmdb(titulo, tipo="multi", lang="es-MX", year=None):
    titulo = TITULOS_MAP.get(titulo, titulo)
    url = f"https://api.themoviedb.org/3/search/{tipo}"
    params = {"api_key": API_KEY, "query": titulo, "language": lang}
    if tipo == "movie" and not year:
        return None
    if year and tipo == "movie":
        params["year"] = year
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if results:
            return results[0]
        if "making of" in titulo.lower():
            titulo_base = titulo.lower().replace("making of", "").strip()
            params["query"] = titulo_base
            r2 = requests.get(url, params=params, timeout=10)
            r2.raise_for_status()
            results2 = r2.json().get("results", [])
            return results2[0] if results2 else None
        return None
    except Exception:
        if lang != "en-US":
            return buscar_tmdb(titulo, tipo, "en-US", year)
    return None

def obtener_info_serie(tv_id, temporada, episodio, lang="es-MX"):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{temporada}/episode/{episodio}"
    params = {"api_key": API_KEY, "language": lang}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if (not data.get("overview") or not data.get("name")) and lang != "en-US":
            return obtener_info_serie(tv_id, temporada, episodio, "en-US")
        return data
    except Exception:
        return {}

# ----------------------
# PARSEO DE EPISODIOS
# ----------------------
def parse_episode_num(ep_text):
    if not ep_text:
        return None, None
    ep_text = ep_text.strip().upper().replace(" ", "")
    patterns = [r"S(\d{1,2})E(\d{1,2})", r"(\d{1,2})x(\d{1,2})", r"(\d{1,2})[E](\d{1,2})"]
    for p in patterns:
        match = re.match(p, ep_text)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None

def _get_desc_text(desc_el):
    if desc_el is None:
        return ""
    try:
        return "".join(desc_el.itertext()).strip()
    except Exception:
        return (desc_el.text or "").strip()

# ----------------------
# DESCARGAR GUIAS
# ----------------------
if not os.path.exists(EPG_FILE):
    r = requests.get(EPG_URL, timeout=60)
    r.raise_for_status()
    with gzip.open(io.BytesIO(r.content), 'rb') as f_in:
        with open(EPG_FILE, 'wb') as f_out:
            f_out.write(f_in.read())

for idx, url in enumerate(NUEVAS_EPGS, start=1):
    temp_file = f"epg_nueva_{idx}.xml"
    if not os.path.exists(temp_file):
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with gzip.open(io.BytesIO(r.content), 'rb') as f_in:
            with open(temp_file, 'wb') as f_out:
                f_out.write(f_in.read())
    EPG_FILES_TEMP.append(temp_file)

# ----------------------
# PROCESAR UNA EPG
# ----------------------
def procesar_epg(input_file, output_file, escribir_raiz=False):
    tree = ET.parse(input_file)
    root = tree.getroot()

    mode = "wb" if escribir_raiz else "ab"
    with open(output_file, mode) as f:
        if escribir_raiz:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>\n<tv>\n')

        for ch in root.findall("channel"):
            if ch.get("id") in CANALES_USAR:
                f.write(ET.tostring(ch, encoding="utf-8"))

        for elem in root.findall("programme"):
            canal = elem.get("channel")
            if canal not in CANALES_USAR:
                continue

            title_el = elem.find("title")
            sub_el = elem.find("sub-title")
            desc_el = elem.find("desc")
            date_el = elem.find("date")
            ep_el = elem.find("episode-num")
            ep_text = ep_el.text.strip() if ep_el is not None and ep_el.text else ""
            temporada, episodio = parse_episode_num(ep_text)
            categorias = [(c.text or "").lower() for c in elem.findall("category")]
            existing_desc = _get_desc_text(desc_el)
            titulo_original = title_el.text.strip() if title_el is not None and title_el.text else "Sin título"
            titulo_norm = normalizar_texto(titulo_original)

            es_serie = any("serie" in c for c in categorias) or (temporada is not None and episodio is not None)
            es_pelicula = not es_serie

            # ----------------------
            # Lógica de libre albedrío responsable
            # ----------------------
            if es_serie and temporada and episodio:
                search_res = buscar_tmdb(titulo_norm, "tv")
                nombre_ep, overview = ep_text, ""
                if search_res:
                    tv_id = search_res.get("id")
                    epi_info = obtener_info_serie(tv_id, temporada, episodio)
                    # Detecta si la sinopsis actual no corresponde al episodio
                    if existing_desc and "S" not in existing_desc and len(existing_desc) < 100:
                        # Reemplaza con sinopsis correcta
                        overview = epi_info.get("overview") or rellenar_descripcion(titulo_original, "serie", temporada, episodio)
                    nombre_ep = epi_info.get("name") or ep_text
                    nombre_ep = traducir_a_espanol(nombre_ep)
                    overview = traducir_a_espanol(overview) if overview else existing_desc

                if sub_el is None or not (sub_el.text or "").strip():
                    if sub_el is None:
                        sub_el = ET.SubElement(elem, "sub-title")
                    sub_el.text = nombre_ep

                if desc_el is None:
                    desc_el = ET.SubElement(elem, "desc")
                desc_el.text = overview if overview else existing_desc

                if title_el is None:
                    title_el = ET.SubElement(elem, "title")
                title_el.text = f"{titulo_original} (S{temporada:02d}E{episodio:02d})"

            elif es_pelicula:
                anio_epg = date_el.text.strip() if (date_el is not None and date_el.text) else ""
                search_res = buscar_tmdb(titulo_norm, "movie", year=anio_epg if anio_epg else None)
                anio, overview, titulo_es = "", "", titulo_original
                if search_res:
                    anio = (search_res.get("release_date") or "")[:4]
                    titulo_es = search_res.get("title") or titulo_original
                    overview = search_res.get("overview") or rellenar_descripcion(titulo_original, "pelicula")
                    titulo_es = traducir_a_espanol(titulo_es)
                    overview = traducir_a_espanol(overview)

                if (date_el is None or not (date_el.text or "").strip()) and anio:
                    if date_el is None:
                        date_el = ET.SubElement(elem, "date")
                    date_el.text = anio

                if desc_el is None:
                    desc_el = ET.SubElement(elem, "desc")
                desc_el.text = overview if overview else existing_desc

                if title_el is None:
                    title_el = ET.SubElement(elem, "title")
                title_el.text = f"{titulo_es} ({anio})" if anio else titulo_es

            f.write(ET.tostring(elem, encoding="utf-8"))

# ----------------------
# EJECUTAR
# ----------------------
procesar_epg(EPG_FILE, OUTPUT_FILE, escribir_raiz=True)
for temp_file in EPG_FILES_TEMP:
    procesar_epg(temp_file, OUTPUT_FILE, escribir_raiz=False)

with open(OUTPUT_FILE, "ab") as f:
    f.write(b"</tv>")

with open(OUTPUT_FILE, "rb") as f_in, gzip.open(OUTPUT_FILE + ".gz", "wb") as f_out:
    f_out.writelines(f_in)

print(f"✅ Guía generada: {OUTPUT_FILE} y {OUTPUT_FILE}.gz")
