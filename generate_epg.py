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

# ----------------------
# CANALES
# ----------------------
CANALES_USAR = {
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&amp;E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&amp;.Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
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
    "Canal.Sony.(México).mx",
    "Canal.Star.Channel.(México).mx",
    "Canal.Studio.Universal.(México).mx",
    "Canal.TNT.(México).mx",
    "Canal.TNT.Series.(México).mx",
    "Canal.Universal.TV.(México).mx",
    "Canal.USA.Network.(México).mx",
    "Canal.Warner.TV.(México).mx",
}

# Map de títulos especiales si aplica
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

def buscar_tmdb(titulo, tipo="multi", lang="es-MX"):
    titulo = TITULOS_MAP.get(titulo, titulo)
    url = f"https://api.themoviedb.org/3/search/{tipo}"
    params = {"api_key": API_KEY, "query": titulo, "language": lang}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        return results[0] if results else None
    except Exception:
        if lang != "en-US":
            return buscar_tmdb(titulo, tipo, "en-US")
    return None

def obtener_info_serie(tv_id, temporada, episodio, lang="es-MX"):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{temporada}/episode/{episodio}"
    params = {"api_key": API_KEY, "language": lang}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception:
        if lang != "en-US":
            return obtener_info_serie(tv_id, temporada, episodio, "en-US")
    return {}

def parse_episode_num(ep_text):
    if not ep_text:
        return None, None
    ep_text = ep_text.strip().upper()
    match = re.match(r"S(\d{1,2})E(\d{1,2})", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    match = re.search(r"(\d{1,2})[xE](\d{1,2})", ep_text)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None

# ----------------------
# DESCARGAR GUIA ORIGINAL
# ----------------------
if not os.path.exists(EPG_FILE):
    print("📥 Descargando guía original...")
    r = requests.get(EPG_URL, timeout=60)
    r.raise_for_status()
    with gzip.open(io.BytesIO(r.content), 'rb') as f_in:
        with open(EPG_FILE, 'wb') as f_out:
            f_out.write(f_in.read())
    print("✅ Guía original descargada.")

# ----------------------
# PROCESAR EPG
# ----------------------
def procesar_epg(input_file, output_file):
    tree = ET.parse(input_file)
    root = tree.getroot()

    with open(output_file, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="utf-8"?>\n<tv>\n')

        # --- Guardar todos los canales intactos ---
        for ch in root.findall("channel"):
            f.write(ET.tostring(ch, encoding="utf-8"))

        # --- Procesar programas ---
        context = ET.iterparse(input_file, events=("end",), tag="programme")
        for _, elem in context:
            canal = elem.get("channel")
            if canal not in CANALES_USAR:
                elem.clear()
                continue

            # --- Elementos existentes ---
            title_el = elem.find("title")
            titulo = title_el.text.strip() if title_el is not None else "Sin título"
            titulo_norm = normalizar_texto(titulo)

            desc_el = elem.find("desc")
            date_el = elem.find("date")
            ep_el = elem.find("episode-num")
            ep_text = ep_el.text.strip() if ep_el is not None else ""
            temporada, episodio = parse_episode_num(ep_text)

            # --- Determinar categoría sin modificarla ---
            categorias = [c.text.lower() for c in elem.findall("category")]
            es_serie = any("serie" in c for c in categorias)
            es_pelicula = any("pel" in c or "movie" in c for c in categorias)

            # --- SUB-TITLE EXISTENTE ---
            sub_el = elem.find("sub-title")
            if sub_el is None:
                sub_el = ET.Element("sub-title")
                sub_el.text = ep_text
                elem.append(sub_el)

            # --- CONSULTAR TMDB ---
            if (es_serie and temporada and episodio) or es_pelicula:
                if (desc_el is None or not desc_el.text.strip()) or (es_pelicula and (date_el is None or not date_el.text.strip())):
                    tipo_busqueda = "tv" if es_serie else "movie"
                    search_res = buscar_tmdb(titulo_norm, tipo_busqueda)
                    if search_res:
                        # --- SERIES ---
                        if es_serie:
                            tv_id = search_res.get("id")
                            epi_info = obtener_info_serie(tv_id, temporada, episodio)
                            nombre_ep = epi_info.get("name") or ep_text
                            desc_text = f"{nombre_ep}\n{epi_info.get('overview') or ''}".strip()
                            if desc_el is None:
                                desc_el = ET.SubElement(elem, "desc")
                            desc_el.text = desc_text
                            title_el.text = f"{titulo} (S{temporada:02d}E{episodio:02d}) - {nombre_ep}"
                            sub_el.text = ep_text

                        # --- PELÍCULAS ---
                        else:
                            anio = (search_res.get("release_date") or "????")[:4]
                            overview = search_res.get("overview") or ""
                            if date_el is None:
                                date_el = ET.SubElement(elem, "date")
                            date_el.text = anio
                            if desc_el is None:
                                desc_el = ET.SubElement(elem, "desc")
                            desc_el.text = overview
                            if f"({anio})" not in titulo:
                                title_el.text = f"{titulo} ({anio})"

            # Guardar nodo
            f.write(ET.tostring(elem, encoding="utf-8"))
            elem.clear()

        f.write(b"</tv>")

    # --- Comprimir XML ---
    with open(output_file, "rb") as f_in, gzip.open(output_file + ".gz", "wb") as f_out:
        f_out.writelines(f_in)

# ----------------------
# EJECUCIÓN
# ----------------------
if __name__ == "__main__":
    procesar_epg(EPG_FILE, OUTPUT_FILE)
    print(f"✅ Guía generada: {OUTPUT_FILE} y {OUTPUT_FILE}.gz")
