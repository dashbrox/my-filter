import os
import re
import gzip
import requests
import unicodedata
import lxml.etree as ET

# ----------------------
# CONFIGURACIÓN
# ----------------------
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise RuntimeError("❌ TMDB_API_KEY no está definido en el entorno.")

# Canales a procesar
CANALES_USAR = {
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
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
    "Canal.Warner.TV.(México).mx",
}

# ----------------------
# FUNCIONES AUXILIARES
# ----------------------

def normalizar_texto(texto):
    """Quita acentos y normaliza espacios"""
    if not texto:
        return ""
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    return texto.strip()

def buscar_tmdb(titulo, year=None, tipo="movie"):
    """Busca en TMDB con fallback a inglés"""
    base = "https://api.themoviedb.org/3/search/"
    params = {
        "api_key": API_KEY,
        "query": titulo,
        "language": "es-MX",
    }
    if year:
        params["year"] = year

    r = requests.get(base + tipo, params=params)
    if r.status_code != 200:
        return None
    data = r.json()
    results = data.get("results", [])
    if not results:
        # Fallback a inglés
        params["language"] = "en-US"
        r = requests.get(base + tipo, params=params)
        if r.status_code != 200:
            return None
        data = r.json()
        results = data.get("results", [])
    return results[0] if results else None

def obtener_info_tmdb(titulo, year=None, es_serie=False, temporada=None, episodio=None):
    """Obtiene info de película o serie con fallback"""
    tipo = "tv" if es_serie else "movie"
    info = buscar_tmdb(titulo, year, tipo)
    if not info:
        return None

    # Película
    if not es_serie:
        return {
            "titulo": info.get("title") or info.get("original_title") or titulo,
            "anio": (info.get("release_date") or "????")[:4],
            "descripcion": info.get("overview") or "Sin descripción.",
        }

    # Serie (básica)
    data = {
        "titulo": info.get("name") or info.get("original_name") or titulo,
        "anio": (info.get("first_air_date") or "????")[:4],
        "descripcion": info.get("overview") or "Sin descripción.",
    }

    # Episodio específico si hay temporada+episodio
    if temporada and episodio:
        url = f"https://api.themoviedb.org/3/tv/{info['id']}/season/{temporada}/episode/{episodio}"
        params = {"api_key": API_KEY, "language": "es-MX"}
        r = requests.get(url, params=params)
        if r.status_code == 200:
            epi = r.json()
            data["titulo"] = f"{data['titulo']} (S{int(temporada):02d}E{int(episodio):02d})"
            data["descripcion"] = epi.get("overview") or data["descripcion"]
    return data

# ----------------------
# PROCESAMIENTO STREAMING
# ----------------------

def procesar_epg(input_file, output_file):
    context = ET.iterparse(input_file, events=("end",), tag="programme")

    with open(output_file, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="utf-8"?>\n<tv>\n')

        for _, elem in context:
            canal = elem.get("channel")
            if canal not in CANALES_USAR:
                elem.clear()
                continue

            # Extraer título
            title_el = elem.find("title")
            titulo = title_el.text if title_el is not None else "Sin título"
            titulo_norm = normalizar_texto(titulo)

            # Detectar año entre paréntesis
            year_match = re.search(r"\((\d{4})\)", titulo)
            year = year_match.group(1) if year_match else None

            # Detectar serie con SxxExx
            match_se = re.search(r"S(\d{1,2})E(\d{1,2})", titulo, re.IGNORECASE)
            es_serie = bool(match_se)
            temporada = match_se.group(1) if match_se else None
            episodio = match_se.group(2) if match_se else None

            # Obtener info de TMDB
            info = obtener_info_tmdb(
                titulo_norm, year, es_serie, temporada, episodio
            )

            if info:
                # Reemplazar título
                title_el.text = f"{info['titulo']} ({info['anio']})"

                # Reemplazar descripción
                desc_el = elem.find("desc")
                if desc_el is None:
                    desc_el = ET.SubElement(elem, "desc")
                desc_el.text = info["descripcion"]

            # Guardar nodo en el archivo
            f.write(ET.tostring(elem, encoding="utf-8"))

            # Liberar memoria
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        f.write(b"</tv>")

    # Generar versión comprimida .gz
    with open(output_file, "rb") as f_in, gzip.open(output_file + ".gz", "wb") as f_out:
        f_out.writelines(f_in)

# ----------------------
# EJECUCIÓN
# ----------------------

if __name__ == "__main__":
    procesar_epg("guide.xml", "guide_custom.xml")
    print("✅ Guía personalizada generada: guide_custom.xml y guide_custom.xml.gz")
