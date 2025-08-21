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

BASE_URL = "https://api.themoviedb.org/3/search/multi"

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

def normalizar_titulo(titulo):
    titulo = TITULOS_MAP.get(titulo, titulo)
    titulo_normalized = unicodedata.normalize('NFKD', titulo).encode('ascii', 'ignore').decode()
    return titulo_normalized

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

    # --- ACTUALIZAR TÍTULO ORIGINAL CON EPISODIO ---
    title_elem.text = f"{title_original} ({se_text})" if se_text else title_original

    # --- SUBTÍTULO Fallback ---
    sub_elem = programme.find("sub-title")
    if sub_elem is None and se_text:
        sub_elem = ET.Element("sub-title")
        sub_elem.text = se_text
        programme.append(sub_elem)

    # --- BUSCAR TMDB PARA COMPLETAR INFO ---
    if programme.find("desc") is None or programme.find("date") is None or programme.find("category") is None:
        result = buscar_tmdb(title_to_search)
        if result:
            # Películas
            if result.get("media_type") == "movie":
                release_date = result.get("release_date") or ""
                year = release_date.split("-")[0] if release_date else ""
                title_elem.text = f"{result['title']} ({year})" if year else result['title']

            # Series
            elif result.get("media_type") == "tv":
                # Actualizar título con episodio si no se agregó
                tv_se_text = se_text
                if not tv_se_text:
                    if programme.find("sub-title") is not None and programme.find("sub-title").text:
                        tv_se_text = programme.find("sub-title").text.strip()
                if tv_se_text:
                    title_elem.text = f"{result['name']} ({tv_se_text})"
                else:
                    title_elem.text = result['name']

                # Descripción con primera línea: nombre del episodio
                if programme.find("desc") is None and result.get("overview"):
                    desc_text = f"\"{tv_se_text}\"\n{result['overview']}" if tv_se_text else result['overview']
                    desc = ET.Element("desc", lang="es")
                    desc.text = desc_text
                    programme.append(desc)

            # Fecha (solo películas)
            if programme.find("date") is None and result.get("release_date"):
                date_elem = ET.Element("date")
                date_elem.text = result["release_date"].split("-")[0]
                programme.append(date_elem)

            # Categoría: SOLO si TMDB es confiable y no existe
            existing_cat = programme.find("category")
            if existing_cat is None and result.get("media_type"):
                cat_elem = ET.Element("category", lang="es")
                media_type = result.get("media_type")
                if media_type == "movie":
                    cat_elem.text = "Película"
                elif media_type == "tv":
                    cat_elem.text = "Serie"
                programme.append(cat_elem)
        else:
            print(f"⚠️ No encontrado en TMDB: {title_original}")

# ----------------------
# GUARDAR XML FINAL
# ----------------------
try:
    tree.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado correctamente")
except Exception as e:
    print(f"❌ Error al guardar XML: {e}")
    sys.exit(1)
