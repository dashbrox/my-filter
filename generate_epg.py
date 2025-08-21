import xml.etree.ElementTree as ET
import requests
import re

API_KEY = "TU_API_KEY_DE_TMDB"
BASE_URL_SEARCH = "https://api.themoviedb.org/3/search/multi"

# --- Lista de canales permitidos ---
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
    "Canal.Sony.(México).mx",
    "Canal.Space.(México).mx",
    "Canal.Star.Channel.(México).mx",
    "Canal.Studio.Universal.(México).mx",
    "Canal.TNT.(México).mx",
    "Canal.TNT.Series.(México).mx",
    "Canal.Universal.TV.(México).mx",
    "Canal.USA.Network.(México).mx",
    "Canal.Warner.TV.(México).mx",
}

# --- Funciones auxiliares ---

def titulo_parecido(original: str, tmdb_title: str) -> bool:
    """Detecta si el título ya coincide bastante para no cambiarlo innecesariamente"""
    return original.lower().replace(" ", "") == tmdb_title.lower().replace(" ", "")

def titulo_esta_en_ingles(titulo: str) -> bool:
    """Si contiene palabras inglesas comunes, asumimos que está en inglés"""
    palabras_en = {"the", "of", "and", "season", "episode", "idol", "game", "thrones"}
    return any(p.lower() in titulo.lower() for p in palabras_en)

def buscar_tmdb(titulo, is_tv=False, is_movie=False):
    """Busca en TMDB con filtros"""
    try:
        params = {"api_key": API_KEY, "query": titulo, "language": "es"}
        r = requests.get(BASE_URL_SEARCH, params=params, timeout=10)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None

        # Filtrar según lo que sabemos
        if is_tv:
            results = [r for r in results if r.get("media_type") == "tv"]
        elif is_movie:
            results = [r for r in results if r.get("media_type") == "movie"]

        return results[0] if results else None
    except requests.RequestException:
        return None

def procesar_programa(prog):
    channel = prog.attrib.get("channel")
    if channel not in CHANNEL_WHITELIST:
        return None

    title_elem = prog.find("title")
    if title_elem is None:
        return None

    original_title = title_elem.text.strip()
    ep_num_elem = prog.find("episode-num")
    is_tv = ep_num_elem is not None
    is_movie = not is_tv

    # Usar TMDB solo si parece abreviado o pegado
    usar_tmdb = False
    if re.search(r"[A-Z]\.[A-Z]", original_title):  # abreviación tipo H. Potter
        usar_tmdb = True
    if re.search(r"[a-z][A-Z]", original_title):  # letra pegada tipo deMadagascar
        usar_tmdb = True

    # Buscar en TMDB si aplica
    tmdb_data = None
    if usar_tmdb:
        tmdb_data = buscar_tmdb(original_title, is_tv=is_tv, is_movie=is_movie)

    # Decidir título final
    if tmdb_data and "name" in tmdb_data:
        tmdb_title = tmdb_data.get("name") or tmdb_data.get("title")
        if titulo_esta_en_ingles(original_title):  # Mantener inglés
            final_title = original_title
        elif not titulo_parecido(original_title, tmdb_title):
            final_title = tmdb_title
        else:
            final_title = original_title
    else:
        final_title = original_title

    # Reemplazar en XML
    title_elem.text = final_title

    # Categoría
    cat_elem = ET.Element("category", {"lang": "es"})
    cat_elem.text = "Serie" if is_tv else "Película"
    prog.append(cat_elem)

    return prog

# --- MAIN ---
def procesar_xml(input_file, output_file):
    tree = ET.parse(input_file)
    root = tree.getroot()

    nuevos_programas = []
    for prog in root.findall("programme"):
        nuevo = procesar_programa(prog)
        if nuevo is not None:
            nuevos_programas.append(nuevo)

    root[:] = nuevos_programas
    tree.write(output_file, encoding="utf-8", xml_declaration=True)


if __name__ == "__main__":
    procesar_xml("guide_original.xml", "guide_custom.xml")
