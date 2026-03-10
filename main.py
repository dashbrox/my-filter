import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import json
import time
from datetime import datetime

# --- CONFIGURACION ---
EPG_COUNTRY_CODES = """
al ar am au at by be bo ba br bg ca cl co cr hr cz dk do ec eg sv
fi fr ge de gh gr gt hn hk hu is in id il it jp lv lb lt lu mk my
mt mx me nl nz ni ng no pa py pe ph pl pt ro ru sa rs sg si za kr
es se ch tw th tr ug ua ae gb us uy ve vn zw
""".split()

EPG_URLS = [f"https://iptv-epg.org/files/epg-{code}.xml" for code in EPG_COUNTRY_CODES]
EXTRA_EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
]
EPG_URLS.extend(EXTRA_EPG_URLS)

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"
CACHE_FILE = "api_cache.json"

# --- API KEYS (Insertadas aquí) ---
TMDB_API_KEY = "4b91ab04d4193f521881b390c54d1574"
TVMAZE_API_KEY = "wNSRljutXXdw_I0guL86DPMgp9-ze10M"

# --- CACHÉ GLOBAL ---
api_cache = {}
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            api_cache = json.load(f)
        print(f"Caché cargado: {len(api_cache)} entradas.")
    except Exception:
        api_cache = {}

def save_cache():
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(api_cache, f, ensure_ascii=False, indent=2)

# --- FUNCIONES API ---

def get_tmdb_data(title):
    """Consulta TMDB para obtener año (películas) o ID de serie."""
    cache_key = f"tmdb:{title}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    if not TMDB_API_KEY:
        return None

    url = "https://api.themoviedb.org/3/search/multi"
    params = {'api_key': TMDB_API_KEY, 'query': title, 'language': 'es-ES'}
    
    try:
        r = requests.get(url, params=params, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data['results']:
                item = data['results'][0] 
                result = {
                    'type': item.get('media_type'),
                    'year': None,
                    'id': item.get('id')
                }
                
                date_str = item.get('release_date') or item.get('first_air_date')
                if date_str:
                    result['year'] = date_str.split('-')[0]
                
                api_cache[cache_key] = result
                return result
    except Exception as e:
        print(f"Error TMDB: {e}")
    
    api_cache[cache_key] = None
    return None

def get_tvmaze_episode(show_name, air_date):
    """
    Busca en TVMAZE una serie y un episodio por fecha de emisión.
    """
    cache_key = f"tvmaze:{show_name}:{air_date}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    # 1. Buscar el ID de la serie
    search_url = "https://api.tvmaze.com/singlesearch/shows"
    params = {'q': show_name}
    
    # Agregamos la API Key si está disponible para mayor velocidad y límites
    if TVMAZE_API_KEY:
        params['apikey'] = TVMAZE_API_KEY

    try:
        r_show = requests.get(search_url, params=params, timeout=5)
        if r_show.status_code != 200:
            api_cache[cache_key] = None
            return None
            
        show_data = r_show.json()
        show_id = show_data['id']
        
        # 2. Buscar episodio por fecha
        ep_url = f"https://api.tvmaze.com/shows/{show_id}/episodesbydate"
        # Nota: La key también se puede pasar aquí si el servidor lo requiere, 
        # aunque usualmente con el ID público basta, la mantemos en params global si es necesario.
        
        r_ep = requests.get(ep_url, params={'date': air_date}, timeout=5)
        
        if r_ep.status_code == 200:
            episodes = r_ep.json()
            if episodes:
                ep = episodes[0]
                result = {
                    'season': ep['season'],
                    'episode': ep['number'],
                    'name': ep.get('name')
                }
                api_cache[cache_key] = result
                return result

    except Exception:
        pass

    api_cache[cache_key] = None
    return None

# --- PROCESAMIENTO DE TEXTO ---

def normalize_season_ep(text):
    if not text: return None
    match = re.search(r"S\s*(\d+)\s*E\s*(\d+)", text, re.IGNORECASE)
    if match:
        return f"S{int(match.group(1)):02d} E{int(match.group(2)):02d}"
    return text.upper()

def extract_new_marker(text):
    if not text: return text, False
    has_new = False
    clean = text
    if "ᴺᵉʷ" in clean:
        has_new = True
        clean = clean.replace("ᴺᵉʷ", " ")
    if re.search(r"\bNEW\b", clean, re.IGNORECASE):
        has_new = True
        clean = re.sub(r"\bNEW\b", " ", clean, flags=re.IGNORECASE)
    return " ".join(clean.split()), has_new

def extract_year_regex(text):
    if not text: return text, None
    match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if match:
        year = match.group(1)
        clean = re.sub(r"\(?\b" + re.escape(year) + r"\b\)?", " ", text)
        return " ".join(clean.split()), year
    return text, None

def extract_se_regex(text):
    if not text: return None
    match = re.search(r"S\s*(\d+)\s*E\s*(\d+)", text, re.IGNORECASE)
    if match:
        return normalize_season_ep(text)
    return None

# --- LÓGICA PRINCIPAL ---

def process_programme(title, desc, start_time_str):
    # 1. Limpieza inicial
    clean_title, has_new = extract_new_marker(title)
    
    # Intentamos con Regex primero
    clean_title, year_regex = extract_year_regex(clean_title)
    se_regex = extract_se_regex(clean_title)
    se_desc = extract_se_regex(desc)
    
    final_year = year_regex
    final_se = se_regex or se_desc
    
    # 2. Si falta info, usamos APIs
    if not final_year and not final_se:
        tmdb_data = get_tmdb_data(clean_title)
        
        if tmdb_data:
            if tmdb_data['type'] == 'movie':
                final_year = tmdb_data['year']
            
            elif tmdb_data['type'] == 'tv':
                try:
                    air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
                    tvmaze_data = get_tvmaze_episode(clean_title, air_date)
                    
                    if tvmaze_data:
                        final_se = f"S{tvmaze_data['season']:02d} E{tvmaze_data['episode']:02d}"
                        if not final_year and tmdb_data['year']:
                             final_year = tmdb_data['year']
                    else:
                        final_year = tmdb_data['year']
                        
                except ValueError:
                    pass

    # 3. Construir resultado
    display_title = clean_title
    if final_se:
        display_title += f" ({final_se})"
    elif final_year:
        display_title += f" ({final_year})"
        
    if has_new:
        display_title += " ᴺᵉʷ"
        
    return display_title

def download_xml(url, output_path):
    print(f"Descargando: {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        if url.lower().endswith(".gz"):
            with gzip.GzipFile(fileobj=r.raw) as gz:
                with open(output_path, "wb") as f:
                    for chunk in iter(lambda: gz.read(1024*1024), b''): f.write(chunk)
        else:
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*1024): f.write(chunk)

def main():
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No existe channels.txt")
        return

    with open(CHANNELS_FILE, "r", encoding="utf-8-sig") as f:
        allowed_channels = {line.strip() for line in f if line.strip()}

    written_programmes = set()
    written_channels = set()

    try:
        with open(TEMP_OUTPUT, "wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')
            
            for url in EPG_URLS:
                try:
                    download_xml(url, TEMP_INPUT)
                    context = ET.iterparse(TEMP_INPUT, events=("end",))
                    
                    for event, elem in context:
                        if elem.tag == "channel":
                            ch_id = elem.get("id")
                            if ch_id in allowed_channels and ch_id not in written_channels:
                                out_f.write(ET.tostring(elem, encoding="utf-8"))
                                out_f.write(b"\n")
                                written_channels.add(ch_id)
                            elem.clear()

                        elif elem.tag == "programme":
                            ch_id = elem.get("channel")
                            if ch_id in allowed_channels:
                                title_elem = elem.find("title")
                                desc_elem = elem.find("desc")
                                
                                raw_title = title_elem.text if title_elem is not None else ""
                                raw_desc = desc_elem.text if desc_elem is not None else ""
                                start = elem.get("start", "")

                                new_title = process_programme(raw_title, raw_desc, start)
                                
                                if title_elem is not None:
                                    title_elem.text = new_title

                                prog_key = (ch_id, start, elem.get("stop"), new_title)
                                if prog_key not in written_programmes:
                                    out_f.write(ET.tostring(elem, encoding="utf-8"))
                                    out_f.write(b"\n")
                                    written_programmes.add(prog_key)
                            elem.clear()
                except Exception as e:
                    print(f"Error en fuente {url}: {e}")
                finally:
                    if os.path.exists(TEMP_INPUT): os.remove(TEMP_INPUT)
                    
            out_f.write(b"</tv>\n")
    finally:
        save_cache()

    print("Comprimiendo...")
    with open(TEMP_OUTPUT, "rb") as f_in:
        with gzip.open(OUTPUT_FILE, "wb") as f_out:
            f_out.writelines(f_in)
    
    if os.path.exists(TEMP_OUTPUT): os.remove(TEMP_OUTPUT)
    print(f"Proceso completado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
