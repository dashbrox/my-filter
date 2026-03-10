import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import tempfile
import time
import sys

# --- CONFIGURACION ---
EPG_COUNTRY_CODES = """
al ar am au at by be bo ba br bg ca cl co cr hr cz dk do ec eg sv
fi fr ge de gh gr gt hn hk hu is in id il it jp lv lb lt lu mk my
mt mx me nl nz ni ng no pa py pe ph pl pt ro ru sa rs sg si za kr
es se ch tw th tr ug ua ae gb us uy ve vn zw
""".split()

EPG_URLS = [
    f"https://iptv-epg.org/files/epg-{code}.xml"
    for code in EPG_COUNTRY_CODES
]

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"

# --- REGEX PRECOMPILADAS (Optimización) ---
# Detecta "NEW" como palabra completa
_RE_NEW_MARKER = re.compile(r"\bNEW\b", re.IGNORECASE)
# Detecta "S01E01" o "S01 E01" al inicio de la descripción
_RE_SEASON_EP_DESC = re.compile(r"^(S\d+\s*E\d+)\s+(.*)", re.IGNORECASE | re.DOTALL)
# Detecta "(2023)", "(1999)", etc. en el título para extraer el año
_RE_YEAR_TITLE = re.compile(r"\s*\((19\d{2}|20\d{2})\)\s*$")
# Detecta el patrón S##E## para normalizar su formato
_RE_SEASON_EP_FORMAT = re.compile(r"S(\d+)\s*E(\d+)", re.IGNORECASE)

def normalize_season_ep(text):
    """
    Convierte 'S01E01' o 'S01 E01' a formato 'S01 E01'.
    """
    if not text:
        return None
    
    match = _RE_SEASON_EP_FORMAT.search(text)
    if match:
        s_num = match.group(1)
        e_num = match.group(2)
        # Formato forzado: S## E##
        return f"S{s_num} E{e_num}"
    
    # Si no coincide con el patrón esperado, devuelve el texto limpio en mayúsculas
    return text.upper()

def extract_year_from_title(text):
    """
    Busca un año (YYYY) entre paréntesis al final del título.
    Devuelve (year, cleaned_title) o (None, original_text).
    """
    if not text:
        return None, text

    match = _RE_YEAR_TITLE.search(text)
    if match:
        year = match.group(1)
        # Limpiar el año del título
        clean = text[:match.start()].strip()
        return year, clean
    
    return None, text

def extract_new_marker(text):
    if not text:
        return text, False

    has_new = False
    clean = text

    if "ᴺᵉʷ" in clean:
        has_new = True
        clean = clean.replace("ᴺᵉʷ", " ")

    if _RE_NEW_MARKER.search(clean):
        has_new = True
        clean = _RE_NEW_MARKER.sub(" ", clean)

    clean = " ".join(clean.split())
    return clean, has_new

def extract_season_ep_from_desc(desc_text):
    if not desc_text:
        return None, desc_text

    desc_text = desc_text.strip()
    match = _RE_SEASON_EP_DESC.match(desc_text)
    if match:
        season_ep = normalize_season_ep(match.group(1))
        remaining_desc = match.group(2).strip()
        return season_ep, remaining_desc

    return None, desc_text

def build_final_title(original_title, suffix, has_new):
    """
    Construye el título final.
    suffix: Puede ser "S01 E01" (string) o "2023" (año).
    has_new: Booleano para añadir el marcador especial.
    """
    # Primero limpiamos el título de marcadores NEW
    clean_title, _ = extract_new_marker(original_title)
    
    # Si se pasó has_new=True explícitamente, lo respetamos, 
    # pero como ya llamamos a extract_new_marker para limpiar, 
    # usamos el resultado limpio.
    # Nota: extract_new_marker ya detectó si tenía NEW.
    
    # Re-evaluamos has_new basado en la limpieza inicial
    # (Pasamos has_new desde fuera, pero para evitar duplicados en la lógica de llamada, 
    # mejor asumimos que clean_title ya está limpio).
    
    # Lógica simple: Título Limpio + Sufijo + Marcador New
    final_title = clean_title

    if suffix:
        final_title += f" ({suffix})"

    if has_new:
        final_title += " ᴺᵉʷ"

    return final_title

def download_xml(url, output_path):
    print(f"Descargando: {url}")
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"Error descargando {url}: {e}")
        raise

def process_xml_file(xml_path, allowed_channels, out_f, written_channels, written_programmes):
    context = ET.iterparse(xml_path, events=("start", "end"))
    event, root = next(context)
    
    for event, elem in context:
        if event == "end":
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

                    # 1. Intentar extraer Temporada/Episodio de la descripción
                    season_ep = None
                    if desc_elem is not None and desc_elem.text:
                        season_ep, cleaned_desc = extract_season_ep_from_desc(desc_elem.text)
                        desc_elem.text = cleaned_desc

                    # 2. Determinar sufijo y limpiar título
                    suffix = None
                    has_new_marker = False
                    final_title_text = ""

                    if title_elem is not None and title_elem.text:
                        original_text = title_elem.text
                        
                        # Detectar marcador NEW antes de procesar
                        _, has_new_marker = extract_new_marker(original_text)

                        if season_ep:
                            # Si hay temporada/episodio en descripción, tiene prioridad
                            suffix = season_ep
                            # El título se queda como está (solo se limpia de NEW en build_final_title)
                        else:
                            # Si no hay temporada/episodio, buscar Año en el título
                            year, clean_text = extract_year_from_title(original_text)
                            if year:
                                suffix = year
                                # Actualizamos el texto del título para que ya no tenga el año
                                # build_final_title volverá a limpiar, pero es bueno pasar el texto limpio
                                title_elem.text = clean_text 
                        
                        # Construir el título final
                        title_elem.text = build_final_title(title_elem.text, suffix, has_new_marker)
                        final_title_text = title_elem.text

                    # 3. Escribir si no está duplicado
                    prog_key = (
                        ch_id,
                        elem.get("start", ""),
                        elem.get("stop", ""),
                        final_title_text,
                    )

                    if prog_key not in written_programmes:
                        out_f.write(ET.tostring(elem, encoding="utf-8"))
                        out_f.write(b"\n")
                        written_programmes.add(prog_key)
                
                elem.clear()
        root.clear()

def run_update():
    """Función que ejecuta una sola pasada de actualización."""
    print(f"\n--- Iniciando actualización: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No existe channels.txt")
        return False # Devuelve False para indicar error, pero el loop continuará intentándolo

    try:
        with open(CHANNELS_FILE, "r", encoding="utf-8-sig") as f:
            allowed_channels = {line.strip() for line in f if line.strip()}
    except Exception as e:
        print(f"Error leyendo {CHANNELS_FILE}: {e}")
        return False

    if not allowed_channels:
        print("Error: channels.txt esta vacio")
        return False

    written_channels = set()
    written_programmes = set()
    
    temp_xml = tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".xml")
    temp_xml_path = temp_xml.name
    
    print(f"Generando XML temporal en: {temp_xml_path}")

    try:
        temp_xml.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')
        temp_xml.flush()

        for url in EPG_URLS:
            temp_input = None
            try:
                temp_input = tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".xml")
                download_xml(url, temp_input.name)
                temp_input.close()
                
                process_xml_file(
                    temp_input.name,
                    allowed_channels,
                    temp_xml,
                    written_channels,
                    written_programmes
                )
            except Exception as e:
                print(f"Error procesando {url}: {e}")
            finally:
                if temp_input:
                    try:
                        os.unlink(temp_input.name)
                    except OSError:
                        pass

        temp_xml.write(b"</tv>\n")
        temp_xml.close()

        print("Comprimiendo resultado...")
        with open(temp_xml_path, "rb") as f_in:
            with gzip.open(OUTPUT_FILE, "wb") as f_out:
                while True:
                    chunk = f_in.read(1024 * 1024)
                    if not chunk:
                        break
                    f_out.write(chunk)
        
        print(f"Actualización completada: {OUTPUT_FILE}")
        return True

    except Exception as e:
        print(f"Error general durante la actualización: {e}")
        return False
    finally:
        try:
            if os.path.exists(temp_xml_path):
                os.unlink(temp_xml_path)
        except OSError:
            pass

def main():
    print("Iniciando servicio de actualización de EPG (Cada 6 horas).")
    print("Presiona Ctrl+C para detener el script.")
    
    while True:
        run_update()
        
        # Calcular tiempo de espera
        wait_seconds = 6 * 60 * 60 # 6 horas
        
        print(f"Próxima actualización en 6 horas...")
        
        # Manejo de interrupción elegante para no esperar las 6 horas si se para el script
        try:
            # time.sleep espera el tiempo, pero si se interrumpe lanza KeyboardInterrupt
            # Hacemos un bucle de esperas cortas para poder salir limpiamente si se desea en el futuro
            # o simplemente dormir todo el bloque.
            time.sleep(wait_seconds)
        except KeyboardInterrupt:
            print("\nDeteniendo el servicio por solicitud del usuario.")
            sys.exit(0)

if __name__ == "__main__":
    main()
