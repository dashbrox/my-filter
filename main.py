import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os

# CONFIGURACION
URL_EPG_USA = "https://iptv-epg.org/files/epg-us.xml"
CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"


def normalize_season_ep(text):
    if not text:
        return None
    # Convierte "S04 E06" o "s04e06" a "S04E06"
    return re.sub(r"\s+", "", text.upper())


def extract_new_marker(text):
    if not text:
        return text, False

    has_new = False
    clean = text

    # Detectar la version rara
    if "ᴺᵉʷ" in clean:
        has_new = True
        clean = clean.replace("ᴺᵉʷ", " ")

    # Detectar NEW normal
    if re.search(r"\bNEW\b", clean, re.IGNORECASE):
        has_new = True
        clean = re.sub(r"\bNEW\b", " ", clean, flags=re.IGNORECASE)

    # Limpiar espacios
    clean = " ".join(clean.split())
    return clean, has_new


def extract_season_ep_from_desc(desc_text):
    if not desc_text:
        return None, desc_text

    desc_text = desc_text.strip()

    # Busca S04E06 o S04 E06 al inicio
    match = re.match(r"^(S\d+\s*E\d+)\s+(.*)", desc_text, re.IGNORECASE | re.DOTALL)
    if match:
        season_ep = normalize_season_ep(match.group(1))
        remaining_desc = match.group(2).strip()
        return season_ep, remaining_desc

    return None, desc_text


def build_final_title(original_title, season_ep):
    clean_title, has_new = extract_new_marker(original_title)

    final_title = clean_title

    if season_ep:
        final_title += f" ({season_ep})"

    if has_new:
        final_title += " ᴺᵉʷ"

    return final_title


def safe_remove(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def main():
    # 1. Descargar XML fuente
    print("1. Descargando EPG de EE.UU. al disco...")
    try:
        with requests.get(URL_EPG_USA, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(TEMP_INPUT, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        print("   Descarga completa.")
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    # 2. Leer canales permitidos
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No existe channels.txt")
        safe_remove(TEMP_INPUT)
        return

    try:
        with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
            allowed_channels = {line.strip() for line in f if line.strip()}
    except Exception as e:
        print(f"Error leyendo {CHANNELS_FILE}: {e}")
        safe_remove(TEMP_INPUT)
        return

    if not allowed_channels:
        print("Error: channels.txt esta vacio")
        safe_remove(TEMP_INPUT)
        return

    # 3. Procesar XML
    print("2. Procesando XML...")

    try:
        with open(TEMP_OUTPUT, "wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')

            context = ET.iterparse(TEMP_INPUT, events=("start", "end"))

            for event, elem in context:
                if event != "end":
                    continue

                if elem.tag == "channel":
                    ch_id = elem.get("id")
                    if ch_id in allowed_channels:
                        out_f.write(ET.tostring(elem, encoding="utf-8"))
                    elem.clear()

                elif elem.tag == "programme":
                    ch_id = elem.get("channel")

                    if ch_id in allowed_channels:
                        title_elem = elem.find("title")
                        desc_elem = elem.find("desc")

                        season_ep = None

                        # Extraer temporada/episodio desde la descripcion
                        if desc_elem is not None and desc_elem.text:
                            season_ep, cleaned_desc = extract_season_ep_from_desc(desc_elem.text)
                            desc_elem.text = cleaned_desc

                        # Reescribir titulo
                        if title_elem is not None and title_elem.text:
                            title_elem.text = build_final_title(title_elem.text, season_ep)

                        out_f.write(ET.tostring(elem, encoding="utf-8"))

                    elem.clear()

            out_f.write(b"\n</tv>")

    except Exception as e:
        print(f"Error procesando XML: {e}")
        safe_remove(TEMP_INPUT)
        safe_remove(TEMP_OUTPUT)
        return

    # 4. Comprimir resultado
    print("3. Comprimiendo resultado...")
    try:
        with open(TEMP_OUTPUT, "rb") as f_in:
            with gzip.open(OUTPUT_FILE, "wb") as f_out:
                while True:
                    chunk = f_in.read(1024 * 1024)
                    if not chunk:
                        break
                    f_out.write(chunk)
    except Exception as e:
        print(f"Error comprimiendo: {e}")
        safe_remove(TEMP_INPUT)
        safe_remove(TEMP_OUTPUT)
        return

    # 5. Limpiar temporales
    safe_remove(TEMP_INPUT)
    safe_remove(TEMP_OUTPUT)

    print(f"Listo. Archivo generado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
