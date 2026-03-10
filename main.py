import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import time
from datetime import datetime

# CONFIGURACION
EPG_COUNTRY_CODES = """
ar au at bo ca cl co cr hr do ec sv
gt hn
mx me nl nz ni ng no pa py pe
es se ch tw th tr ug ua ae gb us uy ve vn zw
""".split()

EPG_URLS = [
    f"https://iptv-epg.org/files/epg-{code}.xml"
    for code in EPG_COUNTRY_CODES
]

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"

UPDATE_EVERY_HOURS = 6
UPDATE_INTERVAL_SECONDS = UPDATE_EVERY_HOURS * 3600


def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")


def normalize_season_ep(text):
    if not text:
        return None

    m = re.search(r"S\s*(\d+)\s*E\s*(\d+)", text, re.IGNORECASE)
    if not m:
        return None

    season = int(m.group(1))
    episode = int(m.group(2))
    return f"S{season:02d} E{episode:02d}"


def normalize_year(text):
    if not text:
        return None

    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if not m:
        return None

    return m.group(1)


def extract_new_marker(text):
    if not text:
        return text, False

    has_new = False
    clean = text

    if "ᴺᵉʷ" in clean:
        has_new = True
        clean = clean.replace("ᴺᵉʷ", " ")

    if re.search(r"\bNEW\b", clean, re.IGNORECASE):
        has_new = True
        clean = re.sub(r"\bNEW\b", " ", clean, flags=re.IGNORECASE)

    clean = " ".join(clean.split())
    return clean, has_new


def extract_year_from_title(title_text):
    if not title_text:
        return title_text, None

    clean_title = title_text.strip()

    # Detecta año al final del titulo, por ejemplo:
    # "Avatar (2009)" / "Avatar [2009]" / "Avatar - 2009"
    match = re.match(
        r"^(.*?)(?:\s*[\(\[]\s*(19\d{2}|20\d{2})\s*[\)\]]|\s*-\s*(19\d{2}|20\d{2})|\s+(19\d{2}|20\d{2}))\s*$",
        clean_title
    )
    if match:
        base_title = match.group(1).strip()
        year = match.group(2) or match.group(3) or match.group(4)
        if base_title:
            return base_title, year

    return clean_title, None


def extract_season_ep_from_desc(desc_text):
    if not desc_text:
        return None, desc_text

    desc_text = desc_text.strip()

    match = re.match(
        r"^\s*(S\s*\d+\s*E\s*\d+)\s+(.*)",
        desc_text,
        re.IGNORECASE | re.DOTALL
    )
    if match:
        season_ep = normalize_season_ep(match.group(1))
        remaining_desc = match.group(2).strip()
        return season_ep, remaining_desc

    return None, desc_text


def extract_year_from_desc(desc_text):
    if not desc_text:
        return None, desc_text

    desc_text = desc_text.strip()

    # Detecta año al inicio de la descripcion:
    # "2009 Avatar..." / "(2009) Avatar..." / "[2009] Avatar..."
    match = re.match(
        r"^\s*[\(\[]?\s*(19\d{2}|20\d{2})\s*[\)\]]?[\s\-.:\u2013\u2014]+(.*)",
        desc_text,
        re.DOTALL
    )
    if match:
        year = match.group(1)
        remaining_desc = match.group(2).strip()
        if remaining_desc:
            return year, remaining_desc

    return None, desc_text


def build_final_title(original_title, season_ep=None, year=None):
    clean_title, has_new = extract_new_marker(original_title)
    clean_title, year_in_title = extract_year_from_title(clean_title)

    final_title = clean_title

    meta_parts = []

    if season_ep:
        meta_parts.append(season_ep)
    elif year:
        meta_parts.append(year)
    elif year_in_title:
        meta_parts.append(year_in_title)

    if meta_parts:
        final_title += f" ({' | '.join(meta_parts)})"

    if has_new:
        final_title += " ᴺᵉʷ"

    return final_title


def safe_remove(path):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def download_xml(url, output_path):
    log(f"Descargando: {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def process_xml_file(xml_path, allowed_channels, out_f, written_channels, written_programmes):
    context = ET.iterparse(xml_path, events=("end",))

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

                season_ep = None
                year = None

                if desc_elem is not None and desc_elem.text:
                    season_ep, cleaned_desc = extract_season_ep_from_desc(desc_elem.text)
                    desc_elem.text = cleaned_desc

                    # Solo intenta sacar año si no encontro temporada/episodio
                    if not season_ep:
                        year, cleaned_desc = extract_year_from_desc(desc_elem.text)
                        desc_elem.text = cleaned_desc

                final_title = ""
                if title_elem is not None and title_elem.text:
                    title_elem.text = build_final_title(
                        title_elem.text,
                        season_ep=season_ep,
                        year=year
                    )
                    final_title = title_elem.text

                prog_key = (
                    ch_id,
                    elem.get("start", ""),
                    elem.get("stop", ""),
                    final_title,
                )

                if prog_key not in written_programmes:
                    out_f.write(ET.tostring(elem, encoding="utf-8"))
                    out_f.write(b"\n")
                    written_programmes.add(prog_key)

            elem.clear()


def run_once():
    if not os.path.exists(CHANNELS_FILE):
        log("Error: No existe channels.txt")
        return

    try:
        with open(CHANNELS_FILE, "r", encoding="utf-8-sig") as f:
            allowed_channels = {line.strip() for line in f if line.strip()}
    except Exception as e:
        log(f"Error leyendo {CHANNELS_FILE}: {e}")
        return

    if not allowed_channels:
        log("Error: channels.txt esta vacio")
        return

    written_channels = set()
    written_programmes = set()

    try:
        with open(TEMP_OUTPUT, "wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')

            for url in EPG_URLS:
                try:
                    download_xml(url, TEMP_INPUT)
                    process_xml_file(
                        TEMP_INPUT,
                        allowed_channels,
                        out_f,
                        written_channels,
                        written_programmes
                    )
                except Exception as e:
                    log(f"Error procesando {url}: {e}")
                finally:
                    safe_remove(TEMP_INPUT)

            out_f.write(b"</tv>\n")

    except Exception as e:
        log(f"Error generando XML combinado: {e}")
        safe_remove(TEMP_INPUT)
        safe_remove(TEMP_OUTPUT)
        return

    log("Comprimiendo resultado...")
    try:
        with open(TEMP_OUTPUT, "rb") as f_in:
            with gzip.open(OUTPUT_FILE, "wb") as f_out:
                while True:
                    chunk = f_in.read(1024 * 1024)
                    if not chunk:
                        break
                    f_out.write(chunk)
    except Exception as e:
        log(f"Error comprimiendo: {e}")
        safe_remove(TEMP_OUTPUT)
        return

    safe_remove(TEMP_OUTPUT)
    log(f"Listo. Archivo generado: {OUTPUT_FILE}")


def main():
    log(f"Iniciando actualizacion automatica cada {UPDATE_EVERY_HOURS} horas...")

    while True:
        try:
            run_once()
        except Exception as e:
            log(f"Error general en la ejecucion: {e}")

        log(f"Esperando {UPDATE_EVERY_HOURS} horas para la siguiente actualizacion...")
        time.sleep(UPDATE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
