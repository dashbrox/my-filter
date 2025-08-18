#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import urllib.request
import xml.etree.ElementTree as ET

# Nuevas fuentes de EPG
URLS = [
    "https://www.open-epg.com/files/canada1.xml.gz",
    "https://www.open-epg.com/files/mexico2.xml.gz",
    "https://www.open-epg.com/files/spain1.xml.gz",
    "https://www.open-epg.com/files/unitedstates1.xml.gz",
    "https://www.open-epg.com/files/unitedkingdom4.xml.gz"
]

# Leer los canales desde channels.txt
def load_channels(filename="channels.txt"):
    channels = {}
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and ";" in line:
                key, display = line.split(";", 1)
                channels[key.strip()] = display.strip()
    return channels

# Descargar y descomprimir
def download_and_extract(url):
    print(f"Descargando {url} ...")
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    return gzip.decompress(data)

# Formatear títulos de series/películas con sinopsis
def format_title(title, desc=""):
    title = title.strip()
    desc = desc.strip() if desc else ""
    
    # Detectar series: buscar Sxx Exx en el título o descripción
    m = re.search(r"(?:S|s)(\d+)\s*(?:E|e)(\d+)", title)
    if m:
        season = m.group(1)
        episode = m.group(2)
        epi_title = title.split(")")[-1].strip() if ")" in title else ""
        formatted = f'{title.split("(")[0].strip()} (S{season} E{episode}) "{epi_title}"'
        return formatted, desc
    
    # Detectar año de película: buscar (YYYY) en título o desc
    m2 = re.search(r"\b(19|20)\d{2}\b", title)
    if m2:
        year = m2.group(0)
        title_clean = re.sub(r"\(\d{4}\)", "", title).strip()
        formatted = f"{title_clean} ({year})"
        return formatted, desc
    
    # Por defecto solo título
    return title, desc

def main():
    import re

    channels_to_keep = load_channels()
    root = ET.Element("tv")

    for url in URLS:
        xml_data = download_and_extract(url)
        tree = ET.ElementTree(ET.fromstring(xml_data))

        # Agregar canales filtrados
        for channel in tree.findall("channel"):
            name = "".join(channel.findtext("display-name", default="")).strip()
            if name in channels_to_keep:
                channel.find("display-name").text = channels_to_keep[name]
                root.append(channel)

        # Agregar programas de los canales filtrados
        for programme in tree.findall("programme"):
            ch = programme.attrib.get("channel", "")
            if ch in channels_to_keep:
                title_el = programme.find("title")
                desc_el = programme.find("desc")
                if title_el is not None:
                    title_fmt, desc_fmt = format_title(title_el.text or "", desc_el.text if desc_el is not None else "")
                    title_el.text = title_fmt
                    if desc_el is not None:
                        desc_el.text = desc_fmt
                root.append(programme)

    # Guardar XML final
    tree_out = ET.ElementTree(root)
    try:
        ET.indent(tree_out, space="  ", level=0)  # Python ≥3.9
    except AttributeError:
        pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado con éxito.")

if __name__ == "__main__":
    main()
