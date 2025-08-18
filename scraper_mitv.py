#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import urllib.request
import xml.etree.ElementTree as ET
import re

# URLs de las fuentes que quieres usar
URLS = [
    "https://www.open-epg.com/files/canada1.xml.gz",
    "https://www.open-epg.com/files/mexico2.xml.gz",
    "https://www.open-epg.com/files/spain1.xml.gz",
    "https://www.open-epg.com/files/unitedstates1.xml.gz",
    "https://www.open-epg.com/files/unitedkingdom4.xml.gz",
]

# Canales a filtrar (ejemplo inicial que pediste)
CHANNELS = [
    "HBO 2.mx",
    "HBO FAMILY.mx",
    "HBO.mx",
    "MAX PRIME.mx",
    "MAX UP.mx",
    "MAX.mx"
]

def download_and_extract(url):
    print(f"Descargando {url} ...")
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    return gzip.decompress(data)

def channel_matches(name):
    name_norm = name.lower()
    for ch in CHANNELS:
        if ch.lower() in name_norm:
            return True
    return False

def format_title(title, desc):
    """
    Series: True Blood (S7 E2) "Título del episodio"
    Películas: Harry Potter y el cáliz del fuego (2005)
    """
    m = re.search(r"(?:S|s)(\d+)\s*(?:E|e)(\d+)", title)
    if m:
        season, ep = m.groups()
        # Extraer título de episodio si aparece en la descripción
        epi_title = desc.strip() if desc else ""
        return f'{title} (S{season} E{ep}) "{epi_title}"', desc
    # Buscar año en la descripción o título
    m_year = re.search(r"(\d{4})", desc)
    if m_year:
        year = m_year.group(1)
        return f"{title} ({year})", desc
    return title, desc

def main():
    root = ET.Element("tv")

    for url in URLS:
        xml_data = download_and_extract(url)
        tree = ET.ElementTree(ET.fromstring(xml_data))
        for channel in tree.findall("channel"):
            name = "".join(channel.findtext("display-name", default=""))
            if channel_matches(name):
                root.append(channel)
        for programme in tree.findall("programme"):
            ch = programme.attrib.get("channel", "")
            title_el = programme.find("title")
            desc_el = programme.find("desc")
            if channel_matches(ch) and title_el is not None:
                title_fmt, desc_fmt = format_title(title_el.text or "", desc_el.text if desc_el is not None else "")
                title_el.text = title_fmt
                if desc_el is not None:
                    desc_el.text = desc_fmt
                root.append(programme)

    tree_out = ET.ElementTree(root)
    try:
        ET.indent(tree_out, space="  ", level=0)  # Python ≥3.9
    except AttributeError:
        pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado con éxito.")

if __name__ == "__main__":
    main()
