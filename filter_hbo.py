#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import re
import urllib.request
import xml.etree.ElementTree as ET

# Fuentes de EPG que vamos a combinar
URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
]

# Lista de patrones para filtrar los canales
PATTERNS = [
    # HBO México / Latinoamérica
    r"\bhbo\b.*mexico",
    r"\bhbo\s*2\b.*(latinoamerica|latin america)",
    r"\bhbo\s*signature\b.*(latinoamerica|latin america)",
    r"\bhbo\s*family\b.*(latinoamerica|latin america)",
    r"\bhbo\s*plus\b.*mx",
    r"\bhbo\s*mundi\b",
    r"\bhbo\s*pop\b",

    # Canales México
    r"canal\.?\.?2.*(mexico|xew|las estrellas)",
    r"azteca.*(mexico|xhor)",
    r"\bcinemax\b.*mexico",

    # Canales Canadá
    r"\btsn1\b",
    r"\btsn2\b",
    r"\btsn3\b",
    r"\btsn4\b",

    # Canales USA principales
    r"abc.*(kabc|los angeles)",
    r"abc.*(wabc|new york)",
    r"nbc.*(wnbc|new york)",
    r"nbc.*(knbc|los angeles)",

    # Canales España (Movistar+ Deportes)
    r"m\+\.?deportes(\.\d+)?\.es",

    # Plex
    r"plex\.tv\.t2\.plex",

    # --- NUEVOS US Feeds / Networks ---
    r"hallmark.*eastern",
    r"hallmark.*mystery.*eastern",
    r"hbo\s*2.*eastern",
    r"hbo\s*2.*pacific",
    r"hbo.*comedy.*east",
    r"hbo.*eastern",
    r"hbo.*family.*eastern",
    r"hbo.*family.*pacific",
    r"hbo.*latino.*eastern",
    r"hbo.*pacific",
    r"hbo.*signature.*eastern",
    r"hbo.*zone.*east",
    r"cbs.*wcbs.*new york",
    r"bravo.*usa.*eastern",
    r"fox.*wnyw.*new york",
    r"tennis.*channel",
    r"e!.*entertainment.*eastern",
    r"\bcnn\b",
]

# Normalizar texto (quitar mayúsculas, acentos, caracteres raros)
def normalize(text):
    return (
        text.lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("-", " ")
        .replace("_", " ")
    )

def download_and_extract(url):
    print(f"Descargando {url} ...")
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    return gzip.decompress(data)

def channel_matches(name):
    norm_name = normalize(name)
    return any(re.search(pattern, norm_name) for pattern in PATTERNS)

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
            if channel_matches(ch):
                root.append(programme)

    # Guardar resultado bonito
    tree_out = ET.ElementTree(root)
    try:
        ET.indent(tree_out, space="  ", level=0)  # Python ≥3.9
    except AttributeError:
        pass  # Ignorar si no está disponible
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print("✅ guide_custom.xml generado con éxito.")

if __name__ == "__main__":
    main()
