#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import copy
import gzip
import io
import re
import unicodedata
import urllib.request
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import sys

# Fuentes de EPG a usar
SOURCE_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US_SPORTS1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
]

OUTPUT_FILE = "guide_custom.xml"

# Patrones para identificar los canales (sin acentos, case-insensitive)
PATTERNS = [
    # HBO (LatAm / MX)
    r"\bhbo\b.*mexico",
    r"\bhbo\s*2\b.*(latinoamerica|latin america)",
    r"\bhbo\s*signature\b.*(latinoamerica|latin america)",
    r"\bhbo\s*family\b.*(latinoamerica|latin america)",
    r"\bhbo\s*plus\b.*(latinoamerica|latin america)",
    r"\bhbo\s*mundi\b",
    r"\bhbo\s*pop\b",

    # México
    r"canal\.?\.?2.*(mexico|xew|las estrellas)",  # Canal 2 / Las Estrellas (XEW)
    r"azteca.*(mexico|xhor)",                     # Azteca (XHOR)
    r"\bcinemax\b.*mexico",                       # Cinemax México

    # Canadá – TSN
    r"\btsn\s*1\b",
    r"\btsn\s*2\b",
    r"\btsn\s*3\b",
    r"\btsn\s*4\b",

    # USA – ABC
    r"abc.*kabc.*los angeles",
    r"abc.*wabc.*new york",

    # USA – NBC
    r"nbc.*wnbc.*new york",
    r"nbc.*knbc.*los angeles",

    # España – M+ Deportes
    r"m\+\s*deportes\s*2.*es",
    r"m\+\s*deportes\s*3.*es",
    r"m\+\s*deportes\s*4.*es",
    r"m\+\s*deportes\s*5.*es",
    r"m\+\s*deportes\s*6.*es",
    r"m\+\s*deportes\s*7.*es",
    r"m\+\s*deportes(\s|\.|$).*es",  # M+ Deportes (canal base)

    # Plex
    r"plex\.tv.*t2\.plex",
]
COMPILED = [re.compile(pat, re.I) for pat in PATTERNS]


def normalize(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.lower()


def load_xmltv(url: str) -> bytes:
    try:
        data = urllib.request.urlopen(url).read()
    except Exception as e:
        print(f"❌ Error al descargar {url}: {e}")
        return b""

    if url.endswith(".gz") or data[:2] == b"\x1f\x8b":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
                return gz.read()
        except Exception as e:
            print(f"❌ Error al descomprimir {url}: {e}")
            return b""
    return data


def is_selected_channel(names):
    for name in names:
        n = normalize(name)
        if any(p.search(n) for p in COMPILED):
            return True
    return False


def main():
    selected_channels = []
    selected_ids = set()
    selected_programmes = []

    for url in SOURCE_URLS:
        print(f"📥 Procesando {url}")
        xml_bytes = load_xmltv(url)
        if not xml_bytes:
            continue

        try:
            root = ET.fromstring(xml_bytes)
        except Exception as e:
            print(f"❌ Error al parsear {url}: {e}")
            continue

        # Filtrar canales
        for ch in root.findall("channel"):
            disp_names = [dn.text or "" for dn in ch.findall("display-name")]
            if is_selected_channel(disp_names):
                selected_channels.append(ch)
                cid = ch.attrib.get("id")
                if cid:
                    selected_ids.add(cid)

        # Filtrar programas de esos canales
        for pr in root.findall("programme"):
            if pr.attrib.get("channel") in selected_ids:
                subtitle = pr.find("sub-title")
                epnum = pr.find("episode-num")
                date_elem = pr.find("date")

                # Construir texto extra (T/E)
                extra = ""
                if epnum is not None and epnum.text:
                    match = re.match(r"(\d+)\.(\d+)", epnum.text)
                    if match:
                        season = int(match.group(1)) + 1
                        episode = int(match.group(2)) + 1
                        extra = f"T{season} E{episode}"

                # Año si es película / docu
                year = ""
                if date_elem is not None and date_elem.text and len(date_elem.text) >= 4:
                    year = date_elem.text[:4]

                # Construir título final
                title = pr.find("title")
                if title is not None:
                    final_title = title.text or ""
                    if extra:  # Serie
                        final_title += f" ({extra})"
                        if subtitle is not None and subtitle.text:
                            final_title += f' "{subtitle.text}"'
                    elif year:  # Película / docu
                        final_title += f" ({year})"
                    title.text = final_title

                selected_programmes.append(pr)

    # Crear XML de salida
    out_root = ET.Element("tv")
    for ch in selected_channels:
        out_root.append(copy.deepcopy(ch))
    for pr in selected_programmes:
        out_root.append(copy.deepcopy(pr))

    # Pretty print
    xml_str = ET.tostring(out_root, encoding="utf-8")
    pretty = minidom.parseString(xml_str).toprettyxml(indent="  ")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(pretty)

    print(f"✅ Generado {OUTPUT_FILE}")
    print(f"Canales seleccionados: {len(selected_channels)}")
    print(f"Programas copiados: {len(selected_programmes)}")


if __name__ == "__main__":
    main()
