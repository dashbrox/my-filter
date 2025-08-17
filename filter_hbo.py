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

SOURCE_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
OUTPUT_FILE = "guide_custom.xml"

# Patrones para identificar los canales (sin acentos, case-insensitive)
PATTERNS = [
    # HBO
    r"\bhbo\b.*mexico",
    r"\bhbo\s*2\b.*(latinoamerica|latin america)",
    r"\bhbo\s*signature\b.*(latinoamerica|latin america)",
    r"\bhbo\s*family\b.*(latinoamerica|latin america)",
    r"\bhbo\s*plus\b.*(latinoamerica|latin america)",
    r"\bhbo\s*mundi\b",
    r"\bhbo\s*pop\b",

    # Nuevos canales
    r"canal\.?\.?2.*(mexico|xew|las estrellas)",  # Canal 2 / Las Estrellas (XEW)
    r"azteca.*(mexico|xhor)",                     # Azteca (XHOR)
    r"\bcinemax\b.*mexico",                       # Cinemax México
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
        sys.exit(1)

    if url.endswith(".gz") or data[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            return gz.read()
    return data


def is_selected_channel(names):
    for name in names:
        n = normalize(name)
        if any(p.search(n) for p in COMPILED):
            return True
    return False


def main():
    xml_bytes = load_xmltv(SOURCE_URL)
    root = ET.fromstring(xml_bytes)

    selected_channels = []
    selected_ids = set()

    for ch in root.findall("channel"):
        disp_names = [dn.text or "" for dn in ch.findall("display-name")]
        if is_selected_channel(disp_names):
            selected_channels.append(ch)
            cid = ch.attrib.get("id")
            if cid:
                selected_ids.add(cid)

    selected_programmes = []
    for pr in root.findall("programme"):
        if pr.attrib.get("channel") in selected_ids:
            subtitle = pr.find("sub-title")
            epnum = pr.find("episode-num")
            date_elem = pr.find("date")

            # Construir texto extra (T/E) sin HTML
            extra = ""
            if epnum is not None and epnum.text:
                match = re.match(r"(\d+)\.(\d+)", epnum.text)
                if match:
                    season = int(match.group(1)) + 1
                    episode = int(match.group(2)) + 1
                    extra = f"T{season} E{episode}"

            # Obtener año si es película o documental
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
                elif year:  # Película o documental
                    final_title += f" ({year})"
                title.text = final_title

            selected_programmes.append(pr)

    # Crear XML de salida
    out_root = ET.Element("tv", root.attrib)
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
