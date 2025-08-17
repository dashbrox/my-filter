#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import copy
import gzip
import io
import re
import unicodedata
import urllib.request
import xml.etree.ElementTree as ET

SOURCE_URL = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
OUTPUT_FILE = "guide_custom.xml"

# Patrones para identificar los canales HBO (sin acentos, case-insensitive)
PATTERNS = [
    r"\bhbo\b.*mexico",
    r"\bhbo\s*2\b.*(latinoamerica|latin america)",
    r"\bhbo\s*signature\b.*(latinoamerica|latin america)",
    r"\bhbo\s*family\b.*(latinoamerica|latin america)",
    r"\bhbo\s*plus\b",
    r"\bhbo\s*mundi\b",
    r"\bhbo\s*pop\b",
]
COMPILED = [re.compile(pat, re.I) for pat in PATTERNS]


def normalize(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.lower()


def load_xmltv(url: str) -> bytes:
    data = urllib.request.urlopen(url).read()
    if url.endswith(".gz") or data[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            return gz.read()
    return data


def is_hbo_channel(names):
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

    # Filtrar canales HBO
    for ch in root.findall("channel"):
        disp_names = [dn.text or "" for dn in ch.findall("display-name")]
        if is_hbo_channel(disp_names):
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
            desc = pr.find("desc")

            # Construir texto extra (T/E)
            extra = ""
            if epnum is not None and epnum.text:
                parts = re.findall(r'\d+', epnum.text)
                if len(parts) >= 2:
                    season = int(parts[0]) + 1
                    episode = int(parts[1]) + 1
                    extra = f"<b>T{season} E{episode}</b>"

            # Obtener año si es película o documental
            year = ""
            if date_elem is not None and date_elem.text and len(date_elem.text) >= 4:
                year = date_elem.text[:4]

            # Construir título final
            title_elem = pr.find("title")
            title_text = title_elem.text if title_elem is not None and title_elem.text else "Sin título"

            if extra:  # Serie
                title_text += f" ({extra})"
                if subtitle is not None and subtitle.text:
                    title_text += f' "{subtitle.text}"'
            elif year:  # Película o documental
                title_text += f" ({year})"

            if title_elem is not None:
                title_elem.text = title_text
            else:
                t_elem = ET.Element("title")
                t_elem.text = title_text
                pr.insert(0, t_elem)

            # Crear sinopsis automática si no existe
            if desc is None:
                desc = ET.Element("desc")
                pr.append(desc)

            if not desc.text or desc.text.strip() == "":
                if extra:  # Serie
                    subtitle_text = f': "{subtitle.text}"' if subtitle is not None and subtitle.text else ""
                    desc.text = f"Episodio {extra} de {title_elem.text.split(' (')[0]}{subtitle_text}"
                elif year:  # Película/documental
                    desc.text = f"Película de {year}: {title_elem.text.split(' (')[0]}"
                else:
                    desc.text = title_elem.text

            selected_programmes.append(pr)

    # Crear XML de salida
    out_root = ET.Element("tv", root.attrib)
    for ch in selected_channels:
        out_root.append(copy.deepcopy(ch))
    for pr in selected_programmes:
        out_root.append(copy.deepcopy(pr))

    tree = ET.ElementTree(out_root)
    # Indentar para legibilidad (Python 3.9+)
    try:
        ET.indent(tree, space="  ")
    except AttributeError:
        pass

    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    print(f"✅ Generado {OUTPUT_FILE}")
    print(f"Canales HBO: {len(selected_channels)}")
    print(f"Programas copiados: {len(selected_programmes)}")


if __name__ == "__main__":
    main()
