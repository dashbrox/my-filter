#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import re
import urllib.request
import xml.etree.ElementTree as ET

# Fuentes de EPG que vamos a combinar
URLS = [
    # México: principal + secundarias
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",  # principal
    "https://www.open-epg.com/files/mexico1.xml.gz",               # secundaria
    "https://www.open-epg.com/files/mexico2.xml.gz",               # secundaria

    # Otras fuentes
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
]

# Lista de patrones para filtrar los canales
PATTERNS = [
    r"\bhbo\b.*mexico",
    r"\bhbo\s*2\b.*(latinoamerica|latin america)",
    r"\bhbo\s*signature\b.*(latinoamerica|latin america)",
    r"\bhbo\s*family\b.*(latinoamerica|latin america)",
    r"\bhbo\s*plus\b.*mx",
    r"\bhbo\s*mundi\b",
    r"\bhbo\s*pop\b",
    r"canal\.?\.?2.*(mexico|xew|las estrellas)",
    r"azteca.*(mexico|xhor)",
    r"\bcinemax\b.*mexico",
    r"\btsn[1-4]\b",
    r"abc.*(kabc|los angeles)",
    r"abc.*(wabc|new york)",
    r"nbc.*(wnbc|new york)",
    r"nbc.*(knbc|los angeles)",
    r"m\+\.?deportes(\.\d+)?\.es",
    r"plex\.tv\.t2\.plex",
    r"hallmark.*eastern",
    r"hallmark.*mystery.*eastern",
    r"hbo\s*2.*(eastern|pacific)",
    r"hbo.*comedy.*east",
    r"hbo.*eastern",
    r"hbo.*family.*(eastern|pacific)",
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

def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[áàä]', 'a', text)
    text = re.sub(r'[éèë]', 'e', text)
    text = re.sub(r'[íìï]', 'i', text)
    text = re.sub(r'[óòö]', 'o', text)
    text = re.sub(r'[úùü]', 'u', text)
    text = re.sub(r'[^a-z0-9 ]', ' ', text)
    return text

def download_and_extract(url: str) -> bytes:
    print(f"📥 Descargando {url} ...")
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    return gzip.decompress(data)

def channel_matches(name: str) -> bool:
    norm_name = normalize(name)
    return any(re.search(p, norm_name) for p in PATTERNS)

def format_episode(epnum: str, system: str = "") -> str:
    if not epnum:
        return ""

    if system == "xmltv_ns":
        parts = epnum.split(".")
        if len(parts) >= 2:
            try:
                season = int(parts[0]) + 1
                episode = int(parts[1]) + 1
                return f"(S{season:02d} E{episode:02d})"
            except ValueError:
                return ""

    match = re.search(r'[Ss]?(\d{1,2})[xEe-](\d{1,3})', epnum)
    if match:
        return f"(S{int(match.group(1)):02d} E{int(match.group(2)):02d})"

    match = re.search(r'[Ss](\d)(\d{2})$', epnum)
    if match:
        return f"(S{int(match.group(1)):02d} E{int(match.group(2)):02d})"

    return ""

def merge_programmes(primary, secondary):
    ep_primary = primary.find("episode-num")
    ep_secondary = secondary.find("episode-num")
    if (ep_primary is None or not (ep_primary.text or "").strip()) and ep_secondary is not None:
        primary.append(ep_secondary)

    desc_primary = primary.find("desc")
    desc_secondary = secondary.find("desc")
    if (desc_primary is None or not (desc_primary.text or "").strip()) and desc_secondary is not None:
        primary.append(desc_secondary)

    return primary

def process_programme(programme):
    title_elem = programme.find("title")
    if title_elem is None:
        return

    title = title_elem.text or ""

    epnum_elem = programme.find("episode-num")
    if epnum_elem is not None:
        epnum = epnum_elem.text or ""
        system = epnum_elem.attrib.get("system", "")
        ep_formatted = format_episode(epnum, system)
    else:
        ep_formatted = ""

    if ep_formatted:
        full_title = f"{title} {ep_formatted}".strip()
    else:
        categories = [c.text.lower() for c in programme.findall("category") if c.text]
        is_movie_or_doc = any(
            kw in categories for kw in ["movie", "film", "pelicula", "documentary", "documental"]
        )
        if is_movie_or_doc:
            date_text = programme.findtext("date")
            year = date_text[:4] if date_text and len(date_text) >= 4 else ""
            full_title = f"{title} ({year})" if year else title.strip()
        else:
            full_title = title.strip()

    title_elem.text = full_title

def main():
    root = ET.Element("tv")
    seen_channels = set()
    programmes_by_key = {}

    for url in URLS:
        try:
            xml_data = download_and_extract(url)
        except Exception as e:
            print(f"⚠️ No se pudo descargar {url}: {e}")
            continue

        tree = ET.ElementTree(ET.fromstring(xml_data))

        for channel in tree.findall("channel"):
            chan_id = channel.attrib.get("id", "")
            name = channel.findtext("display-name", default="")
            if channel_matches(chan_id) or channel_matches(name):
                if chan_id not in seen_channels:
                    root.append(channel)
                    seen_channels.add(chan_id)

        for programme in tree.findall("programme"):
            ch = programme.attrib.get("channel", "")
            key = (ch, programme.attrib.get("start"))
            if not channel_matches(ch):
                continue

            # MX1 + secundarias
            if key in programmes_by_key and url != URLS[0]:
                programmes_by_key[key] = merge_programmes(programmes_by_key[key], programme)
                continue

            if url == URLS[0]:
                process_programme(programme)
                programmes_by_key[key] = programme

            # En el futuro podrías agregar la misma lógica para US1, CA1, ES1, PLEX1

    for prog in programmes_by_key.values():
        root.append(prog)

    tree_out = ET.ElementTree(root)
    try:
        ET.indent(tree_out, space="  ", level=0)
    except AttributeError:
        pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print(f"✅ guide_custom.xml generado con {len(seen_channels)} canales y {len(programmes_by_key)} programas.")

if __name__ == "__main__":
    main()
