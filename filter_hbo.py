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
    """Normaliza el texto para filtrar más fácilmente"""
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

def format_episode(epnum: str) -> str:
    """Convierte E122, 1x22, etc. -> (T1 E22)"""
    match = re.match(r'E?(\d{1,2})[x-]?(\d{1,2})', epnum)
    if match:
        season = int(match.group(1))
        episode = int(match.group(2))
        return f"(T{season} E{episode})"
    return ""

def main():
    root = ET.Element("tv")
    seen_channels = set()
    seen_programmes = set()

    for url in URLS:
        xml_data = download_and_extract(url)
        tree = ET.ElementTree(ET.fromstring(xml_data))

        # Filtrar canales
        for channel in tree.findall("channel"):
            chan_id = channel.attrib.get("id", "")
            name = channel.findtext("display-name", default="")
            if channel_matches(chan_id) or channel_matches(name):
                if chan_id not in seen_channels:
                    root.append(channel)
                    seen_channels.add(chan_id)

        # Filtrar y mejorar programas
        for programme in tree.findall("programme"):
            ch = programme.attrib.get("channel", "")
            key = (ch, programme.attrib.get("start"))
            if channel_matches(ch) and key not in seen_programmes:
                title = programme.findtext("title", default="")
                epnum = programme.findtext("episode-num", default="")

                # Si tiene episodio -> serie, si no -> película
                ep_formatted = format_episode(epnum)
                if ep_formatted:
                    full_title = f"{title} {ep_formatted}".strip()
                else:
                    full_title = title.strip()

                programme.find("title").text = full_title
                root.append(programme)
                seen_programmes.add(key)

    tree_out = ET.ElementTree(root)
    try:
        ET.indent(tree_out, space="  ", level=0)  # Python ≥3.9
    except AttributeError:
        pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print(f"✅ guide_custom.xml generado con {len(seen_channels)} canales y {len(seen_programmes)} programas.")

if __name__ == "__main__":
    main()
