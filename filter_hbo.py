#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import re
import urllib.request
import xml.etree.ElementTree as ET
import io

URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
]

SECONDARY_URLS = {
    "MX1": [
        "https://www.open-epg.com/files/mexico1.xml.gz",
        "https://www.open-epg.com/files/mexico2.xml.gz",
    ],
    "ES1": [
        "https://www.open-epg.com/files/spain1.xml.gz",
        "https://www.open-epg.com/files/spain2.xml.gz",
        "https://www.open-epg.com/files/spain3.xml.gz",
        "https://www.open-epg.com/files/spain4.xml.gz",
        "https://www.open-epg.com/files/spain5.xml.gz",
        "https://www.open-epg.com/files/spain6.xml.gz",
    ],
    "US1": [
        "https://www.open-epg.com/files/unitedstates1.xml.gz",
        "https://www.open-epg.com/files/unitedstates2.xml.gz",
        "https://www.open-epg.com/files/unitedstates3.xml.gz",
    ],
    "CA1": [
        "https://www.open-epg.com/files/canada1.xml.gz",
        "https://www.open-epg.com/files/canada2.xml.gz",
        "https://www.open-epg.com/files/canada3.xml.gz",
        "https://www.open-epg.com/files/canada4.xml.gz",
        "https://www.open-epg.com/files/canada5.xml.gz",
    ],
}

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
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            return f.read()
    except OSError:
        return data

def get_channel_base_and_suffix(name: str):
    """Extrae la parte base y la terminación geográfica del canal"""
    name = normalize(name)
    parts = name.split()
    base = ' '.join([p for p in parts if not re.match(r'^[a-z]{2,3}$', p)])  # ignora sufijos
    suffix_match = re.search(r'\.(mx|us|ca|es)$', name)
    suffix = suffix_match.group(1) if suffix_match else ''
    return base.strip(), suffix

def channels_are_equivalent(ch1, ch2):
    base1, suf1 = get_channel_base_and_suffix(ch1)
    base2, suf2 = get_channel_base_and_suffix(ch2)
    return base1 == base2 and suf1 == suf2

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
    return ""

def merge_programmes(primary, secondary):
    ep_primary = primary.find("episode-num")
    ep_secondary = secondary.find("episode-num")
    if (ep_primary is None or not ep_primary.text) and ep_secondary is not None:
        primary.append(ep_secondary)
    desc_primary = primary.find("desc")
    desc_secondary = secondary.find("desc")
    if (desc_primary is None or not desc_primary.text) and desc_secondary is not None:
        primary.append(desc_secondary)
    return primary

def process_programme(programme):
    title_elem = programme.find("title")
    if title_elem is None:
        return programme
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
        is_movie_or_doc = any(kw in categories for kw in ["movie","film","pelicula","documentary","documental"])
        if is_movie_or_doc:
            date_text = programme.findtext("date")
            year = date_text[:4] if date_text and len(date_text)>=4 else ""
            full_title = f"{title} ({year})" if year else title.strip()
        else:
            full_title = title.strip()
    title_elem.text = full_title
    return programme

def load_secondary_programmes(principal_code):
    sec_dict = {}
    for sec_url in SECONDARY_URLS.get(principal_code, []):
        try:
            sec_data = download_and_extract(sec_url)
            tree = ET.ElementTree(ET.fromstring(sec_data))
            for prog in tree.findall("programme"):
                key = (prog.attrib.get("channel"), prog.attrib.get("start"))
                sec_dict[key] = prog
        except Exception as e:
            print(f"⚠️ No se pudo descargar secundaria {sec_url}: {e}")
    return sec_dict

def main():
    root = ET.Element("tv")
    seen_channels = set()
    seen_programmes = set()

    for url in URLS:
        try:
            xml_data = download_and_extract(url)
        except Exception as e:
            print(f"⚠️ No se pudo descargar {url}: {e}")
            continue

        tree = ET.ElementTree(ET.fromstring(xml_data))

        principal_code = None
        for code in SECONDARY_URLS:
            if code in url:
                principal_code = code
                break
        sec_dict = load_secondary_programmes(principal_code) if principal_code else {}

        for channel in tree.findall("channel"):
            chan_id = channel.attrib.get("id","")
            name = channel.findtext("display-name","")
            if chan_id not in seen_channels:
                root.append(channel)
                seen_channels.add(chan_id)

        for programme in tree.findall("programme"):
            ch = programme.attrib.get("channel","")
            key = (ch, programme.attrib.get("start"))
            if key not in seen_programmes:
                # Buscar coincidencia en secundarias usando canal equivalente
                for sec_key, sec_prog in sec_dict.items():
                    if channels_are_equivalent(ch, sec_key[0]) and sec_key[1] == programme.attrib.get("start"):
                        programme = merge_programmes(programme, sec_prog)
                        break
                programme = process_programme(programme)
                root.append(programme)
                seen_programmes.add(key)

    tree_out = ET.ElementTree(root)
    try:
        ET.indent(tree_out, space="  ", level=0)
    except AttributeError:
        pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print(f"✅ guide_custom.xml generado con {len(seen_channels)} canales y {len(seen_programmes)} programas.")

if __name__ == "__main__":
    main()
