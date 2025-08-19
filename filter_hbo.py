#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip, re, urllib.request, xml.etree.ElementTree as ET, io, os, hashlib

URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
]

SECONDARY_URLS = {
    "MX1": ["https://www.open-epg.com/files/mexico1.xml.gz","https://www.open-epg.com/files/mexico2.xml.gz"],
    "ES1": ["https://www.open-epg.com/files/spain1.xml.gz","https://www.open-epg.com/files/spain2.xml.gz","https://www.open-epg.com/files/spain3.xml.gz","https://www.open-epg.com/files/spain4.xml.gz","https://www.open-epg.com/files/spain5.xml.gz","https://www.open-epg.com/files/spain6.xml.gz"],
    "US1": ["https://www.open-epg.com/files/unitedstates1.xml.gz","https://www.open-epg.com/files/unitedstates2.xml.gz","https://www.open-epg.com/files/unitedstates3.xml.gz"],
    "CA1": ["https://www.open-epg.com/files/canada1.xml.gz","https://www.open-epg.com/files/canada2.xml.gz","https://www.open-epg.com/files/canada3.xml.gz","https://www.open-epg.com/files/canada4.xml.gz","https://www.open-epg.com/files/canada5.xml.gz"],
}

PATTERNS = [
    r"\bhbo\b.*mexico", r"\bhbo\s*2\b.*(latinoamerica|latin america)", r"\bhbo\s*signature\b.*(latinoamerica|latin america)",
    r"\bhbo\s*family\b.*(latinoamerica|latin america)", r"\bhbo\s*plus\b.*mx", r"\bhbo\s*mundi\b", r"\bhbo\s*pop\b",
    r"canal\.?\.?2.*(mexico|xew|las estrellas)", r"azteca.*(mexico|xhor)", r"\bcinemax\b.*mexico", r"\btsn[1-4]\b",
    r"abc.*(kabc|los angeles)", r"abc.*(wabc|new york)", r"nbc.*(wnbc|new york)", r"nbc.*(knbc|los angeles)",
    r"m\+\.?deportes(\.\d+)?\.es", r"plex\.tv\.t2\.plex", r"hallmark.*eastern", r"hallmark.*mystery.*eastern",
    r"hbo\s*2.*(eastern|pacific)", r"hbo.*comedy.*east", r"hbo.*eastern", r"hbo.*family.*(eastern|pacific)",
    r"hbo.*latino.*eastern", r"hbo.*pacific", r"hbo.*signature.*eastern", r"hbo.*zone.*east",
    r"cbs.*wcbs.*new york", r"bravo.*usa.*eastern", r"fox.*wnyw.*new york", r"tennis.*channel",
    r"e!.*entertainment.*eastern", r"\bcnn\b",
]

def normalize(text): return re.sub(r'[^a-z0-9 ]',' ', re.sub(r'[áàä]','a', re.sub(r'[éèë]','e', re.sub(r'[íìï]','i', re.sub(r'[óòö]','o', re.sub(r'[úùü]','u', text.lower()))))))

def download_and_extract(url):
    cache_file = f"/tmp/{hashlib.md5(url.encode()).hexdigest()}.xml"
    if os.path.exists(cache_file):
        return open(cache_file,"rb").read()
    print(f"📥 Descargando {url} ...")
    with urllib.request.urlopen(url) as resp:
        data = resp.read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f: data = f.read()
    except: pass
    with open(cache_file,"wb") as f: f.write(data)
    return data

def get_channel_base_and_suffix(name):
    name = normalize(name); parts = name.split()
    base = ' '.join([p for p in parts if not re.match(r'^[a-z]{2,3}$',p)])
    suffix_match = re.search(r'\.(mx|us|ca|es)$', name)
    return base.strip(), suffix_match.group(1) if suffix_match else ''

def channels_are_equivalent(ch1,ch2): return get_channel_base_and_suffix(ch1)[0]==get_channel_base_and_suffix(ch2)[0]

def format_episode(epnum, system=""):
    if not epnum: return ""
    if system=="xmltv_ns":
        parts = epnum.split(".")
        if len(parts)>=2:
            try: return f"(S{int(parts[0])+1:02d} E{int(parts[1])+1:02d})"
            except: return ""
    m=re.search(r'[Ss]?(\d{1,2})[xEe-](\d{1,3})',epnum)
    return f"(S{int(m.group(1)):02d} E{int(m.group(2)):02d})" if m else ""

def merge_programmes(primary,secondary):
    for tag in ["episode-num","desc"]:
        p=primary.find(tag); s=secondary.find(tag)
        if (p is None or not p.text) and s is not None: primary.append(s)
    return primary

def process_programme(prog):
    title_elem = prog.find("title")
    if title_elem is None: return prog
    title = title_elem.text or ""
    epnum_elem = prog.find("episode-num")
    ep_formatted = format_episode(epnum_elem.text or "", epnum_elem.attrib.get("system","")) if epnum_elem is not None else ""
    if ep_formatted:
        title_elem.text = f"{title} {ep_formatted}"
    else:
        categories = [c.text.lower() for c in prog.findall("category") if c.text]
        if any(k in categories for k in ["movie","film","pelicula","documentary","documental"]):
            year = (prog.findtext("date") or "")[:4]
            title_elem.text = f"{title} ({year})" if year else title
        else:
            title_elem.text = title
    return prog

def load_secondary_programmes(principal_code):
    sec_dict={}
    for sec_url in SECONDARY_URLS.get(principal_code,[]):
        try:
            tree = ET.ElementTree(ET.fromstring(download_and_extract(sec_url)))
            for prog in tree.findall("programme"):
                sec_dict[(prog.attrib.get("channel"), prog.attrib.get("start"))]=prog
        except Exception as e: print(f"⚠️ No se pudo descargar secundaria {sec_url}: {e}")
    return sec_dict

def channel_matches(chan_name):
    n = normalize(chan_name)
    return any(re.search(p,n) for p in PATTERNS)

def main():
    root = ET.Element("tv"); seen_channels=set(); seen_programmes=set()

    for url in URLS:
        try: tree = ET.ElementTree(ET.fromstring(download_and_extract(url)))
        except Exception as e: print(f"⚠️ No se pudo descargar {url}: {e}"); continue

        principal_code = next((c for c in SECONDARY_URLS if c in url), None)
        sec_dict = load_secondary_programmes(principal_code) if principal_code else {}

        # Filtrar canales de interés antes de procesar programas
        for channel in tree.findall("channel"):
            chan_id = channel.attrib.get("id","")
            name = channel.findtext("display-name","")
            if chan_id not in seen_channels and channel_matches(name):
                root.append(channel); seen_channels.add(chan_id)

        for programme in tree.findall("programme"):
            ch = programme.attrib.get("channel","")
            key = (ch, programme.attrib.get("start"))
            if key in seen_programmes: continue
            # Buscar en secundarias solo si es canal de interés
            if any(channels_are_equivalent(ch, sc[0]) and sc[1]==programme.attrib.get("start") for sc in sec_dict):
                sec_prog = next(sec_dict[sc] for sc in sec_dict if channels_are_equivalent(ch, sc[0]) and sc[1]==programme.attrib.get("start"))
                programme = merge_programmes(programme, sec_prog)
            programme = process_programme(programme)
            root.append(programme); seen_programmes.add(key)

    tree_out = ET.ElementTree(root)
    try: ET.indent(tree_out, space="  ", level=0)
    except: pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print(f"✅ guide_custom.xml generado con {len(seen_channels)} canales y {len(seen_programmes)} programas.")

if __name__=="__main__": main()
