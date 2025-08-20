#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re, urllib.request, xml.etree.ElementTree as ET, io, os, hashlib, copy

# -------------------- Configuración --------------------
URLS = [
    "https://raw.githubusercontent.com/K-vanc/Tempest-EPG-Generator/refs/heads/main/Siteconfigs/Multi%20Nation/%5BENC%5D%5BEX%5Dmi.tv_1.channel.xml",
    "https://raw.githubusercontent.com/K-vanc/Tempest-EPG-Generator/refs/heads/main/Siteconfigs/Multi%20Nation/%5BENC%5D%5BEX%5Dmi.tv_2.channel.xml",
]

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
    # Patrón específico para [MX] HBO HD
    r"\[MX\]\s*HBO\s*HD",
]

# -------------------- Funciones --------------------
def normalize(text):
    text = text.lower()
    text = re.sub(r'[áàä]','a', text)
    text = re.sub(r'[éèë]','e', text)
    text = re.sub(r'[íìï]','i', text)
    text = re.sub(r'[óòö]','o', text)
    text = re.sub(r'[úùü]','u', text)
    text = re.sub(r'[^a-z0-9 ]',' ', text)
    return text

def download(url, timeout=20):
    cache_file = f"/tmp/{hashlib.md5(url.encode()).hexdigest()}.xml"
    if os.path.exists(cache_file):
        return open(cache_file,"rb").read()
    print(f"📥 Descargando {url} ...", flush=True)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = resp.read()
    except Exception as e:
        print(f"❌ Error descargando {url}: {e}", flush=True)
        return b""
    with open(cache_file,"wb") as f:
        f.write(data)
    return data

def format_episode(epnum, system=""):
    if not epnum: return ""
    if system=="xmltv_ns":
        parts = epnum.split(".")
        if len(parts)>=2:
            try: return f"(S{int(parts[0])+1:02d} E{int(parts[1])+1:02d})"
            except: return ""
    m=re.search(r'[Ss]?(\d{1,2})[xEe-](\d{1,3})',epnum)
    return f"(S{int(m.group(1)):02d} E{int(m.group(2)):02d})" if m else ""

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

def channel_matches(chan_name):
    n = normalize(chan_name)
    return any(re.search(p,n) for p in PATTERNS)

# -------------------- Main --------------------
def main():
    root = ET.Element("tv")
    seen_channels = set()
    seen_programmes = set()

    for url in URLS:
        raw_data = download(url)
        if not raw_data:
            continue
        tree = ET.iterparse(io.BytesIO(raw_data), events=("end",))

        for event, elem in tree:
            if elem.tag == "channel":
                chan_id = elem.attrib.get("id","")
                name = elem.findtext("display-name","")
                if chan_id not in seen_channels and channel_matches(name):
                    root.append(copy.deepcopy(elem))
                    seen_channels.add(chan_id)
                elem.clear()
            elif elem.tag == "programme":
                ch = elem.attrib.get("channel","")
                key = (ch, elem.attrib.get("start"))
                if key in seen_programmes:
                    elem.clear()
                    continue
                elem = process_programme(elem)
                root.append(copy.deepcopy(elem))
                seen_programmes.add(key)
                elem.clear()

    tree_out = ET.ElementTree(root)
    try: ET.indent(tree_out, space="  ", level=0)
    except: pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print(f"✅ guide_custom.xml generado con {len(seen_channels)} canales y {len(seen_programmes)} programas.", flush=True)

if __name__=="__main__":
    main()
