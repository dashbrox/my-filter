#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip, re, urllib.request, xml.etree.ElementTree as ET, io, os, hashlib, copy

# -------------------- Configuración --------------------
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

def download_and_extract(url, timeout=20):
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

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            data = f.read()
    except Exception:
        pass

    with open(cache_file,"wb") as f:
        f.write(data)
    return data

def get_channel_base_and_suffix(name):
    name = normalize(name)
    parts = name.split()
    base = ' '.join([p for p in parts if not re.match(r'^[a-z]{2,3}$',p)])
    suffix_match = re.search(r'\.(mx|us|ca|es)$', name)
    return base.strip(), suffix_match.group(1) if suffix_match else ''

def channels_are_equivalent(ch1,ch2):
    return get_channel_base_and_suffix(ch1)[0]==get_channel_base_and_suffix(ch2)[0]

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
    for tag in ["episode-num","desc","date"]:
        p=primary.find(tag); s=secondary.find(tag)
        if (p is None or not (p.text or "").strip()) and s is not None:
            primary.append(copy.deepcopy(s))
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
            raw_data = download_and_extract(sec_url)
            if not raw_data:
                continue
            tree = ET.iterparse(io.BytesIO(raw_data), events=("end",))
            for event, prog in tree:
                if prog.tag == "programme":
                    key = (prog.attrib.get("channel"), prog.attrib.get("start"))
                    sec_dict[key] = copy.deepcopy(prog)
                    prog.clear()
        except Exception as e:
            print(f"⚠️ No se pudo descargar secundaria {sec_url}: {e}", flush=True)
    return sec_dict

def channel_matches(chan_name):
    n = normalize(chan_name)
    return any(re.search(p,n) for p in PATTERNS)

# -------------------- Main --------------------
def main():
    root = ET.Element("tv")
    seen_channels = set()
    seen_programmes = set()

    for url in URLS:
        try:
            raw_data = download_and_extract(url)
            if not raw_data:
                continue
            tree = ET.iterparse(io.BytesIO(raw_data), events=("end",))
        except Exception as e:
            print(f"⚠️ No se pudo procesar {url}: {e}", flush=True)
            continue

        principal_code = next((c for c in SECONDARY_URLS if c in url), None)
        sec_dict = load_secondary_programmes(principal_code) if principal_code else {}

        for event, elem in tree:
            if elem.tag == "channel":
                chan_id = elem.attrib.get("id","")
                name = elem.findtext("display-name","")
                if chan_id not in seen_channels and channel_matches(name):
                    root.append(copy.deepcopy(elem))  # ✅ Clonar antes de limpiar
                    seen_channels.add(chan_id)
                elem.clear()
            elif elem.tag == "programme":
                ch = elem.attrib.get("channel","")
                key = (ch, elem.attrib.get("start"))
                if key in seen_programmes:
                    elem.clear(); continue
                if key in sec_dict:
                    elem = merge_programmes(elem, sec_dict[key])
                elem = process_programme(elem)
                root.append(copy.deepcopy(elem))  # ✅ Clonar antes de limpiar
                seen_programmes.add(key)
                elem.clear()

    tree_out = ET.ElementTree(root)
    try: ET.indent(tree_out, space="  ", level=0)
    except: pass
    tree_out.write("guide_custom.xml", encoding="utf-8", xml_declaration=True)
    print(f"✅ guide_custom.xml generado con {len(seen_channels)} canales y {len(seen_programmes)} programas.", flush=True)

if __name__=="__main__":
    main()
