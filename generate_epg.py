#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import gzip
import requests
import xml.etree.ElementTree as ET
from io import BytesIO
from datetime import datetime
import json

# ----------------------------
# CONFIG
# ----------------------------
GUIDE_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CA1.xml.gz",
]

CHANNEL_FILTER = {
    "Canal.2.de.México.(Canal.Las.Estrellas.-.XEW).mx",
    "Canal.A&E.(México).mx",
    "Canal.AMC.(México).mx",
    "Canal.Animal.Planet.(México).mx",
    "Canal.Atreseries.(Internacional).mx",
    "Canal.AXN.(México).mx",
    "Canal.Azteca.Uno.mx",
    "Canal.Cinecanal.(México).mx",
    "Canal.Cinemax.(México).mx",
    "Canal.Discovery.Channel.(México).mx",
    "Canal.Discovery.Home.&.Health.(México).mx",
    "Canal.Discovery.World.Latinoamérica.mx",
    "Canal.Disney.Channel.(México).mx",
    "Canal.DW.(Latinoamérica).mx",
    "Canal.E!.Entertainment.Television.(México).mx",
    "Canal.Elgourmet.mx",
    "Canal.Europa.Europa.mx",
    "Canal.Film.&.Arts.mx",
    "Canal.FX.(México).mx",
    "Canal.HBO.2.Latinoamérica.mx",
    "Canal.HBO.Family.Latinoamérica.mx",
    "Canal.HBO.(México).mx",
    "Canal.HBO.Mundi.mx",
    "Canal.HBO.Plus.mx",
    "Canal.HBO.Pop.mx",
    "Canal.HBO.Signature.Latinoamérica.mx",
    "Canal.Investigation.Discovery.(México).mx",
    "Canal.Lifetime.(México).mx",
    "Canal.MTV.00s.mx",
    "Canal.MTV.Hits.mx",
    "Canal.National.Geographic.(México).mx",
    "Canal.Pánico.mx",
    "Canal.Paramount.Channel.(México).mx",
    "Canal.Space.(México).mx",
    "Canal.Sony.(México).mx",
    "Canal.Star.Channel.(México).mx",
    "Canal.Studio.Universal.(México).mx",
    "Canal.TNT.(México).mx",
    "Canal.TNT.Series.(México).mx",
    "Canal.Universal.TV.(México).mx",
    "Canal.USA.Network.(México).mx",
    "Canal.Warner.TV.(México).mx",
    "plex.tv.T2.plex",
    "TSN1.ca", "TSN2.ca", "TSN3.ca", "TSN4.ca",
    "Eurosport.2.es", "Eurosport.es",
    "M+.Deportes.2.es", "M+.Deportes.3.es", "M+.Deportes.4.es", "M+.Deportes.5.es", "M+.Deportes.6.es", "M+.Deportes.7.es", "M+.Deportes.es",
    "Movistar.Plus.es",
    "ABC.(WABC).New.York,.NY.us", "CBS.(WCBS).New.York,.NY.us", "FOX.(WNYW).New.York,.NY.us", "NBC.(WNBC).New.York,.NY.us",
    "ABC.(KABC).Los.Angeles,.CA.us", "NBC.(KNBC).Los.Angeles,.CA.us",
    "Bravo.USA.-.Eastern.Feed.us", "E!.Entertainment.USA.-.Eastern.Feed.us",
    "Hallmark.-.Eastern.Feed.us", "Hallmark.Mystery.Eastern.-.HD.us",
    "CW.(KFMB-TV2).San.Diego,.CA.us",
    "CNN.us", "The.Tennis.Channel.us",
    "HBO.-.Eastern.Feed.us", "HBO.Latino.(HBO.7).-.Eastern.us", "HBO.2.-.Eastern.Feed.us", "HBO.Comedy.HD.-.East.us",
    "HBO.Family.-.Eastern.Feed.us", "HBO.Signature.(HBO.3).-.Eastern.us", "HBO.Zone.HD.-.East.us",
    "Starz.Cinema.HD.-.Eastern.us", "Starz.Comedy.HD.-.Eastern.us", "Starz.-.Eastern.us", "Starz.Edge.-.Eastern.us",
    "Starz.Encore.Action.-.Eastern.us", "Starz.Encore.Black.-.Eastern.us", "Starz.Encore.Classic.-.Eastern.us", "Starz.Encore.-.Eastern.us",
    "Starz.Encore.Family.-.Eastern.us", "Starz.Encore.on.Demand.us", "Starz.Encore.-.Pacific.us", "Starz.Encore.Suspense.-.Eastern.us",
    "Starz.Encore.Westerns.-.Eastern.us", "Starz.In.Black.-.Eastern.us", "Starz.Kids.and.Family.-.Eastern.us", "Starz.On.Demand.us",
    "Starz.-.Pacific.us", "MoreMax..Eastern.us"
}

OMDB_KEY = os.getenv("OMDB_API_KEY")
TMDB_KEY = os.getenv("TMDB_API_KEY")

# ----------------------------
# HELPERS
# ----------------------------
def download_and_parse(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    buf = BytesIO(r.content)
    with gzip.open(buf, "rb") as f:
        xml_data = f.read()
    return ET.fromstring(xml_data)

def query_omdb(title, year=None):
    params = {"t": title, "apikey": OMDB_KEY}
    if year: params["y"] = year
    r = requests.get("http://www.omdbapi.com/", params=params)
    if r.status_code == 200:
        return r.json()
    return {}

def query_tmdb(title, year=None):
    params = {"api_key": TMDB_KEY, "query": title, "language": "es"}
    if year: params["year"] = year
    r = requests.get("https://api.themoviedb.org/3/search/movie", params=params)
    if r.status_code == 200:
        data = r.json()
        if data.get("results"):
            return data["results"][0]
    return {}

# ----------------------------
# MAIN
# ----------------------------
def main():
    root_out = ET.Element("tv")

    for url in GUIDE_URLS:
        print(f"Descargando {url} ...")
        xml_root = download_and_parse(url)

        for ch in xml_root.findall("channel"):
            ch_id = ch.attrib.get("id")
            if ch_id not in CHANNEL_FILTER:
                continue
            root_out.append(ch)

        for prog in xml_root.findall("programme"):
            ch_id = prog.attrib.get("channel")
            if ch_id not in CHANNEL_FILTER:
                continue

            title_el = prog.find("title")
            desc_el = prog.find("desc")

            title = title_el.text if title_el is not None else ""

            # Detectar si es serie o película (muy simple: temporada/episodio en title o desc)
            if re.search(r"S\d+E\d+", title, re.IGNORECASE):
                # Formato Serie
                if desc_el is None or not desc_el.text.strip():
                    # Buscar en TMDB
                    data = query_tmdb(title)
                    if data:
                        desc = data.get("overview")
                        if desc:
                            if desc_el is None:
                                desc_el = ET.SubElement(prog, "desc", attrib={"lang": "es"})
                            desc_el.text = desc
            else:
                # Película
                year_match = re.search(r"\((\d{4})\)", title)
                year = year_match.group(1) if year_match else None
                clean_title = re.sub(r"\(\d{4}\)", "", title).strip()

                data = query_omdb(clean_title, year)
                if not data or data.get("Response") == "False":
                    data = query_tmdb(clean_title, year)

                if data:
                    if "Year" in data:
                        title_el.text = f"{clean_title} ({data['Year']})"
                    if desc_el is None or not desc_el.text.strip() or re.match(r"^[A-Za-z0-9 ,.!?]+$", desc_el.text):
                        overview = data.get("Plot") or data.get("overview")
                        if overview:
                            if desc_el is None:
                                desc_el = ET.SubElement(prog, "desc", attrib={"lang": "es"})
                            desc_el.text = overview

            root_out.append(prog)

    tree = ET.ElementTree(root_out)
    out_file = "guide_custom.xml"
    tree.write(out_file, encoding="utf-8", xml_declaration=True)
    print(f"✅ Guía generada: {out_file}")


if __name__ == "__main__":
    main()
