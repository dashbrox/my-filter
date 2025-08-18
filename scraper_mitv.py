import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
from xml.etree.ElementTree import Element, SubElement, tostring
import xml.dom.minidom
import pytz
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Canales a scrapear
CANALS = {
    "HBO Oeste MX": "https://mi.tv/mx/canales/hbo-oeste",
    "HBO 2 MX": "https://mi.tv/mx/canales/hbo-2",
    "HBO Family MX": "https://mi.tv/mx/canales/hbo-family",
    "HBO Plus MX": "https://mi.tv/mx/canales/hbo-plus-mexico",
    "Max Prime AR": "https://mi.tv/ar/canales/max-prime",
    "HBO Signature CO": "https://mi.tv/co/canales/hbo-signature-hd",
    "Max MX": "https://mi.tv/mx/canales/max",
    "Max Up AR": "https://mi.tv/ar/canales/max-up"
}

# Convertir hora AM/PM de mi.tv a formato EPG
def parsear_hora(hora_str, tz, fecha):
    try:
        hora = datetime.strptime(hora_str, "%I:%M %p")
    except:
        return None
    dt = datetime.combine(fecha, hora.time())
    dt = tz.localize(dt)
    return dt

# Formatear título (series y películas)
def formatear_titulo(titulo, detalles):
    if "Temporada" in detalles and "Episodio" in detalles:
        # Serie
        temp = None
        ep = None
        epi_titulo = ""
        partes = detalles.split(",")
        for p in partes:
            if "Temporada" in p:
                temp = p.strip().replace("Temporada", "").strip()
            if "Episodio" in p:
                ep_partes = p.strip().replace("Episodio", "").split(":")
                ep = ep_partes[0].strip()
                if len(ep_partes) > 1:
                    epi_titulo = ep_partes[1].strip()
        if temp and ep:
            return f'{titulo} (S{temp} E{ep}) "{epi_titulo}"'
    elif detalles.isdigit() and len(detalles) == 4:
        # Película con año
        return f"{titulo} ({detalles})"
    return titulo

# Obtener la programación de 3 días
def obtener_eventos(nombre, url):
    print(f"Descargando {nombre} ...")

    if "/mx/" in url:
        tz = pytz.timezone("America/Mexico_City")
    elif "/ar/" in url:
        tz = pytz.timezone("America/Argentina/Buenos_Aires")
    elif "/co/" in url:
        tz = pytz.timezone("America/Bogota")
    else:
        tz = pytz.utc

    eventos = []
    hoy = date.today()

    for i in range(3):
        fecha = hoy + timedelta(days=i)
        url_dia = f"{url}?date={fecha.isoformat()}"
        print(f"  Día {fecha}: {url_dia}")

        try:
            resp = requests.get(url_dia, headers=HEADERS, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"❌ Error descargando {url_dia}: {e}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        bloques = soup.select(".program-list li")

        for idx, ev in enumerate(bloques):
            hora = ev.select_one(".time")
            titulo = ev.select_one(".title")
            detalle = ev.select_one(".subtitle")

            if hora and titulo:
                inicio_dt = parsear_hora(hora.get_text(strip=True), tz, fecha)
                if not inicio_dt:
                    continue

                inicio = inicio_dt.strftime("%Y%m%d%H%M%S %z")

                # Hora de fin
                if idx + 1 < len(bloques):
                    sig_hora = bloques[idx+1].select_one(".time")
                    if sig_hora:
                        fin_dt = parsear_hora(sig_hora.get_text(strip=True), tz, fecha)
                        if fin_dt and fin_dt < inicio_dt:
                            fin_dt += timedelta(days=1)
                    else:
                        fin_dt = inicio_dt + timedelta(hours=1)
                else:
                    fin_dt = inicio_dt + timedelta(hours=1)

                fin = fin_dt.strftime("%Y%m%d%H%M%S %z")

                titulo_final = titulo.get_text(strip=True)
                if detalle:
                    titulo_final = formatear_titulo(titulo_final, detalle.get_text(strip=True))

                eventos.append({
                    "start": inicio,
                    "stop": fin,
                    "title": titulo_final
                })

        time.sleep(2)  # Evitar saturar el sitio

    return eventos

# Generar XML EPG
def generar_xml(data):
    tv = Element("tv")
    for canal, eventos in data.items():
        canal_id = canal.replace(" ", "_")
        ch = SubElement(tv, "channel", id=canal_id)
        SubElement(ch, "display-name").text = canal
        for ev in eventos:
            prog = SubElement(tv, "programme", channel=canal_id, start=ev["start"], stop=ev["stop"])
            SubElement(prog, "title").text = ev["title"]
            SubElement(prog, "desc").text = ""  # Para compatibilidad Plex/Jellyfin
    return xml.dom.minidom.parseString(tostring(tv)).toprettyxml(indent="  ")

if __name__ == "__main__":
    data = {}
    for nombre, url in CANALS.items():
        try:
            eventos = obtener_eventos(nombre, url)
            data[nombre] = eventos
        except Exception as e:
            print(f"❌ Error con {nombre} ({url}): {e}")

    xml_str = generar_xml(data)
    with open("guide_custom.xml", "w", encoding="utf-8") as f:
        f.write(xml_str)

    print("✅ Guía generada en guide_custom.xml")
