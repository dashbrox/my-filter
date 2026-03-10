import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os

# CONFIGURACIÓN
URL_EPG_USA = "https://iptv-epg.org/files/epg-us.xml" # Usamos el de USA porque Bravo.us está aquí
CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"

def main():
    print("1. Descargando EPG de EE.UU. (puede tardar unos segundos)...")
    response = requests.get(URL_EPG_USA, stream=True)
    
    # Descomprimimos en memoria para leerlo
    print("2. Procesando XML...")
    xml_content = gzip.decompress(response.content)
    
    # Parseamos el XML
    root = ET.fromstring(xml_content)
    
    # Leemos tu lista de canales permitidos
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No encontré channels.txt")
        return

    with open(CHANNELS_FILE, 'r') as f:
        # Creamos un conjunto (set) para búsqueda rápida: {'Bravo.us'}
        allowed_channels = set(line.strip() for line in f if line.strip())

    # Creamos la raíz del nuevo XML
    new_root = ET.Element("tv")
    
    # Variables para copiar info del canal
    channel_info = {}

    # PASO A: Encontrar definición del canal (<channel>)
    for channel in root.findall('channel'):
        ch_id = channel.get('id')
        if ch_id in allowed_channels:
            # Guardamos el elemento canal para copiarlo luego
            channel_info[ch_id] = channel

    # PASO B: Procesar programas (<programme>) y transformar
    count = 0
    for programme in root.findall('programme'):
        ch_id = programme.get('channel')
        
        # Si el programa es de un canal que queremos...
        if ch_id in allowed_channels:
            # --- INICIO TRANSFORMACIÓN PEDIDA ---
            
            # 1. Obtener Descripción
            desc_elem = programme.find('desc')
            if desc_elem is not None and desc_elem.text:
                desc_text = desc_elem.text
                
                # Buscar patrón "S04 E06" o similar al inicio
                # Regex: Busca "S" + números + espacio + "E" + números
                match = re.match(r'^(S\d+\s*E\d+)\s+(.*)', desc_text, re.DOTALL)
                
                if match:
                    season_ep = match.group(1) # "S04 E06"
                    real_desc = match.group(2) # El resto de la descripción
                    
                    # 2. Actualizar Descripción (limpiar S04 E06)
                    desc_elem.text = real_desc
                    
                    # 3. Actualizar Título
                    title_elem = programme.find('title')
                    if title_elem is not None:
                        original_title = title_elem.text
                        
                        # Lógica: Título Original + (S04 E06) + ᴺᵉʷ si existía
                        # Buscamos si el título ya tenía el símbolo raro para no borrarlo
                        
                        # El usuario quiere el ᴺᵉʷ al final.
                        # Extraemos el símbolo si existe en el título original
                        suffix = ""
                        if "ᴺᵉʷ" in original_title:
                            # Lo quitamos del título base para ponerlo al final ordenado
                            clean_title = original_title.replace("ᴺᵉʷ", "").strip()
                            suffix = " ᴺᵉʷ"
                        else:
                            clean_title = original_title.strip()

                        # Montamos el nuevo título: "Nombre (S04 E06) ᴺᵉʷ"
                        # Nota: No podemos ponerlo en rojo, pero mantenemos el carácter.
                        title_elem.text = f"{clean_title} ({season_ep}){suffix}"
            
            # --- FIN TRANSFORMACIÓN ---
            
            # Agregamos el programa modificado al nuevo árbol
            new_root.append(programme)
            count += 1

    # Agregamos la info del canal al final
    for ch_id, ch_elem in channel_info.items():
        new_root.append(ch_elem)

    print(f"3. Se procesaron {count} programas para Bravo.us")

    # Guardar comprimido
    tree = ET.ElementTree(new_root)
    
    # Generamos XML en memoria
    import io
    xml_bytes = io.BytesIO()
    tree.write(xml_bytes, encoding='utf-8', xml_declaration=True)
    
    with gzip.open(OUTPUT_FILE, 'wb') as f:
        f.write(xml_bytes.getvalue())
        
    print(f"4. ¡Listo! Archivo generado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
