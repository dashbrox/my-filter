import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import io

# CONFIGURACIÓN
# Usamos el de USA porque Bravo.us está ahí.
# Nota: La URL termina en .xml, por lo que el archivo viene descomprimido.
URL_EPG_USA = "https://iptv-epg.org/files/epg-us.xml" 
CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"

def main():
    print("1. Descargando EPG de EE.UU...")
    # Descargamos el contenido
    response = requests.get(URL_EPG_USA)
    
    # VERIFICAMOS SI VIENE COMPRIMIDO O NO
    content = response.content
    if content[:2] == b'\x1f\x8b': # Magic numbers de GZIP
        print("   El archivo viene comprimido, descomprimiendo...")
        xml_content = gzip.decompress(content)
    else:
        print("   El archivo viene en texto plano, procesando directamente...")
        xml_content = content

    print("2. Analizando XML (esto puede tardar unos segundos)...")
    
    # Parseamos el XML
    root = ET.fromstring(xml_content)
    
    # Leemos tu lista de canales permitidos
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No encontré channels.txt")
        return

    with open(CHANNELS_FILE, 'r') as f:
        allowed_channels = set(line.strip() for line in f if line.strip())

    # Creamos la raíz del nuevo XML
    new_root = ET.Element("tv")
    
    channel_info = {}

    # PASO A: Copiar info de canales
    for channel in root.findall('channel'):
        ch_id = channel.get('id')
        if ch_id in allowed_channels:
            channel_info[ch_id] = channel

    # PASO B: Procesar programas y transformar
    count = 0
    for programme in root.findall('programme'):
        ch_id = programme.get('channel')
        
        if ch_id in allowed_channels:
            # --- TRANSFORMACIÓN ---
            
            desc_elem = programme.find('desc')
            if desc_elem is not None and desc_elem.text:
                desc_text = desc_elem.text
                
                # Buscar "S04 E06" al principio
                match = re.match(r'^(S\d+\s*E\d+)\s+(.*)', desc_text, re.DOTALL)
                
                if match:
                    season_ep = match.group(1) # "S04 E06"
                    real_desc = match.group(2) # Resto descripción
                    
                    # Actualizar Descripción
                    desc_elem.text = real_desc
                    
                    # Actualizar Título
                    title_elem = programme.find('title')
                    if title_elem is not None:
                        original_title = title_elem.text
                        
                        suffix = ""
                        if "ᴺᵉʷ" in original_title:
                            clean_title = original_title.replace("ᴺᵉʷ", "").strip()
                            suffix = " ᴺᵉʷ"
                        else:
                            clean_title = original_title.strip()

                        # Formato final: Título (S04 E06) ᴺᵉʷ
                        title_elem.text = f"{clean_title} ({season_ep}){suffix}"
            
            # Agregamos al nuevo árbol
            new_root.append(programme)
            count += 1

    # Agregamos info de canales
    for ch_id, ch_elem in channel_info.items():
        new_root.append(ch_elem)

    print(f"3. Se procesaron {count} programas.")

    # Guardar comprimido (.gz)
    tree = ET.ElementTree(new_root)
    xml_bytes = io.BytesIO()
    tree.write(xml_bytes, encoding='utf-8', xml_declaration=True)
    
    with gzip.open(OUTPUT_FILE, 'wb') as f:
        f.write(xml_bytes.getvalue())
        
    print(f"4. ¡Éxito! Archivo generado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
