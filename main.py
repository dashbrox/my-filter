import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import io

# CONFIGURACIÓN
URL_EPG_USA = "https://iptv-epg.org/files/epg-us.xml"
CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"

def main():
    # 1. Descargar archivo al disco (seguro para memoria)
    print("1. Descargando EPG de EE.UU. al disco...")
    try:
        with requests.get(URL_EPG_USA, stream=True) as r:
            r.raise_for_status()
            with open(TEMP_INPUT, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("   Descarga completa.")
    except Exception as e:
        print(f"Error descargando: {e}")
        return

    # 2. Leer canales permitidos
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No existe channels.txt")
        return
    
    with open(CHANNELS_FILE, 'r') as f:
        allowed_channels = set(line.strip() for line in f if line.strip())

    # 3. Procesar XML
    print("2. Procesando XML...")
    
    with open("output_temp.xml", 'wb') as out_f:
        out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')
        
        context = ET.iterparse(TEMP_INPUT, events=('start', 'end'))
        
        for event, elem in context:
            if event == 'end':
                if elem.tag == 'channel':
                    ch_id = elem.get('id')
                    if ch_id in allowed_channels:
                        out_f.write(ET.tostring(elem, encoding='utf-8'))
                    elem.clear()

                elif elem.tag == 'programme':
                    ch_id = elem.get('channel')
                    if ch_id in allowed_channels:
                        
                        title_elem = elem.find('title')
                        desc_elem = elem.find('desc')
                        
                        # --- LÓGICA DE TRANSFORMACIÓN MEJORADA ---
                        
                        # Variables por defecto
                        season_ep = None
                        new_desc_text = None
                        
                        # 1. PROCESAR DESCRIPCIÓN PRIMERO (Para extraer S04E06 y Título Episodio)
                        if desc_elem is not None and desc_elem.text:
                            desc_text = desc_elem.text
                            
                            # Regex para encontrar "S04 E06" o "S04E06" al inicio
                            match = re.match(r'^(S\d+\s*E\d+)\s+(.*)', desc_text, re.IGNORECASE | re.DOTALL)
                            
                            if match:
                                season_ep = match.group(1) # Ej: "S04 E06"
                                content = match.group(2)   # Ej: "Gossip Boy\nAs Alesia..."
                                
                                # Separar el título del episodio del resto
                                # Asumimos que el título está en la primera línea
                                if '\n' in content:
                                    parts = content.split('\n', 1)
                                    ep_title = parts[0].strip()
                                    rest_desc = parts[1].strip()
                                    # Formato: <b>Titulo Episodio</b> + salto de linea + resto
                                    new_desc_text = f"<b>{ep_title}</b>\n{rest_desc}"
                                else:
                                    # Si no hay salto de linea, ponemos todo en negrita
                                    new_desc_text = f"<b>{content.strip()}</b>"
                                
                                # Actualizar la descripción en el elemento XML
                                desc_elem.text = new_desc_text
                            else:
                                # Si no coincide el patrón, mantener descripción original
                                pass

                        # 2. PROCESAR TÍTULO
                        if title_elem is not None and title_elem.text:
                            original_title = title_elem.text
                            
                            # Detectar si es NEW (caracteres raros o texto normal)
                            has_new = False
                            clean_title = original_title
                            
                            # Buscar caracteres unicode ᴺᵉʷ
                            if "ᴺᵉʷ" in clean_title:
                                has_new = True
                                clean_title = clean_title.replace("ᴺᵉʷ", "").strip()
                            # Buscar texto NEW (case insensitive)
                            elif re.search(r'\bNEW\b', clean_title, re.IGNORECASE):
                                has_new = True
                                clean_title = re.sub(r'\s*NEW\s*', '', clean_title, flags=re.IGNORECASE).strip()
                            
                            # Limpiar espacios dobles
                            clean_title = " ".join(clean_title.split())
                            
                            # Construir Título Final
                            final_title = clean_title
                            
                            # Agregar Temporada/Episodio si lo encontramos en la descripción
                            if season_ep:
                                final_title += f" ({season_ep})"
                                
                            # Agregar NEW formateado
                            if has_new:
                                final_title += " <b>NEW</b>"
                            
                            title_elem.text = final_title

                        # --- FIN TRANSFORMACIÓN ---
                        
                        out_f.write(ET.tostring(elem, encoding='utf-8'))
                    
                    elem.clear()

        out_f.write(b'</tv>')

    # 4. Comprimir
    print("3. Comprimiendo resultado...")
    with open("output_temp.xml", 'rb') as f_in:
        with gzip.open(OUTPUT_FILE, 'wb') as f_out:
            f_out.writelines(f_in)

    # 5. Limpiar
    if os.path.exists(TEMP_INPUT):
        os.remove(TEMP_INPUT)
    if os.path.exists("output_temp.xml"):
        os.remove("output_temp.xml")

    print(f"¡Listo! Archivo generado: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
