# My-Filter – Guía EPG personalizada

Este repositorio genera `guide_custom.xml` con programación actualizada para tus canales favoritos.  
El script descarga la programación de **hoy, mañana y pasado mañana** de los canales de mi.tv o de tus fuentes EPG personalizadas y la convierte en formato EPG compatible con Plex, Jellyfin y otros reproductores.

## Canales incluidos por defecto

- HBO 2.mx  
- HBO FAMILY.mx  
- HBO.mx  
- MAX PRIME.mx  
- MAX UP.mx  
- MAX.mx  

> Estos canales se toman de `channels.txt` y puedes modificarlos si deseas agregar o quitar canales.

## Instalación de dependencias

Antes de ejecutar el scraper, instala las librerías necesarias:

```bash
pip install -r requirements.txt
