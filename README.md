# Guía EPG Personalizada

Esta guía genera automáticamente una **EPG de 72 horas para México**, usando la guía base de [EPGShare01](https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz) y **enriquecida con sinopsis, año, categoría y temporada/episodio** gracias a **TMDB**.

La idea es mantener la información de la guía original y solo completar los campos que falten.

---

## 📡 URL de la guía final

La guía final se publica vía GitHub Pages:
https://dashbrox.github.io/my-filter/guide_custom.xml


---

## ⚙️ Configuración del proyecto

1. Guardar tu **API Key v3 TMDB** como secreto en GitHub:
   - Nombre del secreto: `TMDB_API_KEY`  
   - Valor: tu API Key v3

2. Guardar tu **Personal Access Token (PAT)** de GitHub como secreto:
   - Nombre del secreto: `MY_PAT`  
   - Valor: tu token con permisos de `repo` para hacer push automático

3. Activar **GitHub Pages**:
   - Branch: `main`  
   - Folder: `/ (root)`  
   - Esto permite que `guide_custom.xml` se sirva públicamente en la URL de arriba.

---

## 📂 Archivos del proyecto

- `generate_epg.py` → Script Python que genera `guide_custom.xml`.  
- `requirements.txt` → Dependencias Python (`requests`, `lxml`).  
- `.github/workflows/update_epg.yml` → Workflow de GitHub Actions que corre cada 12 horas y hace push automático.  

---

## 🔄 Funcionamiento del workflow

- Se ejecuta automáticamente cada 12 horas.  
- Descarga la guía base de EPGShare01.  
- Recorre cada programa y completa información faltante usando TMDB:  
  - Sinopsis (`desc`)  
  - Año (`date`)  
  - Categoría (`category`)  
  - Subtítulo con temporada/episodio (`sub-title`) si falta  
- Hace commit y push **solo si hay cambios**.  

---

## 📝 Nota

- Este proyecto es **personal y gratuito**, usando la API TMDB solo para enriquecer la guía.  
- No modifica programas que ya tengan información completa.  
- Puedes usar `guide_custom.xml` en tu reproductor IPTV (TiviMate, IPTV Smarters, etc.).
