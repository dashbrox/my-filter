for programme in root.findall("programme"):
    title_elem = programme.find("title")
    if title_elem is None or not title_elem.text:
        continue

    title = title_elem.text

    # Subtítulo: temporada/episodio
    sub_elem = programme.find("sub-title")
    if sub_elem is None:
        match = re.search(r"S(\d+)E(\d+)", title, re.IGNORECASE)
        if match:
            sub_elem = ET.Element("sub-title")
            sub_elem.text = match.group(0)
            programme.append(sub_elem)

    # Descripción y datos desde TMDB
    if programme.find("desc") is None:
        result = buscar_tmdb(title)
        if result:  # Solo si se encontró algo en TMDB
            # Sinopsis
            if result.get("overview"):
                desc = ET.Element("desc", lang="es")
                desc.text = result["overview"]
                programme.append(desc)

            # Fecha
            if programme.find("date") is None:
                release_date = result.get("release_date") or result.get("first_air_date")
                if release_date:
                    date_elem = ET.Element("date")
                    date_elem.text = release_date.split("-")[0]
                    programme.append(date_elem)

            # Categoría
            if programme.find("category") is None:
                cat_elem = ET.Element("category", lang="es")
                if result.get("media_type"):
                    cat_elem.text = "Película" if result["media_type"] == "movie" else "Serie"
                programme.append(cat_elem)
