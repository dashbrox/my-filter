# Descripción
if programme.find("desc") is None:
    result = buscar_tmdb(title)
    if result:  # Solo continuar si se encontró algo
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
    else:
        # Si no se encontró en TMDB, simplemente ignorar
        pass
