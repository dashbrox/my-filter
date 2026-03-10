import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import json
import time
import unicodedata
from difflib import SequenceMatcher
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# CONFIGURACION
# =========================

EPG_COUNTRY_CODES = """
al ar am au at by be bo ba br bg ca cl co cr hr cz dk do ec eg sv
fi fr ge de gh gr gt hn hk hu is in id il it jp lv lb lt lu mk my
mt mx me nl nz ni ng no pa py pe ph pl pt ro ru sa rs sg si za kr
es se ch tw th tr ug ua ae gb us uy ve vn zw
""".split()

EPG_URLS = [f"https://iptv-epg.org/files/epg-{code}.xml" for code in EPG_COUNTRY_CODES]
EPG_URLS += [
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
]

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"
CACHE_FILE = "api_cache.json"

# Usa variables de entorno / secrets en GitHub Actions
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

# Si es True, remueve <episode-num> para que muchas apps no muestren Sxx Exx fuera del title.
FORCE_SEASON_EPISODE_IN_TITLE_ONLY = True

# Si es True, elimina por completo <sub-title>.
REMOVE_SUBTITLE_ENTIRELY = False

# Tiempos / red
DOWNLOAD_TIMEOUT = (20, 120)  # connect, read
API_TIMEOUT = (5, 10)
MAX_RETRIES = 2
USER_AGENT = "xmltv-title-normalizer/1.0"

# Feeds que se trataran como Latinoamerica
LATAM_FEED_CODES = {
    "ar", "bo", "br", "cl", "co", "cr", "do", "ec", "sv",
    "gt", "hn", "mx", "ni", "pa", "py", "pe", "uy", "ve"
}

STOPWORDS = {
    "de", "la", "el", "los", "las", "un", "una", "unos", "unas",
    "y", "o", "en", "por", "para", "con", "sin", "del", "al",
    "que", "se", "su", "sus", "the", "a", "an", "and", "of", "to", "in"
}

# =========================
# SESION HTTP
# =========================

def build_session():
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session

SESSION = build_session()

# =========================
# CACHE
# =========================

api_cache = {}
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            api_cache = json.load(f)
        print(f"Caché cargado: {len(api_cache)} entradas.", flush=True)
    except Exception:
        api_cache = {}

def save_cache():
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(api_cache, f, ensure_ascii=False, indent=2)

# =========================
# UTILS TEXTO
# =========================

def normalize_text(text):
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())

def token_set(text):
    return {
        t for t in normalize_text(text).split()
        if len(t) > 2 and t not in STOPWORDS
    }

def overlap_score(a, b):
    ta = token_set(a)
    tb = token_set(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(len(ta), len(tb))

def norm_lang(lang):
    return (lang or "").strip().lower().replace("_", "-")

def is_latam_feed(url):
    m = re.search(r"epg-([a-z]{2})\.xml", url.lower())
    return bool(m and m.group(1) in LATAM_FEED_CODES)

def extract_new_marker(text):
    if not text:
        return "", False
    has_new = False
    clean = text
    if "ᴺᵉʷ" in clean:
        has_new = True
        clean = clean.replace("ᴺᵉʷ", " ")
    if re.search(r"\bNEW\b", clean, re.IGNORECASE):
        has_new = True
        clean = re.sub(r"\bNEW\b", " ", clean, flags=re.IGNORECASE)
    return " ".join(clean.split()), has_new

def extract_year_regex(text):
    if not text:
        return "", None
    match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if match:
        year = match.group(1)
        clean = re.sub(r"\(?\b" + re.escape(year) + r"\b\)?", " ", text)
        return " ".join(clean.split()), year
    return text, None

def normalize_season_ep_from_numbers(season, episode):
    try:
        return f"S{int(season):02d} E{int(episode):02d}"
    except Exception:
        return None

def normalize_season_ep(text):
    if not text:
        return None
    patterns = [
        r"\bS\s*(\d+)\s*E\s*(\d+)\b",
        r"\bT\s*(\d+)\s*E\s*(\d+)\b",
        r"\b(\d+)\s*x\s*(\d+)\b",
        r"\bSeason\s*(\d+)\s*Episode\s*(\d+)\b",
        r"\bTemporada\s*(\d+)\s*Episodio\s*(\d+)\b",
        r"\bTemporada\s*(\d+)\s*Cap[ií]tulo\s*(\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return normalize_season_ep_from_numbers(match.group(1), match.group(2))
    return None

def extract_se_regex(text):
    return normalize_season_ep(text)

def extract_xmltv_episode_num(elem):
    for ep in elem.findall("episode-num"):
        system = (ep.get("system") or "").strip().lower()
        value = (ep.text or "").strip()
        if not value:
            continue

        # xmltv_ns: zero-based, formato tipo "1 . 3 ."
        if system == "xmltv_ns":
            nums = re.findall(r"\d+", value)
            if len(nums) >= 2:
                season = int(nums[0]) + 1
                episode = int(nums[1]) + 1
                return normalize_season_ep_from_numbers(season, episode)

        # on-screen / genérico
        se = normalize_season_ep(value)
        if se:
            return se
    return None

def infer_media_type_from_desc(desc):
    d = normalize_text(desc)
    tv_hints = ["temporada", "episodio", "capitulo", "serie", "novela", "reality", "miniserie"]
    movie_hints = ["pelicula", "film", "largometraje", "cine", "documental"]
    tv_score = sum(1 for w in tv_hints if w in d)
    movie_score = sum(1 for w in movie_hints if w in d)
    if tv_score > movie_score:
        return "tv"
    if movie_score > tv_score:
        return "movie"
    return None

def pick_best_localized_text(parent, tag, prefer_latam=False):
    elems = parent.findall(tag)
    if not elems:
        return ""

    candidates = []
    for e in elems:
        text = (e.text or "").strip()
        if not text:
            continue
        lang = norm_lang(e.get("lang"))
        candidates.append((lang, text))

    if not candidates:
        return ""

    if prefer_latam:
        priority = [
            "es-419", "es-mx", "es-ar", "es-co", "es-cl", "es-pe",
            "es-us", "es", ""
        ]
    else:
        priority = ["es-es", "es", "en-us", "en", ""]

    for wanted in priority:
        for lang, text in candidates:
            if lang == wanted:
                return text

    return candidates[0][1]

# =========================
# TMDB
# =========================

def tmdb_search_multi(title, language):
    if not TMDB_API_KEY:
        return None

    cache_key = f"tmdb_search:{normalize_text(title)}:{language}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    url = "https://api.themoviedb.org/3/search/multi"
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "language": language,
    }

    try:
        r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            api_cache[cache_key] = data
            return data
    except Exception as e:
        print(f"Error TMDB search: {e}", flush=True)

    api_cache[cache_key] = None
    return None

def get_tmdb_localized_title(tmdb_id, media_type, prefer_latam=False):
    if not TMDB_API_KEY or not tmdb_id or media_type not in ("movie", "tv"):
        return None

    lang_chain = ["es-MX", "es-419", "es-AR", "es-CO", "es"] if prefer_latam else ["es-ES", "es"]

    for lang in lang_chain:
        cache_key = f"tmdb_title:{media_type}:{tmdb_id}:{lang}"
        if cache_key in api_cache:
            cached = api_cache[cache_key]
            if cached:
                return cached
            continue

        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
        params = {"api_key": TMDB_API_KEY, "language": lang}

        try:
            r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                title = (data.get("title") or data.get("name") or "").strip()
                if title:
                    api_cache[cache_key] = title
                    return title
        except Exception as e:
            print(f"Error TMDB localized title: {e}", flush=True)

        api_cache[cache_key] = None

    return None

def get_tmdb_data(title, desc="", prefer_latam=False):
    if not TMDB_API_KEY or not title:
        return None

    cache_key = f"tmdb_best:{normalize_text(title)}:{normalize_text(desc)[:120]}:{'latam' if prefer_latam else 'default'}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    search_lang = "es-MX" if prefer_latam else "es-ES"
    data = tmdb_search_multi(title, search_lang)
    if not data or not data.get("results"):
        api_cache[cache_key] = None
        return None

    expected_type = infer_media_type_from_desc(desc)
    norm_title = normalize_text(title)

    best_item = None
    best_score = -999.0

    for item in data.get("results", [])[:10]:
        media_type = item.get("media_type")
        if media_type not in ("movie", "tv"):
            continue

        candidate_title = item.get("title") or item.get("name") or ""
        overview = item.get("overview") or ""

        score = 0.0

        # 1) Titulo
        title_ratio = SequenceMatcher(None, norm_title, normalize_text(candidate_title)).ratio()
        score += title_ratio * 5

        # 2) Sinopsis XML vs overview de TMDB
        if desc and overview:
            score += overlap_score(desc, overview) * 4

        # 3) Bonus si description sugiere movie/tv y coincide
        if expected_type and media_type == expected_type:
            score += 2

        # 4) Castigo si hay sinopsis pero TMDB no trae overview util
        if desc and not overview.strip():
            score -= 1

        # 5) Bonus pequeno por popularidad para desempates
        popularity = item.get("popularity") or 0
        try:
            score += min(float(popularity) / 1000.0, 0.5)
        except Exception:
            pass

        if score > best_score:
            best_score = score
            best_item = item

    # Umbral conservador para evitar falsos positivos
    if not best_item or best_score < 2.5:
        api_cache[cache_key] = None
        return None

    result = {
        "type": best_item.get("media_type"),
        "year": None,
        "id": best_item.get("id"),
        "localized_title": None,
    }

    date_str = best_item.get("release_date") or best_item.get("first_air_date")
    if date_str:
        result["year"] = date_str.split("-")[0]

    result["localized_title"] = get_tmdb_localized_title(
        result["id"], result["type"], prefer_latam=prefer_latam
    )

    api_cache[cache_key] = result
    return result

# =========================
# TVMAZE
# =========================

def get_tvmaze_episode(show_name, air_date):
    cache_key = f"tvmaze:{normalize_text(show_name)}:{air_date}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    search_url = "https://api.tvmaze.com/singlesearch/shows"
    params = {"q": show_name}

    try:
        r_show = SESSION.get(search_url, params=params, timeout=API_TIMEOUT)
        if r_show.status_code != 200:
            api_cache[cache_key] = None
            return None

        show_data = r_show.json()
        show_id = show_data.get("id")
        if not show_id:
            api_cache[cache_key] = None
            return None

        ep_url = f"https://api.tvmaze.com/shows/{show_id}/episodesbydate"
        r_ep = SESSION.get(ep_url, params={"date": air_date}, timeout=API_TIMEOUT)

        if r_ep.status_code == 200:
            episodes = r_ep.json()
            if episodes:
                ep = episodes[0]
                result = {
                    "season": ep.get("season"),
                    "episode": ep.get("number"),
                    "name": ep.get("name"),
                }
                api_cache[cache_key] = result
                return result
    except Exception as e:
        print(f"Error TVMaze: {e}", flush=True)

    api_cache[cache_key] = None
    return None

# =========================
# XML HELPERS
# =========================

def strip_leading_se_from_text(text):
    if not text:
        return ""
    text = re.sub(r"^\s*(S|T)\s*\d+\s*E\s*\d+\s*[:\-]?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\s*\d+\s*x\s*\d+\s*[:\-]?\s*", "", text, flags=re.IGNORECASE)
    return " ".join(text.split()).strip()

def replace_all_title_elements(elem, new_title, prefer_latam=False):
    title_elems = elem.findall("title")
    for t in title_elems:
        elem.remove(t)

    new_title_elem = ET.Element("title")
    new_title_elem.text = new_title
    if prefer_latam:
        new_title_elem.set("lang", "es")
    elem.insert(0, new_title_elem)

def normalize_subtitle_elements(elem, prefer_latam=False):
    subtitle_elems = elem.findall("sub-title")
    if not subtitle_elems:
        return

    best_subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    best_subtitle = strip_leading_se_from_text(best_subtitle)

    for s in subtitle_elems:
        elem.remove(s)

    if REMOVE_SUBTITLE_ENTIRELY:
        return

    if best_subtitle:
        new_sub = ET.Element("sub-title")
        new_sub.text = best_subtitle
        if prefer_latam:
            new_sub.set("lang", "es")
        title_index = 0
        for i, child in enumerate(list(elem)):
            if child.tag == "title":
                title_index = i
                break
        elem.insert(title_index + 1, new_sub)

def normalize_episode_num_elements(elem):
    if not FORCE_SEASON_EPISODE_IN_TITLE_ONLY:
        return
    for ep in list(elem.findall("episode-num")):
        elem.remove(ep)

# =========================
# PROCESAMIENTO PRINCIPAL
# =========================

def process_programme(elem, start_time_str, prefer_latam=False):
    raw_title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam)
    raw_desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)

    clean_title, has_new = extract_new_marker(raw_title)
    clean_title, year_regex = extract_year_regex(clean_title)

    se_title = extract_se_regex(clean_title)
    se_desc = extract_se_regex(raw_desc)
    se_xml = extract_xmltv_episode_num(elem)

    final_year = year_regex
    final_se = se_title or se_desc or se_xml
    final_title = clean_title.strip()

    need_tmdb = not final_year or (prefer_latam and not final_title)
    need_tv = not final_se

    tmdb_data = None
    if need_tmdb or need_tv or prefer_latam:
        tmdb_data = get_tmdb_data(final_title, raw_desc, prefer_latam=prefer_latam)

    # Titulo traducido para feeds LatAm
    if prefer_latam and tmdb_data and tmdb_data.get("localized_title"):
        final_title = tmdb_data["localized_title"].strip()

    # Año para peliculas / fallback para series
    if not final_year and tmdb_data:
        if tmdb_data.get("type") == "movie":
            final_year = tmdb_data.get("year")
        elif tmdb_data.get("type") == "tv":
            final_year = tmdb_data.get("year")

    # Temporada / episodio para series
    if not final_se and tmdb_data and tmdb_data.get("type") == "tv":
        try:
            air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
            tvmaze_data = get_tvmaze_episode(final_title or clean_title, air_date)
            if tvmaze_data and tvmaze_data.get("season") is not None and tvmaze_data.get("episode") is not None:
                final_se = normalize_season_ep_from_numbers(tvmaze_data["season"], tvmaze_data["episode"])
        except ValueError:
            pass

    display_title = final_title
    if final_se:
        display_title += f" ({final_se})"
    elif final_year:
        display_title += f" ({final_year})"

    if has_new:
        display_title += " ᴺᵉʷ"

    return display_title

def download_xml(url, output_path):
    print(f"Descargando: {url}", flush=True)
    with SESSION.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
        r.raise_for_status()
        if url.lower().endswith(".gz"):
            with gzip.GzipFile(fileobj=r.raw) as gz:
                with open(output_path, "wb") as f:
                    for chunk in iter(lambda: gz.read(1024 * 1024), b""):
                        f.write(chunk)
        else:
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

def main():
    print("Iniciando script...", flush=True)

    if not os.path.exists(CHANNELS_FILE):
        print("Error: No existe channels.txt", flush=True)
        return

    with open(CHANNELS_FILE, "r", encoding="utf-8-sig") as f:
        allowed_channels = {line.strip() for line in f if line.strip()}

    if not allowed_channels:
        print("Error: channels.txt está vacío", flush=True)
        return

    written_programmes = set()
    written_channels = set()

    t_global = time.time()

    try:
        with open(TEMP_OUTPUT, "wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')

            for idx, url in enumerate(EPG_URLS, start=1):
                t0 = time.time()
                processed_programmes = 0
                prefer_latam = is_latam_feed(url)

                try:
                    print(f"[{idx}/{len(EPG_URLS)}] Fuente: {url}", flush=True)
                    download_xml(url, TEMP_INPUT)
                    print(f"Descarga lista en {time.time() - t0:.1f}s", flush=True)

                    context = ET.iterparse(TEMP_INPUT, events=("end",))

                    for event, elem in context:
                        if elem.tag == "channel":
                            ch_id = elem.get("id")
                            if ch_id in allowed_channels and ch_id not in written_channels:
                                out_f.write(ET.tostring(elem, encoding="utf-8"))
                                out_f.write(b"\n")
                                written_channels.add(ch_id)
                            elem.clear()

                        elif elem.tag == "programme":
                            processed_programmes += 1
                            if processed_programmes % 5000 == 0:
                                print(
                                    f"{url} -> programmes procesados: {processed_programmes} | "
                                    f"canales escritos: {len(written_channels)} | "
                                    f"programas escritos: {len(written_programmes)}",
                                    flush=True,
                                )

                            ch_id = elem.get("channel")
                            if ch_id in allowed_channels:
                                start = elem.get("start", "")
                                stop = elem.get("stop", "")

                                new_title = process_programme(elem, start, prefer_latam=prefer_latam)
                                replace_all_title_elements(elem, new_title, prefer_latam=prefer_latam)
                                normalize_subtitle_elements(elem, prefer_latam=prefer_latam)
                                normalize_episode_num_elements(elem)

                                prog_key = (ch_id, start, stop)
                                if prog_key not in written_programmes:
                                    out_f.write(ET.tostring(elem, encoding="utf-8"))
                                    out_f.write(b"\n")
                                    written_programmes.add(prog_key)

                            elem.clear()

                    del context

                    print(
                        f"Fuente terminada en {time.time() - t0:.1f}s | "
                        f"programmes leidos: {processed_programmes}",
                        flush=True,
                    )

                except Exception as e:
                    print(f"Error en fuente {url}: {e}", flush=True)

                finally:
                    if os.path.exists(TEMP_INPUT):
                        os.remove(TEMP_INPUT)

            out_f.write(b"</tv>\n")
    finally:
        save_cache()

    print("Comprimiendo...", flush=True)
    with open(TEMP_OUTPUT, "rb") as f_in:
        with gzip.open(OUTPUT_FILE, "wb") as f_out:
            f_out.writelines(f_in)

    if os.path.exists(TEMP_OUTPUT):
        os.remove(TEMP_OUTPUT)

    print(
        f"Proceso completado: {OUTPUT_FILE} | "
        f"canales: {len(written_channels)} | "
        f"programas: {len(written_programmes)} | "
        f"tiempo total: {time.time() - t_global:.1f}s",
        flush=True,
    )

if __name__ == "__main__":
    main()
