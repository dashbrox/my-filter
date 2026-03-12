import requests
import gzip
import xml.etree.ElementTree as ET
import re
import os
import json
import time
import unicodedata
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# CONFIGURACION
# =========================

EPG_COUNTRY_CODES = """
al ar am au at by be bo ba br bg ca co cl cr hr cz dk do ec eg sv
fi fr ge de gh gr gt hn hk hu is in id il it jp lv lb lt lu mk my
mt mx me nl nz ni ng no pa py pe ph pl pt ro ru sa rs sg si za kr
es se ch tw th tr ug ua ae gb us uy ve vn zw
""".split()

EPG_URLS = [f"https://iptv-epg.org/files/epg-{code}.xml" for code in EPG_COUNTRY_CODES]
EPG_URLS += [
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz",
]

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"
CACHE_FILE = "api_cache.json"

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

FORCE_SEASON_EPISODE_IN_TITLE_ONLY = True
REMOVE_SUBTITLE_ENTIRELY = False

DOWNLOAD_TIMEOUT = (20, 120)
API_TIMEOUT = (5, 10)
MAX_RETRIES = 2
USER_AGENT = "xmltv-title-normalizer/1.5"

LATAM_FEED_CODES = {
    "ar", "bo", "br", "cl", "co", "cr", "do", "ec", "sv",
    "gt", "hn", "mx", "ni", "pa", "py", "pe", "uy", "ve"
}

IBERO_SPANISH_CODES = LATAM_FEED_CODES | {"es"}

STOPWORDS = {
    "de", "la", "el", "los", "las", "un", "una", "unos", "unas",
    "y", "o", "en", "por", "para", "con", "sin", "del", "al",
    "que", "se", "su", "sus", "the", "a", "an", "and", "of", "to", "in"
}
SPANISH_MINOR_WORDS = {
    "a", "al", "ante", "bajo", "cabe", "con", "contra", "de", "del",
    "desde", "durante", "en", "entre", "hacia", "hasta", "mediante",
    "para", "por", "segun", "según", "sin", "so", "sobre", "tras",
    "y", "e", "o", "u", "ni", "pero", "mas", "más",
    "el", "la", "los", "las", "lo",
    "un", "una", "unos", "unas",
    "vs", "v"
}

SPANISH_TITLE_LANGS = {
    "es", "es-419", "es-mx", "es-ar", "es-co", "es-cl", "es-pe", "es-us", "es-es"
}

# Ajustes manuales por canal (en minutos)
# Negativo = mover la guía hacia atrás
# Positivo = mover la guía hacia adelante
CHANNEL_TIME_OFFSETS = {
    "WarnerChannel.pe": -3,
    "HBO.mx": -11,
    "HBOXtreme.co": -1,
}

# Restricción opcional por canal -> fuente(s) permitida(s)
# Si un canal no aparece aquí, funciona normal con cualquier fuente.
CHANNEL_SOURCE_RULES = {
    "Space.co": ["https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz"],
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

def get_feed_code(url):
    u = url.lower()

    m = re.search(r"epg-([a-z]{2})\.xml", u)
    if m:
        return m.group(1)

    m = re.search(r"epg_ripper_([a-z]{2})\d*\.xml\.gz", u)
    if m:
        return m.group(1)

    return None

def is_latam_feed(url):
    code = get_feed_code(url)
    return code in LATAM_FEED_CODES

def use_spanish_season_episode_format(url):
    code = get_feed_code(url)
    return code in IBERO_SPANISH_CODES

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
        season_num = int(season)
        episode_num = int(episode)
        return f"S{season_num:02d} E{episode_num:02d}"
    except Exception:
        return None

def extract_labeled_season_episode(text):
    """
    Detecta formatos variados y devuelve (season, episode) si encuentra ambos.
    Ejemplos:
    - Season 1 Episode 2
    - Temporada 1 Episodio 2
    - Temp 1 Ep 2
    - Episode 2 Season 1
    - Ep. 2 Temp. 1
    - Capítulo 3 Temporada 2
    """
    if not text:
        return None

    patterns = [
        r"\bseason\s*(\d+)\s*[,:\-]?\s*episode\s*(\d+)\b",
        r"\btemporada\s*(\d+)\s*[,:\-]?\s*(?:episodio|capitulo|capítulo|ep|cap)\s*(\d+)\b",
        r"\btemp\.?\s*(\d+)\s*[,:\-]?\s*(?:ep\.?|episodio|cap\.?|capitulo|capítulo)\s*(\d+)\b",
        r"\b(?:episode|ep)\.?\s*(\d+)\s*[,:\-]?\s*season\s*(\d+)\b",
        r"\b(?:episodio|capitulo|capítulo|ep\.?|cap\.?)\s*(\d+)\s*[,:\-]?\s*temporada\s*(\d+)\b",
        r"\b(?:ep\.?|episodio|cap\.?|capitulo|capítulo)\s*(\d+)\s*[,:\-]?\s*temp\.?\s*(\d+)\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            g1 = m.group(1)
            g2 = m.group(2)

            if "episode" in pattern or "ep" in pattern and "season" in pattern and pattern.startswith(r"\b(?:episode"):
                return normalize_season_ep_from_numbers(g2, g1)
            if ("episodio" in pattern or "capitulo" in pattern or "capítulo" in pattern or "cap" in pattern) and ("temporada" in pattern or "temp" in pattern) and pattern.startswith(r"\b(?:episodio"):
                return normalize_season_ep_from_numbers(g2, g1)
            return normalize_season_ep_from_numbers(g1, g2)

    return None

def normalize_season_ep(text):
    if not text:
        return None

    patterns = [
        r"\bS\s*(\d+)\s*E\s*(\d+)\b",
        r"\bS\s*(\d+)\s*[:.\-]?\s*E\s*(\d+)\b",
        r"\bT\s*(\d+)\s*E\s*(\d+)\b",
        r"\bT\s*(\d+)\s*[:.\-]?\s*E\s*(\d+)\b",
        r"\b(\d+)\s*x\s*(\d+)\b",
        r"\b(\d+)\s*-\s*(\d+)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return normalize_season_ep_from_numbers(match.group(1), match.group(2))

    labeled = extract_labeled_season_episode(text)
    if labeled:
        return labeled

    return None

def extract_se_regex(text):
    return normalize_season_ep(text)

def extract_xmltv_episode_num(elem):
    for ep in elem.findall("episode-num"):
        system = (ep.get("system") or "").strip().lower()
        value = (ep.text or "").strip()
        if not value:
            continue

        if system == "xmltv_ns":
            nums = re.findall(r"\d+", value)
            if len(nums) >= 2:
                season = int(nums[0]) + 1
                episode = int(nums[1]) + 1
                return normalize_season_ep_from_numbers(season, episode)

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
        priority = ["es", "es-es", "en", "en-us", ""]

    for wanted in priority:
        for lang, text in candidates:
            if lang == wanted:
                return text

    return candidates[0][1]

def has_spanish_variant(parent, tag):
    for e in parent.findall(tag):
        text = (e.text or "").strip()
        if not text:
            continue
        if norm_lang(e.get("lang")) in SPANISH_TITLE_LANGS:
            return True
    return False

def detect_sequel_marker(text):
    if not text:
        return None

    norm = normalize_text(text)
    patterns = [
        r"\b(?:parte|part)\s+(2|3|4|5|6|7|8|9|ii|iii|iv|v|vi|vii|viii|ix|x)\b",
        r"\b(2|3|4|5|6|7|8|9|ii|iii|iv|v|vi|vii|viii|ix|x)\b$",
    ]

    for pattern in patterns:
        m = re.search(pattern, norm, re.IGNORECASE)
        if m:
            return m.group(1).lower()
    return None

def strip_leading_se_from_text(text):
    if not text:
        return ""

    text = text.strip()

    patterns = [
        r"^\s*\(?\s*[ST]\s*\d+\s*[:.\-]?\s*E\s*\d+\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*\d+\s*x\s*\d+\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*Season\s*\d+\s*[,:\-]?\s*Episode\s*\d+\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*Temporada\s*\d+\s*[,:\-]?\s*(?:Episodio|Cap[ií]tulo)\s*\d+\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*Temp\.?\s*\d+\s*[,:\-]?\s*(?:Ep\.?|Cap\.?)\s*\d+\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*(?:Episode|Ep\.?)\s*\d+\s*[,:\-]?\s*Season\s*\d+\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*(?:Episodio|Cap[ií]tulo|Ep\.?|Cap\.?)\s*\d+\s*[,:\-]?\s*(?:Temporada|Temp\.?)\s*\d+\s*\)?\s*[:.\-–—]?\s*",
    ]

    changed = True
    while changed:
        changed = False
        for pattern in patterns:
            new_text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
            if new_text != text:
                text = new_text
                changed = True

    return " ".join(text.split()).strip()

def format_season_episode_display(se_text, use_spanish=False):
    if not se_text:
        return None

    m = re.match(r"^\s*S(\d{2})\s*E(\d{2})\s*$", se_text, flags=re.IGNORECASE)
    if not m:
        return se_text

    season = int(m.group(1))
    episode = int(m.group(2))

    if use_spanish:
        return f"Temp. {season} Ep. {episode}"

    return f"Season {season} Episode {episode}"

def spanish_title_case(text):
    if not text:
        return ""

    parts = re.split(r"(\s+)", text.strip())
    capitalize_next = True  # primera palabra y primera después de ":"

    def transform_word(word, force_capitalize=False):
        if not word:
            return word

        if re.fullmatch(r"[A-ZÁÉÍÓÚÜÑ0-9]{2,6}", word):
            return word

        if re.fullmatch(r"[ivxlcdmIVXLCDM]+", word):
            return word.upper()

        if "-" in word:
            subparts = word.split("-")
            rebuilt = []
            for idx, sub in enumerate(subparts):
                rebuilt.append(transform_word(sub, force_capitalize=(force_capitalize or idx > 0)))
            return "-".join(rebuilt)

        low = word.lower()

        if low in {"vs", "v"}:
            return low

        if not force_capitalize and low in SPANISH_MINOR_WORDS:
            return low

        return low[:1].upper() + low[1:]

    def transform_token(token, force_capitalize=False):
        m = re.match(r'^([\"“”¿¡(\[]*)(.*?)([\"”?!:;.,)\]]*)$', token)
        if not m:
            return token, False

        prefix, core, suffix = m.groups()

        if not core:
            return token, ":" in suffix

        new_core = transform_word(core, force_capitalize=force_capitalize)
        return f"{prefix}{new_core}{suffix}", ":" in suffix

    for i, part in enumerate(parts):
        if not part or part.isspace():
            continue

        parts[i], should_capitalize_next = transform_token(
            part,
            force_capitalize=capitalize_next
        )
        capitalize_next = should_capitalize_next

    return "".join(parts)

def split_episode_title_from_desc(desc_text):
    """
    Si la descripción empieza con temporada/episodio + título:
    - S4 E13 Título
    - Temp. 4 Ep. 13 Título
    - Season 4 Episode 13 Title
    devuelve ('Título', 'Sinopsis...')
    """
    if not desc_text:
        return None, desc_text

    text = desc_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return None, text

    first_line, sep, rest = text.partition("\n")
    first_line_clean = first_line.strip()

    se = normalize_season_ep(first_line_clean)
    if not se:
        return None, text

    ep_title = strip_leading_se_from_text(first_line_clean).strip()
    if not ep_title or len(ep_title) < 3:
        return None, text

    remaining = rest.strip()
    return ep_title, remaining

def first_line(text):
    if not text:
        return ""
    return text.replace("\r\n", "\n").replace("\r", "\n").split("\n", 1)[0].strip()

# =========================
# AJUSTE HORARIO POR CANAL
# =========================

def shift_xmltv_datetime(xmltv_dt, minutes):
    """
    Ajusta una fecha XMLTV conservando el timezone si existe.
    Ejemplos:
    20260311123000 +0000
    20260311123000
    """
    if not xmltv_dt or not minutes:
        return xmltv_dt

    xmltv_dt = xmltv_dt.strip()
    m = re.match(r"^(\d{14})(?:\s*([+-]\d{4}))?$", xmltv_dt)
    if not m:
        return xmltv_dt

    base = m.group(1)
    tz = m.group(2)

    dt = datetime.strptime(base, "%Y%m%d%H%M%S")
    dt = dt + timedelta(minutes=minutes)

    if tz:
        return f"{dt.strftime('%Y%m%d%H%M%S')} {tz}"
    return dt.strftime("%Y%m%d%H%M%S")

def apply_channel_offset(elem):
    ch_id = elem.get("channel")
    offset = CHANNEL_TIME_OFFSETS.get(ch_id, 0)

    if not offset:
        return

    start = elem.get("start")
    stop = elem.get("stop")

    if start:
        elem.set("start", shift_xmltv_datetime(start, offset))
    if stop:
        elem.set("stop", shift_xmltv_datetime(stop, offset))

def is_source_allowed_for_channel(channel_id, source_url):
    allowed_sources = CHANNEL_SOURCE_RULES.get(channel_id)
    if not allowed_sources:
        return True
    return source_url in allowed_sources

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

    cache_key = (
        f"tmdb_best:{normalize_text(title)}:"
        f"{normalize_text(desc)[:120]}:"
        f"{'latam' if prefer_latam else 'default'}"
    )
    if cache_key in api_cache:
        return api_cache[cache_key]

    search_lang = "es-MX" if prefer_latam else "es-ES"
    data = tmdb_search_multi(title, search_lang)
    if not data or not data.get("results"):
        api_cache[cache_key] = None
        return None

    expected_type = infer_media_type_from_desc(desc)
    norm_title = normalize_text(title)
    source_sequel = detect_sequel_marker(title)

    best_item = None
    best_score = -999.0

    for item in data.get("results", [])[:10]:
        media_type = item.get("media_type")
        if media_type not in ("movie", "tv"):
            continue

        candidate_title = (item.get("title") or item.get("name") or "").strip()
        candidate_original = (item.get("original_title") or item.get("original_name") or "").strip()
        overview = item.get("overview") or ""

        norm_candidate_title = normalize_text(candidate_title)
        norm_candidate_original = normalize_text(candidate_original)
        candidate_sequel = detect_sequel_marker(candidate_title) or detect_sequel_marker(candidate_original)

        score = 0.0

        title_ratio = SequenceMatcher(None, norm_title, norm_candidate_title).ratio()
        score += title_ratio * 5

        if norm_title and norm_title == norm_candidate_title:
            score += 7

        if norm_title and norm_candidate_original and norm_title == norm_candidate_original:
            score += 4

        if desc and overview:
            score += overlap_score(desc, overview) * 6

        if expected_type and media_type == expected_type:
            score += 2

        if desc and not overview.strip():
            score -= 1.5

        if source_sequel is None and candidate_sequel is not None:
            score -= 6

        if source_sequel is not None and candidate_sequel is not None:
            if source_sequel == candidate_sequel:
                score += 1.5
            else:
                score -= 7

        if source_sequel is None and candidate_sequel is not None and norm_candidate_title.startswith(norm_title + " "):
            score -= 2

        popularity = item.get("popularity") or 0
        try:
            score += min(float(popularity) / 1000.0, 0.3)
        except Exception:
            pass

        if score > best_score:
            best_score = score
            best_item = item

    if not best_item or best_score < 3.5:
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

def replace_all_title_elements(elem, new_title, prefer_latam=False):
    title_elems = elem.findall("title")
    for t in title_elems:
        elem.remove(t)

    new_title_elem = ET.Element("title")
    new_title_elem.text = new_title
    if prefer_latam:
        new_title_elem.set("lang", "es")
    elem.insert(0, new_title_elem)

def set_or_replace_subtitle(elem, subtitle_text, prefer_latam=False):
    for s in list(elem.findall("sub-title")):
        elem.remove(s)

    if REMOVE_SUBTITLE_ENTIRELY or not subtitle_text:
        return

    new_sub = ET.Element("sub-title")
    new_sub.text = subtitle_text
    if prefer_latam:
        new_sub.set("lang", "es")

    title_index = 0
    for i, child in enumerate(list(elem)):
        if child.tag == "title":
            title_index = i
            break
    elem.insert(title_index + 1, new_sub)

def set_or_replace_desc(elem, desc_text, prefer_latam=False):
    for d in list(elem.findall("desc")):
        elem.remove(d)

    if not desc_text:
        return

    new_desc = ET.Element("desc")
    new_desc.text = desc_text
    if prefer_latam:
        new_desc.set("lang", "es")

    children = list(elem)
    insert_index = len(children)

    for i, child in enumerate(children):
        if child.tag in ("sub-title", "title"):
            insert_index = i + 1

    elem.insert(insert_index, new_desc)

def normalize_subtitle_and_desc(elem, prefer_latam=False, is_series=False):
    current_subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    current_desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)

    extracted_ep_title = None
    cleaned_desc = current_desc.strip() if current_desc else ""

    ep_title_from_desc, desc_without_ep_title = split_episode_title_from_desc(current_desc)
    if ep_title_from_desc:
        extracted_ep_title = ep_title_from_desc
        cleaned_desc = (desc_without_ep_title or "").strip()

    chosen_subtitle = current_subtitle.strip() if current_subtitle else ""
    if not chosen_subtitle and extracted_ep_title:
        chosen_subtitle = extracted_ep_title

    chosen_subtitle = strip_leading_se_from_text(chosen_subtitle).strip()

    should_spanish_case_subtitle = bool(chosen_subtitle) and (
        prefer_latam or has_spanish_variant(elem, "sub-title") or extracted_ep_title is not None
    )
    if chosen_subtitle and should_spanish_case_subtitle:
        chosen_subtitle = spanish_title_case(chosen_subtitle)

    set_or_replace_subtitle(elem, chosen_subtitle, prefer_latam=prefer_latam)

    final_desc = cleaned_desc

    if is_series and chosen_subtitle:
        first = first_line(cleaned_desc)
        if normalize_text(first) == normalize_text(chosen_subtitle):
            final_desc = cleaned_desc
        elif cleaned_desc:
            final_desc = f"{chosen_subtitle}\n{cleaned_desc}"
        else:
            final_desc = chosen_subtitle

    set_or_replace_desc(elem, final_desc, prefer_latam=prefer_latam)

def normalize_episode_num_elements(elem):
    if not FORCE_SEASON_EPISODE_IN_TITLE_ONLY:
        return
    for ep in list(elem.findall("episode-num")):
        elem.remove(ep)

# =========================
# PROCESAMIENTO PRINCIPAL
# =========================

def process_programme(
    elem,
    start_time_str,
    prefer_latam=False,
    spanish_season_episode_format=False
):
    raw_title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam)
    raw_desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)
    xml_has_spanish_title = has_spanish_variant(elem, "title")

    clean_title, has_new = extract_new_marker(raw_title)
    clean_title, year_regex = extract_year_regex(clean_title)

    se_title = extract_se_regex(clean_title)
    se_desc = extract_se_regex(raw_desc)
    se_xml = extract_xmltv_episode_num(elem)

    final_year = year_regex
    final_se = se_title or se_desc or se_xml
    final_title = clean_title.strip()

    should_translate = prefer_latam and not xml_has_spanish_title
    need_tmdb = (not final_year) or should_translate
    need_tv = not final_se

    tmdb_data = None
    if need_tmdb or need_tv:
        tmdb_data = get_tmdb_data(final_title, raw_desc, prefer_latam=prefer_latam)

    if should_translate and tmdb_data and tmdb_data.get("localized_title"):
        final_title = tmdb_data["localized_title"].strip()

    if not final_year and tmdb_data:
        final_year = tmdb_data.get("year")

    if not final_se and tmdb_data and tmdb_data.get("type") == "tv":
        try:
            air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d")

            query_title = clean_title or final_title
            tvmaze_data = get_tvmaze_episode(query_title, air_date)

            if not tvmaze_data and final_title and final_title != query_title:
                tvmaze_data = get_tvmaze_episode(final_title, air_date)

            if tvmaze_data and tvmaze_data.get("season") is not None and tvmaze_data.get("episode") is not None:
                final_se = normalize_season_ep_from_numbers(tvmaze_data["season"], tvmaze_data["episode"])
        except ValueError:
            pass

    if final_title and (prefer_latam or xml_has_spanish_title):
        final_title = spanish_title_case(final_title)

    display_title = final_title
    is_series = bool(final_se) or bool(tmdb_data and tmdb_data.get("type") == "tv")

    if final_se:
        display_se = format_season_episode_display(
            final_se,
            use_spanish=spanish_season_episode_format
        )
        display_title += f" | {display_se}"

    if has_new:
        display_title += " ᴺᵉʷ"

    return display_title, is_series

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
                spanish_season_episode_format = use_spanish_season_episode_format(url)

                try:
                    print(f"[{idx}/{len(EPG_URLS)}] Fuente: {url}", flush=True)
                    download_xml(url, TEMP_INPUT)
                    print(f"Descarga lista en {time.time() - t0:.1f}s", flush=True)

                    context = ET.iterparse(TEMP_INPUT, events=("end",))

                    for event, elem in context:
                        if elem.tag == "channel":
                            ch_id = elem.get("id")
                            if (
                                ch_id in allowed_channels
                                and is_source_allowed_for_channel(ch_id, url)
                                and ch_id not in written_channels
                            ):
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
                            if ch_id in allowed_channels and is_source_allowed_for_channel(ch_id, url):
                                start = elem.get("start", "")
                                stop = elem.get("stop", "")

                                new_title, is_series = process_programme(
                                    elem,
                                    start,
                                    prefer_latam=prefer_latam,
                                    spanish_season_episode_format=spanish_season_episode_format
                                )

                                replace_all_title_elements(elem, new_title, prefer_latam=prefer_latam)
                                normalize_subtitle_and_desc(elem, prefer_latam=prefer_latam, is_series=is_series)
                                normalize_episode_num_elements(elem)

                                apply_channel_offset(elem)

                                start = elem.get("start", "")
                                stop = elem.get("stop", "")

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
