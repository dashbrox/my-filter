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
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
]

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"
CACHE_FILE = "api_cache.json"
REVIEW_FILE = "review_queue.json"

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

# Regla principal del usuario:
# - TMDb / TVMaze SOLO completan faltantes.
# - NO reemplazan datos existentes, salvo correcciones menores
#   de ortografia/redaccion (misma cadena normalizada).
PRESERVE_EXISTING_XML_DATA = True
ALLOW_MINOR_TEXT_CORRECTIONS = True
USE_STRUCTURED_FIELDS_FOR_ENRICHMENT = True

# Conservamos episode-num si existe; si falta, lo agregamos.
FORCE_SEASON_EPISODE_IN_TITLE_ONLY = True
REMOVE_SUBTITLE_ENTIRELY = False

DOWNLOAD_TIMEOUT = (20, 120)
API_TIMEOUT = (5, 10)
MAX_RETRIES = 2
USER_AGENT = "xmltv-title-normalizer/2.1-enrichment-only"

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

CHANNEL_TIME_OFFSETS = {
    "HBO.mx": -11,
    "HBOXtreme.co": -1,
}

EPG_CO1 = "https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz"
EPG_ES1 = "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz"

CHANNEL_SOURCE_RULES = {
    "Space.co": [EPG_CO1],
    "M+.Estrenos.es": [EPG_ES1],
    "DAZN.F1.es": [EPG_ES1],
}

METADATA_INPUT_JSON = "metadata_input.json"
METADATA_OUTPUT_JSON = "metadata_output.json"

TMDB_STRONG_SCORE = 82.0
TMDB_CONFIDENT_SCORE = 74.0
TMDB_MIN_GAP = 7.0
TVMAZE_STRONG_SCORE = 80.0
TVMAZE_CONFIDENT_SCORE = 70.0
TVMAZE_MIN_GAP = 6.0

review_queue = []
review_seen = set()

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

def save_review_queue():
    if not review_queue:
        return
    with open(REVIEW_FILE, "w", encoding="utf-8") as f:
        json.dump(review_queue, f, ensure_ascii=False, indent=2)
    print(f"Casos para revisión manual: {len(review_queue)} -> {REVIEW_FILE}", flush=True)

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


def text_ratio(a, b):
    a = normalize_text(a)
    b = normalize_text(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


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


def strip_html(text):
    if not text:
        return ""
    return re.sub(r"<[^>]+>", " ", text)


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


def extract_candidate_year(text):
    if not text:
        return None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return m.group(1) if m else None


def normalize_season_ep_from_numbers(season, episode):
    try:
        season_num = int(season)
        episode_num = int(episode)
        return f"S{season_num:02d} E{episode_num:02d}"
    except Exception:
        return None


def parse_season_episode_text(se_text):
    if not se_text:
        return None, None
    m = re.match(r"^\s*S(\d{2})\s*E(\d{2})\s*$", se_text, flags=re.IGNORECASE)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def extract_labeled_season_episode(text):
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

            if "episode" in pattern or ("ep" in pattern and "season" in pattern and pattern.startswith(r"\b(?:episode")):
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
    tv_hints = ["temporada", "episodio", "capitulo", "serie", "novela", "reality", "miniserie", "season", "episode"]
    movie_hints = ["pelicula", "film", "largometraje", "cine", "documental", "movie"]
    tv_score = sum(1 for w in tv_hints if w in d)
    movie_score = sum(1 for w in movie_hints if w in d)
    if tv_score > movie_score:
        return "tv"
    if movie_score > tv_score:
        return "movie"
    return None


def infer_media_type_from_context(title, desc, subtitle, has_se=False):
    if has_se:
        return "tv"
    desc_hint = infer_media_type_from_desc(desc)
    if desc_hint:
        return desc_hint
    sub_hint = infer_media_type_from_desc(subtitle)
    if sub_hint:
        return sub_hint
    t = normalize_text(title)
    if any(w in t for w in ("episodio", "temporada", "capitulo", "season", "episode")):
        return "tv"
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
    capitalize_next = True

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
    if not desc_text:
        return None, desc_text

    text = desc_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return None, text

    first_ln, sep, rest = text.partition("\n")
    first_line_clean = first_ln.strip()

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


def clean_series_query(text):
    if not text:
        return ""

    text = text.strip()
    text = re.sub(r":default\s*$", "", text, flags=re.IGNORECASE)

    se_cut_patterns = [
        r"^(.*?)(?::\s*)?s\d+\s*e\d+\b.*$",
        r"^(.*?)(?::\s*)?t\d+\s*e\d+\b.*$",
        r"^(.*?)(?::\s*)?\d+\s*x\s*\d+\b.*$",
        r"^(.*?)(?::\s*)?season\s*\d+\s*episode\s*\d+\b.*$",
        r"^(.*?)(?::\s*)?temporada\s*\d+\s*(?:episodio|cap[ií]tulo)\s*\d+\b.*$",
        r"^(.*?)(?::\s*)?temp\.?\s*\d+\s*ep\.?\s*\d+\b.*$",
    ]

    for pattern in se_cut_patterns:
        m = re.match(pattern, text, flags=re.IGNORECASE)
        if m and m.group(1).strip():
            text = m.group(1).strip()
            break

    text = re.sub(r"\bS\s*\d+\s*E\s*\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bT\s*\d+\s*E\s*\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d+\s*x\s*\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSeason\s*\d+\s*Episode\s*\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bTemporada\s*\d+\s*(?:Episodio|Cap[ií]tulo)\s*\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bTemp\.?\s*\d+\s*Ep\.?\s*\d+\b", " ", text, flags=re.IGNORECASE)

    text = re.sub(r"\s+", " ", text).strip(" :-|")
    return text.strip()


def extract_season_episode_numbers(text):
    if not text:
        return None, None

    patterns = [
        r"\bS\s*(\d+)\s*E\s*(\d+)\b",
        r"\bT\s*(\d+)\s*E\s*(\d+)\b",
        r"\b(\d+)\s*x\s*(\d+)\b",
        r"\bSeason\s*(\d+)\s*Episode\s*(\d+)\b",
        r"\bTemporada\s*(\d+)\s*Episodio\s*(\d+)\b",
        r"\bTemp\.?\s*(\d+)\s*Ep\.?\s*(\d+)\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return int(m.group(1)), int(m.group(2))

    return None, None


def year_score(source_year, candidate_date):
    if not source_year or not candidate_date:
        return 0.0
    try:
        sy = int(str(source_year)[:4])
        cy = int(str(candidate_date)[:4])
    except Exception:
        return 0.0

    diff = abs(sy - cy)
    if diff == 0:
        return 20.0
    if diff == 1:
        return 6.0
    if diff == 2:
        return -2.0
    return -16.0


def is_minor_text_correction(existing, candidate):
    if not existing or not candidate:
        return False
    ne = normalize_text(existing)
    nc = normalize_text(candidate)
    if not ne or ne != nc:
        return False
    return existing.strip() != candidate.strip()


def maybe_apply_minor_text_correction(existing, candidate):
    if not existing:
        return candidate or existing
    if ALLOW_MINOR_TEXT_CORRECTIONS and candidate and is_minor_text_correction(existing, candidate):
        return candidate
    return existing


def mark_for_review(channel_id, start, title, reason, details=None):
    key = (channel_id or "", start or "", normalize_text(title), reason)
    if key in review_seen:
        return
    review_seen.add(key)
    review_queue.append({
        "channel": channel_id,
        "start": start,
        "title": title,
        "reason": reason,
        "details": details or {},
    })

# =========================
# AJUSTE HORARIO POR CANAL
# =========================

def shift_xmltv_datetime(xmltv_dt, minutes):
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

def tmdb_get(url, params, cache_key):
    if cache_key in api_cache:
        return api_cache[cache_key]
    try:
        r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            api_cache[cache_key] = data
            return data
    except Exception as e:
        print(f"Error TMDB request: {e}", flush=True)
    api_cache[cache_key] = None
    return None


def tmdb_search_multi(title, language):
    if not TMDB_API_KEY:
        return None
    return tmdb_get(
        "https://api.themoviedb.org/3/search/multi",
        {"api_key": TMDB_API_KEY, "query": title, "language": language},
        f"tmdb_search_multi:{normalize_text(title)}:{language}"
    )


def tmdb_search_tv(title, language="es-ES", year=None):
    if not TMDB_API_KEY:
        return None
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "language": language,
    }
    if year:
        params["first_air_date_year"] = int(year)
        params["year"] = int(year)
    return tmdb_get(
        "https://api.themoviedb.org/3/search/tv",
        params,
        f"tmdb_search_tv:{normalize_text(title)}:{language}:{year or ''}"
    )


def tmdb_search_movie(title, language="es-ES", year=None, region=None):
    if not TMDB_API_KEY:
        return None
    params = {
        "api_key": TMDB_API_KEY,
        "query": title,
        "language": language,
    }
    if year:
        params["year"] = int(year)
        params["primary_release_year"] = int(year)
    if region:
        params["region"] = region
    return tmdb_get(
        "https://api.themoviedb.org/3/search/movie",
        params,
        f"tmdb_search_movie:{normalize_text(title)}:{language}:{year or ''}:{region or ''}"
    )


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


def score_tmdb_candidate(query_title, query_desc, year_hint, expected_type, item):
    media_type = item.get("media_type") or expected_type
    candidate_title = (item.get("title") or item.get("name") or "").strip()
    candidate_original = (item.get("original_title") or item.get("original_name") or "").strip()
    overview = (item.get("overview") or "").strip()
    date_str = item.get("release_date") or item.get("first_air_date") or ""
    source_sequel = detect_sequel_marker(query_title)
    cand_sequel = detect_sequel_marker(candidate_title) or detect_sequel_marker(candidate_original)

    score = 0.0

    title_best = max(text_ratio(query_title, candidate_title), text_ratio(query_title, candidate_original))
    score += title_best * 55.0

    nq = normalize_text(query_title)
    nt = normalize_text(candidate_title)
    no = normalize_text(candidate_original)

    if nq and nq == nt:
        score += 15.0
    if nq and nq == no:
        score += 10.0

    if query_desc and overview:
        score += overlap_score(query_desc, overview) * 28.0

    score += year_score(year_hint, date_str)

    if expected_type and media_type == expected_type:
        score += 8.0
    elif expected_type and media_type and media_type != expected_type:
        score -= 12.0

    if source_sequel is None and cand_sequel is not None:
        score -= 6.0
    elif source_sequel and cand_sequel:
        if source_sequel == cand_sequel:
            score += 3.0
        else:
            score -= 12.0

    popularity = item.get("popularity") or 0
    try:
        score += min(float(popularity) / 1500.0, 1.5)
    except Exception:
        pass

    return score


def classify_match(best_score, gap, strong_thr, confident_thr, min_gap):
    if best_score >= strong_thr and gap >= min_gap:
        return "strong"
    if best_score >= confident_thr and gap >= max(4.0, min_gap - 2.0):
        return "confident"
    return "weak"


def tmdb_pick_best_match(title, desc="", year_hint=None, expected_type=None, prefer_latam=False, broad=False):
    if not TMDB_API_KEY or not title:
        return None

    mode = "broad" if broad else "strict"
    cache_key = (
        f"tmdb_best_v2:{normalize_text(title)}:{normalize_text(desc)[:120]}:"
        f"{year_hint or ''}:{expected_type or ''}:{'latam' if prefer_latam else 'default'}:{mode}"
    )
    if cache_key in api_cache:
        return api_cache[cache_key]

    search_lang = "es-MX" if prefer_latam else "es-ES"
    results = []

    if expected_type == "movie":
        data = tmdb_search_movie(title, language=search_lang, year=year_hint)
        if broad and (not data or not data.get("results")):
            data = tmdb_search_movie(title, language=search_lang)
        if data and data.get("results"):
            for item in data.get("results", [])[:10]:
                item = dict(item)
                item["media_type"] = "movie"
                results.append(item)
    elif expected_type == "tv":
        data = tmdb_search_tv(title, language=search_lang, year=year_hint)
        if broad and (not data or not data.get("results")):
            data = tmdb_search_tv(title, language=search_lang)
        if data and data.get("results"):
            for item in data.get("results", [])[:10]:
                item = dict(item)
                item["media_type"] = "tv"
                results.append(item)
    else:
        multi = tmdb_search_multi(title, search_lang)
        if multi and multi.get("results"):
            results.extend([item for item in multi.get("results", [])[:10] if item.get("media_type") in ("movie", "tv")])
        if broad:
            movie = tmdb_search_movie(title, language=search_lang, year=year_hint)
            if movie and movie.get("results"):
                for item in movie.get("results", [])[:5]:
                    item = dict(item)
                    item["media_type"] = "movie"
                    results.append(item)
            tv = tmdb_search_tv(title, language=search_lang, year=year_hint)
            if tv and tv.get("results"):
                for item in tv.get("results", [])[:5]:
                    item = dict(item)
                    item["media_type"] = "tv"
                    results.append(item)

    unique = []
    seen = set()
    for item in results:
        key = (item.get("media_type"), item.get("id"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)

    if not unique:
        api_cache[cache_key] = None
        return None

    ranked = []
    for item in unique:
        score = score_tmdb_candidate(title, desc, year_hint, expected_type, item)
        ranked.append((score, item))

    ranked.sort(key=lambda x: x[0], reverse=True)
    best_score, best_item = ranked[0]
    second_score = ranked[1][0] if len(ranked) > 1 else -999.0
    gap = best_score - second_score
    status = classify_match(best_score, gap, TMDB_STRONG_SCORE, TMDB_CONFIDENT_SCORE, TMDB_MIN_GAP)

    date_str = best_item.get("release_date") or best_item.get("first_air_date") or ""
    result = {
        "provider": "tmdb",
        "status": status,
        "score": round(best_score, 2),
        "gap": round(gap, 2),
        "type": best_item.get("media_type"),
        "year": date_str[:4] if date_str else None,
        "id": best_item.get("id"),
        "localized_title": get_tmdb_localized_title(best_item.get("id"), best_item.get("media_type"), prefer_latam=prefer_latam),
        "title": (best_item.get("title") or best_item.get("name") or "").strip(),
        "original_title": (best_item.get("original_title") or best_item.get("original_name") or "").strip(),
        "overview": (best_item.get("overview") or "").strip(),
    }
    api_cache[cache_key] = result
    return result


def tmdb_find_best_tv_match(show_name, prefer_latam=False):
    result = tmdb_pick_best_match(show_name, expected_type="tv", prefer_latam=prefer_latam, broad=True)
    if not result or result.get("type") != "tv":
        return None
    return result

# =========================
# TVMAZE
# =========================

def tvmaze_search_show_candidates(show_name):
    cache_key = f"tvmaze_search_show_candidates:{normalize_text(show_name)}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    url = "https://api.tvmaze.com/search/shows"
    try:
        r = SESSION.get(url, params={"q": show_name}, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json() or []
            api_cache[cache_key] = data
            return data
    except Exception as e:
        print(f"Error TVMaze search show: {e}", flush=True)

    api_cache[cache_key] = None
    return None


def score_tvmaze_show_candidate(show_name, desc, item):
    show = item.get("show") or {}
    candidate = (show.get("name") or "").strip()
    summary = strip_html(show.get("summary") or "")
    premiered = (show.get("premiered") or "").strip()

    score = 0.0
    score += text_ratio(show_name, candidate) * 68.0

    if normalize_text(show_name) == normalize_text(candidate):
        score += 14.0

    if desc and summary:
        score += overlap_score(desc, summary) * 22.0

    # bonos pequeños por shows activos / con premiered
    if premiered:
        score += 2.0

    return score


def tvmaze_pick_show(show_name, desc=""):
    if not show_name:
        return None

    cache_key = f"tvmaze_pick_show:{normalize_text(show_name)}:{normalize_text(desc)[:120]}"
    if cache_key in api_cache:
        return api_cache[cache_key]

    results = tvmaze_search_show_candidates(show_name)
    if not results:
        api_cache[cache_key] = None
        return None

    ranked = []
    for item in results[:10]:
        score = score_tvmaze_show_candidate(show_name, desc, item)
        ranked.append((score, item))

    ranked.sort(key=lambda x: x[0], reverse=True)
    best_score, best_item = ranked[0]
    second_score = ranked[1][0] if len(ranked) > 1 else -999.0
    gap = best_score - second_score
    status = classify_match(best_score, gap, TVMAZE_STRONG_SCORE, TVMAZE_CONFIDENT_SCORE, TVMAZE_MIN_GAP)

    show = best_item.get("show") or {}
    premiered = (show.get("premiered") or "").strip()
    result = {
        "provider": "tvmaze",
        "status": status,
        "score": round(best_score, 2),
        "gap": round(gap, 2),
        "type": "tv",
        "id": show.get("id"),
        "localized_title": show.get("name"),
        "year": premiered[:4] if re.match(r"^\d{4}-\d{2}-\d{2}$", premiered) else None,
        "summary": strip_html(show.get("summary") or ""),
    }
    api_cache[cache_key] = result
    return result


def score_episode_candidate(ep, wanted_ep_title="", wanted_desc=""):
    score = 0.0
    ep_name = (ep.get("name") or "").strip()
    ep_summary = strip_html(ep.get("summary") or "").strip()

    if wanted_ep_title and ep_name:
        score += text_ratio(wanted_ep_title, ep_name) * 65.0
    elif ep_name:
        score += 5.0

    if wanted_desc and ep_summary:
        score += overlap_score(wanted_desc, ep_summary) * 28.0

    if ep.get("season") is not None:
        score += 3.0
    if ep.get("number") is not None:
        score += 3.0
    return score


def tvmaze_get_episode(show_name, air_date, episode_title="", episode_desc="", season=None, episode=None):
    cache_key = (
        f"tvmaze_ep_v2:{normalize_text(show_name)}:{air_date}:{normalize_text(episode_title)}:"
        f"{normalize_text(episode_desc)[:100]}:{season}:{episode}"
    )
    if cache_key in api_cache:
        return api_cache[cache_key]

    show_match = tvmaze_pick_show(show_name, desc=episode_desc)
    if not show_match or not show_match.get("id"):
        api_cache[cache_key] = None
        return None

    show_id = show_match.get("id")

    try:
        if season is not None and episode is not None:
            ep_url = f"https://api.tvmaze.com/shows/{show_id}/episodebynumber"
            r_ep = SESSION.get(ep_url, params={"season": season, "number": episode}, timeout=API_TIMEOUT)
            if r_ep.status_code == 200:
                ep = r_ep.json() or {}
                ep_score = score_episode_candidate(ep, wanted_ep_title=episode_title, wanted_desc=episode_desc)
                result = {
                    "provider": "tvmaze",
                    "status": "strong" if ep_score >= TVMAZE_CONFIDENT_SCORE else "confident",
                    "score": round(ep_score, 2),
                    "gap": 999.0,
                    "show_id": show_id,
                    "season": ep.get("season"),
                    "episode": ep.get("number"),
                    "name": ep.get("name"),
                    "show_title": show_match.get("localized_title"),
                    "year": show_match.get("year"),
                }
                api_cache[cache_key] = result
                return result

        ep_url = f"https://api.tvmaze.com/shows/{show_id}/episodesbydate"
        r_ep = SESSION.get(ep_url, params={"date": air_date}, timeout=API_TIMEOUT)
        if r_ep.status_code == 200:
            episodes = r_ep.json() or []
            if not episodes:
                api_cache[cache_key] = None
                return None

            ranked = []
            for ep in episodes:
                s = score_episode_candidate(ep, wanted_ep_title=episode_title, wanted_desc=episode_desc)
                ranked.append((s, ep))

            ranked.sort(key=lambda x: x[0], reverse=True)
            best_score, ep = ranked[0]
            second_score = ranked[1][0] if len(ranked) > 1 else -999.0
            gap = best_score - second_score
            status = classify_match(best_score, gap, TVMAZE_STRONG_SCORE, TVMAZE_CONFIDENT_SCORE, TVMAZE_MIN_GAP)

            result = {
                "provider": "tvmaze",
                "status": status,
                "score": round(best_score, 2),
                "gap": round(gap, 2),
                "show_id": show_id,
                "season": ep.get("season"),
                "episode": ep.get("number"),
                "name": ep.get("name"),
                "show_title": show_match.get("localized_title"),
                "year": show_match.get("year"),
            }
            api_cache[cache_key] = result
            return result
    except Exception as e:
        print(f"Error TVMaze episode: {e}", flush=True)

    api_cache[cache_key] = None
    return None


def tvmaze_get_show_main_info(show_name, desc=""):
    return tvmaze_pick_show(show_name, desc=desc)


def tvmaze_get_episode_info(show_name, air_date, episode_title="", episode_desc="", season=None, episode=None):
    return tvmaze_get_episode(
        show_name,
        air_date,
        episode_title=episode_title,
        episode_desc=episode_desc,
        season=season,
        episode=episode,
    )

# =========================
# RESOLUCION JSON EXACTA
# =========================

def resolve_metadata_key(key, prefer_latam=False):
    if not key or ":" not in key:
        return None

    if key.startswith("tvmaze:"):
        parts = key.split(":", 2)
        if len(parts) != 3:
            return None

        _, raw_show_name, raw_date = parts
        show_name = clean_series_query(raw_show_name)
        air_date = raw_date.strip()
        season_num, episode_num = extract_season_episode_numbers(raw_show_name)

        if not show_name or not re.match(r"^\d{4}-\d{2}-\d{2}$", air_date):
            return None

        result = tvmaze_get_episode_info(
            show_name,
            air_date,
            season=season_num,
            episode=episode_num,
        )
        if not result or result.get("status") not in ("strong", "confident"):
            return None
        return {
            "type": "tv",
            "year": result.get("year"),
            "id": result.get("show_id"),
            "localized_title": result.get("show_title"),
            "season": result.get("season"),
            "episode": result.get("episode"),
        }

    if key.startswith("tmdb_best:"):
        payload = key[len("tmdb_best:"):].strip()
        season_num, episode_num = extract_season_episode_numbers(payload)
        show_name = clean_series_query(payload)
        if not show_name:
            return None

        expected_type = "tv" if season_num is not None and episode_num is not None else None
        result = tmdb_pick_best_match(
            show_name,
            expected_type=expected_type,
            prefer_latam=prefer_latam,
            broad=True,
        )
        if not result or result.get("status") not in ("strong", "confident"):
            return None

        output = {
            "type": result.get("type"),
            "year": result.get("year"),
            "id": result.get("id"),
            "localized_title": result.get("localized_title") or result.get("title"),
            "season": season_num,
            "episode": episode_num,
        }
        return output

    return None


def resolve_metadata_map(input_map, prefer_latam=False):
    output = {}
    for key, value in input_map.items():
        if value is not None:
            output[key] = value
            continue
        resolved = resolve_metadata_key(key, prefer_latam=prefer_latam)
        output[key] = resolved
    return output


def complete_null_metadata_entries(input_json_path, output_json_path, prefer_latam=False):
    with open(input_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    completed = resolve_metadata_map(data, prefer_latam=prefer_latam)

    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(completed, f, ensure_ascii=False, indent=2)

    save_cache()
    save_review_queue()
    print(f"JSON completado: {output_json_path}", flush=True)

# =========================
# XML HELPERS
# =========================

def remove_all_children(elem, tag):
    for child in list(elem.findall(tag)):
        elem.remove(child)


def replace_all_title_elements(elem, new_title, prefer_latam=False):
    title_elems = elem.findall("title")
    for t in title_elems:
        elem.remove(t)

    new_title_elem = ET.Element("title")
    new_title_elem.text = new_title
    if prefer_latam:
        new_title_elem.set("lang", "es")
    elem.insert(0, new_title_elem)


def set_or_replace_subtitle(elem, subtitle_text, prefer_latam=False, only_if_missing=False):
    current = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    if only_if_missing and current:
        return

    remove_all_children(elem, "sub-title")

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


def set_or_replace_desc(elem, desc_text, prefer_latam=False, only_if_missing=False):
    current = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)
    if only_if_missing and current:
        return

    remove_all_children(elem, "desc")

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


def get_existing_xml_date_year(elem):
    for d in elem.findall("date"):
        text = (d.text or "").strip()
        m = re.match(r"^(19\d{2}|20\d{2})", text)
        if m:
            return m.group(1)
    return None


def set_date_if_missing(elem, year):
    if not year:
        return
    if get_existing_xml_date_year(elem):
        return
    date_elem = ET.Element("date")
    date_elem.text = str(year)

    children = list(elem)
    insert_index = len(children)
    for i, child in enumerate(children):
        if child.tag in ("title", "sub-title", "desc"):
            insert_index = i + 1
    elem.insert(insert_index, date_elem)


def set_episode_num_if_missing(elem, se_text):
    if not se_text or extract_xmltv_episode_num(elem):
        return
    ep = ET.Element("episode-num")
    ep.set("system", "onscreen")
    ep.text = se_text

    children = list(elem)
    insert_index = len(children)
    for i, child in enumerate(children):
        if child.tag in ("title", "sub-title", "desc", "date"):
            insert_index = i + 1
    elem.insert(insert_index, ep)


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
# LOGICA DE ENRIQUECIMIENTO
# =========================

def choose_conservative_title(existing_title, suggested_title, prefer_latam=False, xml_has_spanish_title=False):
    if not suggested_title:
        return existing_title

    if not existing_title:
        return suggested_title

    candidate = suggested_title
    if (prefer_latam or xml_has_spanish_title) and candidate:
        candidate = spanish_title_case(candidate)

    if ALLOW_MINOR_TEXT_CORRECTIONS and is_minor_text_correction(existing_title, candidate):
        return candidate
    return existing_title


def get_existing_context(elem, prefer_latam=False):
    raw_title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam)
    raw_desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)
    raw_subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)

    clean_title, has_new = extract_new_marker(raw_title)
    clean_title, title_year = extract_year_regex(clean_title)

    title_se = extract_se_regex(clean_title)
    subtitle_se = extract_se_regex(raw_subtitle)
    desc_se = extract_se_regex(raw_desc)
    xml_se = extract_xmltv_episode_num(elem)

    ep_title_from_desc, desc_without_ep_title = split_episode_title_from_desc(raw_desc)
    xml_has_spanish_title = has_spanish_variant(elem, "title")
    existing_xml_year = get_existing_xml_date_year(elem)

    existing_year = existing_xml_year or title_year
    existing_se = xml_se or subtitle_se or title_se or desc_se

    expected_type = infer_media_type_from_context(clean_title, raw_desc, raw_subtitle, has_se=bool(existing_se))

    return {
        "raw_title": raw_title,
        "raw_desc": raw_desc,
        "raw_subtitle": raw_subtitle,
        "clean_title": clean_title.strip(),
        "title_year": title_year,
        "existing_year": existing_year,
        "existing_se": existing_se,
        "has_new": has_new,
        "ep_title_from_desc": ep_title_from_desc,
        "desc_without_ep_title": desc_without_ep_title,
        "xml_has_spanish_title": xml_has_spanish_title,
        "expected_type": expected_type,
    }


def resolve_movie_fill(title, desc, existing_year, prefer_latam=False):
    year_hint = existing_year or extract_candidate_year(desc) or extract_candidate_year(title)
    strict = tmdb_pick_best_match(
        title,
        desc=desc,
        year_hint=year_hint,
        expected_type="movie",
        prefer_latam=prefer_latam,
        broad=False,
    )
    if strict and strict.get("status") in ("strong", "confident"):
        return strict

    broad = tmdb_pick_best_match(
        title,
        desc=desc,
        year_hint=year_hint,
        expected_type="movie",
        prefer_latam=prefer_latam,
        broad=True,
    )
    return broad


def resolve_tv_fill(show_name, desc, air_date, episode_title="", season=None, episode=None, prefer_latam=False):
    tmdb_show = tmdb_pick_best_match(
        show_name,
        desc=desc,
        expected_type="tv",
        prefer_latam=prefer_latam,
        broad=True,
    )

    tvmaze_ep = tvmaze_get_episode(
        show_name,
        air_date,
        episode_title=episode_title,
        episode_desc=desc,
        season=season,
        episode=episode,
    )

    return tmdb_show, tvmaze_ep


def process_programme(
    elem,
    start_time_str,
    prefer_latam=False,
    spanish_season_episode_format=False,
    channel_id=None,
):
    ctx = get_existing_context(elem, prefer_latam=prefer_latam)

    raw_title = ctx["raw_title"]
    raw_desc = ctx["raw_desc"]
    raw_subtitle = ctx["raw_subtitle"]
    clean_title = ctx["clean_title"]
    existing_year = ctx["existing_year"]
    existing_se = ctx["existing_se"]
    has_new = ctx["has_new"]
    xml_has_spanish_title = ctx["xml_has_spanish_title"]
    expected_type = ctx["expected_type"]

    episode_title_hint = (raw_subtitle or ctx["ep_title_from_desc"] or "").strip()
    desc_for_lookup = (ctx["desc_without_ep_title"] or raw_desc or "").strip()
    season_num, episode_num = parse_season_episode_text(existing_se) if existing_se else (None, None)

    final_title = clean_title.strip()
    final_year = existing_year
    final_se = existing_se

    should_translate = prefer_latam and not xml_has_spanish_title
    movie_match = None
    tmdb_show = None
    tvmaze_ep = None

    try:
        air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        air_date = None

    # Películas / contenido sin episodio: completar año y/o localizar título sin cambiar formato final.
    if expected_type == "movie" or (expected_type is None and not existing_se):
        need_year = not final_year
        if need_year or should_translate or not final_title:
            movie_match = resolve_movie_fill(clean_title or raw_title, raw_desc, existing_year, prefer_latam=prefer_latam)
            if movie_match and movie_match.get("status") in ("strong", "confident"):
                if should_translate and movie_match.get("localized_title"):
                    final_title = movie_match.get("localized_title").strip()
                elif not final_title and (movie_match.get("localized_title") or movie_match.get("title")):
                    final_title = (movie_match.get("localized_title") or movie_match.get("title") or "").strip()
                elif raw_title:
                    minor_candidate = movie_match.get("localized_title") or movie_match.get("title")
                    final_title = choose_conservative_title(raw_title, minor_candidate, prefer_latam=prefer_latam, xml_has_spanish_title=xml_has_spanish_title)
                    clean_minor, _ = extract_new_marker(final_title)
                    clean_minor, _ = extract_year_regex(clean_minor)
                    final_title = clean_minor.strip() or clean_title

                if not final_year and movie_match.get("year"):
                    final_year = movie_match.get("year")
            elif need_year:
                mark_for_review(
                    channel_id,
                    start_time_str,
                    raw_title,
                    "movie_match_not_confident",
                    details=movie_match or {"query": clean_title or raw_title},
                )

    # Series / episodios: completar temporada/episodio sin cambiar el modo final de mostrarlo.
    if existing_se or expected_type == "tv" or (movie_match and movie_match.get("type") == "tv"):
        if air_date and (not final_se or not final_year or should_translate or not final_title):
            show_query = clean_series_query(clean_title or final_title or raw_title)
            if show_query:
                tmdb_show, tvmaze_ep = resolve_tv_fill(
                    show_query,
                    desc_for_lookup,
                    air_date,
                    episode_title=episode_title_hint,
                    season=season_num,
                    episode=episode_num,
                    prefer_latam=prefer_latam,
                )

                if should_translate and tmdb_show and tmdb_show.get("status") in ("strong", "confident") and tmdb_show.get("localized_title"):
                    final_title = tmdb_show.get("localized_title").strip()
                elif not final_title and tmdb_show and tmdb_show.get("status") in ("strong", "confident"):
                    final_title = (tmdb_show.get("localized_title") or tmdb_show.get("title") or final_title).strip()
                elif raw_title and tmdb_show:
                    minor_candidate = tmdb_show.get("localized_title") or tmdb_show.get("title")
                    final_title = choose_conservative_title(raw_title, minor_candidate, prefer_latam=prefer_latam, xml_has_spanish_title=xml_has_spanish_title)
                    clean_minor, _ = extract_new_marker(final_title)
                    clean_minor, _ = extract_year_regex(clean_minor)
                    final_title = clean_minor.strip() or clean_title

                if not final_year and tmdb_show and tmdb_show.get("status") in ("strong", "confident") and tmdb_show.get("year"):
                    final_year = tmdb_show.get("year")

                if not final_se and tvmaze_ep and tvmaze_ep.get("status") in ("strong", "confident"):
                    s = tvmaze_ep.get("season")
                    e = tvmaze_ep.get("episode")
                    if s is not None and e is not None:
                        final_se = normalize_season_ep_from_numbers(s, e)
                elif not final_se:
                    details = {
                        "show_query": show_query,
                        "tmdb_show": tmdb_show,
                        "tvmaze_episode": tvmaze_ep,
                        "episode_title_hint": episode_title_hint,
                    }
                    mark_for_review(channel_id, start_time_str, raw_title, "tv_episode_not_confident", details=details)

    if not final_title:
        final_title = clean_title.strip() or raw_title

    if final_title and (prefer_latam or xml_has_spanish_title or should_translate):
        final_title = spanish_title_case(final_title)

    if not existing_year and final_year:
        set_date_if_missing(elem, final_year)

    display_title = final_title
    is_series = bool(final_se) or bool(existing_se) or bool((tmdb_show and tmdb_show.get("type") == "tv") or (movie_match and movie_match.get("type") == "tv"))

    if final_se:
        display_se = format_season_episode_display(
            final_se,
            use_spanish=spanish_season_episode_format
        )
        display_title += f" | {display_se}"

    if has_new:
        display_title += " ᴺᵉʷ"

    return display_title, is_series


# =========================
# I/O PRINCIPAL
# =========================

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

                                process_programme(
                                    elem,
                                    start,
                                    prefer_latam=prefer_latam,
                                    spanish_season_episode_format=spanish_season_episode_format,
                                    channel_id=ch_id,
                                )

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
        save_review_queue()

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
