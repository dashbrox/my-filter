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
ar bo ca co cl cr do ec sv gt
hn it mx py pe es gb us uy ve 
""".split()

MITV_BASE = "https://github.com/dashbrox/otherepg/raw/refs/heads/master/guides/mi.tv"

EPG_MITV_AR = f"{MITV_BASE}/ar.xml"
EPG_MITV_CL = f"{MITV_BASE}/cl.xml"
EPG_MITV_CO = f"{MITV_BASE}/co.xml"
EPG_MITV_GT = f"{MITV_BASE}/gt.xml"
EPG_MITV_HN = f"{MITV_BASE}/hn.xml"
EPG_MITV_MX = f"{MITV_BASE}/mx.xml"
EPG_MITV_PE = f"{MITV_BASE}/pe.xml"
EPG_MITV_PY = f"{MITV_BASE}/py.xml"
EPG_MITV_SV = f"{MITV_BASE}/sv.xml"

EPG_URLS = [f"https://iptv-epg.org/files/epg-{code}.xml" for code in EPG_COUNTRY_CODES]
EPG_URLS += [
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://helmerluzo.github.io/RakutenTV_HL/epg/RakutenTV.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    EPG_MITV_AR,
    EPG_MITV_CL,
    EPG_MITV_CO,
    EPG_MITV_GT,
    EPG_MITV_HN,
    EPG_MITV_MX,
    EPG_MITV_PE,
    EPG_MITV_PY,
    EPG_MITV_SV,
]
EPG_URLS = list(dict.fromkeys(EPG_URLS))

CHANNELS_FILE = "channels.txt"
OUTPUT_FILE = "guia.xml.gz"
TEMP_INPUT = "temp_input.xml"
TEMP_OUTPUT = "output_temp.xml"
CACHE_FILE = "api_cache.json"

CACHE_MAX_AGE_DAYS = 5
CACHE_MAX_AGE_SECONDS = CACHE_MAX_AGE_DAYS * 24 * 60 * 60

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

FORCE_SEASON_EPISODE_IN_TITLE_ONLY = True
REMOVE_SUBTITLE_ENTIRELY = False

DOWNLOAD_TIMEOUT = (20, 120)
API_TIMEOUT = (5, 10)
MAX_RETRIES = 2
USER_AGENT = "xmltv-title-normalizer/1.7-Universal"

LATAM_FEED_CODES = {
    "ar", "bo", "br", "cl", "co", "cr", "do", "ec", "sv",
    "gt", "hn", "mx", "ni", "pa", "py", "pe", "uy", "ve"
}

IBERO_SPANISH_CODES = LATAM_FEED_CODES | {"es"}
TVMAZE_AUTHORITATIVE_COUNTRY_CODES = {"us", "gb", "ca"}

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

KNOWN_ACRONYMS = {
    "TV", "HD", "SD", "UHD", "FHD", "4K", "8K", "3D",
    "HBO", "CNN", "BBC", "FBI", "CIA", "CSI", "NCIS",
    "SWAT", "UFC", "USA", "UK", "FX", "AXN", "AMC",
    "TNT", "TCM", "MTV", "VH1", "DW", "DAZN",
    "EPG", "TMDB", "TVMAZE",
    "AM", "PM"
}

CHANNEL_TIME_OFFSETS = {
    "Lifetime.ar": +11,
    "WarnerChannel.gt": -2,
}

EPG_ES1 = "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz"
EPG_RAKUTEN = "https://helmerluzo.github.io/RakutenTV_HL/epg/RakutenTV.xml.gz"
EPG_IT = "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz"
EPG_RAKUTEN2 = "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz"
EPG_MITV = EPG_MITV_GT

CHANNEL_SOURCE_RULES = {
    "M+.Estrenos.es": [EPG_ES1],
    "DAZN.F1.es": [EPG_ES1],
    "tennis-plus": [EPG_RAKUTEN],
    "fashion-tv": [EPG_RAKUTEN],
    "SuperTennis.HD.it": [EPG_IT],
    "UK:.Tennis.Channel.be": [EPG_RAKUTEN2],
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

def now_ts():
    return int(time.time())

def purge_old_cache():
    global api_cache
    cutoff = now_ts() - CACHE_MAX_AGE_SECONDS
    cleaned = {}

    for key, value in api_cache.items():
        if isinstance(value, dict) and "ts" in value and "data" in value:
            try:
                if int(value["ts"]) >= cutoff:
                    cleaned[key] = value
            except Exception:
                pass

    removed = len(api_cache) - len(cleaned)
    api_cache = cleaned

    if removed:
        print(f"Caché limpiado: {removed} entradas expiradas.", flush=True)

def cache_get(key):
    entry = api_cache.get(key)
    if not isinstance(entry, dict):
        return None
    if "ts" not in entry or "data" not in entry:
        return None
    try:
        if int(entry["ts"]) < now_ts() - CACHE_MAX_AGE_SECONDS:
            api_cache.pop(key, None)
            return None
    except Exception:
        api_cache.pop(key, None)
        return None
    return entry["data"]

def cache_set(key, data):
    if data is None and key.startswith("tmdb_best_v2:"):
        match = re.search(r"tmdb_best_v2:([^:]+):", key)
        if match:
            title = match.group(1).lower()
            NON_INDEXABLE = re.compile(r"\b(alto frontera|cam alert|patrulla policial|noticiero|infomercial|televenta|paid programming|highlights|resumen|episodio \d+|temporada \d+)\b", re.IGNORECASE)
            if NON_INDEXABLE.search(title):
                return
    api_cache[key] = {
        "ts": now_ts(),
        "data": data
    }

if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            api_cache = json.load(f)
        purge_old_cache()
        print(f"Caché cargado: {len(api_cache)} entradas vigentes.", flush=True)
    except Exception:
        api_cache = {}

def save_cache():
    purge_old_cache()
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(api_cache, f, ensure_ascii=False, indent=2)

# =========================
# ALIAS MANUALES DE CANALES
# =========================

CHANNEL_EQUIVALENCE = {}

def normalize_mitv_channel_id(ch_id):
    if not ch_id:
        return ch_id
    ch_id = ch_id.strip()
    match = re.match(r"^([a-z]{2})#(.+)$", ch_id, re.IGNORECASE)
    if match:
        country = match.group(1).lower()
        rest = match.group(2).strip().replace("-", "")
        return f"{country}.{rest}"
    return ch_id

def build_channel_alias_map():
    alias_to_canonical = {}
    for canonical_id, aliases in CHANNEL_EQUIVALENCE.items():
        canonical_norm = normalize_mitv_channel_id(canonical_id)
        alias_to_canonical[canonical_norm] = canonical_norm
        for alias in aliases:
            alias_norm = normalize_mitv_channel_id(alias)
            alias_to_canonical[alias_norm] = canonical_norm
    return alias_to_canonical

CHANNEL_ALIAS_MAP = build_channel_alias_map()

def canonical_channel_id(ch_id):
    if not ch_id:
        return ch_id
    ch_id = normalize_mitv_channel_id(ch_id)
    return CHANNEL_ALIAS_MAP.get(ch_id, ch_id)

# =========================
# UTILS TEXTO Y SIMILITUD
# =========================

def normalize_text(text):
    if not text:
        return ""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())

def get_channel_country_code(ch_id):
    if not ch_id:
        return None
    s = normalize_mitv_channel_id(ch_id).lower().strip()
    match = re.match(r"^([a-z]{2})\.", s)
    if match:
        return match.group(1)
    match = re.search(r'\.([a-z]{2})(?:\.|$|hd|sd|\d)', s)
    if match:
        return match.group(1)
    return None

def tokenize_channel_id(ch_id):
    if not ch_id:
        return set()
    ch_id = normalize_mitv_channel_id(ch_id)
    country = get_channel_country_code(ch_id)
    s = ch_id.lower()
    s = re.sub(r'\([^)]*\)', ' ', s)
    if country:
        s = re.sub(rf'^{country}\.', ' ', s)
        s = re.sub(rf'\.{country}(?:\.|$)', ' ', s)
    s = s.replace('.', ' ').replace('_', ' ').replace('-', ' ')
    tokens = s.split()
    noise = {'hd', 'sd', 'fhd', '4k', '1080p', '720p'}
    tokens = [t for t in tokens if t not in noise]
    if country:
        tokens.append(country)
    return set(tokens)

def normalize_channel_id_for_matching(ch_id):
    if not ch_id:
        return ""
    canonical = canonical_channel_id(ch_id)
    if canonical != ch_id:
        return canonical.lower().strip()
    country = get_channel_country_code(ch_id) or ""
    generic_words = {'channel', 'tv', 'television', 'network', 'cable', 'satelital'}
    tokens = tokenize_channel_id(ch_id)
    nums = {t for t in tokens if t.isdigit()}
    core = sorted(tokens - generic_words - nums - ({country} if country else set()))
    parts = core[:]
    if nums:
        parts.extend(sorted(nums))
    if country:
        parts.append(country)
    return " ".join(parts).strip()

def is_same_channel(id1, id2):
    c1 = canonical_channel_id(id1)
    c2 = canonical_channel_id(id2)
    if c1 and c2 and c1 == c2:
        return True, 1.0
    tokens1 = tokenize_channel_id(id1)
    tokens2 = tokenize_channel_id(id2)
    if not tokens1 or not tokens2:
        return False, 0.0
    country1 = get_channel_country_code(id1)
    country2 = get_channel_country_code(id2)
    if country1 and country2:
        if country1 != country2:
            return False, 0.0
    else:
        return False, 0.0
    nums1 = {t for t in tokens1 if t.isdigit()}
    nums2 = {t for t in tokens2 if t.isdigit()}
    if nums1 and nums2:
        if nums1 != nums2:
            return False, 0.0
    elif nums1 or nums2:
        return False, 0.0
    generic_words = {'channel', 'tv', 'television', 'network', 'cable', 'satelital'}
    core1 = tokens1 - generic_words - nums1 - {country1}
    core2 = tokens2 - generic_words - nums2 - {country2}
    if not core1 or not core2:
        return False, 0.0
    if core1 == core2:
        return True, 1.0
    allowed_extra_tokens = {'bros', 'brothers', 'intl', 'international'}
    extras = (core1 - core2) | (core2 - core1)
    if (core1.issubset(core2) or core2.issubset(core1)) and extras <= allowed_extra_tokens:
        return True, 0.95
    return False, 0.0

def calculate_channel_similarity(id1, id2):
    match, score = is_same_channel(id1, id2)
    if match:
        return score
    norm1 = normalize_channel_id_for_matching(id1)
    norm2 = normalize_channel_id_for_matching(id2)
    if not norm1 or not norm2:
        return 0.0
    return SequenceMatcher(None, norm1, norm2).ratio() * 0.5

def text_similarity(a, b):
    na = normalize_text(a)
    nb = normalize_text(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    ratio = SequenceMatcher(None, na, nb).ratio()
    if na in nb or nb in na:
        ratio = max(ratio, min(len(na), len(nb)) / max(len(na), len(nb)))
    return ratio

TITLE_CASE_OVERRIDES = {
    normalize_text("DTF St. Louis"): "DTF St. Louis",
    normalize_text("M3GAN 2.0"): "M3GAN 2.0",
    normalize_text("e.t. el extraterrestre"): "E.T. el Extraterrestre",
    normalize_text("e.t"): "E.T.",
    normalize_text("kung fu panda"): "Kung Fu Panda",
    normalize_text("escarabajo azul"): "Escarabajo Azul",
    normalize_text("xxx"): "xXx",
    normalize_text("xxx triple x"): "xXx - Triple X",
    normalize_text("g.i. joe"): "G.I. Joe",
    normalize_text("g.i. joe: snake eyes"): "G.I. Joe: Snake Eyes",
    normalize_text("georgie y mandy - su primer matrimonio"): "Georgie y Mandy: Primer matrimonio",
    normalize_text("georgie y mandy: su primer matrimonio"): "Georgie y Mandy: Primer matrimonio",
    normalize_text("georgie and mandy's first marriage"): "Georgie y Mandy: Primer matrimonio",
    normalize_text("the big bang theory"): "The Big Bang Theory",
    normalize_text("la teoría del big bang"): "La Teoría del Big Bang",
    normalize_text("dos hombres y medio"): "Dos Hombres y Medio",
    normalize_text("cobra kai"): "Cobra Kai",
    normalize_text("karate kid:leyendas"): "Karate Kid: Leyendas",
    normalize_text("karate kid"): "Karate Kid",
    normalize_text("los juegos del hambre:sinsajo"): "Los Juegos del Hambre: Sinsajo",
    normalize_text("los juegos del hambre"): "Los Juegos del Hambre",
    normalize_text("el hombre araña"): "El Hombre Araña",
    normalize_text("jurassic world:el reino caído"): "Jurassic World: El Reino Caído",
    normalize_text("hombre a medias"): "Hombre a Medias",
    normalize_text("el regreso"): "El Regreso",
    normalize_text("cumbres borrascosas"): "Cumbres Borrascosas",
    normalize_text("tierra de zombies"): "Tierra de Zombies",
}

def strip_accents(text):
    if not text:
        return ""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")

def should_replace_with_localized_title(source_title, localized_title):
    if not source_title or not localized_title:
        return False
    src_norm = normalize_text(source_title)
    loc_norm = normalize_text(localized_title)
    if src_norm != loc_norm:
        return False
    src_clean = " ".join(source_title.split()).strip().lower()
    loc_clean = " ".join(localized_title.split()).strip().lower()
    if src_clean == loc_clean:
        return False
    if strip_accents(src_clean) == strip_accents(loc_clean):
        return True
    return False

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
    if "/guides/mi.tv/" in u:
        m = re.search(r"/([a-z]{2})\.xml$", u)
        if m:
            return m.group(1)
    country_aliases = {
        "peru": "pe", "argentina": "ar", "mexico": "mx", "colombia": "co",
        "chile": "cl", "uruguay": "uy", "venezuela": "ve", "ecuador": "ec",
        "bolivia": "bo", "paraguay": "py", "panama": "pa", "costa-rica": "cr",
        "costarica": "cr", "guatemala": "gt", "honduras": "hn", "nicaragua": "ni",
        "elsalvador": "sv", "salvador": "sv", "dominican": "do", "dominicana": "do",
        "spain": "es", "espana": "es", "españa": "es",
    }
    for alias, code in country_aliases.items():
        if alias in u:
            return code
    return None

def is_latam_feed(url):
    code = get_feed_code(url)
    return code in LATAM_FEED_CODES

def use_spanish_season_episode_format(url):
    code = get_feed_code(url)
    return code in IBERO_SPANISH_CODES

def should_use_tvmaze_authoritative(url, channel_id):
    channel_code = get_channel_country_code(channel_id)
    if channel_code in TVMAZE_AUTHORITATIVE_COUNTRY_CODES:
        return True
    feed_code = get_feed_code(url)
    if not channel_code and feed_code in TVMAZE_AUTHORITATIVE_COUNTRY_CODES:
        return True
    return False

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
    stripped = text.strip()
    if re.fullmatch(r"(19\d{2}|20\d{2})", stripped):
        return text, None
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
    if not text:
        return None
    patterns = [
        (r"\bseason\s*(\d+)\s*[,:\-]?\s*episode\s*(\d+)\b", False),
        (r"\btemporada\s*(\d+)\s*[,:\-]?\s*(?:episodio|capitulo|capítulo|ep|cap)\s*(\d+)\b", False),
        (r"\btemp\.?\s*(\d+)\s*[,:\-]?\s*(?:ep\.?|episodio|cap\.?|capitulo|capítulo)\s*(\d+)\b", False),
        (r"\b(?:episode|ep)\.?\s*(\d+)\s*[,:\-]?\s*season\s*(\d+)\b", True),
        (r"\b(?:episodio|capitulo|capítulo|ep\.?|cap\.?)\s*(\d+)\s*[,:\-]?\s*temporada\s*(\d+)\b", True),
        (r"\b(?:ep\.?|episodio|cap\.?|capitulo|capítulo)\s*(\d+)\s*[,:\-]?\s*temp\.?\s*(\d+)\b", True),
    ]
    for pattern, reverse_groups in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            continue
        g1 = m.group(1)
        g2 = m.group(2)
        if reverse_groups:
            return normalize_season_ep_from_numbers(g2, g1)
        return normalize_season_ep_from_numbers(g1, g2)
    return None

def normalize_season_ep(text):
    if not text:
        return None
    patterns = [
        r"\bS\s*(\d+)\s*E\s*(\d+)\b",
        r"\bS\s*(\d+)\s*[:.\-]?\s*E\s*(\d+)\b",
        r"\bS(\d{1,2})E(\d{1,2})\b",
        r"\bT\s*(\d+)\s*E\s*(\d+)\b",
        r"\bT\s*(\d+)\s*[:.\-]?\s*E\s*(\d+)\b",
        r"\bT(\d{1,2})E(\d{1,2})\b",
        r"\b(\d+)\s*x\s*(\d+)\b",
        r"\b(\d+)\s*-\s*(\d+)\b",
        r"\bTemp\.?\s*(\d+)\s*Ep\.?\s*(\d+)\b",
        r"\bTemporada\s*(\d+)\s*(?:Episodio|Cap[ií]tulo|Ep\.?|Cap\.?)\s*(\d+)\b",
        r"\bSeason\s*(\d+)\s*Episode\s*(\d+)\b",
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
        priority = ["es-419", "es-mx", "es-ar", "es-co", "es-cl", "es-pe", "es-us", "es", ""]
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
        r"^\s*\(?\s*S\d{1,2}E\d{1,2}\s*\)?\s*[:.\-–—]?\s*",
        r"^\s*\(?\s*T\d{1,2}E\d{1,2}\s*\)?\s*[:.\-–—]?\s*",
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

def strip_leading_se_from_desc(desc_text):
    if not desc_text:
        return ""
    text = desc_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    first_line, sep, rest = text.partition("\n")
    patterns = [
        r"^\s*\(?\s*[ST]\s*\d+\s*[:.\-]?\s*E\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*S\d{1,2}E\d{1,2}\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*T\d{1,2}E\d{1,2}\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*\d+\s*x\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*Season\s*\d+\s*[,:\-]?\s*Episode\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*Temporada\s*\d+\s*[,:\-]?\s*(?:Episodio|Capitulo|Capítulo)\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*Temp\.?\s*\d+\s*[,:\-]?\s*(?:Ep\.?|Cap\.?|Episodio|Capitulo|Capítulo)\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
    ]
    cleaned_first = first_line
    for pattern in patterns:
        new_first = re.sub(pattern, "", cleaned_first, flags=re.IGNORECASE).strip()
        if new_first != cleaned_first:
            cleaned_first = new_first
            break
    if sep:
        return f"{cleaned_first}\n{rest.strip()}".strip()
    return cleaned_first

def strip_se_from_title(text):
    if not text:
        return ""
    out = " ".join(text.strip().split())
    patterns = [
        r"^\s*\(?\s*[ST]\s*\d+\s*[:.\-]?\s*E\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*S\d{1,2}E\d{1,2}\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*T\d{1,2}E\d{1,2}\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*\d+\s*x\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*Season\s*\d+\s*[,:\-]?\s*Episode\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*Temporada\s*\d+\s*[,:\-]?\s*(?:Episodio|Cap[ií]tulo)\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"^\s*\(?\s*Temp\.?\s*\d+\s*[,:\-]?\s*(?:Ep\.?|Cap\.?)\s*\d+\s*\)?\s*[:.\-–—|]?\s*",
        r"\s*\|\s*\(?\s*[ST]\s*\d+\s*[:.\-]?\s*E\s*\d+\s*\)?\s*$",
        r"\s*\|\s*\(?\s*S\d{1,2}E\d{1,2}\s*\)?\s*$",
        r"\s*\|\s*\(?\s*T\d{1,2}E\d{1,2}\s*\)?\s*$",
        r"\s*[-–—]\s*\(?\s*[ST]\s*\d+\s*[:.\-]?\s*E\s*\d+\s*\)?\s*$",
        r"\s*\|\s*\(?\s*\d+\s*x\s*\d+\s*\)?\s*$",
        r"\s*[-–—]\s*\(?\s*\d+\s*x\s*\d+\s*\)?\s*$",
        r"\s*\|\s*\(?\s*Season\s*\d+\s*[,:\-]?\s*Episode\s*\d+\s*\)?\s*$",
        r"\s*\|\s*\(?\s*Temporada\s*\d+\s*[,:\-]?\s*(?:Episodio|Cap[ií]tulo)\s*\d+\s*\)?\s*$",
        r"\s*\|\s*\(?\s*Temp\.?\s*\d+\s*[,:\-]?\s*(?:Ep\.?|Cap\.?)\s*\d+\s*\)?\s*$",
        r"\s+\(?\s*[ST]\s*\d+\s*[:.\-]?\s*E\s*\d+\s*\)?\s*$",
        r"\s+\(?\s*S\d{1,2}E\d{1,2}\s*\)?\s*$",
        r"\s+\(?\s*T\d{1,2}E\d{1,2}\s*\)?\s*$",
        r"\s+\(?\s*\d+\s*x\s*\d+\s*\)?\s*$",
        r"\s+\(?\s*Season\s*\d+\s*[,:\-]?\s*Episode\s*\d+\s*\)?\s*$",
        r"\s+\(?\s*Temporada\s*\d+\s*[,:\-]?\s*(?:Episodio|Cap[ií]tulo)\s*\d+\s*\)?\s*$",
        r"\s+\(?\s*Temp\.?\s*\d+\s*[,:\-]?\s*(?:Ep\.?|Cap\.?)\s*\d+\s*\)?\s*$",
    ]
    changed = True
    while changed:
        changed = False
        for pattern in patterns:
            new_out = re.sub(pattern, "", out, flags=re.IGNORECASE).strip()
            if new_out != out:
                out = new_out
                changed = True
    return " ".join(out.split()).strip()

def remove_episode_title_from_series_title(title_text, subtitle_text=""):
    if not title_text:
        return ""
    base = " ".join(title_text.strip().split())
    subtitle = " ".join((subtitle_text or "").strip().split())
    if not subtitle:
        return base
    norm_sub = normalize_text(subtitle)
    if not norm_sub:
        return base
    separators = [":", "|", "-", "–", "—"]
    for sep in separators:
        if sep in base:
            left, right = base.rsplit(sep, 1)
            left = left.strip()
            right = right.strip()
            if normalize_text(right) == norm_sub and left:
                return left
    return base

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

def is_dotted_acronym(word):
    if not word:
        return False
    return bool(re.fullmatch(r"(?:[A-Za-zÁÉÍÓÚÜÑ]\.){2,}[A-Za-zÁÉÍÓÚÜÑ]?\.?", word))

def normalize_dotted_acronym(word):
    letters = re.findall(r"[A-Za-zÁÉÍÓÚÜÑ]", word or "")
    if len(letters) >= 2:
        return ".".join(letter.upper() for letter in letters) + "."
    return word

def normalize_time_abbreviation(word):
    if not word:
        return word
    cleaned = word.strip()
    m = re.fullmatch(r"(?i)(a|p)\.?\s*m\.?", cleaned)
    if m:
        return f"{m.group(1).upper()}.M."
    return word

def normalize_plain_acronym(word):
    compact = re.sub(r"[^0-9A-Za-zÁÉÍÓÚÜÑ]", "", word or "")
    return compact.upper()

def should_preserve_allcaps_token(word):
    if not word:
        return False
    if is_dotted_acronym(word):
        return True
    compact = normalize_plain_acronym(word)
    if not compact:
        return False
    if compact in KNOWN_ACRONYMS:
        return True
    if re.fullmatch(r"(?:\d+[A-ZÁÉÍÓÚÜÑ]+|[A-ZÁÉÍÓÚÜÑ]+\d+)", compact):
        return True
    return False

# =========================
# CAPA 1: DETECCIÓN DE IDIOMA
# =========================
def detect_title_language(text):
    if not text:
        return "unknown"
    english_indicators = {"the", "and", "of", "in", "to", "for", "with", "on", "at", "by"}
    spanish_indicators = {"el", "la", "los", "las", "de", "del", "y", "en", "por", "para", "con"}
    tokens = normalize_text(text).split()
    if not tokens:
        return "unknown"
    en_score = sum(1 for t in tokens if t in english_indicators)
    es_score = sum(1 for t in tokens if t in spanish_indicators)
    if en_score > es_score:
        return "en"
    elif es_score > en_score:
        return "es"
    return "unknown"

# =========================
# CAPA 2: CAPITALIZACIÓN INTELIGENTE
# =========================
def smart_title_case(text, prefer_latam=False, detected_lang=None):
    if not text:
        return ""
    # Normalizar espacio después de dos puntos
    text = re.sub(r':(\S)', r': \1', text)
    
    if detected_lang is None:
        detected_lang = detect_title_language(text)
    
    # Preservar capitalización original si es inglés y parece correcto
    if detected_lang == "en" and not prefer_latam:
        words = text.split()
        if words and words[0][0].isupper() and any(w[0].isupper() for w in words[1:4] if len(w) > 3):
            return text.strip()
    
    segments = re.split(r"(:|\s-\s|–)", text)
    final_parts = []
    for part in segments:
        if part in [":", " - ", "–"]:
            final_parts.append(part)
            continue
        words = part.split()
        if not words:
            continue
        capitalized_words = []
        for idx, word in enumerate(words):
            if should_preserve_allcaps_token(word) or has_special_uppercase_pattern(word):
                capitalized_words.append(word)
                continue
            if re.fullmatch(r"[IVXLCDM]+", word):
                capitalized_words.append(word)
                continue
            if "-" in word:
                subparts = word.split("-")
                rebuilt = [smart_title_case(sub, prefer_latam, detected_lang) for sub in subparts]
                capitalized_words.append("-".join(rebuilt))
                continue
            if idx == 0:
                capitalized_words.append(word[:1].upper() + word[1:].lower())
                continue
            if idx == len(words) - 1 and len(word) > 2:
                capitalized_words.append(word[:1].upper() + word[1:].lower())
                continue
            low = word.lower()
            minor_set = SPANISH_MINOR_WORDS if detected_lang != "en" else {"a", "an", "and", "the", "of", "in", "to", "for"}
            if low in minor_set:
                capitalized_words.append(low)
            else:
                capitalized_words.append(word[:1].upper() + word[1:].lower())
        final_parts.append(" ".join(capitalized_words))
    result = "".join(final_parts)
    return re.sub(r"\s+", " ", result).strip()

def split_episode_title_from_desc(desc_text):
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
    if not sep:
        return None, text
    if len(ep_title) > 100 or len(ep_title.split()) > 12:
        return None, text
    remaining = rest.strip()
    return ep_title, remaining

def first_line(text):
    if not text:
        return ""
    return text.replace("\r\n", "\n").replace("\r", "\n").split("\n", 1)[0].strip()

def strip_html_tags(text):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    return " ".join(text.split()).strip()

def has_special_uppercase_pattern(token):
    if not token:
        return False
    stripped = re.sub(r'^[\"“”¿¡(\[]+|[\"”?!:;.,)\]]+$', "", token)
    if not stripped:
        return False
    if is_dotted_acronym(stripped):
        return True
    letters = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑ]", "", stripped)
    if len(letters) < 2:
        return False
    upper_count = sum(1 for c in letters if c.isupper())
    if stripped.isupper() and upper_count >= 2:
        return True
    if re.search(r"[A-ZÁÉÍÓÚÜÑ].*\d|\d.*[A-ZÁÉÍÓÚÜÑ]", stripped) and upper_count >= 2:
        return True
    if upper_count >= 2 and stripped != stripped[:1].upper() + stripped[1:].lower():
        return True
    return False

def preserve_special_casing(base_title, canonical_title):
    if not base_title or not canonical_title:
        return base_title
    if normalize_text(base_title) != normalize_text(canonical_title):
        return base_title
    base_tokens = base_title.split()
    canon_tokens = canonical_title.split()
    if len(base_tokens) != len(canon_tokens):
        return base_title
    merged = []
    for bt, ct in zip(base_tokens, canon_tokens):
        bt_norm = normalize_text(bt)
        ct_norm = normalize_text(ct)
        if bt_norm != ct_norm:
            merged.append(bt)
            continue
        if has_special_uppercase_pattern(ct):
            merged.append(ct)
        else:
            merged.append(bt)
    return " ".join(merged)

def apply_title_case_overrides(title):
    if not title:
        return title
    key = normalize_text(title)
    override = TITLE_CASE_OVERRIDES.get(key)
    if override:
        return override
    return title

def is_ambiguous_title(title):
    tokens = [t for t in normalize_text(title).split() if len(t) > 2]
    return len(tokens) <= 2

def extract_candidate_year(item):
    date_str = item.get("release_date") or item.get("first_air_date") or ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str[:4]
    return None

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
    ch_id = canonical_channel_id(elem.get("channel"))
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
    canonical_id = canonical_channel_id(channel_id)
    allowed_sources = CHANNEL_SOURCE_RULES.get(canonical_id)
    if not allowed_sources:
        return True
    return source_url in allowed_sources

# =========================
# XML HELPERS
# =========================

def clone_element(elem):
    return ET.fromstring(ET.tostring(elem, encoding="utf-8"))

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

def normalize_subtitle_and_desc(elem, prefer_latam=False, is_series=False, preferred_subtitle=None, preferred_desc=None):
    current_subtitle = preferred_subtitle if preferred_subtitle and preferred_subtitle.strip() else pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    current_desc = preferred_desc if preferred_desc and preferred_desc.strip() else pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)
    current_subtitle = (current_subtitle or "").strip()
    current_desc = strip_html_tags((current_desc or "").strip())
    extracted_ep_title = None
    cleaned_desc = current_desc.strip() if current_desc else ""
    ep_title_from_desc, desc_without_ep_title = split_episode_title_from_desc(current_desc)
    if ep_title_from_desc:
        extracted_ep_title = ep_title_from_desc
        cleaned_desc = (desc_without_ep_title or "").strip()
    else:
        cleaned_desc = (current_desc or "").strip()
    cleaned_desc = strip_leading_se_from_desc(cleaned_desc)
    chosen_subtitle = current_subtitle.strip() if current_subtitle else ""
    if not chosen_subtitle and extracted_ep_title:
        chosen_subtitle = extracted_ep_title
    chosen_subtitle = strip_leading_se_from_text(chosen_subtitle).strip()
    should_spanish_case_subtitle = bool(chosen_subtitle) and (prefer_latam or has_spanish_variant(elem, "sub-title") or extracted_ep_title is not None)
    if chosen_subtitle and should_spanish_case_subtitle:
        chosen_subtitle = smart_title_case(chosen_subtitle, prefer_latam=prefer_latam)
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
# TMDB
# =========================

def tmdb_search_multi(title, language, year=None):
    if not TMDB_API_KEY or not title:
        return None
    cache_key = f"tmdb_search:{normalize_text(title)}:{language}:{year or 'sin_año'}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    url = "https://api.themoviedb.org/3/search/multi"
    params = {"api_key": TMDB_API_KEY, "query": title, "language": language}
    if year:
        params["year"] = year
    try:
        r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            cache_set(cache_key, data)
            return data
    except Exception as e:
        print(f"Error TMDB search: {e}", flush=True)
    cache_set(cache_key, None)
    return None

def get_tmdb_latam_title_via_translations(tmdb_id, media_type):
    if not TMDB_API_KEY or not tmdb_id or media_type not in ("movie", "tv"):
        return None
    cache_key = f"tmdb_translations:{media_type}:{tmdb_id}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    endpoint = "movie" if media_type == "movie" else "tv"
    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}/translations"
    params = {"api_key": TMDB_API_KEY}
    try:
        r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            translations = data.get("translations", [])
            priority = [
                ("es", "MX"), ("es", "419"), ("es", "AR"), ("es", "CO"),
                ("es", "CL"), ("es", "PE"), ("es", "US"), ("es", "ES")
            ]
            for iso_639, iso_3166 in priority:
                for t in translations:
                    if t.get("iso_639_1") == iso_639 and t.get("iso_3166_1") == iso_3166:
                        title = (t.get("data", {}).get("title") or "").strip()
                        if title:
                            cache_set(cache_key, title)
                            return title
            for t in translations:
                if t.get("iso_639_1") == "es":
                    title = (t.get("data", {}).get("title") or "").strip()
                    if title:
                        cache_set(cache_key, title)
                        return title
    except Exception as e:
        print(f"Error TMDB translations: {e}", flush=True)
    cache_set(cache_key, None)
    return None

def get_tmdb_localized_title(tmdb_id, media_type, prefer_latam=False):
    if not TMDB_API_KEY or not tmdb_id or media_type not in ("movie", "tv"):
        return None
    if prefer_latam:
        latam_title = get_tmdb_latam_title_via_translations(tmdb_id, media_type)
        if latam_title:
            return latam_title
    lang_chain = ["es-MX", "es-419", "es-AR", "es-CO", "es"] if prefer_latam else ["es-ES", "es"]
    for lang in lang_chain:
        cache_key = f"tmdb_title:{media_type}:{tmdb_id}:{lang}"
        cached = cache_get(cache_key)
        if cached is not None:
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
                    cache_set(cache_key, title)
                    return title
        except Exception as e:
            print(f"Error TMDB localized title: {e}", flush=True)
        cache_set(cache_key, None)
    return None

def get_tmdb_latam_overview(tmdb_id, media_type):
    if not TMDB_API_KEY or not tmdb_id or media_type not in ("movie", "tv"):
        return None
    cache_key = f"tmdb_overview_latam:{media_type}:{tmdb_id}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    endpoint = "movie" if media_type == "movie" else "tv"
    url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "es-MX"}
    try:
        r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            overview = (data.get("overview") or "").strip()
            cache_set(cache_key, overview)
            return overview
    except Exception as e:
        print(f"Error TMDB overview LATAM: {e}", flush=True)
    cache_set(cache_key, None)
    return None

def get_tmdb_episode_data(tv_id, season_num, episode_num):
    """Obtiene título y sinopsis de un episodio. Prioridad: ES -> EN -> None."""
    if not TMDB_API_KEY or not tv_id:
        return None

    cache_key_es = f"tmdb_ep_data:{tv_id}:{season_num}:{episode_num}:es-MX"
    cached = cache_get(cache_key_es)
    if cached is not None:
        return cached

    url = f"https://api.themoviedb.org/3/tv/{tv_id}/season/{season_num}/episode/{episode_num}"
    params_es = {"api_key": TMDB_API_KEY, "language": "es-MX"}

    try:
        r = SESSION.get(url, params=params_es, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            title = (data.get("name") or "").strip()
            overview = (data.get("overview") or "").strip()
            if title or overview:
                result = {"episode_title": title, "overview": overview, "_lang": "es"}
                cache_set(cache_key_es, result)
                return result
    except Exception as e:
        print(f"Error TMDB episode ES: {e}", flush=True)

    cache_key_en = f"tmdb_ep_en:{tv_id}:{season_num}:{episode_num}"
    cached_en = cache_get(cache_key_en)
    if cached_en is not None:
        return cached_en

    params_en = {"api_key": TMDB_API_KEY, "language": "en"}
    try:
        r = SESSION.get(url, params=params_en, timeout=API_TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            result = {
                "episode_title": (data.get("name") or "").strip(),
                "overview": (data.get("overview") or "").strip(),
                "_lang": "en"
            }
            cache_set(cache_key_en, result)
            return result
    except Exception as e:
        print(f"Error TMDB episode EN: {e}", flush=True)

    cache_set(cache_key_en, None)
    return None

NON_INDEXABLE = re.compile(r"\b(alto frontera|cam alert|patrulla policial|noticiero|infomercial|televenta|paid programming|highlights|resumen|hechos|eyewitness|cbs news|nbc4|pix11|fox 5|sportscentre|deportes|nba|nfl|mlb|tennis|formula 1|f1|motogp)\b", re.IGNORECASE)

def get_tmdb_data(title, desc="", subtitle="", year=None, prefer_latam=False, search_title_en=None):
    if not TMDB_API_KEY or not title:
        return None
    if NON_INDEXABLE.search(f"{title} {desc}"):
        return None
        
    cache_key = f"tmdb_best_v2:{normalize_text(title)}:{normalize_text(subtitle)}:{normalize_text(desc)[:160]}:{year or ''}:{'latam' if prefer_latam else 'default'}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    
    clean_query = re.split(r"[:|–—]", title)[0].strip()
    clean_query = re.sub(r"\b(episodio|temporada|capitulo|cap\.)\s*\d+", "", clean_query, flags=re.IGNORECASE).strip()
    clean_query = re.sub(r"\b(s\d{1,2}\s*e\d{1,2}|t\d{1,2}\s*e\d{1,2})\b", "", clean_query, flags=re.IGNORECASE).strip()
    
    if not clean_query or len(clean_query) < 4:
        cache_set(cache_key, None)
        return None
    
    expected_type = infer_media_type_from_desc(desc)
    if expected_type == "movie" and not year:
        cache_set(cache_key, None)
        return None

    search_query = search_title_en if search_title_en and prefer_latam else clean_query
    search_lang = "en" if search_title_en and prefer_latam else ("es-MX" if prefer_latam else "es-ES")
    data = tmdb_search_multi(search_query, search_lang, year=year if expected_type == "movie" else None)
    
    if not data or not data.get("results"):
        cache_set(cache_key, None)
        return None
        
    norm_title = normalize_text(title)
    source_sequel = detect_sequel_marker(title)
    ambiguous_title = is_ambiguous_title(title)
    best_item = None
    best_score = -999.0
    for item in data.get("results", [])[:10]:
        media_type = item.get("media_type")
        if media_type not in ("movie", "tv"):
            continue
        candidate_title = (item.get("title") or item.get("name") or "").strip()
        candidate_original = (item.get("original_title") or item.get("original_name") or "").strip()
        overview = item.get("overview") or ""
        candidate_year = extract_candidate_year(item)
        title_ratio = max(text_similarity(title, candidate_title), text_similarity(title, candidate_original))
        desc_ratio = overlap_score(desc, overview) if desc and overview else 0.0
        min_title_ratio = 0.78 if ambiguous_title else 0.58
        if title_ratio < min_title_ratio:
            continue
        if desc and overview:
            if title_ratio < 0.93 and desc_ratio < 0.18:
                continue
            if ambiguous_title and desc_ratio < 0.12:
                continue
        elif ambiguous_title and desc:
            continue
        if year and candidate_year:
            try:
                if abs(int(year) - int(candidate_year)) > 1:
                    continue
            except Exception:
                pass
        candidate_sequel = detect_sequel_marker(candidate_title) or detect_sequel_marker(candidate_original)
        score = 0.0
        score += title_ratio * 8.0
        score += desc_ratio * 9.0
        if norm_title and normalize_text(candidate_title) == norm_title:
            score += 4.0
        if norm_title and candidate_original and normalize_text(candidate_original) == norm_title:
            score += 3.0
        if expected_type and media_type == expected_type:
            score += 2.0
        elif expected_type and media_type != expected_type:
            score -= 2.5
        if year and candidate_year:
            if str(year) == str(candidate_year):
                score += 1.5
            else:
                score -= 1.0
        if source_sequel is None and candidate_sequel is not None:
            score -= 5.0
        elif source_sequel is not None and candidate_sequel is not None:
            if source_sequel == candidate_sequel:
                score += 1.5
            else:
                score -= 7.0
        popularity = item.get("popularity") or 0
        try:
            score += min(float(popularity) / 2000.0, 0.15)
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best_item = item
    if not best_item or best_score < 5.5:
        cache_set(cache_key, None)
        return None
    result = {
        "type": best_item.get("media_type"),
        "year": extract_candidate_year(best_item),
        "id": best_item.get("id"),
        "localized_title": None,
        "canonical_title": (best_item.get("title") or best_item.get("name") or "").strip() or None,
        "match_score": round(best_score, 3),
    }
    result["localized_title"] = get_tmdb_localized_title(result["id"], result["type"], prefer_latam=prefer_latam)
    cache_set(cache_key, result)
    return result

# =========================
# TVMAZE
# =========================

def get_tvmaze_episode(show_name, air_date, desc="", subtitle="", year=None):
    cache_key = f"tvmaze_v3:{normalize_text(show_name)}:{air_date}:{normalize_text(subtitle)}:{normalize_text(desc)[:160]}:{year or ''}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        r_show = SESSION.get("https://api.tvmaze.com/search/shows", params={"q": show_name}, timeout=API_TIMEOUT)
        if r_show.status_code != 200:
            cache_set(cache_key, None)
            return None
        results = r_show.json() or []
        if not results:
            cache_set(cache_key, None)
            return None
        ambiguous_title = is_ambiguous_title(show_name)
        ranked_shows = []
        for entry in results[:10]:
            show = entry.get("show") or {}
            show_title = (show.get("name") or "").strip()
            show_summary = strip_html_tags(show.get("summary") or "")
            premiered = (show.get("premiered") or "")[:4]
            title_ratio = text_similarity(show_name, show_title)
            desc_ratio = overlap_score(desc, show_summary) if desc and show_summary else 0.0
            min_title_ratio = 0.78 if ambiguous_title else 0.55
            if title_ratio < min_title_ratio:
                continue
            if desc and show_summary:
                if title_ratio < 0.92 and desc_ratio < 0.14:
                    continue
                if ambiguous_title and desc_ratio < 0.10:
                    continue
            elif ambiguous_title and desc:
                continue
            score = title_ratio * 8.0 + desc_ratio * 7.0
            if year and premiered:
                try:
                    if abs(int(year) - int(premiered)) <= 1:
                        score += 0.8
                except Exception:
                    pass
            ranked_shows.append((score, show, show_summary))
        if not ranked_shows:
            cache_set(cache_key, None)
            return None
        ranked_shows.sort(key=lambda x: x[0], reverse=True)
        best_episode = None
        best_score = -999.0
        subtitle_hint = strip_leading_se_from_text(subtitle or "").strip() or ""
        for base_score, show, show_summary in ranked_shows[:5]:
            show_id = show.get("id")
            if not show_id:
                continue
            r_ep = SESSION.get(f"https://api.tvmaze.com/shows/{show_id}/episodesbydate", params={"date": air_date}, timeout=API_TIMEOUT)
            if r_ep.status_code != 200:
                continue
            episodes = r_ep.json() or []
            if not episodes:
                continue
            for ep in episodes:
                ep_name = (ep.get("name") or "").strip()
                ep_summary = strip_html_tags(ep.get("summary") or "")
                ep_score = base_score
                if subtitle_hint and ep_name:
                    ep_title_ratio = text_similarity(subtitle_hint, ep_name)
                    if ep_title_ratio < 0.45 and len(episodes) > 1:
                        continue
                    ep_score += ep_title_ratio * 5.0
                if ep_score > best_score:
                    best_score = ep_score
                    best_episode = {
                        "season": ep.get("season"),
                        "episode": ep.get("number"),
                        "name": ep_name,
                        "summary": ep_summary,
                        "show_id": show_id,
                        "show_name": (show.get("name") or "").strip(),
                        "show_summary": show_summary,
                        "match_score": round(ep_score, 3),
                    }
        if not best_episode or best_score < 5.4:
            cache_set(cache_key, None)
            return None
        cache_set(cache_key, best_episode)
        return best_episode
    except Exception as e:
        print(f"Error TVMaze: {e}", flush=True)
    cache_set(cache_key, None)
    return None

# =========================
# PROCESAMIENTO PRINCIPAL
# =========================

def is_tba(text):
    if not text:
        return True
    t = text.strip().lower()
    return t in {"tba", "tbd", "unknown", "na", "n/a", ""}

def process_programme(elem, start_time_str, channel_id=None, prefer_latam=False, spanish_season_episode_format=False, tvmaze_authoritative=False):
    raw_title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam)
    raw_subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    raw_desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)
    xml_has_spanish_title = has_spanish_variant(elem, "title")
    clean_title, has_new = extract_new_marker(raw_title)
    clean_title, year_regex = extract_year_regex(clean_title)
    se_title = extract_se_regex(clean_title)
    se_desc = extract_se_regex(raw_desc)
    se_xml = extract_xmltv_episode_num(elem)
    final_year = year_regex
    final_se = se_title or se_desc or se_xml
    desc_episode_title, _ = split_episode_title_from_desc(raw_desc)
    subtitle_hint = strip_leading_se_from_text(raw_subtitle or "").strip() or (desc_episode_title or "")
    base_title = strip_se_from_title(clean_title) or clean_title.strip()
    base_title = remove_episode_title_from_series_title(base_title, subtitle_hint)
    
    # Datos base iniciales
    final_title = base_title
    preferred_subtitle = raw_subtitle.strip() if raw_subtitle else ""
    preferred_desc = raw_desc.strip() if raw_desc else ""
    canonical_title = None
    tvmaze_title_applied = False

    # Determinar estrategia de autoridad
    channel_country = get_channel_country_code(channel_id) if channel_id else None
    is_us_gb_ca = channel_country in {"us", "gb", "ca"}
    is_series = bool(final_se) or infer_media_type_from_desc(raw_desc) == "tv"
    
    # 1. Autoridad primaria
    tmdb_data = None
    tvmaze_data = None
    use_tmdb = not is_us_gb_ca or not is_series
    use_tvmaze = (is_us_gb_ca and is_series) or tvmaze_authoritative

    if use_tmdb:
        tmdb_prefer_latam = prefer_latam and not is_us_gb_ca
        tmdb_data = get_tmdb_data(
            base_title, desc=raw_desc, subtitle=subtitle_hint, 
            year=final_year, prefer_latam=tmdb_prefer_latam,
            search_title_en=base_title if is_us_gb_ca else None
        )
        if tmdb_data:
            canonical_title = tmdb_data.get("localized_title") or tmdb_data.get("canonical_title") or canonical_title
            if tmdb_prefer_latam and canonical_title:
                final_title = canonical_title
            elif is_us_gb_ca and canonical_title:
                final_title = canonical_title

    if use_tvmaze and not final_se:
        pass

    # 2. Fallback cruzado para episodios (TV)
    ep_title_api = None
    ep_desc_api = None
    show_title_api = None

    if use_tvmaze or is_series:
        air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d") if start_time_str and len(start_time_str) >= 8 else None
        query_title = final_title or base_title
        tvmaze_data = get_tvmaze_episode(query_title, air_date, desc=raw_desc, subtitle=subtitle_hint, year=final_year)
        
        if not tvmaze_data and clean_title:
            fallback = strip_se_from_title(clean_title) or clean_title
            fallback = remove_episode_title_from_series_title(fallback, subtitle_hint)
            if fallback != query_title:
                tvmaze_data = get_tvmaze_episode(fallback, air_date, desc=raw_desc, subtitle=subtitle_hint, year=final_year)

        if tvmaze_data:
            show_title_api = (tvmaze_data.get("show_name") or "").strip()
            ep_title_api = (tvmaze_data.get("name") or "").strip()
            ep_desc_api = (tvmaze_data.get("summary") or "").strip()
            
            if not final_se and tvmaze_data.get("season") is not None and tvmaze_data.get("episode") is not None:
                final_se = normalize_season_ep_from_numbers(tvmaze_data["season"], tvmaze_data["episode"])

            if tvmaze_authoritative or (is_us_gb_ca and show_title_api):
                if show_title_api and normalize_text(show_title_api) != normalize_text(final_title):
                    final_title = show_title_api
                    tvmaze_title_applied = True
            if is_tba(ep_desc_api):
                ep_desc_api = None
    elif is_series and not use_tvmaze:
        se_match = re.match(r"S(\d{2})\s*E(\d{2})", final_se) if final_se else None
        if se_match and tmdb_data and tmdb_data.get("type") == "tv":
            ep_data = get_tmdb_episode_data(tmdb_data["id"], int(se_match.group(1)), int(se_match.group(2)))
            if ep_data:
                if not is_tba(ep_data.get("episode_title")):
                    ep_title_api = ep_data["episode_title"].strip()
                if not is_tba(ep_data.get("overview")):
                    ep_desc_api = ep_data["overview"].strip()

    # 3. Completado cruzado (TMDB <-> TVMaze)
    if is_tba(ep_title_api) or is_tba(ep_desc_api):
        if tmdb_data and tmdb_data.get("type") == "tv" and final_se:
            se_match = re.match(r"S(\d{2})\s*E(\d{2})", final_se)
            if se_match:
                ep_data = get_tmdb_episode_data(tmdb_data["id"], int(se_match.group(1)), int(se_match.group(2)))
                if ep_data:
                    if is_tba(ep_title_api) and not is_tba(ep_data.get("episode_title")):
                        ep_title_api = ep_data["episode_title"].strip()
                    if is_tba(ep_desc_api) and not is_tba(ep_data.get("overview")):
                        ep_desc_api = ep_data["overview"].strip()

    # 4. Aplicar API data (REGLA ESTRICTA: episodio va al subtítulo, no al título principal)
    if ep_title_api and not is_tba(ep_title_api):
        preferred_subtitle = ep_title_api
        tvmaze_title_applied = True
        
    if ep_desc_api and not is_tba(ep_desc_api):
        preferred_desc = strip_html_tags(ep_desc_api)
    elif is_tba(preferred_desc):
        preferred_desc = raw_desc.strip() if raw_desc else ""

    if not final_year and tmdb_data:
        final_year = tmdb_data.get("year")

    # 5. CAPA 3 & 4: Pipeline de capitalización profesional
    title_lang = detect_title_language(final_title)
    
    if final_title:
        final_title = smart_title_case(final_title, prefer_latam=prefer_latam, detected_lang=title_lang)
    
    if canonical_title:
        final_title = preserve_special_casing(final_title, canonical_title)
    if base_title and not tvmaze_title_applied:
        final_title = preserve_special_casing(final_title, base_title)
    final_title = apply_title_case_overrides(final_title)
    
    display_title = final_title
    is_series_final = bool(final_se) or bool(tmdb_data and tmdb_data.get("type") == "tv")
    
    if final_se:
        display_se = format_season_episode_display(final_se, use_spanish=spanish_season_episode_format)
        display_title += f" | {display_se}"
    if has_new:
        display_title += " ᴺᵉʷ"
        
    if not preferred_subtitle or is_tba(preferred_subtitle):
        preferred_subtitle = raw_subtitle.strip() if raw_subtitle else ""

    return display_title, is_series_final, preferred_subtitle, preferred_desc

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

    allowed_canonical = {canonical_channel_id(ch) for ch in allowed_channels}

    good_sources = set()
    for sources in CHANNEL_SOURCE_RULES.values():
        for s in sources:
            good_sources.add(s)

    CHANNEL_ID_ALIASES = {}

    print("Analizando fuentes priorizadas...", flush=True)

    for url in good_sources:
        if url not in EPG_URLS:
            continue
        try:
            download_xml(url, TEMP_INPUT)

            context = ET.iterparse(TEMP_INPUT, events=("start", "end"))
            _, root = next(context)

            for event, elem in context:
                if event != "end":
                    continue

                if elem.tag == "channel":
                    real_id = elem.get("id")
                    if real_id:
                        canonical_real = canonical_channel_id(real_id)

                        for user_id in allowed_channels:
                            if canonical_real == canonical_channel_id(user_id):
                                CHANNEL_ID_ALIASES.setdefault(real_id, [])
                                if user_id not in CHANNEL_ID_ALIASES[real_id]:
                                    CHANNEL_ID_ALIASES[real_id].append(user_id)
                                    print(f"  -> Match validado: '{real_id}' == '{user_id}'", flush=True)

                    root.remove(elem)

                elif elem.tag == "programme":
                    root.remove(elem)

            del context
            if os.path.exists(TEMP_INPUT):
                os.remove(TEMP_INPUT)
        except Exception as e:
            print(f"Error escaneando fuente {url}: {e}", flush=True)

    written_programmes = set()
    written_channels = set()

    try:
        with open(TEMP_OUTPUT, "wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')

            for idx, url in enumerate(EPG_URLS, start=1):
                processed_programmes = 0
                prefer_latam = is_latam_feed(url)
                spanish_season_episode_format = use_spanish_season_episode_format(url)

                try:
                    print(f"[{idx}/{len(EPG_URLS)}] Fuente: {url}", flush=True)
                    download_xml(url, TEMP_INPUT)

                    context = ET.iterparse(TEMP_INPUT, events=("start", "end"))
                    _, root = next(context)

                    for event, elem in context:
                        if event != "end":
                            continue

                        if elem.tag == "channel":
                            ch_id = elem.get("id")
                            canonical_ch_id = canonical_channel_id(ch_id)

                            should_write = (
                                canonical_ch_id in allowed_canonical
                                and is_source_allowed_for_channel(ch_id, url)
                                and canonical_ch_id not in written_channels
                            )
                            if should_write:
                                channel_elem = clone_element(elem)
                                channel_elem.set("id", canonical_ch_id)
                                out_f.write(ET.tostring(channel_elem, encoding="utf-8"))
                                out_f.write(b"\n")
                                written_channels.add(canonical_ch_id)

                            root.remove(elem)

                        elif elem.tag == "programme":
                            processed_programmes += 1
                            if processed_programmes % 5000 == 0:
                                print(f"Procesando... {processed_programmes} programas", flush=True)

                            ch_id = elem.get("channel")
                            canonical_ch_id = canonical_channel_id(ch_id)
                            is_ch_allowed = canonical_ch_id in allowed_canonical and is_source_allowed_for_channel(ch_id, url)

                            if is_ch_allowed:
                                start = elem.get("start", "")
                                tvmaze_authoritative = should_use_tvmaze_authoritative(url, ch_id)
                                new_title, is_series, preferred_subtitle, preferred_desc = process_programme(
                                    elem,
                                    start,
                                    channel_id=ch_id,
                                    prefer_latam=prefer_latam,
                                    spanish_season_episode_format=spanish_season_episode_format,
                                    tvmaze_authoritative=tvmaze_authoritative,
                                )
                                replace_all_title_elements(elem, new_title, prefer_latam=prefer_latam)
                                normalize_subtitle_and_desc(
                                    elem,
                                    prefer_latam=prefer_latam,
                                    is_series=is_series,
                                    preferred_subtitle=preferred_subtitle,
                                    preferred_desc=preferred_desc,
                                )
                                normalize_episode_num_elements(elem)
                                apply_channel_offset(elem)

                                cloned_programme = clone_element(elem)
                                cloned_programme.set("channel", canonical_ch_id)

                                start = cloned_programme.get("start")
                                stop = cloned_programme.get("stop")
                                prog_key = (canonical_ch_id, start, stop)

                                if prog_key not in written_programmes:
                                    out_f.write(ET.tostring(cloned_programme, encoding="utf-8"))
                                    out_f.write(b"\n")
                                    written_programmes.add(prog_key)

                            root.remove(elem)

                    del context
                    print(f"Fuente terminada: {url.split('/')[-1]}", flush=True)

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

    print(f"Proceso completado: {OUTPUT_FILE} | canales: {len(written_channels)} | programas: {len(written_programmes)}", flush=True)

if __name__ == "__main__":
    main()
