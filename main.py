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
# CONFIGURACIÓN
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
USER_AGENT = "xmltv-title-normalizer/3.1-Universal"

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

CHANNEL_SOURCE_RULES = {}

# =========================
# SESIÓN HTTP
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
        rest = match.group(2).strip()
        rest = rest.replace("-", "")
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
    ch_id = CHANNEL_ALIAS_MAP.get(ch_id, ch_id)
    return ch_id.lower()

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

# --- MEJORA: limpieza de espacios en guiones y signos ---
def clean_punctuation_spacing(text):
    if not text:
        return text
    # Elimina espacios alrededor de guiones (convierte " - " en "-")
    text = re.sub(r'\s*-\s*', '-', text)
    # Espacios antes/después de signos de exclamación e interrogación
    text = re.sub(r'\s+([!¡?¿])', r'\1', text)
    text = re.sub(r'([!¡?¿])\s+', r'\1', text)
    # Apóstrofos pegados
    text = re.sub(r"\b'\s+", "'", text)
    text = re.sub(r"\s+'\b", "'", text)
    # Unifica espacios múltiples
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

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

def extract_year_from_image(elem):
    for img in elem.findall("image"):
        url = img.text or ""
        m = re.search(r'[_-](\d{4})[_-]', url)
        if m:
            return m.group(1)
    return None

def normalize_season_ep_from_numbers(season, episode):
    try:
        season_num = int(season)
        episode_num = int(episode)
        return f"S{season_num:02d} E{episode_num:02d}"
    except Exception:
        return None

def normalize_season_ep(text):
    if not text:
        return None
    patterns = [
        r"\bS\s*(\d+)\s*E\s*(\d+)\b",
        r"\bT\s*(\d+)\s*E\s*(\d+)\b",
        r"\bTemporada\s*(\d+)\s*Episodio\s*(\d+)\b",
        r"\bSeason\s*(\d+)\s*Episode\s*(\d+)\b",
        r"\b(\d+)\s*x\s*(\d+)\b",
        r"\bS(\d{1,2})E(\d{1,2})\b",
        r"\bT(\d{1,2})E(\d{1,2})\b",
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

def infer_media_type_from_desc(desc, categories=None):
    d = normalize_text(desc)
    tv_hints = ["temporada", "episodio", "capitulo", "serie", "novela", "reality", "miniserie"]
    movie_hints = ["pelicula", "film", "largometraje", "cine", "documental"]
    if categories:
        cats = [normalize_text(c) for c in categories]
        if any(c == "serie" for c in cats):
            return "tv"
        if any(c == "pelicula" for c in cats):
            return "movie"
    tv_score = sum(1 for w in tv_hints if w in d)
    movie_score = sum(1 for w in movie_hints if w in d)
    if tv_score > movie_score:
        return "tv"
    if movie_score > tv_score:
        return "movie"
    return None

def pick_best_localized_text(elem, tag, prefer_latam=False):
    elems = elem.findall(tag)
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
        priority = ["en", "en-us", ""]
    for wanted in priority:
        for lang, text in candidates:
            if lang == wanted:
                return text
    return candidates[0][1]

def has_spanish_variant(elem, tag):
    for e in elem.findall(tag):
        if not (e.text or "").strip():
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

def strip_se_from_title(text):
    if not text:
        return ""
    out = " ".join(text.strip().split())
    patterns = [
        r"\s+\|\s+S\d{1,2}\s*E\d{1,2}\s*$",
        r"\s+\(S\d{1,2}\s*E\d{1,2}\)\s*$",
        r"\s+S\d{1,2}\s*E\d{1,2}\s*$",
        r"\s+-\s+S\d{1,2}\s*E\d{1,2}\s*$",
        r"^\s*S\d{1,2}\s*E\d{1,2}\s*[-:]\s*",
    ]
    for pat in patterns:
        out = re.sub(pat, "", out, flags=re.IGNORECASE).strip()
    return out

def spanish_title_case(text):
    if not text:
        return ""
    text = clean_punctuation_spacing(text)
    tokens = re.findall(r"[\w\.]+|[^\w\s]", text, re.UNICODE)
    result = []
    capitalize_next = True
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if re.fullmatch(r"[^\w\s]+", token):
            result.append(token)
            if token in ":¡!¿?":
                capitalize_next = True
            else:
                capitalize_next = False
            i += 1
            continue
        low = token.lower()
        if re.fullmatch(r"(?:\w\.)+\w?", token):
            result.append(token)
            i += 1
            continue
        if token.isupper() and low not in SPANISH_MINOR_WORDS:
            result.append(token)
            i += 1
            continue
        if capitalize_next or low not in SPANISH_MINOR_WORDS:
            result.append(low[0].upper() + low[1:] if len(low) > 1 else low.upper())
        else:
            result.append(low)
        capitalize_next = False
        i += 1
    return " ".join(result).replace(" .", ".").replace(" ,", ",").replace(" :", ":").replace("( ", "(").replace(" )", ")")

def preserve_special_casing(base_title, canonical_title):
    if not base_title or not canonical_title:
        return base_title
    if normalize_text(base_title) != normalize_text(canonical_title):
        return base_title
    base_words = base_title.split()
    canon_words = canonical_title.split()
    if len(base_words) != len(canon_words):
        return base_title
    merged = []
    for bw, cw in zip(base_words, canon_words):
        if bw.upper() == cw.upper():
            merged.append(cw)
        else:
            merged.append(bw)
    return " ".join(merged)

def extract_english_title(elem):
    invalid = {"comedia", "drama", "terror", "acción", "accion", "suspenso",
               "romance", "aventura", "ciencia ficción", "serie", "película",
               "pelicula", "documental"}
    for t in elem.findall("title"):
        lang = norm_lang(t.get("lang"))
        if lang.startswith("en"):
            text = (t.text or "").strip()
            if text and text.lower() not in invalid and len(text.split()) >= 2:
                return text
    return None

# =========================
# TMDB
# =========================

def extract_candidate_year(item):
    date_str = item.get("release_date") or item.get("first_air_date") or ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return date_str[:4]
    return None

def tmdb_search_multi(query, language, year=None):
    if not TMDB_API_KEY:
        return None
    cache_key = f"tmdb_search:{normalize_text(query)}:{language}:{year or ''}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    url = "https://api.themoviedb.org/3/search/multi"
    params = {"api_key": TMDB_API_KEY, "query": query, "language": language}
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

def tmdb_get_localized_details(tmdb_id, media_type, prefer_latam=False):
    if not TMDB_API_KEY or not tmdb_id or media_type not in ("movie", "tv"):
        return None, None
    if prefer_latam:
        lang_chain = ["es-MX", "es-419", "es-AR", "es-CO", "es-CL", "es-PE", "es-US", "es"]
    else:
        lang_chain = ["en-US", "en"]
    for lang in lang_chain:
        cache_key = f"tmdb_details:{media_type}:{tmdb_id}:{lang}"
        cached = cache_get(cache_key)
        if cached is not None:
            if cached:
                return cached.get("title"), cached.get("overview")
            continue
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
        params = {"api_key": TMDB_API_KEY, "language": lang}
        try:
            r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                title = (data.get("title") or data.get("name") or "").strip()
                overview = (data.get("overview") or "").strip()
                if title:
                    result = {"title": title, "overview": overview}
                    cache_set(cache_key, result)
                    return title, overview
        except Exception as e:
            print(f"Error TMDB localized details: {e}", flush=True)
        cache_set(cache_key, None)
    return None, None

def tmdb_get_episode_details(tmdb_id, season, episode, prefer_latam=False):
    if not TMDB_API_KEY or not tmdb_id or season is None or episode is None:
        return None, None
    if prefer_latam:
        lang_chain = ["es-MX", "es-419", "es-AR", "es-CO", "es-CL", "es-PE", "es-US", "es"]
    else:
        lang_chain = ["en-US", "en"]
    for lang in lang_chain:
        cache_key = f"tmdb_episode:{tmdb_id}:{season}:{episode}:{lang}"
        cached = cache_get(cache_key)
        if cached is not None:
            if cached:
                return cached.get("name"), cached.get("overview")
            continue
        url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}/episode/{episode}"
        params = {"api_key": TMDB_API_KEY, "language": lang}
        try:
            r = SESSION.get(url, params=params, timeout=API_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                name = (data.get("name") or "").strip()
                overview = (data.get("overview") or "").strip()
                if name:
                    result = {"name": name, "overview": overview}
                    cache_set(cache_key, result)
                    return name, overview
        except Exception as e:
            print(f"Error TMDB episode details: {e}", flush=True)
        cache_set(cache_key, None)
    return None, None

def _score_tmdb_item(item, title, desc, year, expected_type, source_sequel, ambiguous_title):
    candidate_title = (item.get("title") or item.get("name") or "").strip()
    candidate_original = (item.get("original_title") or item.get("original_name") or "").strip()
    overview = item.get("overview") or ""
    candidate_year = extract_candidate_year(item)
    title_ratio = max(text_similarity(title, candidate_title), text_similarity(title, candidate_original))
    desc_ratio = overlap_score(desc, overview) if desc and overview else 0.0
    min_title_ratio = 0.78 if ambiguous_title else 0.58
    if title_ratio < min_title_ratio:
        return None
    if desc and overview:
        if title_ratio < 0.93 and desc_ratio < 0.18:
            return None
        if ambiguous_title and desc_ratio < 0.12:
            return None
    elif ambiguous_title and desc:
        return None
    if year and candidate_year:
        try:
            if abs(int(year) - int(candidate_year)) > 1:
                return None
        except Exception:
            pass
    candidate_sequel = detect_sequel_marker(candidate_title) or detect_sequel_marker(candidate_original)
    score = 0.0
    score += title_ratio * 8.0
    score += desc_ratio * 9.0
    if normalize_text(title) == normalize_text(candidate_title):
        score += 4.0
    if normalize_text(title) == normalize_text(candidate_original):
        score += 3.0
    if expected_type and item.get("media_type") == expected_type:
        score += 2.0
    elif expected_type and item.get("media_type") != expected_type:
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
    return score

def _build_tmdb_result(best_item, prefer_latam, season=None, episode=None):
    result = {
        "type": best_item.get("media_type"),
        "year": extract_candidate_year(best_item),
        "id": best_item.get("id"),
        "canonical_title": (best_item.get("title") or best_item.get("name") or "").strip() or None,
        "match_score": 0.0
    }
    loc_title, loc_overview = tmdb_get_localized_details(result["id"], result["type"], prefer_latam)
    if loc_title:
        result["localized_title"] = loc_title
        result["localized_overview"] = loc_overview
    if result["type"] == "tv" and season is not None and episode is not None:
        ep_name, ep_overview = tmdb_get_episode_details(result["id"], season, episode, prefer_latam)
        if ep_name:
            result["episode_name"] = ep_name
        if ep_overview:
            result["episode_overview"] = ep_overview
    return result

def get_tmdb_data(title, desc="", subtitle="", year=None, prefer_latam=False,
                  season=None, episode=None, english_title=None):
    if not TMDB_API_KEY or not title:
        return None
    cache_key = f"tmdb_v4:{normalize_text(title)}:{normalize_text(english_title or '')}:{year or ''}:{season or ''}:{episode or ''}:{'latam' if prefer_latam else 'eng'}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    expected_type = infer_media_type_from_desc(desc)
    source_sequel = detect_sequel_marker(title)
    ambiguous_title = is_ambiguous_title(title)

    if english_title:
        data = tmdb_search_multi(english_title, "en-US", year=year)
        if data and data.get("results"):
            best_item = None
            best_score = -999.0
            for item in data["results"][:10]:
                if item.get("media_type") not in ("movie", "tv"):
                    continue
                sc = _score_tmdb_item(item, title, desc, year, expected_type, source_sequel, ambiguous_title)
                if sc is not None and sc > best_score:
                    best_score = sc
                    best_item = item
            if best_item and best_score >= 5.5:
                result = _build_tmdb_result(best_item, prefer_latam, season, episode)
                result["match_score"] = round(best_score, 3)
                cache_set(cache_key, result)
                return result

    search_lang = "es-MX" if prefer_latam else "en-US"
    data = tmdb_search_multi(title, search_lang, year=year)
    if data and data.get("results"):
        best_item = None
        best_score = -999.0
        for item in data["results"][:10]:
            if item.get("media_type") not in ("movie", "tv"):
                continue
            sc = _score_tmdb_item(item, title, desc, year, expected_type, source_sequel, ambiguous_title)
            if sc is not None and sc > best_score:
                best_score = sc
                best_item = item
        if best_item and best_score >= 5.5:
            result = _build_tmdb_result(best_item, prefer_latam, season, episode)
            result["match_score"] = round(best_score, 3)
            cache_set(cache_key, result)
            return result

    cache_set(cache_key, None)
    return None

# =========================
# TVMAZE (MEJORADO: búsqueda ±1 día)
# =========================

def get_tvmaze_episode(show_name, air_date, desc="", subtitle="", year=None, english_title=None):
    cache_key = f"tvmaze_v4:{normalize_text(show_name)}:{air_date}:{year or ''}:{english_title or ''}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    query = english_title if english_title else show_name
    try:
        r_show = SESSION.get("https://api.tvmaze.com/search/shows", params={"q": query}, timeout=API_TIMEOUT)
        if r_show.status_code != 200:
            cache_set(cache_key, None)
            return None
        results = r_show.json() or []
        if not results:
            cache_set(cache_key, None)
            return None
        best_episode = None
        best_score = -999.0
        for entry in results[:5]:
            show = entry.get("show") or {}
            show_title = (show.get("name") or "").strip()
            if not show_title:
                continue
            show_id = show.get("id")
            
            # --- MEJORA: probar fecha original, un día antes y un día después ---
            dates_to_try = [air_date]
            try:
                d = datetime.strptime(air_date, "%Y-%m-%d")
                dates_to_try.append((d - timedelta(days=1)).strftime("%Y-%m-%d"))
                dates_to_try.append((d + timedelta(days=1)).strftime("%Y-%m-%d"))
            except Exception:
                pass
            dates_to_try = list(dict.fromkeys(dates_to_try))  # elimina duplicados
            
            episodes = []
            for candidate_date in dates_to_try:
                r_ep = SESSION.get(f"https://api.tvmaze.com/shows/{show_id}/episodesbydate", params={"date": candidate_date}, timeout=API_TIMEOUT)
                if r_ep.status_code == 200:
                    episodes = r_ep.json() or []
                    if episodes:
                        break
            
            for ep in episodes:
                ep_name = (ep.get("name") or "").strip()
                ep_summary = strip_html_tags(ep.get("summary") or "")
                ep_score = text_similarity(show_name, show_title) * 8.0
                if subtitle:
                    ep_score += text_similarity(subtitle, ep_name) * 3.0
                if ep_score > best_score:
                    best_score = ep_score
                    best_episode = {
                        "season": ep.get("season"),
                        "episode": ep.get("number"),
                        "name": ep_name,
                        "summary": ep_summary,
                        "show_id": show_id,
                        "show_name": show_title,
                        "match_score": round(ep_score, 3)
                    }
        if best_episode and best_score < 5.0:
            best_episode = None
        cache_set(cache_key, best_episode)
        return best_episode
    except Exception as e:
        print(f"Error TVMaze: {e}", flush=True)
    cache_set(cache_key, None)
    return None

# =========================
# PROCESAMIENTO PRINCIPAL
# =========================

def split_episode_title_from_desc(desc_text):
    if not desc_text:
        return None, desc_text
    text = desc_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    first_line, sep, rest = text.partition("\n")
    se = normalize_season_ep(first_line.strip())
    if not se:
        return None, text
    ep_title = strip_se_from_title(first_line.strip())
    if not ep_title or len(ep_title) < 3:
        return None, text
    if len(ep_title) > 100 or len(ep_title.split()) > 12:
        return None, text
    return ep_title, rest.strip() if sep else ""

def strip_leading_se_from_text(text):
    if not text:
        return ""
    text = text.strip()
    return re.sub(r"^[SsTt]\d{1,2}\s*[Ee]\d{1,2}\s*[-:]\s*", "", text).strip()

def strip_leading_se_from_desc(desc_text):
    if not desc_text:
        return ""
    text = desc_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.splitlines()
    if lines:
        first_line = lines[0].strip()
        se_pattern = r'^\s*(?:S\d{1,2}\s*E\d{1,2}|T\d{1,2}\s*E\d{1,2}|Temp\.\s*\d+\s*Ep\.\s*\d+|Temporada\s*\d+\s*Episodio\s*\d+|Season\s*\d+\s*Episode\s*\d+)\s*[-–—:]\s*'
        if re.search(se_pattern, first_line, re.IGNORECASE):
            return "\n".join(lines[1:]).strip()
    return text

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

def apply_title_case_overrides(title):
    key = normalize_text(title)
    return TITLE_CASE_OVERRIDES.get(key, title)

def is_ambiguous_title(title):
    tokens = [t for t in normalize_text(title).split() if len(t) > 2]
    return len(tokens) <= 2

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

def remove_episode_title_from_series_title(title_text, subtitle_text=""):
    if not title_text:
        return ""
    base = " ".join(title_text.strip().split())
    subtitle = " ".join((subtitle_text or "").strip().split())
    if not subtitle:
        return base
    norm_sub = normalize_text(subtitle)
    separators = [":", "|", "-", "–", "—"]
    for sep in separators:
        if sep in base:
            left, right = base.rsplit(sep, 1)
            if normalize_text(right.strip()) == norm_sub and len(left.strip()) > 0:
                return left.strip()
    return base

def process_programme(elem, start_time_str, prefer_latam=False,
                      spanish_season_episode_format=False, tvmaze_authoritative=False):
    spanish_title = pick_best_localized_text(elem, "title", prefer_latam=True)
    english_title = extract_english_title(elem)

    raw_title = spanish_title if spanish_title else pick_best_localized_text(elem, "title", prefer_latam=False)
    raw_subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    raw_desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)

    xml_has_spanish_title = has_spanish_variant(elem, "title")

    clean_title, has_new = extract_new_marker(raw_title)
    clean_title, year_regex = extract_year_regex(clean_title)

    se_title = extract_se_regex(clean_title)
    se_desc = extract_se_regex(raw_desc)
    se_xml = extract_xmltv_episode_num(elem)
    final_se = se_title or se_desc or se_xml

    base_title = strip_se_from_title(clean_title)
    subtitle_hint = strip_leading_se_from_text(raw_subtitle or "").strip()
    base_title = remove_episode_title_from_series_title(base_title, subtitle_hint)

    image_year = extract_year_from_image(elem)
    final_year = year_regex or image_year
    final_title = base_title

    tmdb_season = None
    tmdb_episode = None
    if final_se:
        m = re.match(r"^S(\d{2})\s*E(\d{2})\s*$", final_se, re.IGNORECASE)
        if m:
            tmdb_season = int(m.group(1))
            tmdb_episode = int(m.group(2))

    should_translate = prefer_latam and not xml_has_spanish_title

    tmdb_data = get_tmdb_data(
        final_title if final_title else raw_title,
        desc=raw_desc,
        subtitle=subtitle_hint,
        year=final_year,
        prefer_latam=prefer_latam,
        season=tmdb_season,
        episode=tmdb_episode,
        english_title=english_title
    )

    canonical_title = None
    tvmaze_data = None
    preferred_subtitle = None
    preferred_desc = None
    tvmaze_title_applied = False

    if tmdb_data:
        loc_title = (tmdb_data.get("localized_title") or "").strip()
        canon_from_tmdb = (tmdb_data.get("canonical_title") or "").strip()

        if loc_title and (should_translate or prefer_latam):
            final_title = loc_title
            canonical_title = loc_title
        elif canon_from_tmdb and (should_translate or prefer_latam):
            if text_similarity(final_title, canon_from_tmdb) > 0.4:
                final_title = canon_from_tmdb
                canonical_title = canon_from_tmdb
        elif canon_from_tmdb:
            canonical_title = canon_from_tmdb

        ep_overview = tmdb_data.get("episode_overview") or ""
        loc_overview = tmdb_data.get("localized_overview") or ""
        if ep_overview:
            preferred_desc = ep_overview
        elif loc_overview:
            preferred_desc = loc_overview

        if tmdb_data.get("type") == "tv":
            ep_name = tmdb_data.get("episode_name") or ""
            if ep_name and not preferred_subtitle:
                preferred_subtitle = ep_name

    if not final_year and tmdb_data and tmdb_data.get("year"):
        final_year = tmdb_data.get("year")

    should_try_tvmaze = (tmdb_data and tmdb_data.get("type") == "tv") or final_se
    if should_try_tvmaze:
        try:
            air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
            query_title = final_title or base_title
            tvmaze_data = get_tvmaze_episode(
                query_title, air_date,
                desc=raw_desc, subtitle=subtitle_hint, year=final_year,
                english_title=english_title
            )
            if not final_se and tvmaze_data and tvmaze_data.get("season") is not None:
                final_se = normalize_season_ep_from_numbers(
                    tvmaze_data["season"], tvmaze_data["episode"]
                )
            if tvmaze_authoritative and tvmaze_data and not tvmaze_title_applied:
                show_name = (tvmaze_data.get("show_name") or "").strip()
                if show_name:
                    final_title = show_name
                    canonical_title = show_name
                    tvmaze_title_applied = True
                ep_name = (tvmaze_data.get("name") or "").strip()
                if ep_name:
                    preferred_subtitle = ep_name
                ep_sum = strip_html_tags(tvmaze_data.get("summary") or "")
                if ep_sum:
                    preferred_desc = ep_sum
        except ValueError:
            pass

    if not tmdb_data and not tvmaze_data:
        final_title = base_title or clean_title or raw_title
        preferred_subtitle = raw_subtitle or None
        preferred_desc = raw_desc or None

    final_title = clean_punctuation_spacing(final_title)

    if prefer_latam or xml_has_spanish_title:
        final_title = spanish_title_case(final_title)
    else:
        if canonical_title:
            final_title = preserve_special_casing(final_title, canonical_title)
    final_title = apply_title_case_overrides(final_title)

    display_title = final_title
    is_series = bool(final_se) or (tmdb_data and tmdb_data.get("type") == "tv")
    if final_se:
        display_se = format_season_episode_display(final_se, use_spanish=spanish_season_episode_format)
        display_title += f" | {display_se}"
    if has_new:
        display_title += " ᴺᵉʷ"

    return display_title, is_series, preferred_subtitle, preferred_desc

# =========================
# Funciones de XML auxiliares
# =========================

def clone_element(elem):
    return ET.fromstring(ET.tostring(elem, encoding="utf-8"))

def replace_all_title_elements(elem, new_title, prefer_latam=False):
    for t in elem.findall("title"):
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

def normalize_subtitle_and_desc(elem, prefer_latam, is_series, preferred_subtitle, preferred_desc):
    current_subtitle = preferred_subtitle if preferred_subtitle else pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam)
    current_desc = preferred_desc if preferred_desc else pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam)
    current_desc = strip_html_tags(current_desc or "")
    extracted_ep_title = None
    cleaned_desc = current_desc
    ep_title_from_desc, desc_without_ep_title = split_episode_title_from_desc(current_desc)
    if ep_title_from_desc:
        extracted_ep_title = ep_title_from_desc
        cleaned_desc = desc_without_ep_title
    cleaned_desc = strip_leading_se_from_desc(cleaned_desc)
    chosen_subtitle = current_subtitle.strip() if current_subtitle else ""
    if not chosen_subtitle and extracted_ep_title:
        chosen_subtitle = extracted_ep_title
    chosen_subtitle = strip_leading_se_from_text(chosen_subtitle).strip()
    if chosen_subtitle and prefer_latam:
        chosen_subtitle = spanish_title_case(chosen_subtitle)
    set_or_replace_subtitle(elem, chosen_subtitle, prefer_latam)
    if is_series and chosen_subtitle:
        first = first_line(cleaned_desc)
        if normalize_text(first) != normalize_text(chosen_subtitle):
            cleaned_desc = f"{chosen_subtitle}\n{cleaned_desc}"
    set_or_replace_desc(elem, cleaned_desc, prefer_latam)

def normalize_episode_num_elements(elem):
    if FORCE_SEASON_EPISODE_IN_TITLE_ONLY:
        for ep in list(elem.findall("episode-num")):
            elem.remove(ep)

# =========================
# MAIN
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
    print("Iniciando script enriquecido v3.1...", flush=True)
    if not os.path.exists(CHANNELS_FILE):
        print("Error: No existe channels.txt", flush=True)
        return
    with open(CHANNELS_FILE, "r", encoding="utf-8-sig") as f:
        allowed_channels = {line.strip() for line in f if line.strip()}
    if not allowed_channels:
        print("Error: channels.txt vacío", flush=True)
        return
    allowed_canonical = {canonical_channel_id(ch) for ch in allowed_channels}

    # --- NUEVO: para controlar la primera fuente que provee cada canal ---
    channel_source_assigned = {}

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
                                CHANNEL_ID_ALIASES.setdefault(real_id, []).append(user_id)
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
                spanish_se_format = use_spanish_season_episode_format(url)
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
                            if (canonical_ch_id in allowed_canonical
                                    and is_source_allowed_for_channel(ch_id, url)
                                    and canonical_ch_id not in written_channels):
                                # Asignar la primera fuente para este canal (solo si aún no tiene)
                                if canonical_ch_id not in channel_source_assigned:
                                    channel_source_assigned[canonical_ch_id] = url
                                # Si ya tiene una fuente asignada y es diferente a la actual, saltar
                                if channel_source_assigned[canonical_ch_id] != url:
                                    root.remove(elem)
                                    continue
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
                            if canonical_ch_id in allowed_canonical and is_source_allowed_for_channel(ch_id, url):
                                # --- NUEVO: filtrar por primera fuente asignada ---
                                if canonical_ch_id not in channel_source_assigned:
                                    channel_source_assigned[canonical_ch_id] = url
                                if channel_source_assigned[canonical_ch_id] != url:
                                    root.remove(elem)
                                    continue
                                # --- fin de la adición ---
                                start = elem.get("start", "")
                                tvmaze_auth = should_use_tvmaze_authoritative(url, ch_id)
                                new_title, is_series, pref_sub, pref_desc = process_programme(
                                    elem, start,
                                    prefer_latam=prefer_latam,
                                    spanish_season_episode_format=spanish_se_format,
                                    tvmaze_authoritative=tvmaze_auth,
                                )
                                replace_all_title_elements(elem, new_title, prefer_latam)
                                normalize_subtitle_and_desc(elem, prefer_latam, is_series, pref_sub, pref_desc)
                                normalize_episode_num_elements(elem)
                                apply_channel_offset(elem)
                                cloned = clone_element(elem)
                                cloned.set("channel", canonical_ch_id)
                                prog_key = (canonical_ch_id, cloned.get("start"), cloned.get("stop"))
                                if prog_key not in written_programmes:
                                    out_f.write(ET.tostring(cloned, encoding="utf-8"))
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

def apply_channel_offset(elem):
    ch_id = canonical_channel_id(elem.get("channel"))
    offset = CHANNEL_TIME_OFFSETS.get(ch_id, 0)
    if not offset:
        return
    for attr in ("start", "stop"):
        val = elem.get(attr)
        if val:
            m = re.match(r"^(\d{14})(?:\s*([+-]\d{4}))?$", val)
            if m:
                dt = datetime.strptime(m.group(1), "%Y%m%d%H%M%S") + timedelta(minutes=offset)
                tz = m.group(2)
                new_val = f"{dt.strftime('%Y%m%d%H%M%S')} {tz}" if tz else dt.strftime("%Y%m%d%H%M%S")
                elem.set(attr, new_val)

def is_source_allowed_for_channel(channel_id, source_url):
    allowed = CHANNEL_SOURCE_RULES.get(canonical_channel_id(channel_id))
    if not allowed:
        return True
    return source_url in allowed

if __name__ == "__main__":
    main()
