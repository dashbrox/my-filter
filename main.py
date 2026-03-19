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
ar au bo ca co cl cr do ec sv gt hn it mx ni pa py pe za es se gb us uy ve
""".split()

EPG_URLS = [f"https://iptv-epg.org/files/epg-{code}.xml" for code in EPG_COUNTRY_CODES]
EPG_URLS += [
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PE1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz",
    "https://helmerluzo.github.io/RakutenTV_HL/epg/RakutenTV.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_SV1.xml.gz",
    "https://iptv-epg.org/files/epg-uy.xml",
    "https://www.open-epg.com/files/peru2.xml.gz",
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
USER_AGENT = "xmltv-title-normalizer/1.6-Universal"

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
}

EPG_CO1 = "https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz"
EPG_ES1 = "https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz"
EPG_RAKUTEN = "https://helmerluzo.github.io/RakutenTV_HL/epg/RakutenTV.xml.gz"
EPG_IT = "https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz"
EPG_RAKUTEN2 = "https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz"
EPG_MX = "https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz"
EPG_SV1 = "https://epgshare01.online/epgshare01/epg_ripper_SV1.xml.gz"
EPG_UY1 = "https://iptv-epg.org/files/epg-uy.xml"
EPG_PE2 = "https://www.open-epg.com/files/peru2.xml.gz"

CHANNEL_SOURCE_RULES = {
    "Space.co": [EPG_CO1],
    "M+.Estrenos.es": [EPG_ES1],
    "DAZN.F1.es": [EPG_ES1],
    "tennis-plus": [EPG_RAKUTEN],
    "fashion-tv": [EPG_RAKUTEN],
    "SuperTennis.HD.it": [EPG_IT],
    "UK:.Tennis.Channel.be": [EPG_RAKUTEN2],
    "Canal.DW.(Latinoamérica).mx": [EPG_MX],
    "Canal.Universal.Cinema.sv": [EPG_SV1],
    "Canal.Universal.Comedy.sv": [EPG_SV1],
    "Canal.Universal.Crime.sv": [EPG_SV1],
    "Canal.Universal.Premiere.sv": [EPG_SV1],
    "ENTERTAINMENTTELEVISION.uy": [EPG_UY1],
    "WARNER CHANNEL.pe": [EPG_PE2],
    "WARNER CHANNEL HD.pe": [EPG_PE2],
}

CHANNEL_EQUIVALENCE = {
    "WARNER CHANNEL.pe": [
        "WARNER CHANNEL HD.pe",
        "WARNER.CHANNEL.(Warner).pe",
        "WarnerChannel.pe",
    ],
}

METADATA_SOURCES = [
    "https://epgshare01.online/epgshare01/epg_ripper_PE1.xml.gz",
    "https://iptv-epg.org/files/epg-pe.xml",
]

MATCH_MAX_MINUTES = 720
MATCH_INDIVIDUAL_MAX_MINUTES = 180
MATCH_GOOD_SCORE = 8.5
MATCH_INDIVIDUAL_GOOD_SCORE = 10.5
SUSPICIOUS_REPEAT_THRESHOLD = 3

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

def build_channel_alias_map():
    alias_to_canonical = {}
    for canonical_id, aliases in CHANNEL_EQUIVALENCE.items():
        alias_to_canonical[canonical_id] = canonical_id
        for alias in aliases:
            alias_to_canonical[alias] = canonical_id
    return alias_to_canonical

CHANNEL_ALIAS_MAP = build_channel_alias_map()

def canonical_channel_id(ch_id):
    if not ch_id:
        return ch_id
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
    match = re.search(r'\.([a-z]{2})(?:\.|$|hd|sd|\d)', ch_id.lower())
    if match:
        return match.group(1)
    return None

def tokenize_channel_id(ch_id):
    if not ch_id:
        return set()

    country = get_channel_country_code(ch_id)

    s = ch_id.lower()
    s = re.sub(r'\([^)]*\)', ' ', s)

    if country:
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

def spanish_title_case(text):
    if not text:
        return ""
    parts = re.split(r"(\s+)", text.strip())
    capitalize_next = True

    def transform_word(word, force_capitalize=False):
        if not word:
            return word
        time_fixed = normalize_time_abbreviation(word)
        if time_fixed != word:
            return time_fixed
        if is_dotted_acronym(word):
            return normalize_dotted_acronym(word)
        if should_preserve_allcaps_token(word):
            return normalize_plain_acronym(word)
        if re.fullmatch(r"[IVXLCDM]+", word):
            return word
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
        if suffix == ":" and is_dotted_acronym(new_core) and not new_core.endswith("."):
            new_core += "."
        return f"{prefix}{new_core}{suffix}", ":" in suffix

    for i, part in enumerate(parts):
        if not part or part.isspace():
            continue
        parts[i], should_capitalize_next = transform_token(part, force_capitalize=capitalize_next)
        capitalize_next = should_capitalize_next
    return "".join(parts)

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
    canonical_id = canonical_channel_id(channel_id)
    allowed_sources = CHANNEL_SOURCE_RULES.get(canonical_id)
    if not allowed_sources:
        return True
    return source_url in allowed_sources

# =========================
# METADATA FIX LOGIC
# =========================

def parse_xmltv_dt_to_utc(xmltv_dt):
    if not xmltv_dt:
        return None
    xmltv_dt = xmltv_dt.strip()
    m = re.match(r"^(\d{14})(?:\s*([+-]\d{4}))?$", xmltv_dt)
    if not m:
        return None
    base = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    tz = m.group(2)
    if not tz:
        return base
    sign = 1 if tz[0] == "+" else -1
    hours = int(tz[1:3])
    minutes = int(tz[3:5])
    offset = timedelta(hours=hours, minutes=minutes) * sign
    return base - offset

def programme_duration_minutes(elem):
    start_dt = parse_xmltv_dt_to_utc(elem.get("start", ""))
    stop_dt = parse_xmltv_dt_to_utc(elem.get("stop", ""))
    if not start_dt or not stop_dt:
        return None
    return max(0, int((stop_dt - start_dt).total_seconds() // 60))

def build_programme_signature(elem, prefer_latam=False):
    title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam).strip()
    subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam).strip()
    desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam).strip()
    ep = extract_xmltv_episode_num(elem) or ""
    return (normalize_text(title), normalize_text(subtitle), normalize_text(desc), normalize_text(ep))

def clone_element(elem):
    return ET.fromstring(ET.tostring(elem, encoding="utf-8"))

def copy_editorial_metadata(target_elem, source_elem):
    for tag in ("title", "sub-title", "desc", "episode-num"):
        for child in list(target_elem.findall(tag)):
            target_elem.remove(child)
    source_children = list(source_elem)
    insert_index = 0
    for child in source_children:
        if child.tag in ("title", "sub-title", "desc", "episode-num"):
            target_elem.insert(insert_index, clone_element(child))
            insert_index += 1

def get_programme_title_base(elem, prefer_latam=False):
    title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam).strip()
    return strip_se_from_title(title) or title

def get_series_key(elem, prefer_latam=False):
    return normalize_text(get_programme_title_base(elem, prefer_latam=prefer_latam))

def metadata_quality_score(elem, prefer_latam=False):
    title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam).strip()
    subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam).strip()
    desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam).strip()
    ep = extract_xmltv_episode_num(elem)
    score = 0
    if title:
        score += 2
    if subtitle:
        score += 2
    if desc:
        score += 2
    if ep:
        score += 2
    if subtitle and normalize_text(subtitle) != normalize_text(title):
        score += 1
    if desc and len(desc) > 50:
        score += 1
    return score

def is_metadata_suspicious(elem, prev_elem=None, next_elem=None, prefer_latam=False):
    title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam).strip()
    subtitle = pick_best_localized_text(elem, "sub-title", prefer_latam=prefer_latam).strip()
    desc = pick_best_localized_text(elem, "desc", prefer_latam=prefer_latam).strip()
    ep = extract_xmltv_episode_num(elem)
    if not subtitle or not desc or not ep:
        return True
    if subtitle and normalize_text(subtitle) == normalize_text(title):
        return True
    if prev_elem is not None:
        if build_programme_signature(prev_elem, prefer_latam=prefer_latam) == build_programme_signature(elem, prefer_latam=prefer_latam):
            return True
    if next_elem is not None:
        if build_programme_signature(next_elem, prefer_latam=prefer_latam) == build_programme_signature(elem, prefer_latam=prefer_latam):
            return True
    return False

def anchor_match(schedule_elem, metadata_elem, prefer_latam=False):
    if schedule_elem is None or metadata_elem is None:
        return False
    schedule_key = get_series_key(schedule_elem, prefer_latam=prefer_latam)
    metadata_key = get_series_key(metadata_elem, prefer_latam=prefer_latam)
    if not schedule_key or not metadata_key:
        return False
    if schedule_key == metadata_key:
        return True
    title_ratio = text_similarity(schedule_key, metadata_key)
    if title_ratio >= 0.93:
        return True
    schedule_desc = pick_best_localized_text(schedule_elem, "desc", prefer_latam=prefer_latam)
    metadata_desc = pick_best_localized_text(metadata_elem, "desc", prefer_latam=prefer_latam)
    if schedule_desc and metadata_desc and overlap_score(schedule_desc, metadata_desc) >= 0.45:
        return True
    return False

def get_anchor_title(elem, prefer_latam=False):
    if elem is None:
        return ""
    title = pick_best_localized_text(elem, "title", prefer_latam=prefer_latam).strip()
    return strip_se_from_title(title) or title

def titles_anchor_similar(a, b):
    if not a or not b:
        return False
    na = normalize_text(a)
    nb = normalize_text(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if text_similarity(a, b) >= 0.65:
        return True
    if overlap_score(a, b) >= 0.25:
        return True
    return False

def extract_episode_from_desc_or_xml(elem):
    ep = extract_xmltv_episode_num(elem)
    if ep:
        return ep
    desc = pick_best_localized_text(elem, "desc", prefer_latam=True) or pick_best_localized_text(elem, "desc", prefer_latam=False)
    return extract_se_regex(desc)

def score_metadata_match(schedule_elem, metadata_elem, prefer_latam=False):
    schedule_start = parse_xmltv_dt_to_utc(schedule_elem.get("start", ""))
    metadata_start = parse_xmltv_dt_to_utc(metadata_elem.get("start", ""))
    if not schedule_start or not metadata_start:
        return -999.0
    delta_minutes = abs((schedule_start - metadata_start).total_seconds()) / 60.0
    if delta_minutes > MATCH_MAX_MINUTES:
        return -999.0

    schedule_title = pick_best_localized_text(schedule_elem, "title", prefer_latam=prefer_latam)
    metadata_title = pick_best_localized_text(metadata_elem, "title", prefer_latam=prefer_latam)
    schedule_desc = pick_best_localized_text(schedule_elem, "desc", prefer_latam=prefer_latam)
    metadata_desc = pick_best_localized_text(metadata_elem, "desc", prefer_latam=prefer_latam)
    schedule_base = strip_se_from_title(schedule_title) or schedule_title
    metadata_base = strip_se_from_title(metadata_title) or metadata_title

    title_ratio = text_similarity(schedule_base, metadata_base)
    desc_ratio = overlap_score(schedule_desc, metadata_desc) if schedule_desc and metadata_desc else 0.0

    dur_a = programme_duration_minutes(schedule_elem)
    dur_b = programme_duration_minutes(metadata_elem)
    duration_score = 0.0
    if dur_a is not None and dur_b is not None:
        diff = abs(dur_a - dur_b)
        if diff <= 5:
            duration_score = 2.0
        elif diff <= 10:
            duration_score = 1.0
        elif diff > 20:
            return -999.0

    score = 0.0
    score += max(0.0, 4.0 - (delta_minutes / 10.0))
    score += title_ratio * 5.0
    score += desc_ratio * 4.0
    score += duration_score
    metadata_ep = extract_xmltv_episode_num(metadata_elem)
    if metadata_ep:
        score += 0.5
    return round(score, 3)

def score_metadata_match_loose(schedule_elem, metadata_elem, prefer_latam=False):
    schedule_title = pick_best_localized_text(schedule_elem, "title", prefer_latam=prefer_latam)
    metadata_title = pick_best_localized_text(metadata_elem, "title", prefer_latam=prefer_latam)
    schedule_desc = pick_best_localized_text(schedule_elem, "desc", prefer_latam=prefer_latam)
    metadata_desc = pick_best_localized_text(metadata_elem, "desc", prefer_latam=prefer_latam)
    schedule_base = strip_se_from_title(schedule_title) or schedule_title
    metadata_base = strip_se_from_title(metadata_title) or metadata_title
    title_ratio = text_similarity(schedule_base, metadata_base)
    desc_ratio = overlap_score(schedule_desc, metadata_desc) if schedule_desc and metadata_desc else 0.0
    dur_a = programme_duration_minutes(schedule_elem)
    dur_b = programme_duration_minutes(metadata_elem)
    duration_score = 0.0
    if dur_a is not None and dur_b is not None:
        diff = abs(dur_a - dur_b)
        if diff <= 5:
            duration_score = 2.0
        elif diff <= 10:
            duration_score = 1.0
        elif diff > 20:
            duration_score = -3.0
    score = 0.0
    score += title_ratio * 6.0
    score += desc_ratio * 5.0
    score += duration_score
    metadata_ep = extract_xmltv_episode_num(metadata_elem)
    if metadata_ep:
        score += 0.5
    return round(score, 3)

def score_individual_match(schedule_elem, metadata_elem, prefer_latam=False):
    schedule_start = parse_xmltv_dt_to_utc(schedule_elem.get("start", ""))
    metadata_start = parse_xmltv_dt_to_utc(metadata_elem.get("start", ""))
    if not schedule_start or not metadata_start:
        return -999.0
    delta_minutes = abs((schedule_start - metadata_start).total_seconds()) / 60.0
    if delta_minutes > MATCH_INDIVIDUAL_MAX_MINUTES:
        return -999.0
    score = score_metadata_match(schedule_elem, metadata_elem, prefer_latam=prefer_latam)
    if score <= -999:
        return score
    score += max(0.0, 2.0 - (delta_minutes / 90.0))
    return round(score, 3)

def get_run_boundaries(schedule_entries):
    runs = []
    run_start = 0
    for i in range(1, len(schedule_entries) + 1):
        same_run = False
        if i < len(schedule_entries):
            prev_elem = schedule_entries[i - 1]["elem"]
            curr_elem = schedule_entries[i]["elem"]
            prev_sig = build_programme_signature(prev_elem, prefer_latam=False)
            curr_sig = build_programme_signature(curr_elem, prefer_latam=False)
            prev_stop = prev_elem.get("stop", "")
            curr_start = curr_elem.get("start", "")
            same_run = (
                normalize_channel_id_for_matching(schedule_entries[i - 1]["channel"]) ==
                normalize_channel_id_for_matching(schedule_entries[i]["channel"])
                and prev_sig == curr_sig
                and prev_stop == curr_start
            )
        if not same_run:
            runs.append((run_start, i))
            run_start = i
    return runs

def get_suspicious_run_ranges(schedule_entries):
    suspicious_runs = []
    for start_idx, end_idx in get_run_boundaries(schedule_entries):
        run_len = end_idx - start_idx
        if run_len >= SUSPICIOUS_REPEAT_THRESHOLD:
            suspicious_runs.append((start_idx, end_idx))
    return suspicious_runs

def find_metadata_sequence(schedule_entries, start_idx, end_idx, metadata_entries, prefer_latam=False):
    schedule_run = schedule_entries[start_idx:end_idx]
    if not schedule_run:
        return []

    normalized_channel = normalize_channel_id_for_matching(schedule_run[0]["channel"])
    schedule_series_key = get_series_key(schedule_run[0]["elem"], prefer_latam=prefer_latam)
    run_len = len(schedule_run)

    metadata_by_source = {}
    for idx, item in enumerate(metadata_entries):
        if normalize_channel_id_for_matching(item["channel"]) != normalized_channel:
            continue
        if get_series_key(item["elem"], prefer_latam=prefer_latam) != schedule_series_key:
            continue
        metadata_by_source.setdefault(item["source_url"], []).append((idx, item))

    if not metadata_by_source:
        return []

    schedule_prev = schedule_entries[start_idx - 1]["elem"] if start_idx > 0 else None
    schedule_next = schedule_entries[end_idx]["elem"] if end_idx < len(schedule_entries) else None

    best_sequence = []
    best_score = -999.0

    for source_url, source_items in metadata_by_source.items():
        only_items = [x[1] for x in source_items]
        for local_start in range(len(only_items)):
            seq = only_items[local_start:local_start + run_len]
            if len(seq) < run_len:
                continue
            total = 0.0
            ok = True
            last_ep_num = None
            for sched_item, meta_item in zip(schedule_run, seq):
                meta_elem = meta_item["elem"]
                score = score_metadata_match_loose(sched_item["elem"], meta_elem, prefer_latam=prefer_latam)
                if score < 3.5:
                    ok = False
                    break
                ep_num = extract_xmltv_episode_num(meta_elem)
                if ep_num:
                    m = re.match(r"^\s*S(\d{2})\s*E(\d{2})\s*$", ep_num, flags=re.IGNORECASE)
                    if m:
                        ep_index = (int(m.group(1)), int(m.group(2)))
                        if last_ep_num is not None and ep_index <= last_ep_num:
                            ok = False
                            break
                        last_ep_num = ep_index
                total += score
            if not ok:
                continue

            meta_prev = only_items[local_start - 1]["elem"] if local_start > 0 else None
            meta_next = only_items[local_start + run_len]["elem"] if local_start + run_len < len(only_items) else None

            anchors_available = 0
            anchors_matched = 0
            if schedule_prev is not None:
                anchors_available += 1
                if anchor_match(schedule_prev, meta_prev, prefer_latam=prefer_latam):
                    anchors_matched += 1
            if schedule_next is not None:
                anchors_available += 1
                if anchor_match(schedule_next, meta_next, prefer_latam=prefer_latam):
                    anchors_matched += 1
            if anchors_available == 0:
                continue
            if anchors_matched != anchors_available:
                continue
            if total > best_score:
                best_score = total
                best_sequence = seq
    return best_sequence

def apply_metadata_fix(schedule_entries, metadata_entries, prefer_latam=False, spanish_season_episode_format=False):
    if not schedule_entries or not metadata_entries:
        return

    suspicious_runs = get_suspicious_run_ranges(schedule_entries)
    touched_indexes = set()

    for start_idx, end_idx in suspicious_runs:
        metadata_seq = find_metadata_sequence(schedule_entries, start_idx, end_idx, metadata_entries, prefer_latam=prefer_latam)
        if len(metadata_seq) != (end_idx - start_idx):
            continue
        for offset, meta_item in enumerate(metadata_seq):
            idx = start_idx + offset
            schedule_elem = schedule_entries[idx]["elem"]
            copy_editorial_metadata(schedule_elem, meta_item["elem"])
            start_time_str = schedule_elem.get("start", "")
            new_title, is_series = process_programme(schedule_elem, start_time_str, prefer_latam, spanish_season_episode_format)
            replace_all_title_elements(schedule_elem, new_title, prefer_latam=prefer_latam)
            normalize_subtitle_and_desc(schedule_elem, prefer_latam=prefer_latam, is_series=is_series)
            normalize_episode_num_elements(schedule_elem)
            touched_indexes.add(idx)

    for idx, sched_item in enumerate(schedule_entries):
        if idx in touched_indexes:
            continue
        schedule_elem = sched_item["elem"]
        prev_elem = schedule_entries[idx - 1]["elem"] if idx > 0 else None
        next_elem = schedule_entries[idx + 1]["elem"] if idx + 1 < len(schedule_entries) else None

        if not is_metadata_suspicious(schedule_elem, prev_elem=prev_elem, next_elem=next_elem, prefer_latam=prefer_latam):
            continue

        normalized_channel = normalize_channel_id_for_matching(sched_item["channel"])
        schedule_series_key = get_series_key(schedule_elem, prefer_latam=prefer_latam)

        best_meta = None
        best_score = -999.0
        for meta_item in metadata_entries:
            if normalize_channel_id_for_matching(meta_item["channel"]) != normalized_channel:
                continue
            if get_series_key(meta_item["elem"], prefer_latam=prefer_latam) != schedule_series_key:
                continue
            meta_elem = meta_item["elem"]
            score = score_individual_match(schedule_elem, meta_elem, prefer_latam=prefer_latam)
            if score <= -999:
                continue
            meta_prev = meta_item.get("prev_elem")
            meta_next = meta_item.get("next_elem")
            if prev_elem is not None and anchor_match(prev_elem, meta_prev, prefer_latam=prefer_latam):
                score += 2.0
            if next_elem is not None and anchor_match(next_elem, meta_next, prefer_latam=prefer_latam):
                score += 2.0
            if metadata_quality_score(meta_elem, prefer_latam=prefer_latam) <= metadata_quality_score(schedule_elem, prefer_latam=prefer_latam):
                score -= 2.5
            if score > best_score:
                best_score = score
                best_meta = meta_item

        if best_meta is None or best_score < MATCH_INDIVIDUAL_GOOD_SCORE:
            continue

        copy_editorial_metadata(schedule_elem, best_meta["elem"])
        start_time_str = schedule_elem.get("start", "")
        new_title, is_series = process_programme(schedule_elem, start_time_str, prefer_latam, spanish_season_episode_format)
        replace_all_title_elements(schedule_elem, new_title, prefer_latam=prefer_latam)
        normalize_subtitle_and_desc(schedule_elem, prefer_latam=prefer_latam, is_series=is_series)
        normalize_episode_num_elements(schedule_elem)

# =========================
# WARNER POR BLOQUES DE 3 HORAS
# =========================

def floor_to_hour(dt_obj):
    return dt_obj.replace(minute=0, second=0, microsecond=0)

def floor_to_window(dt_obj, hours=3):
    floored = floor_to_hour(dt_obj)
    window_hour = (floored.hour // hours) * hours
    return floored.replace(hour=window_hour)

def get_xmltv_day_key(elem):
    dt_obj = parse_xmltv_dt_to_utc(elem.get("start", ""))
    if not dt_obj:
        return None
    return dt_obj.strftime("%Y%m%d")

def slice_entries_by_day(entries):
    days = {}
    for item in entries:
        day_key = get_xmltv_day_key(item["elem"])
        if not day_key:
            continue
        days.setdefault(day_key, []).append(item)
    for day_key in days:
        days[day_key].sort(key=lambda x: parse_xmltv_dt_to_utc(x["elem"].get("start", "")) or datetime.min)
    return days

def split_schedule_day_into_3h_windows(day_entries):
    windows = []
    if not day_entries:
        return windows

    starts = [parse_xmltv_dt_to_utc(item["elem"].get("start", "")) for item in day_entries]
    starts = [x for x in starts if x is not None]
    if not starts:
        return windows

    first_dt = min(starts)
    last_dt = max(starts)
    current = floor_to_window(first_dt, hours=3)

    while current <= last_dt + timedelta(hours=3):
        window_end = current + timedelta(hours=3)
        block = []
        for item in day_entries:
            start_dt = parse_xmltv_dt_to_utc(item["elem"].get("start", ""))
            if start_dt is None:
                continue
            if current <= start_dt < window_end:
                block.append(item)
        if block:
            windows.append({
                "window_start": current,
                "window_end": window_end,
                "items": block
            })
        current = window_end

    return windows

def check_anchor_soft(schedule_elem, metadata_elem, prefer_latam=False):
    if schedule_elem is None and metadata_elem is None:
        return True
    if schedule_elem is None or metadata_elem is None:
        return False

    t1 = get_anchor_title(schedule_elem, prefer_latam=prefer_latam)
    t2 = get_anchor_title(metadata_elem, prefer_latam=prefer_latam)

    if titles_anchor_similar(t1, t2):
        return True

    d1 = programme_duration_minutes(schedule_elem)
    d2 = programme_duration_minutes(metadata_elem)

    if d1 and d2:
        if d1 > 45 and d2 > 45 and text_similarity(t1, t2) >= 0.20:
            return True
        if d1 <= 45 and d2 <= 45 and text_similarity(t1, t2) >= 0.20:
            return True

    if overlap_score(t1, t2) > 0.2:
        return True

    return False

def is_monotonic_episode_block(items):
    last_ep = None
    for item in items:
        donor_ep = extract_episode_from_desc_or_xml(item["elem"])
        if donor_ep:
            m = re.match(r"^\s*S(\d{2})\s*E(\d{2})\s*$", donor_ep, flags=re.IGNORECASE)
            if m:
                curr_ep = (int(m.group(1)), int(m.group(2)))
                if last_ep is not None and curr_ep <= last_ep:
                    return False
                last_ep = curr_ep
    return True

def build_metadata_candidate_slices(metadata_entries, count_needed):
    candidates = []
    if count_needed <= 0:
        return candidates

    by_source_day = {}
    for item in metadata_entries:
        source_url = item["source_url"]
        day_key = get_xmltv_day_key(item["elem"])
        if not day_key:
            continue
        by_source_day.setdefault((source_url, day_key), []).append(item)

    for key, items in by_source_day.items():
        items.sort(key=lambda x: parse_xmltv_dt_to_utc(x["elem"].get("start", "")) or datetime.min)
        if len(items) < count_needed:
            continue
        source_url, day_key = key
        for idx in range(0, len(items) - count_needed + 1):
            slice_items = items[idx:idx + count_needed]
            candidates.append({
                "source_url": source_url,
                "day_key": day_key,
                "start_idx": idx,
                "end_idx": idx + count_needed,
                "items": slice_items,
                "all_items": items,
            })
    return candidates

def score_block_candidate(schedule_block_items, candidate, prefer_latam=False):
    donor_items = candidate["items"]
    if len(schedule_block_items) != len(donor_items):
        return -999.0

    sched_prev = None
    sched_next = None
    donor_prev = None
    donor_next = None

    total_score = 0.0
    strict_title_hits = 0

    for sched_item, donor_item in zip(schedule_block_items, donor_items):
        sched_elem = sched_item["elem"]
        donor_elem = donor_item["elem"]

        score = score_metadata_match_loose(sched_elem, donor_elem, prefer_latam=prefer_latam)
        if score < 1.5:
            return -999.0

        s_title = get_programme_title_base(sched_elem, prefer_latam=prefer_latam)
        d_title = get_programme_title_base(donor_elem, prefer_latam=prefer_latam)
        if titles_anchor_similar(s_title, d_title):
            strict_title_hits += 1

        total_score += score

    if strict_title_hits == 0:
        return -999.0

    if not is_monotonic_episode_block(donor_items):
        return -999.0

    first_sched_idx = schedule_block_items[0].get("_global_idx")
    last_sched_idx = schedule_block_items[-1].get("_global_idx")

    if first_sched_idx is not None and first_sched_idx > 0:
        sched_prev = schedule_block_items[0]["_all_schedule_entries"][first_sched_idx - 1]["elem"]
    if last_sched_idx is not None and last_sched_idx + 1 < len(schedule_block_items[0]["_all_schedule_entries"]):
        sched_next = schedule_block_items[0]["_all_schedule_entries"][last_sched_idx + 1]["elem"]

    all_donor_items = candidate["all_items"]
    if candidate["start_idx"] > 0:
        donor_prev = all_donor_items[candidate["start_idx"] - 1]["elem"]
    if candidate["end_idx"] < len(all_donor_items):
        donor_next = all_donor_items[candidate["end_idx"]]["elem"]

    prev_ok = check_anchor_soft(sched_prev, donor_prev, prefer_latam=prefer_latam)
    next_ok = check_anchor_soft(sched_next, donor_next, prefer_latam=prefer_latam)

    if not prev_ok and not next_ok:
        return -999.0

    if prev_ok:
        total_score += 5.0
    if next_ok:
        total_score += 5.0

    first_sched_title = get_programme_title_base(schedule_block_items[0]["elem"], prefer_latam=prefer_latam)
    last_sched_title = get_programme_title_base(schedule_block_items[-1]["elem"], prefer_latam=prefer_latam)
    first_donor_title = get_programme_title_base(donor_items[0]["elem"], prefer_latam=prefer_latam)
    last_donor_title = get_programme_title_base(donor_items[-1]["elem"], prefer_latam=prefer_latam)

    if titles_anchor_similar(first_sched_title, first_donor_title):
        total_score += 2.5
    if titles_anchor_similar(last_sched_title, last_donor_title):
        total_score += 2.5

    return round(total_score, 3)

def apply_metadata_fix_warner_by_3h_blocks(schedule_entries, metadata_entries, prefer_latam=False, spanish_season_episode_format=False):
    if not schedule_entries or not metadata_entries:
        return

    canonical = "WARNER CHANNEL.pe"

    schedule_entries = [
        item for item in schedule_entries
        if canonical_channel_id(item["channel"]) == canonical
    ]
    metadata_entries = [
        item for item in metadata_entries
        if canonical_channel_id(item["channel"]) == canonical
    ]

    if not schedule_entries or not metadata_entries:
        return

    schedule_entries.sort(key=lambda x: parse_xmltv_dt_to_utc(x["elem"].get("start", "")) or datetime.min)

    for idx, item in enumerate(schedule_entries):
        item["_global_idx"] = idx
        item["_all_schedule_entries"] = schedule_entries

    schedule_days = slice_entries_by_day(schedule_entries)

    for _, day_entries in sorted(schedule_days.items()):
        windows = split_schedule_day_into_3h_windows(day_entries)

        for window in windows:
            block_items = window["items"]
            if len(block_items) < 2:
                continue

            candidates = build_metadata_candidate_slices(metadata_entries, len(block_items))
            if not candidates:
                continue

            best_candidate = None
            best_score = -999.0

            for candidate in candidates:
                score = score_block_candidate(block_items, candidate, prefer_latam=prefer_latam)
                if score > best_score:
                    best_score = score
                    best_candidate = candidate

            if not best_candidate or best_score < 8.0:
                continue

            for sched_item, donor_item in zip(block_items, best_candidate["items"]):
                schedule_elem = sched_item["elem"]
                donor_elem = donor_item["elem"]

                copy_editorial_metadata(schedule_elem, donor_elem)

                start_time_str = schedule_elem.get("start", "")
                new_title, is_series = process_programme(
                    schedule_elem,
                    start_time_str,
                    prefer_latam,
                    spanish_season_episode_format
                )
                replace_all_title_elements(schedule_elem, new_title, prefer_latam=prefer_latam)
                normalize_subtitle_and_desc(schedule_elem, prefer_latam=prefer_latam, is_series=is_series)
                normalize_episode_num_elements(schedule_elem)

# =========================
# TMDB
# =========================

def tmdb_search_multi(title, language):
    if not TMDB_API_KEY:
        return None
    cache_key = f"tmdb_search:{normalize_text(title)}:{language}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    url = "https://api.themoviedb.org/3/search/multi"
    params = {"api_key": TMDB_API_KEY, "query": title, "language": language}
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

def get_tmdb_localized_title(tmdb_id, media_type, prefer_latam=False):
    if not TMDB_API_KEY or not tmdb_id or media_type not in ("movie", "tv"):
        return None
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

def get_tmdb_data(title, desc="", subtitle="", year=None, prefer_latam=False):
    if not TMDB_API_KEY or not title:
        return None
    cache_key = f"tmdb_best_v2:{normalize_text(title)}:{normalize_text(subtitle)}:{normalize_text(desc)[:160]}:{year or ''}:{'latam' if prefer_latam else 'default'}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    search_lang = "es-MX" if prefer_latam else "es-ES"
    data = tmdb_search_multi(title, search_lang)
    if not data or not data.get("results"):
        cache_set(cache_key, None)
        return None
    expected_type = infer_media_type_from_desc(desc)
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
    cache_key = f"tvmaze_v2:{normalize_text(show_name)}:{air_date}:{normalize_text(subtitle)}:{normalize_text(desc)[:160]}:{year or ''}"
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
            ranked_shows.append((score, show))
        if not ranked_shows:
            cache_set(cache_key, None)
            return None
        ranked_shows.sort(key=lambda x: x[0], reverse=True)
        best_episode = None
        best_score = -999.0
        subtitle_hint = strip_leading_se_from_text(subtitle or "").strip()
        for base_score, show in ranked_shows[:5]:
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
                        "show_id": show_id,
                        "show_name": show.get("name"),
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
    else:
        cleaned_desc = (current_desc or "").strip()
    cleaned_desc = strip_leading_se_from_desc(cleaned_desc)
    chosen_subtitle = current_subtitle.strip() if current_subtitle else ""
    if not chosen_subtitle and extracted_ep_title:
        chosen_subtitle = extracted_ep_title
    chosen_subtitle = strip_leading_se_from_text(chosen_subtitle).strip()
    should_spanish_case_subtitle = bool(chosen_subtitle) and (prefer_latam or has_spanish_variant(elem, "sub-title") or extracted_ep_title is not None)
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

def process_programme(elem, start_time_str, prefer_latam=False, spanish_season_episode_format=False):
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
    final_title = base_title
    should_translate = prefer_latam and not xml_has_spanish_title
    need_tmdb = (not final_year) or should_translate or prefer_latam
    need_tv = not final_se
    tmdb_data = None
    canonical_title = None
    tvmaze_data = None
    source_canonical_title = base_title or clean_title or raw_title

    if need_tmdb or need_tv:
        tmdb_data = get_tmdb_data(final_title, desc=raw_desc, subtitle=subtitle_hint, year=final_year, prefer_latam=prefer_latam)

    if tmdb_data:
        localized_title = (tmdb_data.get("localized_title") or "").strip()
        canonical_from_tmdb = (tmdb_data.get("canonical_title") or "").strip()
        if localized_title:
            canonical_title = localized_title
            if should_translate or should_replace_with_localized_title(final_title, localized_title):
                final_title = localized_title
        elif canonical_from_tmdb:
            canonical_title = canonical_from_tmdb

    if not final_year and tmdb_data:
        final_year = tmdb_data.get("year")

    should_try_tvmaze = False
    if tmdb_data and tmdb_data.get("type") == "tv":
        should_try_tvmaze = True
    elif final_se:
        should_try_tvmaze = True

    if should_try_tvmaze:
        try:
            air_date = datetime.strptime(start_time_str[:8], "%Y%m%d").strftime("%Y-%m-%d")
            query_title = final_title or base_title or clean_title
            tvmaze_data = get_tvmaze_episode(query_title, air_date, desc=raw_desc, subtitle=subtitle_hint, year=final_year)
            if not tvmaze_data and clean_title and clean_title != query_title:
                fallback_title = strip_se_from_title(clean_title) or clean_title
                fallback_title = remove_episode_title_from_series_title(fallback_title, subtitle_hint)
                if fallback_title != query_title:
                    tvmaze_data = get_tvmaze_episode(fallback_title, air_date, desc=raw_desc, subtitle=subtitle_hint, year=final_year)
            if not final_se and tvmaze_data and tvmaze_data.get("season") is not None and tvmaze_data.get("episode") is not None:
                final_se = normalize_season_ep_from_numbers(tvmaze_data["season"], tvmaze_data["episode"])
            if tvmaze_data and tvmaze_data.get("show_name"):
                tvmaze_show_name = tvmaze_data["show_name"].strip()
                if normalize_text(final_title) == normalize_text(tvmaze_show_name):
                    canonical_title = tvmaze_show_name
        except ValueError:
            pass

    if final_title and (prefer_latam or xml_has_spanish_title):
        final_title = spanish_title_case(final_title)
    if canonical_title:
        final_title = preserve_special_casing(final_title, canonical_title)
    if source_canonical_title:
        final_title = preserve_special_casing(final_title, source_canonical_title)
    final_title = apply_title_case_overrides(final_title)
    display_title = final_title
    is_series = bool(final_se) or bool(tmdb_data and tmdb_data.get("type") == "tv")
    if final_se:
        display_se = format_season_episode_display(final_se, use_spanish=spanish_season_episode_format)
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

def collect_metadata_entries(url, allowed_channels_set):
    entries = []
    allowed_canonical = {canonical_channel_id(ch) for ch in allowed_channels_set}

    try:
        print(f"Cargando metadata universal desde: {url}", flush=True)
        download_xml(url, TEMP_INPUT)
        context = ET.iterparse(TEMP_INPUT, events=("end",))
        previous_kept = None

        for event, elem in context:
            if elem.tag != "programme":
                elem.clear()
                continue

            ch_id = elem.get("channel")
            canonical_id = canonical_channel_id(ch_id)

            if canonical_id in allowed_canonical:
                cloned = clone_element(elem)
                cloned.set("channel", canonical_id)
                entry = {
                    "channel": canonical_id,
                    "elem": cloned,
                    "source_url": url,
                    "prev_elem": clone_element(previous_kept["elem"]) if previous_kept else None,
                    "next_elem": None,
                }
                if previous_kept is not None:
                    previous_kept["next_elem"] = clone_element(cloned)
                entries.append(entry)
                previous_kept = entry

            elem.clear()

        del context
    except Exception as e:
        print(f"Error cargando metadata: {e}", flush=True)
    finally:
        if os.path.exists(TEMP_INPUT):
            os.remove(TEMP_INPUT)

    print(f"Metadata cargada: {len(entries)} programas", flush=True)
    return entries

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
            context = ET.iterparse(TEMP_INPUT, events=("end",))
            for event, elem in context:
                if elem.tag == "channel":
                    real_id = elem.get("id")
                    if not real_id:
                        elem.clear()
                        continue

                    canonical_real = canonical_channel_id(real_id)

                    for user_id in allowed_channels:
                        if canonical_real == canonical_channel_id(user_id):
                            CHANNEL_ID_ALIASES.setdefault(real_id, [])
                            if user_id not in CHANNEL_ID_ALIASES[real_id]:
                                CHANNEL_ID_ALIASES[real_id].append(user_id)
                                print(f"  -> Match validado: '{real_id}' == '{user_id}'", flush=True)

                elem.clear()

            del context
            if os.path.exists(TEMP_INPUT):
                os.remove(TEMP_INPUT)
        except Exception as e:
            print(f"Error escaneando fuente {url}: {e}", flush=True)

    metadata_entries = []
    for metadata_url in METADATA_SOURCES:
        metadata_entries.extend(collect_metadata_entries(metadata_url, allowed_channels))

    metadata_channels_available = {
        canonical_channel_id(item["channel"])
        for item in metadata_entries
    }

    written_programmes = set()
    written_channels = set()

    try:
        with open(TEMP_OUTPUT, "wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')

            for idx, url in enumerate(EPG_URLS, start=1):
                processed_programmes = 0
                prefer_latam = is_latam_feed(url)
                spanish_season_episode_format = use_spanish_season_episode_format(url)
                source_programmes_to_write = []

                try:
                    print(f"[{idx}/{len(EPG_URLS)}] Fuente: {url}", flush=True)
                    download_xml(url, TEMP_INPUT)
                    context = ET.iterparse(TEMP_INPUT, events=("end",))

                    for event, elem in context:
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

                            elem.clear()

                        elif elem.tag == "programme":
                            processed_programmes += 1
                            if processed_programmes % 5000 == 0:
                                print(f"Procesando... {processed_programmes} programas", flush=True)

                            ch_id = elem.get("channel")
                            canonical_ch_id = canonical_channel_id(ch_id)
                            is_ch_allowed = canonical_ch_id in allowed_canonical and is_source_allowed_for_channel(ch_id, url)

                            if is_ch_allowed:
                                start = elem.get("start", "")
                                new_title, is_series = process_programme(elem, start, prefer_latam, spanish_season_episode_format)
                                replace_all_title_elements(elem, new_title, prefer_latam=prefer_latam)
                                normalize_subtitle_and_desc(elem, prefer_latam=prefer_latam, is_series=is_series)
                                normalize_episode_num_elements(elem)
                                apply_channel_offset(elem)

                                cloned_programme = clone_element(elem)
                                cloned_programme.set("channel", canonical_ch_id)

                                source_programmes_to_write.append({
                                    "channel": canonical_ch_id,
                                    "elem": cloned_programme,
                                    "source_url": url
                                })
                            elem.clear()

                    if source_programmes_to_write:
                        schedule_to_fix = [
                            item for item in source_programmes_to_write
                            if canonical_channel_id(item["channel"]) in metadata_channels_available
                        ]

                        if schedule_to_fix and metadata_entries:
                            print("Aplicando corrección de metadatos universal...", flush=True)

                            warner_schedule = [
                                item for item in schedule_to_fix
                                if canonical_channel_id(item["channel"]) == "WARNER CHANNEL.pe"
                            ]
                            other_schedule = [
                                item for item in schedule_to_fix
                                if canonical_channel_id(item["channel"]) != "WARNER CHANNEL.pe"
                            ]

                            if other_schedule:
                                apply_metadata_fix(other_schedule, metadata_entries, prefer_latam, spanish_season_episode_format)

                            if warner_schedule:
                                print(f"Aplicando corrección Warner por bloques de 3 horas a {len(warner_schedule)} programas...", flush=True)
                                apply_metadata_fix_warner_by_3h_blocks(
                                    warner_schedule,
                                    metadata_entries,
                                    prefer_latam=prefer_latam,
                                    spanish_season_episode_format=spanish_season_episode_format
                                )

                        for item in source_programmes_to_write:
                            prog_elem = item["elem"]
                            target_ch_id = item["channel"]

                            if canonical_channel_id(target_ch_id) not in allowed_canonical:
                                continue

                            write_elem = prog_elem
                            write_elem.set("channel", target_ch_id)

                            start = write_elem.get("start")
                            stop = write_elem.get("stop")
                            prog_key = (target_ch_id, start, stop)

                            if prog_key not in written_programmes:
                                out_f.write(ET.tostring(write_elem, encoding="utf-8"))
                                out_f.write(b"\n")
                                written_programmes.add(prog_key)

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
