import os
import re
import json
import time
import requests
import unicodedata
from datetime import datetime, timezone

CACHE_FILE = "api_cache.json"

ZAI_API_KEY = os.getenv("ZAI_API_KEY", "").strip()
ZAI_MODEL = (os.getenv("ZAI_MODEL", "glm-5") or "glm-5").strip()

MAX_ITEMS_PER_RUN = int(os.getenv("AI_WORKER_MAX_ITEMS", "8"))
MIN_CONFIDENCE = float(os.getenv("AI_WORKER_MIN_CONFIDENCE", "0.99"))
SLEEP_BETWEEN_CALLS = float(os.getenv("AI_WORKER_SLEEP_SECONDS", "1.5"))

ZAI_URL = "https://api.z.ai/api/paas/v4/chat/completions"


def clean_text(value):
    if value is None:
        return ""
    return str(value).strip()


def normalize_text(text):
    if not text:
        return ""
    text = str(text).lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def parse_tmdb_best_key(key):
    # tmdb_best:<title_norm>:<desc_norm>:<mode>
    parts = key.split(":", 3)
    if len(parts) != 4 or parts[0] != "tmdb_best":
        return None
    return {
        "title_norm": parts[1],
        "desc_norm": parts[2],
        "mode": parts[3],
    }


def parse_tvmaze_key(key):
    # tvmaze:<title_norm>:<date>
    parts = key.split(":", 2)
    if len(parts) != 3 or parts[0] != "tvmaze":
        return None
    return {
        "title_norm": parts[1],
        "air_date": parts[2],
    }


def is_missing(value):
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def find_incomplete_entries(cache):
    items = []

    for key, value in cache.items():
        if not isinstance(value, dict):
            continue

        ai_key = f"ai_fill:{key}"
        if ai_key in cache:
            continue

        if key.startswith("tmdb_best:"):
            missing = []
            for field in ("type", "year", "id", "localized_title"):
                if is_missing(value.get(field)):
                    missing.append(field)

            if missing:
                parsed = parse_tmdb_best_key(key)
                items.append({
                    "key": key,
                    "kind": "tmdb_best",
                    "value": value,
                    "missing_fields": missing,
                    "parsed": parsed or {},
                })

        elif key.startswith("tvmaze:"):
            missing = []
            for field in ("season", "episode", "name"):
                if is_missing(value.get(field)):
                    missing.append(field)

            if missing:
                parsed = parse_tvmaze_key(key)
                items.append({
                    "key": key,
                    "kind": "tvmaze",
                    "value": value,
                    "missing_fields": missing,
                    "parsed": parsed or {},
                })

    # Prioriza entradas con más huecos
    items.sort(key=lambda x: len(x["missing_fields"]), reverse=True)
    return items


def normalized_match(a, b, threshold=0.90):
    na = normalize_text(a)
    nb = normalize_text(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if na in nb or nb in na:
        return True

    # similitud simple por solapamiento de tokens
    ta = set(na.split())
    tb = set(nb.split())
    if not ta or not tb:
        return False
    overlap = len(ta & tb) / max(len(ta), len(tb))
    return overlap >= threshold


def collect_related_evidence(cache, item):
    key = item["key"]
    value = item["value"]
    kind = item["kind"]
    parsed = item["parsed"]

    evidence = {
        "current_key": key,
        "current_value": value,
        "related_entries": []
    }

    related = []

    if kind == "tmdb_best":
        current_id = value.get("id")
        current_type = value.get("type")
        title_norm = parsed.get("title_norm", "")

        # tmdb_title exacto por id/type
        if current_id and current_type:
            prefix = f"tmdb_title:{current_type}:{current_id}:"
            for k, v in cache.items():
                if k.startswith(prefix):
                    related.append({"key": k, "value": v})

        # tmdb_search relacionados por titulo
        for k, v in cache.items():
            if not k.startswith("tmdb_search:"):
                continue
            if not isinstance(v, dict):
                continue

            if title_norm and title_norm in k:
                related.append({"key": k, "value": v})
                continue

            # si existe id, intenta localizarlo en resultados
            if current_id:
                results = v.get("results") or []
                for r in results[:10]:
                    if r.get("id") == current_id:
                        related.append({"key": k, "value": {"matched_result": r}})
                        break

        # tvmaze con titulo parecido
        if title_norm:
            for k, v in cache.items():
                if not k.startswith("tvmaze:"):
                    continue
                parsed_tv = parse_tvmaze_key(k)
                if not parsed_tv:
                    continue
                if parsed_tv["title_norm"] == title_norm:
                    related.append({"key": k, "value": v})

    elif kind == "tvmaze":
        title_norm = parsed.get("title_norm", "")

        # otros tvmaze del mismo titulo
        for k, v in cache.items():
            if not k.startswith("tvmaze:") or k == key:
                continue
            parsed_tv = parse_tvmaze_key(k)
            if parsed_tv and parsed_tv["title_norm"] == title_norm:
                related.append({"key": k, "value": v})

        # tmdb_best del mismo titulo normalizado
        for k, v in cache.items():
            if not k.startswith("tmdb_best:"):
                continue
            parsed_tmdb = parse_tmdb_best_key(k)
            if parsed_tmdb and parsed_tmdb["title_norm"] == title_norm:
                related.append({"key": k, "value": v})

    # Quitar duplicados por key
    seen = set()
    deduped = []
    for entry in related:
        rk = entry["key"]
        if rk in seen:
            continue
        seen.add(rk)
        deduped.append(entry)

    evidence["related_entries"] = deduped[:20]
    return evidence


def flatten_values(value):
    found = []

    def _walk(v):
        if isinstance(v, dict):
            for vv in v.values():
                _walk(vv)
        elif isinstance(v, list):
            for vv in v:
                _walk(vv)
        else:
            found.append(v)

    _walk(value)
    return found


def field_supported_by_evidence(field, proposed_value, evidence):
    if proposed_value is None:
        return False

    all_values = flatten_values(evidence)

    if field in {"year", "type", "id", "season", "episode"}:
        for v in all_values:
            if v == proposed_value:
                return True
            if str(v).strip() == str(proposed_value).strip():
                return True
        return False

    # strings: deben aparecer textualmente o normalizados
    p = clean_text(proposed_value)
    if not p:
        return False

    for v in all_values:
        vs = clean_text(v)
        if not vs:
            continue
        if p == vs:
            return True
        if normalize_text(p) == normalize_text(vs):
            return True

    return False


def call_zai(item, evidence):
    if not ZAI_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {ZAI_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": ZAI_MODEL,
        "temperature": 0,
        "max_tokens": 500,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Tu unico trabajo es rellenar SOLO campos faltantes. "
                    "No cambies nada existente. "
                    "No inventes nada. "
                    "Usa EXCLUSIVAMENTE la evidencia recibida desde api_cache.json. "
                    "Si no estas 99% seguro, responde can_fill=false. "
                    "Devuelve JSON valido con esta forma exacta: "
                    "{"
                    "\"can_fill\":false,"
                    "\"confidence\":0.0,"
                    "\"missing_fields_filled\":{}"
                    "}."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "entry_key": item["key"],
                        "entry_kind": item["kind"],
                        "current_value": item["value"],
                        "missing_fields": item["missing_fields"],
                        "evidence": evidence,
                        "rules": [
                            "Solo rellenar campos faltantes.",
                            "Nunca modificar campos existentes.",
                            "Nunca inventar valores.",
                            "Solo proponer valores que esten soportados por la evidencia.",
                            "Si no hay 99% de certeza, devolver can_fill=false."
                        ],
                    },
                    ensure_ascii=False,
                ),
            },
        ],
    }

    r = requests.post(ZAI_URL, headers=headers, json=payload, timeout=(10, 60))
    r.raise_for_status()
    data = r.json()
    content = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    if not content:
        return None
    return json.loads(content)


def validate_ai_output(item, evidence, ai_output):
    if not isinstance(ai_output, dict):
        return None

    if not ai_output.get("can_fill"):
        return None

    try:
        confidence = float(ai_output.get("confidence", 0.0))
    except Exception:
        return None

    if confidence < MIN_CONFIDENCE:
        return None

    filled = ai_output.get("missing_fields_filled")
    if not isinstance(filled, dict) or not filled:
        return None

    original = item["value"]
    allowed_missing = set(item["missing_fields"])
    validated = {}

    for field, proposed_value in filled.items():
        if field not in allowed_missing:
            continue

        # No tocar existentes
        if not is_missing(original.get(field)):
            continue

        # Debe estar soportado por evidencia
        if not field_supported_by_evidence(field, proposed_value, evidence):
            continue

        # Validaciones básicas
        if field == "type" and proposed_value not in ("tv", "movie"):
            continue

        if field in {"year", "season", "episode", "id"}:
            try:
                proposed_value = int(proposed_value)
            except Exception:
                continue

        if field == "year":
            if proposed_value < 1900 or proposed_value > 2100:
                continue

        validated[field] = proposed_value

    if not validated:
        return None

    return {
        "source": "ai_worker",
        "confidence": confidence,
        "status": "filled_missing_only",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "missing_fields_filled": validated,
    }


def process_one(cache, item):
    ai_key = f"ai_fill:{item['key']}"
    if ai_key in cache:
        return False

    evidence = collect_related_evidence(cache, item)

    try:
        ai_output = call_zai(item, evidence)
    except Exception as e:
        print(f"Error AI en {item['key']}: {e}", flush=True)
        return False

    validated = validate_ai_output(item, evidence, ai_output)
    if not validated:
        return False

    cache[ai_key] = validated
    print(f"AI fill agregado: {ai_key}", flush=True)
    return True


def main():
    if not ZAI_API_KEY:
        print("No hay ZAI_API_KEY; saliendo.", flush=True)
        return

    cache = load_cache()
    if not cache:
        print("api_cache.json vacío o no existe; saliendo.", flush=True)
        return

    candidates = find_incomplete_entries(cache)
    if not candidates:
        print("No hay entradas incompletas para trabajar.", flush=True)
        return

    processed = 0
    changed = 0

    for item in candidates:
        if processed >= MAX_ITEMS_PER_RUN:
            break

        ok = process_one(cache, item)
        processed += 1
        if ok:
            changed += 1

        time.sleep(SLEEP_BETWEEN_CALLS)

    if changed:
        save_cache(cache)

    print(
        f"AI worker terminado | revisadas: {processed} | nuevas ai_fill: {changed}",
        flush=True,
    )


if __name__ == "__main__":
    main()
