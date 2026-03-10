from __future__ import annotations

import gzip
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import requests


# =========================
# CONFIGURACION
# =========================

EPG_COUNTRY_CODES = """
ar au at bo ca cl co cr hr do ec sv
gt hn
mx me nl nz ni ng no pa py pe
es se ch tw th tr ug ua ae gb us uy ve vn zw
""".split()


@dataclass(frozen=True)
class Config:
    channels_file: Path = Path("channels.txt")
    output_file: Path = Path("guia.xml.gz")
    temp_input_file: Path = Path("temp_input.xml")
    temp_output_file: Path = Path("output_temp.xml")
    update_every_hours: int = 6
    request_timeout: int = 120
    chunk_size: int = 1024 * 1024

    @property
    def epg_urls(self) -> list[str]:
        return [f"https://iptv-epg.org/files/epg-{code}.xml" for code in EPG_COUNTRY_CODES]

    @property
    def update_interval_seconds(self) -> int:
        return self.update_every_hours * 3600


CONFIG = Config()


# =========================
# LOGGING
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# =========================
# HELPERS DE TEXTO
# =========================

SEASON_EP_RE = re.compile(r"S\s*(\d+)\s*E\s*(\d+)", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
NEW_RE = re.compile(r"\bNEW\b", re.IGNORECASE)

TITLE_YEAR_RE = re.compile(
    r"^(.*?)(?:\s*[\(\[]\s*(19\d{2}|20\d{2})\s*[\)\]]|\s*-\s*(19\d{2}|20\d{2})|\s+(19\d{2}|20\d{2}))\s*$"
)

DESC_SEASON_EP_RE = re.compile(
    r"^\s*(S\s*\d+\s*E\s*\d+)\s+(.*)",
    re.IGNORECASE | re.DOTALL,
)

DESC_YEAR_RE = re.compile(
    r"^\s*[\(\[]?\s*(19\d{2}|20\d{2})\s*[\)\]]?[\s\-.:\u2013\u2014]+(.*)",
    re.DOTALL,
)


def normalize_season_ep(text: str | None) -> Optional[str]:
    if not text:
        return None

    match = SEASON_EP_RE.search(text)
    if not match:
        return None

    season = int(match.group(1))
    episode = int(match.group(2))
    return f"S{season:02d} E{episode:02d}"


def extract_new_marker(text: str | None) -> tuple[str, bool]:
    if not text:
        return "", False

    cleaned = text
    has_new = False

    if "ᴺᵉʷ" in cleaned:
        has_new = True
        cleaned = cleaned.replace("ᴺᵉʷ", " ")

    if NEW_RE.search(cleaned):
        has_new = True
        cleaned = NEW_RE.sub(" ", cleaned)

    cleaned = " ".join(cleaned.split())
    return cleaned, has_new


def extract_year_from_title(title_text: str | None) -> tuple[str, Optional[str]]:
    if not title_text:
        return "", None

    clean_title = title_text.strip()
    match = TITLE_YEAR_RE.match(clean_title)

    if not match:
        return clean_title, None

    base_title = match.group(1).strip()
    year = match.group(2) or match.group(3) or match.group(4)

    if base_title:
        return base_title, year

    return clean_title, None


def extract_season_ep_from_desc(desc_text: str | None) -> tuple[Optional[str], str]:
    if not desc_text:
        return None, ""

    cleaned = desc_text.strip()
    match = DESC_SEASON_EP_RE.match(cleaned)

    if not match:
        return None, cleaned

    season_ep = normalize_season_ep(match.group(1))
    remaining_desc = match.group(2).strip()
    return season_ep, remaining_desc


def extract_year_from_desc(desc_text: str | None) -> tuple[Optional[str], str]:
    if not desc_text:
        return None, ""

    cleaned = desc_text.strip()
    match = DESC_YEAR_RE.match(cleaned)

    if not match:
        return None, cleaned

    year = match.group(1)
    remaining_desc = match.group(2).strip()

    if remaining_desc:
        return year, remaining_desc

    return None, cleaned


def build_final_title(
    original_title: str | None,
    season_ep: str | None = None,
    year: str | None = None,
) -> str:
    clean_title, has_new = extract_new_marker(original_title)
    clean_title, year_in_title = extract_year_from_title(clean_title)

    final_title = clean_title
    meta_parts: list[str] = []

    if season_ep:
        meta_parts.append(season_ep)
    elif year:
        meta_parts.append(year)
    elif year_in_title:
        meta_parts.append(year_in_title)

    if meta_parts:
        final_title += f" ({' | '.join(meta_parts)})"

    if has_new:
        final_title += " ᴺᵉʷ"

    return final_title


# =========================
# HELPERS DE ARCHIVOS / I-O
# =========================

def safe_remove(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        logger.exception("No se pudo borrar %s", path)


def load_allowed_channels(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    with path.open("r", encoding="utf-8-sig") as f:
        channels = {line.strip() for line in f if line.strip()}

    if not channels:
        raise ValueError(f"El archivo {path} esta vacio")

    return channels


def download_xml(session: requests.Session, url: str, output_path: Path, chunk_size: int, timeout: int) -> None:
    logger.info("Descargando: %s", url)

    with session.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()

        with output_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)


def gzip_file(source_path: Path, target_path: Path, chunk_size: int) -> None:
    logger.info("Comprimiendo resultado...")

    with source_path.open("rb") as f_in, gzip.open(target_path, "wb") as f_out:
        while True:
            chunk = f_in.read(chunk_size)
            if not chunk:
                break
            f_out.write(chunk)


# =========================
# PROCESAMIENTO XML
# =========================

def process_xml_file(
    xml_path: Path,
    allowed_channels: set[str],
    out_f,
    written_channels: set[str],
    written_programmes: set[tuple[str, str, str, str]],
) -> None:
    context = ET.iterparse(xml_path, events=("end",))

    for _, elem in context:
        if elem.tag == "channel":
            process_channel_element(elem, allowed_channels, out_f, written_channels)

        elif elem.tag == "programme":
            process_programme_element(
                elem=elem,
                allowed_channels=allowed_channels,
                out_f=out_f,
                written_programmes=written_programmes,
            )

        elem.clear()


def process_channel_element(
    elem: ET.Element,
    allowed_channels: set[str],
    out_f,
    written_channels: set[str],
) -> None:
    channel_id = elem.get("id")

    if channel_id in allowed_channels and channel_id not in written_channels:
        out_f.write(ET.tostring(elem, encoding="utf-8"))
        out_f.write(b"\n")
        written_channels.add(channel_id)


def process_programme_element(
    elem: ET.Element,
    allowed_channels: set[str],
    out_f,
    written_programmes: set[tuple[str, str, str, str]],
) -> None:
    channel_id = elem.get("channel")

    if channel_id not in allowed_channels:
        return

    title_elem = elem.find("title")
    desc_elem = elem.find("desc")

    season_ep = None
    year = None

    if desc_elem is not None and desc_elem.text:
        season_ep, cleaned_desc = extract_season_ep_from_desc(desc_elem.text)
        desc_elem.text = cleaned_desc

        if not season_ep:
            year, cleaned_desc = extract_year_from_desc(desc_elem.text)
            desc_elem.text = cleaned_desc

    final_title = ""
    if title_elem is not None and title_elem.text:
        final_title = build_final_title(
            original_title=title_elem.text,
            season_ep=season_ep,
            year=year,
        )
        title_elem.text = final_title

    programme_key = (
        channel_id or "",
        elem.get("start", ""),
        elem.get("stop", ""),
        final_title,
    )

    if programme_key in written_programmes:
        return

    out_f.write(ET.tostring(elem, encoding="utf-8"))
    out_f.write(b"\n")
    written_programmes.add(programme_key)


# =========================
# ORQUESTACION
# =========================

def run_once(config: Config) -> None:
    allowed_channels = load_allowed_channels(config.channels_file)

    written_channels: set[str] = set()
    written_programmes: set[tuple[str, str, str, str]] = set()

    session = requests.Session()

    try:
        with config.temp_output_file.open("wb") as out_f:
            out_f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n')

            for url in config.epg_urls:
                try:
                    download_xml(
                        session=session,
                        url=url,
                        output_path=config.temp_input_file,
                        chunk_size=config.chunk_size,
                        timeout=config.request_timeout,
                    )
                    process_xml_file(
                        xml_path=config.temp_input_file,
                        allowed_channels=allowed_channels,
                        out_f=out_f,
                        written_channels=written_channels,
                        written_programmes=written_programmes,
                    )
                except Exception as e:
                    logger.error("Error procesando %s: %s", url, e)
                finally:
                    safe_remove(config.temp_input_file)

            out_f.write(b"</tv>\n")

        gzip_file(
            source_path=config.temp_output_file,
            target_path=config.output_file,
            chunk_size=config.chunk_size,
        )

        logger.info("Listo. Archivo generado: %s", config.output_file)

    finally:
        safe_remove(config.temp_input_file)
        safe_remove(config.temp_output_file)
        session.close()


def run_forever(config: Config) -> None:
    logger.info(
        "Iniciando actualizacion automatica cada %s horas...",
        config.update_every_hours,
    )

    while True:
        try:
            run_once(config)
        except Exception as e:
            logger.exception("Error general en la ejecucion: %s", e)

        logger.info(
            "Esperando %s horas para la siguiente actualizacion...",
            config.update_every_hours,
        )
        time.sleep(config.update_interval_seconds)


def main() -> None:
    run_forever(CONFIG)


if __name__ == "__main__":
    main()
