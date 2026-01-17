#!/usr/bin/env python3
"""
Robust generator for docs/tools/players_by_birth.json

- tolerant to different CSV header names and BOMs
- tries multiple header variants for name/player_id/birth_date/rank
- adds country code (alpha-2) and flag emoji (when mappable) to the JSON
- use: python3 scripts/generate_birthdate_index.py
"""
from pathlib import Path
import csv
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

OUT_DIR = Path("docs/tools")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "players_by_birth.json"

SENTINEL_RANK = 9999999

# date formats to try (common patterns)
DATE_FORMATS = [
    "%Y-%m-%d",    # 2001-08-16
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%b %d %Y",    # Mar 30 1999
    "%B %d %Y",    # March 30 1999
    "%d %b %Y",    # 30 Mar 1999
    "%d %B %Y",    # 30 March 1999
]

# candidate header names for each field
NAME_KEYS = ["full_name", "full name", "fullname", "name", "player_name", "player name", "Full Name", "FullName"]
ID_KEYS = ["player_id", "player id", "playerid", "id", "player"]
BD_KEYS = ["birth_date", "birth date", "birthdate", "dob", "Birth date", "birth_date.0"]
RANK_KEYS = ["highest_ranking", "best_rank", "best rank", "bestRank", "highest_rank", "rank", "best"]
REPRESENTED_KEYS = ["represented_country", "represented country", "country", "country_code", "represented", "nationality", "nation", "represented_country.0"]

# Minimal-ish map ISO3/aliases -> ISO2. Extend as needed.
ISO3_TO_ALPHA2 = {
    # Common tennis countries
    "ARG":"AR","AUS":"AU","AUT":"AT","BEL":"BE","BGR":"BG","BRA":"BR","CAN":"CA","CHN":"CN",
    "COL":"CO","CZE":"CZ","CRO":"HR","ESP":"ES","EST":"EE","FRA":"FR","FRA":"FR","GBR":"GB","UK":"GB",
    "GER":"DE","DEU":"DE","ITA":"IT","JPN":"JP","KOR":"KR","KAZ":"KZ","NED":"NL","NLD":"NL","NZL":"NZ",
    "POL":"PL","PRT":"PT","ROU":"RO","RUS":"RU","SRB":"RS","SLO":"SI","SWE":"SE","SUI":"CH","CHE":"CH",
    "TPE":"TW","UKR":"UA","USA":"US","US":"US","URU":"UY","MEX":"MX","IND":"IN","IRL":"IE","ISR":"IL",
    "SAF":"ZA","ZAF":"ZA","DNK":"DK","HUN":"HU","NOR":"NO","BLR":"BY","VEN":"VE","CHI":"CL","ECU":"EC",
    "PER":"PE","DOM":"DO","PAN":"PA","CYP":"CY","GRC":"GR","GRE":"GR","LUX":"LU","LTU":"LT","LVA":"LV",
    "MYS":"MY","PHL":"PH","SGP":"SG","THA":"TH","VIE":"VN","ALG":"DZ","MAR":"MA","TUN":"TN","EGY":"EG",
    # add aliases
    "ENGLAND":"GB", "SCOTLAND":"GB", "WALES":"GB", "NORTHMACEDONIA":"MK"
}

def parse_birthdate(raw):
    raw = (raw or "").strip()
    if not raw:
        return ""
    # quick pass: if already yyyy-mm-dd
    if len(raw) >= 8 and len(raw) >= 10 and raw[4:5] == "-":
        try:
            # allows '2001-08-16 00:00:00' etc.
            dt = datetime.fromisoformat(raw.split()[0])
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # try formats
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # try cleaning commas
    raw2 = raw.replace(",", " ").strip()
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(raw2, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    logging.debug("Unparsable birth_date: %r", raw)
    return ""

def extract_best_rank(row):
    for key in RANK_KEYS:
        if key in row and row[key] not in (None, "", "NA"):
            v = str(row[key]).strip()
            # try numeric extract
            digits = "".join(ch for ch in v if ch.isdigit() or ch == ".")
            if digits:
                try:
                    return int(float(digits))
                except Exception:
                    pass
            # fallback: try int conversion
            try:
                return int(v)
            except Exception:
                pass
    return SENTINEL_RANK

def get_field(row, candidates):
    # return first non-empty value found among candidate header names
    for key in candidates:
        if key in row:
            val = row.get(key)
            if val is None:
                continue
            v = str(val).strip()
            if v != "":
                return v
    # if none found, also try case-insensitive search
    for k, v in row.items():
        if k and any(k.lower() == c.lower() for c in candidates):
            val = (v or "").strip()
            if val != "":
                return val
    # final fallback: empty
    return ""

def iso_to_alpha2(code):
    """
    Try to produce an ISO alpha-2 code (e.g. 'FR', 'US') from various inputs:
    - if already 2 letters -> return uppercased
    - if 3 letters -> map via ISO3_TO_ALPHA2 (contains common codes and aliases)
    - otherwise try uppercase direct lookup in mapping
    """
    if not code:
        return ""
    c = code.strip().upper()
    if len(c) == 2 and c.isalpha():
        return c
    if len(c) == 3 and c.isalpha():
        # direct map
        if c in ISO3_TO_ALPHA2:
            return ISO3_TO_ALPHA2[c]
        # sometimes country codes are non-standard like 'GBR' vs 'ENG' etc
        # last attempt: check first 2 letters
        cand = c[:2]
        if cand in ISO3_TO_ALPHA2.values() or cand.isalpha():
            return cand
    # try to match mapping keys ignoring non-alpha
    c_alpha = "".join(ch for ch in c if ch.isalpha())
    if c_alpha in ISO3_TO_ALPHA2:
        return ISO3_TO_ALPHA2[c_alpha]
    return ""

def flag_emoji_from_alpha2(alpha2):
    """Return emoji flag for ISO alpha-2 (e.g. 'FR' -> 🇫🇷)."""
    if not alpha2 or len(alpha2) != 2:
        return ""
    # Regional Indicator Symbol Letter A starts at 127462 = ord('A') + 127397
    try:
        return ''.join(chr(ord(ch) + 127397) for ch in alpha2.upper())
    except Exception:
        return ""

def read_csv(path: Path, circuit: str):
    rows = []
    if not path.exists():
        logging.info("CSV not found, skipping: %s", path)
        return rows
    # open with utf-8-sig to remove BOM if present
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        logging.info("Reading %s (%d columns)", path, len(reader.fieldnames or []))
        for i, r in enumerate(reader):
            full_name = get_field(r, NAME_KEYS)
            player_id = get_field(r, ID_KEYS)
            bd_raw = get_field(r, BD_KEYS)
            bd_norm = parse_birthdate(bd_raw)
            day = bd_norm[8:10] if bd_norm else ""
            month = bd_norm[5:7] if bd_norm else ""
            best_rank = extract_best_rank(r)

            # country extraction
            rep = get_field(r, REPRESENTED_KEYS) or ""
            country_alpha2 = iso_to_alpha2(rep)
            flag_emoji = flag_emoji_from_alpha2(country_alpha2) if country_alpha2 else ""

            if not full_name:
                # if name empty, try composing from other columns (rare), otherwise fallback to id
                alt = get_field(r, ["first_name", "firstname"]) or get_field(r, ["last_name", "lastname"])
                if alt:
                    full_name = alt
                else:
                    if player_id:
                        full_name = f"(id:{player_id})"
                    else:
                        full_name = "(unknown)"
                    logging.warning("Row %d: missing full_name, fallback to %r", i+1, full_name)

            rows.append({
                "full_name": full_name,
                "player_id": player_id or "",
                "circuit": circuit,
                "birth_date": bd_norm,
                "birth_day": day,
                "birth_month": month,
                "best_rank": int(best_rank),
                # new fields
                "country": rep,  # original source value (e.g. 'GBR' or 'GB')
                "country_alpha2": country_alpha2,
                "flag_emoji": flag_emoji,
            })
    return rows

def main():
    all_players = []
    all_players += read_csv(Path("player_data_atp.csv"), "ATP")
    all_players += read_csv(Path("player_data_wta.csv"), "WTA")
    logging.info("Total players indexed: %d", len(all_players))
    with OUT_FILE.open("w", encoding="utf-8") as fh:
        json.dump(all_players, fh, ensure_ascii=False, indent=2)
    logging.info("Wrote %s", OUT_FILE)

if __name__ == "__main__":
    main()
