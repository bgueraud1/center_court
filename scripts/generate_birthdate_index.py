#!/usr/bin/env python3
"""
Robust generator for docs/tools/players_by_birth.json

- tolerant to different CSV header names and BOMs
- tries multiple header variants for name/player_id/birth_date/rank
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

def parse_birthdate(raw):
    raw = (raw or "").strip()
    if not raw:
        return ""
    # quick pass: if already yyyy-mm-dd
    if len(raw) >= 8 and raw[4:5] == "-":
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
