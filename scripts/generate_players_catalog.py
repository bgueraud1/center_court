#!/usr/bin/env python3
"""
Generate a players catalog JSON from ATP and WTA CSV files.

Usage:
  python generate_players_catalog.py \
    --atp player_data_atp.csv \
    --wta player_data_wta.csv \
    --out players_catalog.json
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


NAME_CANDIDATES = [
    "full_name", "full name", "Full Name", "Full_Name", "Full_name", "fullname", "fullName",
    "name", "player_name", "player name", "display_name", "display name"
]


def norm_key(k):
    return k.strip() if isinstance(k, str) else k


def parse_rank(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s == "-" or s.lower() == "nan":
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def parse_height_from_row(row):
    for key in ("height_cm", "height_cm_raw", "height", "height_cm "):
        if key in row and row[key]:
            s = str(row[key]).strip()
            m = re.search(r"([\d.,]+)", s)
            if m:
                try:
                    val = float(m.group(1).replace(",", "."))
                    if val < 5:
                        return int(round(val * 100))
                    if 50 < val < 300:
                        return int(round(val))
                except Exception:
                    pass

    for key in ("height_inches", "height_inches "):
        if key in row and row[key]:
            s = str(row[key])
            m = re.search(r"(\d+)\s*'\s*(\d+)", s)
            if m:
                feet = int(m.group(1))
                inches = int(m.group(2))
                total = feet * 12 + inches
                return int(round(total * 2.54))

    return None


def load_csv(path, source):
    out = []
    if not os.path.isfile(path):
        print(f"[WARN] CSV introuvable: {path}", file=sys.stderr)
        return out

    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        reader.fieldnames = [norm_key(fn) for fn in (reader.fieldnames or [])]

        for idx, row in enumerate(reader):
            row2 = {norm_key(k): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
            row2["_source"] = source
            row2["_csv_row_index"] = idx + 1
            out.append(row2)

    return out


def find_name_in_row(row):
    for k in NAME_CANDIDATES:
        if k in row and row.get(k):
            v = row.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    for k, v in row.items():
        if not v or k.startswith("_"):
            continue
        if isinstance(v, str):
            s = v.strip()
            if len(s) > 3 and re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", s) and " " in s:
                return s

    return None


def normalize_bool_play(s):
    if not s:
        return None
    s2 = str(s).lower()
    if "right" in s2:
        return True
    if "left" in s2:
        return False
    return None


def normalize_twohand(s):
    if not s:
        return None
    s2 = str(s).lower()
    if "two" in s2:
        return True
    if "one" in s2:
        return False
    return None


def extract_year_from_string(s):
    if not s or not isinstance(s, str):
        return None

    s0 = s.strip()
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y",
        "%b %d %Y",
        "%B %d %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]

    for fmt in formats:
        try:
            d = datetime.strptime(s0, fmt)
            y = d.year
            if 1800 <= y <= datetime.now().year:
                return y
        except Exception:
            pass

    try:
        d = datetime.fromisoformat(s0)
        y = d.year
        if 1800 <= y <= datetime.now().year:
            return y
    except Exception:
        pass

    m = re.search(r"\b(18|19|20)\d{2}\b", s0)
    if m:
        try:
            y = int(m.group(0))
            if 1800 <= y <= datetime.now().year:
                return y
        except Exception:
            pass

    return None


def build_player_record(row):
    full_name = find_name_in_row(row) or ""
    player_id = (
        row.get("player_id")
        or row.get("id")
        or row.get("playerid")
        or row.get("player id")
        or ""
    )
    country = (row.get("represented_country") or row.get("represented") or row.get("country") or "").strip()
    rank = parse_rank(
        row.get("highest_ranking")
        or row.get("best_rank")
        or row.get("bestRank")
        or row.get("ranking")
        or row.get("best_rank")
    )
    birth_date = row.get("birth_date") or row.get("birthdate") or row.get("birth_date ")
    birthplace = row.get("birthplace") or row.get("birth_place") or row.get("birth place") or row.get("birthplace ")
    height_cm = parse_height_from_row(row)
    plays = row.get("plays") or row.get("play")
    backhand = row.get("backhand") or ""

    birth_year = extract_year_from_string(birth_date) if birth_date else None
    age = None
    if birth_year:
        try:
            age = datetime.now().year - birth_year
        except Exception:
            age = None

    return {
        "full_name": full_name,
        "player_id": player_id,
        "represented_country": country,
        "rank": rank,
        "birth_date": birth_date,
        "birth_year": birth_year,
        "birthplace": birthplace,
        "height_cm": height_cm,
        "plays": plays,
        "backhand": backhand,
        "right_handed": normalize_bool_play(plays),
        "two_handed": normalize_twohand(backhand),
        "age": age,
        "source": row.get("_source", ""),
        "_raw_row": row,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--atp", required=True, help="CSV ATP")
    ap.add_argument("--wta", required=True, help="CSV WTA")
    ap.add_argument("--out", required=True, help="Fichier JSON de sortie")
    ap.add_argument("--timezone", default="Europe/Paris", help="Fuseau horaire pour generated_at")
    args = ap.parse_args()

    try:
        if ZoneInfo:
            tz = ZoneInfo(args.timezone)
            now = datetime.now(tz)
        else:
            now = datetime.now(timezone.utc)
    except Exception:
        now = datetime.now(timezone.utc)

    atp_rows = load_csv(args.atp, "ATP")
    wta_rows = load_csv(args.wta, "WTA")

    atp_players = [build_player_record(r) for r in atp_rows]
    wta_players = [build_player_record(r) for r in wta_rows]
    all_players = atp_players + wta_players

    missing_name = [p for p in all_players if not p.get("full_name")]
    if missing_name:
        print(f"[WARN] {len(missing_name)} lignes sans full_name.", file=sys.stderr)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "generated_at": now.isoformat(),
                "count": len(all_players),
                "players": all_players,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Wrote: {args.out}")
    print(f"Count: {len(all_players)}")


if __name__ == "__main__":
    main()