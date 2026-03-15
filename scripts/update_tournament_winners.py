#!/usr/bin/env python3
"""
scripts/update_tournament_winners.py

Usage:
  python3 scripts/update_tournament_winners.py --input-list created_files.txt \
      --json-base docs/data/tournaments/json_by_tournaments \
      --alt-json-base docs/data/tournaments/tournaments_by_json \
      --out-wta docs/wta_tournaments_winners.json \
      --out-atp docs/atp_tournaments_winners.json

Ce script :
 - lit created_files.txt (liste des CSV nouvellement générés)
 - essaie d'extraire (tour, id, year) depuis les chemins listés
 - recherche le fichier tournament.json correspondant dans le répertoire json_base (et alt_base)
 - pour chaque tournament.json trouvé, récupère meta.source, meta.tourney_id, meta.year,
   puis cherche le match gagnant (priorité : match_id == MS001/LS001, sinon round == 'F')
 - met à jour le json winners (wta ou atp) en conservant/écrasant l'entrée existante pour (tourney_id,year)
"""

import argparse
import json
import sys
from pathlib import Path
import re

def load_json(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as e:
        print(f"[WARN] failed to load json {path}: {e}")
        return None

def write_json_pretty(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding='utf-8')
    print(f"[OK] wrote {path}")

def find_tournament_json_candidates(json_base, alt_base, tour, tid, year):
    candidates = []
    # standard candidate path: <json_base>/<tour>/<tour>_<tid>_<year>/tournament.json
    p1 = Path(json_base) / tour / f"{tour}_{tid}_{year}" / "tournament.json"
    if p1.exists():
        candidates.append(p1)
    # alternative layout
    p2 = Path(alt_base) / tour / f"{tour}_{tid}_{year}" / "tournament.json"
    if p2.exists() and p2 != p1:
        candidates.append(p2)
    # also try any tournament.json under json_base/tour that contains tid and year in parent name
    base = Path(json_base) / tour
    if base.exists():
        for t in base.rglob("tournament.json"):
            parent = t.parent.name
            if re.search(rf"{tid}", parent) and re.search(rf"{year}", parent):
                if t not in candidates:
                    candidates.append(t)
    return candidates

def get_winner_from_tournament(obj):
    matches = obj.get("matches", [])
    # Prefer explicit match_ids
    for pref in ("MS001", "LS001"):
        for m in matches:
            if m.get("match_id") == pref:
                return m.get("player_id_winner") or "", m.get("winner_player_name") or ""
    # fallback: final round
    for m in matches:
        r = m.get("round", "") or ""
        if r.strip().upper() in ("F", "FINAL"):
            return m.get("player_id_winner") or "", m.get("winner_player_name") or ""
    # fallback: try match_id that starts with MS and round=F, etc.
    for m in matches:
        if (m.get("match_id","").upper().startswith("MS") or m.get("match_id","").upper().startswith("LS")) and (m.get("round","").upper() in ("F","FINAL")):
            return m.get("player_id_winner") or "", m.get("winner_player_name") or ""
    return None, None

def update_winners_file(path: Path, entry_map: dict):
    """
    path: path to winners json (array style)
    entry_map: dict keyed by 'tourneyid_year' -> entry dict
    """
    existing = {}
    if path.exists():
        try:
            v = json.loads(path.read_text(encoding='utf-8'))
            if isinstance(v, list):
                for e in v:
                    key = f"{e.get('tourney_id')}_{e.get('year')}"
                    existing[key] = e
            elif isinstance(v, dict):
                # if user previously stored as dict, accept it
                existing = v
        except Exception as e:
            print(f"[WARN] can't parse existing winners file {path}: {e}")
    # merge / overwrite with entry_map
    for k, v in entry_map.items():
        existing[k] = v
    # produce a stable list of entries (sorted)
    list_out = []
    # if existing originally was dict keyed by key we still convert to list
    for k in sorted(existing.keys()):
        list_out.append(existing[k])
    write_json_pretty(path, list_out)

def parse_id_year_from_path(p: str):
    # try to find patterns like atp_305_2026 or wta-300-2017 or atp/305/2026 etc.
    # return tuples of (tour, id, year) or None
    s = p.replace("\\", "/")
    # pattern examples: atp_305_2026
    m = re.search(r"(atp|wta)[_/-]?[_]?(\d{1,6})[_-](\d{4})", s, re.IGNORECASE)
    if m:
        return m.group(1).lower(), m.group(2), m.group(3)
    # pattern example: .../atp/atp_305_2026/...
    m2 = re.search(r"/(atp|wta)/([^/]*?(\d{1,6})[^/]*)", s, re.IGNORECASE)
    if m2:
        # try to extract id/year from the captured part
        sub = m2.group(2)
        m3 = re.search(r"(\d{1,6}).*?(\d{4})", sub)
        if m3:
            return m2.group(1).lower(), m3.group(1), m3.group(2)
    # try any two numbers in path: id (<=6digits) and year (4digits)
    m4 = re.search(r"(\d{1,6}).*?(\d{4})", s)
    if m4:
        # ambiguous tour -> try to see if path contains 'atp' or 'wta'
        tour = "atp" if "atp" in s.lower() else ("wta" if "wta" in s.lower() else None)
        if tour:
            return tour, m4.group(1), m4.group(2)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-list", required=True, help="created_files.txt")
    parser.add_argument("--json-base", default="docs/data/tournaments/json_by_tournaments", help="base dir where per-tourney jsons were written")
    parser.add_argument("--alt-json-base", default="docs/data/tournaments/tournaments_by_json", help="alternate base layout")
    parser.add_argument("--out-wta", default="docs/wta_tournaments_winners.json")
    parser.add_argument("--out-atp", default="docs/atp_tournaments_winners.json")
    args = parser.parse_args()

    created = Path(args.input_list)
    if not created.exists():
        print(f"[WARN] input list {created} not found -> nothing to do.")
        sys.exit(0)

    lines = [l.strip() for l in created.read_text(encoding='utf-8').splitlines() if l.strip()]
    if not lines:
        print("[INFO] created_files.txt empty -> nothing to do.")
        sys.exit(0)

    winners_atp = {}
    winners_wta = {}

    # process each created file and try to find matching tournament.json
    seen = set()
    for l in lines:
        parsed = parse_id_year_from_path(l)
        if not parsed:
            print(f"[WARN] couldn't parse tour/id/year from path: {l}")
            continue
        tour, tid, year = parsed
        key = f"{tour}_{tid}_{year}"
        if key in seen:
            continue
        seen.add(key)
        # find candidates
        cand = find_tournament_json_candidates(args.json_base, args.alt_json_base, tour, tid, year)
        if not cand:
            print(f"[WARN] no tournament.json candidates for {tour} {tid} {year} (from {l})")
            continue
        # pick first candidate and load
        chosen = None
        for c in cand:
            obj = load_json(c)
            if not obj:
                continue
            # basic sanity check: meta matches tid/year
            meta = obj.get("meta", {})
            if str(meta.get("tourney_id","")) == str(tid) and str(meta.get("year","")) == str(year):
                chosen = c
                tjson = obj
                break
        if not chosen:
            # fallback: accept first valid candidate
            for c in cand:
                obj = load_json(c)
                if obj:
                    chosen = c
                    tjson = obj
                    break
        if not chosen:
            print(f"[WARN] couldn't load any tournament.json for {tour} {tid} {year}")
            continue

        source = tjson.get("meta", {}).get("source", tour.upper())
        tourney_id = tjson.get("meta", {}).get("tourney_id") or tid
        year_v = tjson.get("meta", {}).get("year") or year

        pid, pname = get_winner_from_tournament(tjson)
        if not pid and not pname:
            print(f"[WARN] no winner found in {chosen} for {tour} {tid} {year}")
            continue

        entry = {
            "source": source,
            "tourney_id": str(tourney_id),
            "year": int(year_v) if str(year_v).isdigit() else year_v,
            "player_id_winner": pid,
            "winner_player_name": pname
        }

        if tour.lower() == "atp":
            key2 = f"{tourney_id}_{year_v}"
            winners_atp[key2] = entry
        else:
            key2 = f"{tourney_id}_{year_v}"
            winners_wta[key2] = entry

        print(f"[INFO] found winner for {tour} {tourney_id} {year_v}: {pname} ({pid}) from {chosen}")

    # update files
    if winners_wta:
        update_winners_file(Path(args.out_wta), winners_wta)
    else:
        print("[INFO] no new WTA winners to add.")

    if winners_atp:
        update_winners_file(Path(args.out_atp), winners_atp)
    else:
        print("[INFO] no new ATP winners to add.")

if __name__ == "__main__":
    main()