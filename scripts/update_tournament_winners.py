#!/usr/bin/env python3
"""
scripts/update_tournament_winners.py

Usage:
  python3 scripts/update_tournament_winners.py \
      --json-base docs/data/tournaments/json_by_tournaments \
      --alt-json-base docs/data/tournaments/tournaments_by_json \
      --out-wta docs/wta_tournaments_winners.json \
      --out-atp docs/atp_tournaments_winners.json

Ce script :
 - lit les fichiers winners existants (ATP et WTA)
 - parcourt tous les tournament.json sous json-base et alt-json-base
 - détecte les tournois absents des winners files
 - extrait le vainqueur à partir du tournament.json
 - ajoute les nouvelles entrées aux fichiers winners
"""

import argparse
import json
import sys
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple, Any


def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] failed to load json {path}: {e}")
        return None


def write_json_pretty(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"[OK] wrote {path}")


def normalize_year(v: Any):
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    return int(s) if s.isdigit() else s


def winners_entry_key(entry: dict) -> Optional[Tuple[str, str]]:
    tid = entry.get("tourney_id")
    year = entry.get("year")
    if tid is None or year is None:
        return None
    return (str(tid).strip(), str(year).strip())


def load_existing_winners(path: Path) -> List[dict]:
    if not path.exists():
        return []

    obj = load_json(path)
    if obj is None:
        return []

    if isinstance(obj, list):
        out = []
        for item in obj:
            if isinstance(item, dict):
                out.append(item)
        return out

    if isinstance(obj, dict):
        out = []
        for _, item in obj.items():
            if isinstance(item, dict):
                out.append(item)
        return out

    return []


def build_existing_key_set(entries: List[dict]) -> set:
    keys = set()
    for e in entries:
        k = winners_entry_key(e)
        if k is not None:
            keys.add(k)
    return keys


def find_tournament_json_files(base: Path) -> List[Path]:
    if not base.exists():
        return []
    if base.is_file():
        return [base] if base.name == "tournament.json" else []
    files = []
    for p in base.rglob("tournament.json"):
        if p.is_file():
            files.append(p)
    return sorted(files)


def derive_source_from_path(p: Path) -> Optional[str]:
    s = str(p).replace("\\", "/").lower()
    if "/atp/" in s or s.endswith("/atp") or "/atp_" in s or "atp_" in p.name.lower():
        return "ATP"
    if "/wta/" in s or s.endswith("/wta") or "/wta_" in s or "wta_" in p.name.lower():
        return "WTA"
    return None


def find_tournament_json_candidates(json_bases: List[Path], source: str, tid: str, year: str) -> List[Path]:
    candidates = []

    for base in json_bases:
        if not base.exists():
            continue

        # layout 1: <base>/<source>/<source>_<tid>_<year>/tournament.json
        p1 = base / source.lower() / f"{source.lower()}_{tid}_{year}" / "tournament.json"
        if p1.exists():
            candidates.append(p1)

        # layout 2: any tournament.json under <base>/<source> whose parent contains tid/year
        src_dir = base / source.lower()
        if src_dir.exists():
            for t in src_dir.rglob("tournament.json"):
                parent = t.parent.name
                if re.search(rf"\b{re.escape(str(tid))}\b", parent) and re.search(rf"\b{re.escape(str(year))}\b", parent):
                    candidates.append(t)

    # dedupe while keeping order
    seen = set()
    out = []
    for c in candidates:
        rp = str(c.resolve()) if c.exists() else str(c)
        if rp not in seen:
            seen.add(rp)
            out.append(c)
    return out


def get_winner_from_tournament(obj):
    matches = obj.get("matches", []) or []

    # 1) Priority on explicit match IDs
    for pref in ("MS001", "LS001"):
        for m in matches:
            if str(m.get("match_id", "")).strip() == pref:
                return m.get("player_id_winner") or "", m.get("winner_player_name") or ""

    # 2) Final round
    for m in matches:
        r = str(m.get("round", "") or "").strip().upper()
        if r in ("F", "FINAL"):
            return m.get("player_id_winner") or "", m.get("winner_player_name") or ""

    # 3) Another fallback
    for m in matches:
        mid = str(m.get("match_id", "") or "").upper()
        r = str(m.get("round", "") or "").upper()
        if (mid.startswith("MS") or mid.startswith("LS")) and r in ("F", "FINAL"):
            return m.get("player_id_winner") or "", m.get("winner_player_name") or ""

    return None, None


def update_winners_file(path: Path, entries_to_add: List[dict]):
    existing = load_existing_winners(path)
    existing_map: Dict[Tuple[str, str], dict] = {}

    for e in existing:
        k = winners_entry_key(e)
        if k is not None:
            existing_map[k] = e

    for e in entries_to_add:
        k = winners_entry_key(e)
        if k is not None:
            existing_map[k] = e

    def sort_key(entry: dict):
        tid = str(entry.get("tourney_id", ""))
        year = entry.get("year")
        if isinstance(year, int):
            year_key = year
        else:
            s = str(year).strip()
            year_key = int(s) if s.isdigit() else 999999
        return (year_key, tid)

    out = [existing_map[k] for k in sorted(existing_map.keys(), key=lambda x: (int(x[1]) if str(x[1]).isdigit() else 999999, x[0]))]
    # reorder again with a more explicit stable sort on the actual dicts
    out = sorted(out, key=sort_key)

    write_json_pretty(path, out)


def extract_tourney_meta(obj: dict, path: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    meta = obj.get("meta", {}) or {}

    source = meta.get("source")
    if source:
        source = str(source).strip().upper()

    if source not in ("ATP", "WTA"):
        source = derive_source_from_path(path)

    tid = meta.get("tourney_id")
    year = meta.get("year")

    if tid is None or str(tid).strip() == "":
        # fallback: try to parse from path
        s = str(path).replace("\\", "/")
        m = re.search(r"(atp|wta)[_/-]?(\d{1,6})[_-](\d{4})", s, re.IGNORECASE)
        if m:
            source = source or m.group(1).upper()
            tid = m.group(2)
            year = m.group(3)

    if year is None or str(year).strip() == "":
        s = str(path).replace("\\", "/")
        m = re.search(r"(20\d{2})", s)
        if m:
            year = m.group(1)

    if tid is None or year is None:
        return source, None, None

    return source, str(tid).strip(), str(year).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json-base",
        default="docs/data/tournaments/json_by_tournaments",
        help="base dir where per-tourney jsons were written",
    )
    parser.add_argument(
        "--alt-json-base",
        default="docs/data/tournaments/tournaments_by_json",
        help="alternate base layout",
    )
    parser.add_argument(
        "--out-wta",
        default="docs/wta_tournaments_winners.json",
        help="WTA winners json output",
    )
    parser.add_argument(
        "--out-atp",
        default="docs/atp_tournaments_winners.json",
        help="ATP winners json output",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="verbose debug output",
    )
    args = parser.parse_args()

    json_bases = [Path(args.json_base), Path(args.alt_json_base)]
    out_wta = Path(args.out_wta)
    out_atp = Path(args.out_atp)

    existing_wta = load_existing_winners(out_wta)
    existing_atp = load_existing_winners(out_atp)

    existing_wta_keys = build_existing_key_set(existing_wta)
    existing_atp_keys = build_existing_key_set(existing_atp)

    if args.debug:
        print(f"[DEBUG] existing WTA winners: {len(existing_wta_keys)}")
        print(f"[DEBUG] existing ATP winners: {len(existing_atp_keys)}")

    # Scan all tournament.json files in both bases
    all_candidates: List[Path] = []
    seen_paths = set()
    for base in json_bases:
        for p in find_tournament_json_files(base):
            rp = str(p.resolve())
            if rp not in seen_paths:
                seen_paths.add(rp)
                all_candidates.append(p)

    if args.debug:
        print(f"[DEBUG] found {len(all_candidates)} tournament.json file(s)")

    to_add_wta: List[dict] = []
    to_add_atp: List[dict] = []
    seen_new_keys = set()

    for tpath in all_candidates:
        obj = load_json(tpath)
        if not isinstance(obj, dict):
            continue

        source, tid, year = extract_tourney_meta(obj, tpath)
        if source not in ("ATP", "WTA"):
            if args.debug:
                print(f"[DEBUG] skip (unknown source): {tpath}")
            continue
        if not tid or not year:
            if args.debug:
                print(f"[DEBUG] skip (missing tid/year): {tpath}")
            continue

        key = (tid, year)
        if source == "ATP" and key in existing_atp_keys:
            if args.debug:
                print(f"[DEBUG] already present in ATP winners: {key} from {tpath}")
            continue
        if source == "WTA" and key in existing_wta_keys:
            if args.debug:
                print(f"[DEBUG] already present in WTA winners: {key} from {tpath}")
            continue

        # Avoid duplicate adds within the same run
        run_key = (source, tid, year)
        if run_key in seen_new_keys:
            continue
        seen_new_keys.add(run_key)

        pid, pname = get_winner_from_tournament(obj)
        if not pid and not pname:
            print(f"[WARN] no winner found in {tpath} for {source} {tid} {year}")
            continue

        entry = {
            "source": source,
            "tourney_id": str(tid),
            "year": int(year) if str(year).isdigit() else year,
            "player_id_winner": pid,
            "winner_player_name": pname,
        }

        if source == "ATP":
            to_add_atp.append(entry)
            existing_atp_keys.add(key)
        else:
            to_add_wta.append(entry)
            existing_wta_keys.add(key)

        print(f"[INFO] found missing winner for {source} {tid} {year}: {pname} ({pid}) from {tpath}")

    if to_add_wta:
        update_winners_file(out_wta, to_add_wta)
    else:
        print("[INFO] no new WTA winners to add.")

    if to_add_atp:
        update_winners_file(out_atp, to_add_atp)
    else:
        print("[INFO] no new ATP winners to add.")


if __name__ == "__main__":
    main()