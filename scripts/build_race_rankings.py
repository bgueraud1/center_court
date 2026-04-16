from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import tennis_performance as tp

PARIS_TZ = ZoneInfo("Europe/Paris")

ATP_GS_CUT = 104
ATP_M1000_CUT_DEFAULT = 70
ATP_M1000_CUT_MONTECARLO = 45
ATP_M1000_CUT_PARIS = 45

WTA_COMBINED_1000_IDS = {"609", "902", "1038", "709", "806", "1017", "1020"}
WTA_NON_COMBINED_1000_IDS = {"1003", "718", "1075"}


def today_year() -> int:
    return datetime.now(tz=PARIS_TZ).year


def clean_ascii_key(value: Any) -> str:
    s = tp.clean_str(value)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def norm_event_id(value: Any) -> str:
    s = tp.clean_str(value)
    if not s:
        return ""
    try:
        return str(int(s))
    except Exception:
        return s.lstrip("0") or s


def parse_snapshot_date(path: Path) -> Optional[date]:
    m = re.search(r"data_(\d{4})_(\d{2})_(\d{2})\.csv$", path.name)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def parse_rankings_csv(path: Path, circuit: str) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        rows.extend(reader)

    circuit = circuit.upper()

    if circuit == "WTA":
        by_id: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            pid = norm_event_id(row.get("player_id"))
            if not pid:
                continue
            by_id[pid] = {
                "player_id": pid,
                "full_name": tp.clean_str(row.get("full_name")),
                "ranking": tp.parse_int(row.get("ranking")),
                "points": tp.parse_int(row.get("points")),
                "date": row.get("date"),
            }
        return {"by_id": by_id}

    exact: Dict[str, Dict[str, Any]] = {}
    surname_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for row in rows:
        name = tp.clean_str(row.get("full_name"))
        if not name:
            continue
        payload = {
            "full_name": name,
            "ranking": tp.parse_int(row.get("ranking")),
            "points": tp.parse_int(row.get("points")),
            "date": row.get("date"),
        }
        exact[clean_ascii_key(name)] = payload
        parts = [clean_ascii_key(p) for p in name.split() if p.strip()]
        if parts:
            surname_index[parts[-1]].append(payload)

    return {"exact": exact, "surname_index": surname_index}


def load_ranking_snapshots(root_dir: Path, circuit: str) -> List[Tuple[date, Dict[str, Any]]]:
    base = root_dir / ("atp_rankings" if circuit.upper() == "ATP" else "wta_rankings")
    if not base.exists():
        raise FileNotFoundError(f"Missing ranking directory: {base}")

    snapshots: List[Tuple[date, Dict[str, Any]]] = []
    for path in sorted(base.rglob("data_*.csv")):
        snap_date = parse_snapshot_date(path)
        if snap_date is None:
            continue
        snapshots.append((snap_date, parse_rankings_csv(path, circuit)))

    if not snapshots:
        raise FileNotFoundError(f"No ranking snapshots found in {base}")

    snapshots.sort(key=lambda x: x[0])
    return snapshots


def pick_snapshot(
    snapshots: List[Tuple[date, Dict[str, Any]]],
    when: Optional[date],
) -> Dict[str, Any]:
    if not snapshots:
        raise ValueError("No snapshots available")

    if when is None:
        return snapshots[-1][1]

    eligible = [snap for d, snap in snapshots if d <= when]
    if eligible:
        return eligible[-1]
    return snapshots[0][1]


def resolve_atp_rank(snapshot: Dict[str, Any], player_name: str) -> Tuple[Optional[int], Optional[str]]:
    exact = snapshot.get("exact", {})
    surname_index = snapshot.get("surname_index", {})

    key = clean_ascii_key(player_name)
    if key in exact:
        row = exact[key]
        return row.get("ranking"), row.get("full_name")

    # "C. Alcaraz" -> initial + surname
    m = re.match(r"^\s*([A-Za-z])\.\s*(.+?)\s*$", tp.clean_str(player_name))
    if m:
        initial = clean_ascii_key(m.group(1))
        surname = clean_ascii_key(m.group(2))
        bucket = surname_index.get(surname, [])
        if bucket:
            filtered = []
            for row in bucket:
                tokens = row["full_name"].split()
                first_initial = clean_ascii_key(tokens[0][0]) if tokens and tokens[0] else ""
                if first_initial == initial:
                    filtered.append(row)
            if len(filtered) == 1:
                row = filtered[0]
                return row.get("ranking"), row.get("full_name")
            if len(bucket) == 1:
                row = bucket[0]
                return row.get("ranking"), row.get("full_name")

    parts = [clean_ascii_key(p) for p in tp.clean_str(player_name).split() if p.strip()]
    if parts:
        bucket = surname_index.get(parts[-1], [])
        if len(bucket) == 1:
            row = bucket[0]
            return row.get("ranking"), row.get("full_name")

    return None, None


def resolve_wta_rank(snapshot: Dict[str, Any], player_id: Any) -> Tuple[Optional[int], Optional[str]]:
    pid = norm_event_id(player_id)
    row = snapshot.get("by_id", {}).get(pid)
    if not row:
        return None, None
    return row.get("ranking"), row.get("full_name")


def tournament_date(meta: Dict[str, Any]) -> Optional[date]:
    d = meta.get("start_date") or meta.get("match_date") or meta.get("end_date")
    return d


def build_player_tournaments(root_dir: Path, circuit: str, year: int) -> List[Dict[str, Any]]:
    matches = tp.load_matches(root_dir, circuit, year, period="current_year")
    participations = tp.build_participations(matches, ranking_map={})
    summaries = tp.summarize_players(participations, ranking_map={}, period="current_year")

    rows: List[Dict[str, Any]] = []
    for summary in summaries.values():
        for t in summary.tournaments:
            row = {
                "player_id": summary.player_id,
                "player_name": summary.player_name,
                "event_key": t.get("event_key"),
                "event_id": tp.clean_str(t.get("event_id")),
                "tourney_name": tp.clean_str(t.get("tourney_name")),
                "level": tp.clean_str(t.get("level")),
                "start_date": tp.parse_date(t.get("match_date")),
                "end_date": tp.parse_date(t.get("match_date")),
                "match_date": tp.parse_date(t.get("match_date")),
                "draw_size": tp.parse_int(t.get("draw_size")),
                "points": tp.parse_int(t.get("points_earned")) or 0,
                "best_round_order": tp.parse_int(t.get("best_round_order")) or 999,
                "best_round_label": tp.clean_str(t.get("best_round_label")),
            }
            rows.append(row)

    return rows


def attach_ranks(circuit: str, rows: List[Dict[str, Any]], snapshots: List[Tuple[date, Dict[str, Any]]]) -> None:
    for row in rows:
        snap = pick_snapshot(snapshots, tournament_date(row))
        if circuit.upper() == "ATP":
            ranking, name = resolve_atp_rank(snap, row["player_name"])
        else:
            ranking, name = resolve_wta_rank(snap, row["player_id"])
        row["ranking_at_event"] = ranking
        row["ranking_name"] = name


def classify_atp(row: Dict[str, Any]) -> str:
    level = tp.clean_str(row.get("level"))
    if level.startswith("Grand Slam"):
        return "gs"
    if "1000" in level or "Masters 1000" in level:
        return "m1000"
    return "other"


def classify_wta(row: Dict[str, Any]) -> str:
    level = tp.clean_str(row.get("level"))
    eid = norm_event_id(row.get("event_id"))
    if level.startswith("Grand Slam"):
        return "gs"
    if level.startswith("WTA 1000"):
        if eid in WTA_COMBINED_1000_IDS:
            return "combined"
        if eid in WTA_NON_COMBINED_1000_IDS:
            return "non_combined"
    return "other"


def atp_cut_ok(row: Dict[str, Any]) -> bool:
    rank = row.get("ranking_at_event")
    if rank is None:
        return False

    level = tp.clean_str(row.get("level"))
    event_id = norm_event_id(row.get("event_id"))

    if level.startswith("Grand Slam"):
        return rank <= ATP_GS_CUT

    if "1000" in level or "Masters 1000" in level:
        cut = ATP_M1000_CUT_MONTECARLO if event_id in {"410", "352"} else ATP_M1000_CUT_DEFAULT
        return rank <= cut

    return True


def add_bucket_fields(rows: List[Dict[str, Any]], circuit: str) -> None:
    circuit = circuit.upper()
    for row in rows:
        if circuit == "ATP":
            row["bucket"] = classify_atp(row)
            row["level_tag"] = "ATP 500" if "500" in tp.clean_str(row.get("level")).upper() else "ATP 250" if "250" in tp.clean_str(row.get("level")).upper() else ""
        else:
            row["bucket"] = classify_wta(row)


def apply_atp_replacements(selected: List[Dict[str, Any]], all_other: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Replace up to 3 low mandatory ATP M1000 results with a later ATP 500/250 result
    if the replacement is strictly better in points and happens later.
    """
    def bucket_of(r: Dict[str, Any]) -> str:
        b = tp.clean_str(r.get("bucket"))
        return b if b else classify_atp(r)

    m1000s = [r for r in selected if bucket_of(r) == "m1000"]
    if not m1000s:
        return selected

    selected = selected[:]
    selected_ids = {r["event_key"] for r in selected}

    candidates = [
        r for r in all_other
        if bucket_of(r) == "other" and tp.clean_str(r.get("level_tag")) in {"ATP 500", "ATP 250"}
    ]
    candidates.sort(key=lambda r: (-r["points"], r["start_date"] or date.min, r["event_key"]))

    replacements_done = 0
    for dropped in sorted(m1000s, key=lambda r: (r["points"], r["start_date"] or date.min, r["event_key"])):
        if replacements_done >= 3:
            break

        chosen = None
        for cand in candidates:
            if cand["event_key"] in selected_ids:
                continue
            if not cand.get("start_date") or not dropped.get("start_date"):
                continue
            if cand["start_date"] <= dropped["start_date"]:
                continue
            if cand["points"] <= dropped["points"]:
                continue
            chosen = cand
            break

        if chosen is None:
            continue

        selected = [r for r in selected if r["event_key"] != dropped["event_key"]]
        selected_ids.discard(dropped["event_key"])
        selected.append(chosen)
        selected_ids.add(chosen["event_key"])
        replacements_done += 1

    return selected


def competition_ranks(rows: List[Dict[str, Any]]) -> None:
    rows.sort(key=lambda x: (-x["race_points"], x["player_name"]))
    current_rank = 0
    prev_points = None
    for idx, row in enumerate(rows, start=1):
        if row["race_points"] != prev_points:
            current_rank = idx
            prev_points = row["race_points"]
        row["rank"] = current_rank


def compute_race(circuit: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    circuit = circuit.upper()
    add_bucket_fields(rows, circuit)

    by_player: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_player[r["player_id"]].append(r)

    out: List[Dict[str, Any]] = []

    for player_id, events in by_player.items():
        events.sort(key=lambda r: (r["start_date"] or date.min, r["event_key"]))

        player_name = next(
            (r.get("ranking_name") or r["player_name"] for r in events if r.get("ranking_name") or r["player_name"]),
            player_id,
        )

        if circuit == "ATP":
            mandatory = [r for r in events if r["bucket"] in {"gs", "m1000"} and atp_cut_ok(r)]
            excluded_mandatory = [r for r in events if r["bucket"] in {"gs", "m1000"} and not atp_cut_ok(r)]
            other_pool = [r for r in events if r["bucket"] == "other"]
            extra_other_slots = 6 + len(excluded_mandatory)

            selected_others = sorted(
                other_pool,
                key=lambda r: (-r["points"], r["start_date"] or date.min, r["event_key"]),
            )[:extra_other_slots]

            selected = mandatory + selected_others
            selected = apply_atp_replacements(selected, other_pool)

            race_points = sum(r["points"] for r in selected)

            out.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "race_points": race_points,
                    "mandatory_points": sum(r["points"] for r in selected if r["bucket"] in {"gs", "m1000"}),
                    "other_points": sum(r["points"] for r in selected if r["bucket"] == "other"),
                    "mandatory_events_count": len([r for r in selected if r["bucket"] in {"gs", "m1000"}]),
                    "excluded_mandatory_count": len(excluded_mandatory),
                    "other_slots": extra_other_slots,
                    "events": [
                        {
                            "event_id": r["event_id"],
                            "event_key": r["event_key"],
                            "tourney_name": r["tourney_name"],
                            "level": r["level"],
                            "start_date": r["start_date"].isoformat() if r["start_date"] else None,
                            "points": r["points"],
                            "ranking_at_event": r.get("ranking_at_event"),
                            "ranking_name": r.get("ranking_name"),
                            "bucket": r.get("bucket", classify_atp(r)),
                        }
                        for r in sorted(selected, key=lambda x: (x["start_date"] or date.min, x["event_key"]))
                    ],
                }
            )
            continue

        mandatory_gs = [r for r in events if r["bucket"] == "gs"]
        combined = [r for r in events if r["bucket"] == "combined"]
        non_combined = [r for r in events if r["bucket"] == "non_combined"]
        others = [r for r in events if r["bucket"] == "other"]

        combined_selected = sorted(
            combined,
            key=lambda r: (-r["points"], r["start_date"] or date.min, r["event_key"]),
        )[:6]
        non_combined_selected = sorted(
            non_combined,
            key=lambda r: (-r["points"], r["start_date"] or date.min, r["event_key"]),
        )[:1]
        other_selected = sorted(
            others,
            key=lambda r: (-r["points"], r["start_date"] or date.min, r["event_key"]),
        )[:7]

        selected = mandatory_gs + combined_selected + non_combined_selected + other_selected
        race_points = sum(r["points"] for r in selected)

        out.append(
            {
                "player_id": player_id,
                "player_name": player_name,
                "race_points": race_points,
                "mandatory_points": sum(r["points"] for r in mandatory_gs + combined_selected + non_combined_selected),
                "other_points": sum(r["points"] for r in other_selected),
                "mandatory_events_count": len(mandatory_gs) + len(combined_selected) + len(non_combined_selected),
                "other_slots": 7,
                "events": [
                    {
                        "event_id": r["event_id"],
                        "event_key": r["event_key"],
                        "tourney_name": r["tourney_name"],
                        "level": r["level"],
                        "start_date": r["start_date"].isoformat() if r["start_date"] else None,
                        "points": r["points"],
                        "ranking_at_event": r.get("ranking_at_event"),
                        "ranking_name": r.get("ranking_name"),
                        "bucket": r.get("bucket", classify_wta(r)),
                    }
                    for r in sorted(selected, key=lambda x: (x["start_date"] or date.min, x["event_key"]))
                ],
            }
        )

    competition_ranks(out)
    return out


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def build_race_file(root_dir: Path, circuit: str, year: int, output_path: Path) -> None:
    snapshots = load_ranking_snapshots(root_dir, circuit)
    rows = build_player_tournaments(root_dir, circuit, year)
    attach_ranks(circuit, rows, snapshots)
    race = compute_race(circuit, rows)

    payload = {
        "circuit": circuit.upper(),
        "year": year,
        "generated_at": datetime.now(tz=PARIS_TZ).isoformat(),
        "count": len(race),
        "ranking": race,
    }
    write_json(output_path, payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ATP/WTA race rankings for the current year.")
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root")
    parser.add_argument("--year", type=int, default=today_year(), help="Tournament year to process")
    parser.add_argument("--output-dir", type=Path, default=Path("docs"), help="Where to write the JSON files")
    args = parser.parse_args()

    build_race_file(args.root, "ATP", args.year, args.output_dir / "race_ranking_atp.json")
    build_race_file(args.root, "WTA", args.year, args.output_dir / "race_ranking_wta.json")


if __name__ == "__main__":
    main()