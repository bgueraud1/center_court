from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import tennis_performance as tp

PARIS_TZ = ZoneInfo("Europe/Paris")

ATP_GS_CUT = 104
ATP_M1000_CUT_DEFAULT = 70
ATP_M1000_CUT_MONTECARLO = 45
ATP_M1000_CUT_PARIS = 45

WTA_COMBINED_1000_IDS = {"609", "902", "1038", "709", "806", "1017", "1020"}
WTA_NON_COMBINED_1000_IDS = {"1003", "718", "1075"}


def _today_year() -> int:
    return datetime.now(tz=PARIS_TZ).year


def _norm_text(value: Any) -> str:
    s = tp.clean_str(value)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def _norm_event_id(value: Any) -> str:
    s = tp.clean_str(value)
    if not s:
        return ""
    try:
        return str(int(s))
    except Exception:
        return s.lstrip("0") or s


def _parse_snapshot_date(path: Path) -> Optional[date]:
    m = re.search(r"data_(\d{4})_(\d{2})_(\d{2})\.csv$", path.name)
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _first_date_in_row(row: Dict[str, Any], keys: Iterable[str]) -> Optional[date]:
    for key in keys:
        d = tp.parse_date(row.get(key))
        if d is not None:
            return d
    return None


def discover_last_match_date(root_dir: Path) -> date:
    """
    Scan all ATP/WTA match CSVs and return the latest available date.
    We use match_date first, then date, end_date, start_date.
    """
    bases = [
        root_dir / "docs" / "matches" / "atp_matches",
        root_dir / "docs" / "matches" / "wta_matches",
    ]
    max_date: Optional[date] = None

    for base in bases:
        if not base.exists():
            continue
        for path in base.rglob("*.csv"):
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    d = _first_date_in_row(row, ("match_date", "date", "end_date", "start_date"))
                    if d is None:
                        continue
                    if max_date is None or d > max_date:
                        max_date = d

    if max_date is None:
        raise FileNotFoundError("No match dates found in docs/matches/atp_matches or docs/matches/wta_matches")

    return max_date


def _parse_rankings_csv(path: Path, circuit: str) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        rows.extend(reader)

    circuit = circuit.upper()

    if circuit == "WTA":
        by_id: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            pid = _norm_event_id(row.get("player_id"))
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
        exact[_norm_text(name)] = payload
        parts = [_norm_text(p) for p in name.split() if p.strip()]
        if parts:
            surname_index[parts[-1]].append(payload)

    return {"exact": exact, "surname_index": surname_index}


def load_ranking_snapshots(root_dir: Path, circuit: str) -> List[Tuple[date, Dict[str, Any]]]:
    base = root_dir / ("atp_rankings" if circuit.upper() == "ATP" else "wta_rankings")
    if not base.exists():
        raise FileNotFoundError(f"Missing ranking directory: {base}")

    snapshots: List[Tuple[date, Dict[str, Any]]] = []
    for path in sorted(base.rglob("data_*.csv")):
        snap_date = _parse_snapshot_date(path)
        if snap_date is None:
            continue
        snapshots.append((snap_date, _parse_rankings_csv(path, circuit)))

    if not snapshots:
        raise FileNotFoundError(f"No ranking snapshots found in {base}")

    snapshots.sort(key=lambda x: x[0])
    return snapshots


def pick_snapshot(snapshots: List[Tuple[date, Dict[str, Any]]], when: Optional[date]) -> Dict[str, Any]:
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

    key = _norm_text(player_name)
    if key in exact:
        row = exact[key]
        return row.get("ranking"), row.get("full_name")

    m = re.match(r"^\s*([A-Za-z])\.\s*(.+?)\s*$", tp.clean_str(player_name))
    if m:
        initial = _norm_text(m.group(1))
        surname = _norm_text(m.group(2))
        bucket = surname_index.get(surname, [])
        if bucket:
            filtered = []
            for row in bucket:
                tokens = row["full_name"].split()
                first_initial = _norm_text(tokens[0][0]) if tokens and tokens[0] else ""
                if first_initial == initial:
                    filtered.append(row)
            if len(filtered) == 1:
                row = filtered[0]
                return row.get("ranking"), row.get("full_name")
            if len(bucket) == 1:
                row = bucket[0]
                return row.get("ranking"), row.get("full_name")

    parts = [_norm_text(p) for p in tp.clean_str(player_name).split() if p.strip()]
    if parts:
        bucket = surname_index.get(parts[-1], [])
        if len(bucket) == 1:
            row = bucket[0]
            return row.get("ranking"), row.get("full_name")

    return None, None


def resolve_wta_rank(snapshot: Dict[str, Any], player_id: Any) -> Tuple[Optional[int], Optional[str]]:
    pid = _norm_event_id(player_id)
    row = snapshot.get("by_id", {}).get(pid)
    if not row:
        return None, None
    return row.get("ranking"), row.get("full_name")


def load_window_matches(root_dir: Path, circuit: str, window_start: date, window_end: date) -> List[Dict[str, Any]]:
    """
    tp.load_matches already filters by dates; we call it year by year to cover
    a 52-week window that may cross a year boundary.
    """
    matches: List[Dict[str, Any]] = []
    for year in range(window_start.year, window_end.year + 1):
        matches.extend(
            tp.load_matches(
                root_dir,
                circuit,
                year,
                period="rolling_52_weeks",
                start=window_start,
                end=window_end + timedelta(days=1),
            )
        )
    return matches


def build_event_meta(matches: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    meta: Dict[str, Dict[str, Any]] = {}
    for m in matches:
        ek = m["event_key"]
        cur = meta.setdefault(
            ek,
            {
                "event_key": ek,
                "event_id": tp.clean_str(m.get("event_id")),
                "tourney_name": tp.clean_str(m.get("tourney_name")),
                "level": tp.clean_str(m.get("level_canonical")),
                "start_date": m.get("start_date"),
                "end_date": m.get("end_date"),
                "match_date": m.get("match_date"),
                "draw_size": m.get("draw_size"),
            },
        )
        for key in ("start_date", "end_date", "match_date"):
            v = m.get(key)
            if v is None:
                continue
            if cur.get(key) is None:
                cur[key] = v
            else:
                if key == "start_date" and v < cur[key]:
                    cur[key] = v
                elif key in {"end_date", "match_date"} and v > cur[key]:
                    cur[key] = v
    return meta


def build_player_tournaments(
    root_dir: Path,
    circuit: str,
    window_start: date,
    window_end: date,
    snapshots: List[Tuple[date, Dict[str, Any]]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    matches = load_window_matches(root_dir, circuit, window_start, window_end)
    if not matches:
        return [], {}

    participations = tp.build_participations(matches, ranking_map={})
    summaries = tp.summarize_players(participations, ranking_map={}, period="rolling_52_weeks")
    event_meta = build_event_meta(matches)

    rows: List[Dict[str, Any]] = []
    circuit = circuit.upper()

    for summary in summaries.values():
        for t in summary.tournaments:
            meta = event_meta.get(t["event_key"], {})
            event_date = (
                meta.get("end_date")
                or meta.get("match_date")
                or meta.get("start_date")
                or tp.parse_date(t.get("match_date"))
            )

            row = {
                "player_id": summary.player_id,
                "player_name": summary.player_name,
                "event_key": t.get("event_key"),
                "event_id": tp.clean_str(t.get("event_id") or meta.get("event_id")),
                "tourney_name": tp.clean_str(t.get("tourney_name") or meta.get("tourney_name")),
                "level": tp.clean_str(t.get("level") or meta.get("level")),
                "start_date": meta.get("start_date"),
                "end_date": meta.get("end_date"),
                "match_date": meta.get("match_date") or tp.parse_date(t.get("match_date")),
                "event_date": event_date,
                "draw_size": meta.get("draw_size") or t.get("draw_size"),
                "points": tp.parse_int(t.get("points_earned")) or 0,
                "best_round_order": tp.parse_int(t.get("best_round_order")) or 999,
                "best_round_label": tp.clean_str(t.get("best_round_label")),
            }

            snap = pick_snapshot(snapshots, event_date)
            if circuit == "ATP":
                ranking, ranking_name = resolve_atp_rank(snap, row["player_name"])
            else:
                ranking, ranking_name = resolve_wta_rank(snap, row["player_id"])

            row["ranking_at_event"] = ranking
            row["ranking_name"] = ranking_name
            rows.append(row)

    return rows, event_meta


def classify_atp(row: Dict[str, Any]) -> str:
    level = tp.clean_str(row.get("level"))
    if level.startswith("Grand Slam"):
        return "gs"
    if "1000" in level or "Masters 1000" in level:
        return "m1000"
    return "other"


def classify_wta(row: Dict[str, Any]) -> str:
    level = tp.clean_str(row.get("level"))
    eid = _norm_event_id(row.get("event_id"))
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
    event_id = _norm_event_id(row.get("event_id"))

    if level.startswith("Grand Slam"):
        return rank <= ATP_GS_CUT

    if "1000" in level or "Masters 1000" in level:
        cut = ATP_M1000_CUT_MONTECARLO if event_id in {"410", "352"} else ATP_M1000_CUT_DEFAULT
        return rank <= cut

    return True


def _top_n(events: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return sorted(
        events,
        key=lambda r: (-r["points"], r["event_date"] or date.min, r["event_key"]),
    )[:n]


def apply_atp_replacements(selected: List[Dict[str, Any]], all_other: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remplace jusqu'à 3 mauvais M1000 obligatoires par un meilleur ATP 500/250
    plus tardif. Si le candidat est déjà dans les autres résultats retenus,
    on retire simplement le M1000.
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
    candidates.sort(key=lambda r: (-r["points"], r["event_date"] or date.min, r["event_key"]))

    replacements_done = 0
    for dropped in sorted(m1000s, key=lambda r: (r["points"], r["event_date"] or date.min, r["event_key"])):
        if replacements_done >= 3:
            break

        chosen = None
        for cand in candidates:
            if not cand.get("event_date") or not dropped.get("event_date"):
                continue
            if cand["event_date"] <= dropped["event_date"]:
                continue
            if cand["points"] <= dropped["points"]:
                continue
            chosen = cand
            break

        if chosen is None:
            continue

        selected = [r for r in selected if r["event_key"] != dropped["event_key"]]
        selected_ids.discard(dropped["event_key"])

        if chosen["event_key"] not in selected_ids:
            selected.append(chosen)
            selected_ids.add(chosen["event_key"])

        replacements_done += 1

    return selected


def build_weekly_breakdown(selected_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[date, List[Dict[str, Any]]] = defaultdict(list)

    for ev in selected_events:
        d = ev.get("event_date") or ev.get("end_date") or ev.get("match_date") or ev.get("start_date")
        if d is None:
            continue
        week_start = d - timedelta(days=d.weekday())
        buckets[week_start].append(ev)

    weeks: List[Dict[str, Any]] = []
    for week_start in sorted(buckets.keys()):
        evs = sorted(buckets[week_start], key=lambda r: (r.get("event_date") or date.min, r["event_key"]))
        week_end = week_start + timedelta(days=6)
        weeks.append(
            {
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "points": sum(ev["points"] for ev in evs),
                "events": [
                    {
                        "event_id": ev["event_id"],
                        "event_key": ev["event_key"],
                        "tourney_name": ev["tourney_name"],
                        "level": ev["level"],
                        "event_date": (ev.get("event_date") or ev.get("end_date") or ev.get("match_date") or ev.get("start_date")).isoformat()
                        if (ev.get("event_date") or ev.get("end_date") or ev.get("match_date") or ev.get("start_date"))
                        else None,
                        "points": ev["points"],
                        "ranking_at_event": ev.get("ranking_at_event"),
                        "ranking_name": ev.get("ranking_name"),
                    }
                    for ev in evs
                ],
            }
        )
    return weeks


def competition_ranks(rows: List[Dict[str, Any]]) -> None:
    rows.sort(key=lambda x: (-x["race_points"], x["player_name"]))
    current_rank = 0
    prev_points = None
    for idx, row in enumerate(rows, start=1):
        if row["race_points"] != prev_points:
            current_rank = idx
            prev_points = row["race_points"]
        row["rank"] = current_rank


def build_player_rows(
    root_dir: Path,
    circuit: str,
    window_start: date,
    window_end: date,
    snapshots: List[Tuple[date, Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    raw_rows, _event_meta = build_player_tournaments(root_dir, circuit, window_start, window_end, snapshots)
    circuit = circuit.upper()

    by_player: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in raw_rows:
        row["bucket"] = classify_atp(row) if circuit == "ATP" else classify_wta(row)
        if circuit == "ATP":
            lvl = tp.clean_str(row["level"]).upper()
            if "500" in lvl:
                row["level_tag"] = "ATP 500"
            elif "250" in lvl:
                row["level_tag"] = "ATP 250"
            else:
                row["level_tag"] = ""
        by_player[row["player_id"]].append(row)

    out: List[Dict[str, Any]] = []

    for player_id, events in by_player.items():
        events.sort(key=lambda r: (r.get("event_date") or date.min, r["event_key"]))

        player_name = next(
            (r.get("ranking_name") or r["player_name"] for r in events if r.get("ranking_name") or r["player_name"]),
            player_id,
        )

        window_end_snapshot = pick_snapshot(snapshots, window_end)
        if circuit == "ATP":
            ranking_at_window_end, ranking_name_at_end = resolve_atp_rank(window_end_snapshot, player_name)
        else:
            ranking_at_window_end, ranking_name_at_end = resolve_wta_rank(window_end_snapshot, player_id)

        if circuit == "ATP":
            mandatory = [r for r in events if r["bucket"] in {"gs", "m1000"} and atp_cut_ok(r)]
            excluded_mandatory = [r for r in events if r["bucket"] in {"gs", "m1000"} and not atp_cut_ok(r)]
            other_pool = [r for r in events if r["bucket"] == "other"]
            extra_other_slots = 6 + len(excluded_mandatory)

            selected_others = _top_n(other_pool, extra_other_slots)
            selected = mandatory + selected_others
            selected = apply_atp_replacements(selected, other_pool)

            selected = sorted(selected, key=lambda r: (r.get("event_date") or date.min, r["event_key"]))
            race_points = sum(r["points"] for r in selected)

            out.append(
                {
                    "player_id": player_id,
                    "player_name": player_name,
                    "ranking_at_window_end": ranking_at_window_end,
                    "ranking_name_at_window_end": ranking_name_at_end,
                    "race_points": race_points,
                    "mandatory_points": sum(r["points"] for r in selected if r["bucket"] in {"gs", "m1000"}),
                    "other_points": sum(r["points"] for r in selected if r["bucket"] == "other"),
                    "mandatory_events_count": len([r for r in selected if r["bucket"] in {"gs", "m1000"}]),
                    "excluded_mandatory_count": len(excluded_mandatory),
                    "other_slots": extra_other_slots,
                    "selected_events_count": len(selected),
                    "selected_events": [
                        {
                            "event_id": r["event_id"],
                            "event_key": r["event_key"],
                            "tourney_name": r["tourney_name"],
                            "level": r["level"],
                            "event_date": (r.get("event_date") or r.get("end_date") or r.get("match_date") or r.get("start_date")).isoformat()
                            if (r.get("event_date") or r.get("end_date") or r.get("match_date") or r.get("start_date"))
                            else None,
                            "points": r["points"],
                            "ranking_at_event": r.get("ranking_at_event"),
                            "ranking_name": r.get("ranking_name"),
                            "bucket": r.get("bucket"),
                        }
                        for r in selected
                    ],
                    "weeks": build_weekly_breakdown(selected),
                }
            )
            continue

        mandatory_gs = [r for r in events if r["bucket"] == "gs"]
        combined = [r for r in events if r["bucket"] == "combined"]
        non_combined = [r for r in events if r["bucket"] == "non_combined"]
        others = [r for r in events if r["bucket"] == "other"]

        combined_selected = _top_n(combined, 6)
        non_combined_selected = _top_n(non_combined, 1)
        other_selected = _top_n(others, 7)

        selected = mandatory_gs + combined_selected + non_combined_selected + other_selected
        selected = sorted(selected, key=lambda r: (r.get("event_date") or date.min, r["event_key"]))
        race_points = sum(r["points"] for r in selected)

        out.append(
            {
                "player_id": player_id,
                "player_name": player_name,
                "ranking_at_window_end": ranking_at_window_end,
                "ranking_name_at_window_end": ranking_name_at_end,
                "race_points": race_points,
                "mandatory_points": sum(r["points"] for r in mandatory_gs + combined_selected + non_combined_selected),
                "other_points": sum(r["points"] for r in other_selected),
                "mandatory_events_count": len(mandatory_gs) + len(combined_selected) + len(non_combined_selected),
                "other_slots": 7,
                "selected_events_count": len(selected),
                "selected_events": [
                    {
                        "event_id": r["event_id"],
                        "event_key": r["event_key"],
                        "tourney_name": r["tourney_name"],
                        "level": r["level"],
                        "event_date": (r.get("event_date") or r.get("end_date") or r.get("match_date") or r.get("start_date")).isoformat()
                        if (r.get("event_date") or r.get("end_date") or r.get("match_date") or r.get("start_date"))
                        else None,
                        "points": r["points"],
                        "ranking_at_event": r.get("ranking_at_event"),
                        "ranking_name": r.get("ranking_name"),
                        "bucket": r.get("bucket"),
                    }
                    for r in selected
                ],
                "weeks": build_weekly_breakdown(selected),
            }
        )

    competition_ranks(out)
    return out


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def build_window_file(root_dir: Path, circuit: str, window_start: date, window_end: date, output_path: Path) -> None:
    snapshots = load_ranking_snapshots(root_dir, circuit)
    rows = build_player_rows(root_dir, circuit, window_start, window_end, snapshots)

    payload = {
        "circuit": circuit.upper(),
        "mode": "rolling_52_weeks",
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "window_days": 52 * 7,
        "generated_at": datetime.now(tz=PARIS_TZ).isoformat(),
        "count": len(rows),
        "ranking": rows,
    }
    write_json(output_path, payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ATP/WTA rolling 52-week rankings.")
    parser.add_argument("--root", type=Path, default=Path("."), help="Repository root")
    parser.add_argument("--output-dir", type=Path, default=Path(".docs/"), help="Where to write the JSON files")
    args = parser.parse_args()

    root_dir = args.root.resolve()
    output_dir = args.output_dir.resolve()

    last_match_date = discover_last_match_date(root_dir)
    window_end = last_match_date
    window_start = window_end - timedelta(weeks=52)

    build_window_file(root_dir, "ATP", window_start, window_end, output_dir / "rolling_52w_ranking_atp.json")
    build_window_file(root_dir, "WTA", window_start, window_end, output_dir / "rolling_52w_ranking_wta.json")


if __name__ == "__main__":
    main()