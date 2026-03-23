# scripts/build_open_inscriptions_json.py
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
    PARIS_TZ = ZoneInfo("Europe/Paris")
except Exception:
    from datetime import timezone
    PARIS_TZ = timezone.utc

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

ATP_LEVEL_RESTRICTED = {"CH", "FU"}

def norm_space(s: str) -> str:
    return " ".join(str(s).replace("–", "-").replace("—", "-").split()).strip()

def safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        return default

def parse_date_iso(v: str) -> date:
    return date.fromisoformat(v)

def parse_atp_formatted_date(text: str) -> tuple[date, date]:
    """
    Supports:
      - 4 - 11 January, 2026
      - 26 December 2025 - 02 January, 2026
      - 26 January - 02 February, 2026
    """
    s = norm_space(text)

    patterns = [
        re.compile(
            r"^(?P<sd>\d{1,2})\s+(?P<sm>[A-Za-z]+)\s+(?P<sy>\d{4})\s*-\s*"
            r"(?P<ed>\d{1,2})\s+(?P<em>[A-Za-z]+),?\s+(?P<ey>\d{4})$"
        ),
        re.compile(
            r"^(?P<sd>\d{1,2})\s+(?P<sm>[A-Za-z]+)\s*-\s*"
            r"(?P<ed>\d{1,2})\s+(?P<em>[A-Za-z]+),?\s+(?P<ey>\d{4})$"
        ),
        re.compile(
            r"^(?P<sd>\d{1,2})\s*-\s*"
            r"(?P<ed>\d{1,2})\s+(?P<em>[A-Za-z]+),?\s+(?P<ey>\d{4})$"
        ),
    ]

    for rx in patterns:
        m = rx.match(s)
        if not m:
            continue
        g = m.groupdict()
        end_year = int(g["ey"])
        end_month = MONTHS[g["em"].lower()]
        end_day = int(g["ed"])
        end_date = date(end_year, end_month, end_day)

        if g.get("sy") and g.get("sm"):
            start_year = int(g["sy"])
            start_month = MONTHS[g["sm"].lower()]
            start_day = int(g["sd"])
        elif g.get("sm"):
            start_year = end_year
            start_month = MONTHS[g["sm"].lower()]
            start_day = int(g["sd"])
        else:
            start_year = end_year
            start_month = end_month
            start_day = int(g["sd"])

        start_date = date(start_year, start_month, start_day)
        return start_date, end_date

    raise ValueError(f"ATP FormattedDate impossible à parser: {text!r}")

def normalize_atp_category(t: dict[str, Any]) -> str:
    raw = str(t.get("Type") or "").strip().upper()
    challenger = str(t.get("ChallengerCategory") or "").strip().upper()
    event_type = str(t.get("EventType") or "").strip().upper()
    detail = str(t.get("EventTypeDetail") or "").strip().upper()
    title = str(t.get("Name") or t.get("title") or "").upper()

    if raw in {"250", "500", "1000", "GS", "UC"}:
        return raw
    if challenger in {"CH", "FU"}:
        return challenger
    if "CHALLENGER" in event_type or "CHALLENGER" in detail:
        return "CH"
    if "FUTURE" in event_type or "FUTURE" in detail:
        return "FU"
    if "GRAND SLAM" in title:
        return "GS"
    return raw or "TOUR"

def normalize_wta_category(t: dict[str, Any]) -> str:
    group = t.get("tournamentGroup") or {}
    level = str(group.get("level") or "").strip().upper()
    title = str(t.get("title") or "").upper()
    name = str(group.get("name") or "").upper()

    if "ITF" in level or "ITF" in title or title.startswith("WTT W"):
        return "ITF"
    if "125" in title or "WTA 125" in title:
        return "WTA125"
    if "1000" in title:
        return "WTA1000"
    if "500" in title:
        return "WTA500"
    if "250" in title:
        return "WTA250"
    if "GRAND SLAM" in title or "GS" == level:
        return "GS"
    if level:
        return level
    if name:
        return name
    return "WTA"

def is_opening_sunday(today_paris: date) -> bool:
    return today_paris.weekday() == 6  # Sunday

def atp_open_tournaments(atp_payload: dict[str, Any], target_start_date: date) -> list[dict[str, Any]]:
    open_list: list[dict[str, Any]] = []

    for group in atp_payload.get("TournamentDates", []):
        for t in group.get("Tournaments", []):
            formatted = t.get("FormattedDate")
            if not formatted:
                continue
            try:
                start_dt, end_dt = parse_atp_formatted_date(formatted)
            except Exception:
                continue

            if start_dt != target_start_date:
                continue

            tournament_id = str(t.get("Id") or "").strip()
            if not tournament_id:
                continue

            category = normalize_atp_category(t)
            open_list.append({
                "tour": "ATP",
                "tournament_id": tournament_id,
                "tournament_name": str(t.get("Name") or "").strip(),
                "location": str(t.get("Location") or "").strip(),
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "category": category,
                "draw_size": safe_int(t.get("SglDrawSize"), 0) or 0,
                "display_date": str(group.get("DisplayDate") or "").strip(),
                "source": "ATP",
                "raw": {
                    "type": t.get("Type"),
                    "event_type": t.get("EventType"),
                    "event_type_detail": t.get("EventTypeDetail"),
                    "challenger_category": t.get("ChallengerCategory"),
                },
            })

    return open_list

def wta_open_tournaments(wta_payload: dict[str, Any], target_start_date: date) -> list[dict[str, Any]]:
    open_list: list[dict[str, Any]] = []

    for t in wta_payload.get("content", []):
        start_s = t.get("startDate")
        end_s = t.get("endDate")
        if not start_s or not end_s:
            continue

        try:
            start_dt = parse_date_iso(start_s)
            end_dt = parse_date_iso(end_s)
        except Exception:
            continue

        if start_dt != target_start_date:
            continue

        group = t.get("tournamentGroup") or {}
        tournament_id = str(group.get("id") or "").strip()
        if not tournament_id:
            continue

        category = normalize_wta_category(t)
        open_list.append({
            "tour": "WTA",
            "tournament_id": tournament_id,
            "tournament_name": str(t.get("title") or "").strip(),
            "location": str(t.get("country") or "").strip(),
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "category": category,
            "draw_size": safe_int(t.get("singlesDrawSize"), 0) or 0,
            "display_date": str(t.get("title") or "").strip(),
            "source": "WTA",
            "raw": {
                "level": group.get("level"),
                "tournament_link": t.get("tournamentLink"),
                "surface": t.get("surface"),
            },
        })

    return open_list

def main() -> int:
    workspace = Path(os.environ.get("GITHUB_WORKSPACE", ".")).resolve()

    atp_path = Path(os.environ.get("ATP_TOURNAMENTS_JSON", "docs/atp_tournaments_2026.json"))
    wta_path = Path(os.environ.get("WTA_TOURNAMENTS_JSON", "docs/wta_tournaments_2026.json"))
    out_path = Path(os.environ.get("OUTPUT_JSON", "docs/bracket/open_inscriptions.json"))

    if not atp_path.is_absolute():
        atp_path = workspace / atp_path
    if not wta_path.is_absolute():
        wta_path = workspace / wta_path
    if not out_path.is_absolute():
        out_path = workspace / out_path

    today_paris = datetime.now(PARIS_TZ).date()
    is_open_today = is_opening_sunday(today_paris)
    target_start_date = today_paris + timedelta(days=7)

    payload: dict[str, Any] = {
        "version": 1,
        "timezone": "Europe/Paris",
        "generated_at": datetime.now(PARIS_TZ).isoformat(),
        "current_paris_date": today_paris.isoformat(),
        "registration_window": {
            "is_open_today": is_open_today,
            "open_date": today_paris.isoformat(),
            "close_date": today_paris.isoformat(),
            "target_start_date": target_start_date.isoformat(),
        },
        "open_tournaments": [],
    }

    if is_open_today:
        atp_payload = json.loads(atp_path.read_text(encoding="utf-8")) if atp_path.exists() else {}
        wta_payload = json.loads(wta_path.read_text(encoding="utf-8")) if wta_path.exists() else {}

        open_tournaments = []
        open_tournaments.extend(atp_open_tournaments(atp_payload, target_start_date))
        open_tournaments.extend(wta_open_tournaments(wta_payload, target_start_date))

        open_tournaments.sort(key=lambda x: (x["tour"], x["start_date"], x["tournament_name"]))
        payload["open_tournaments"] = open_tournaments
        payload["registration_window"]["count"] = len(open_tournaments)
    else:
        payload["registration_window"]["count"] = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} with {len(payload['open_tournaments'])} open tournaments.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())