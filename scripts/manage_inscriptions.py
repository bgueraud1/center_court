from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

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

CATEGORY_RULES = {
    "CH": {"min_rank_allowed": 51, "rule_code": "ATP_CH_TOP50_BLOCKED"},
    "FU": {"min_rank_allowed": 201, "rule_code": "ATP_FU_TOP200_BLOCKED"},
    "WTA125": {"min_rank_allowed": 51, "rule_code": "WTA125_TOP50_BLOCKED"},
    "ITF": {"min_rank_allowed": 201, "rule_code": "ITF_TOP200_BLOCKED"},
}


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


def now_paris() -> datetime:
    return datetime.now(PARIS_TZ)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_rule(tour: str, category: str) -> Optional[dict[str, Any]]:
    if tour == "ATP" and category in {"CH", "FU"}:
        return CATEGORY_RULES[category]
    if tour == "WTA" and category in {"WTA125", "ITF"}:
        return CATEGORY_RULES[category]
    return None

def supabase_request(
    method: str,
    supabase_url: str,
    supabase_key: str,
    table: str,
    params: Optional[dict[str, Any]] = None,
    body: Optional[Any] = None,
    extra_headers: Optional[dict[str, str]] = None,
) -> Any:
    base = supabase_url.rstrip("/")
    url = f"{base}/rest/v1/{table.lstrip('/')}"

    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    if extra_headers:
        headers.update(extra_headers)

    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")

    req = Request(url, data=data, headers=headers, method=method.upper())

    try:
        with urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return None
            try:
                return json.loads(raw)
            except Exception:
                return raw
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"Supabase {method} {table} failed: HTTP {e.code} — {detail}") from e
    except URLError as e:
        raise RuntimeError(f"Supabase {method} {table} failed: {e}") from e


def supabase_get_all(
    supabase_url: str,
    supabase_key: str,
    table: str,
    params: dict[str, Any],
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        page_params = dict(params)
        page_params.setdefault("select", "*")
        page_params["limit"] = page_size
        page_params["offset"] = offset

        chunk = supabase_request("GET", supabase_url, supabase_key, table, params=page_params)
        if not chunk:
            break

        if not isinstance(chunk, list):
            raise RuntimeError(f"Unexpected response while reading {table}: {chunk!r}")

        all_rows.extend(chunk)

        if len(chunk) < page_size:
            break

        offset += page_size

    return all_rows


def build_open_tournaments(
    atp_payload: dict[str, Any],
    wta_payload: dict[str, Any],
    anchor_date: date,
) -> list[dict[str, Any]]:
    """
    Open window = tournaments starting between anchor_date + 8 and anchor_date + 14 inclusive.
    Example:
      anchor_date = Sunday 2026-03-29
      window = Monday 2026-04-06 -> Sunday 2026-04-12
    """
    start_window = anchor_date + timedelta(days=8)
    end_window = anchor_date + timedelta(days=14)

    open_list: list[dict[str, Any]] = []

    # ATP
    for group in atp_payload.get("TournamentDates", []):
        for t in group.get("Tournaments", []):
            formatted = t.get("FormattedDate")
            if not formatted:
                continue
            try:
                start_dt, end_dt = parse_atp_formatted_date(formatted)
            except Exception:
                continue

            if not (start_window <= start_dt <= end_window):
                continue

            tournament_id = str(t.get("Id") or "").strip()
            if not tournament_id:
                continue

            category = normalize_atp_category(t)
            rule = get_rule("ATP", category)

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
                "eligibility": {
                    "restricted": bool(rule),
                    "min_rank_allowed": rule["min_rank_allowed"] if rule else None,
                    "rule_code": rule["rule_code"] if rule else None,
                },
                "source": "ATP",
            })

    # WTA
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

        if not (start_window <= start_dt <= end_window):
            continue

        group = t.get("tournamentGroup") or {}
        tournament_id = str(group.get("id") or "").strip()
        if not tournament_id:
            continue

        category = normalize_wta_category(t)
        rule = get_rule("WTA", category)

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
            "eligibility": {
                "restricted": bool(rule),
                "min_rank_allowed": rule["min_rank_allowed"] if rule else None,
                "rule_code": rule["rule_code"] if rule else None,
            },
            "source": "WTA",
        })

    open_list.sort(key=lambda x: (x["tour"], x["start_date"], x["tournament_name"]))
    return open_list


def build_payload(
    anchor_date: date,
    phase: str,
    open_tournaments: list[dict[str, Any]],
) -> dict[str, Any]:
    start_window = anchor_date + timedelta(days=8)
    end_window = anchor_date + timedelta(days=14)

    return {
        "version": 3,
        "timezone": "Europe/Paris",
        "generated_at": now_paris().isoformat(),
        "current_paris_date": now_paris().date().isoformat(),
        "window": {
            "phase": phase,  # open / closed
            "is_open_today": phase == "open",
            "anchor_date": anchor_date.isoformat(),
            "open_date": anchor_date.isoformat(),
            "close_date": anchor_date.isoformat(),
            "window_start_date": start_window.isoformat(),
            "window_end_date": end_window.isoformat(),
            "count": len(open_tournaments) if phase == "open" else 0,
        },
        "tournaments": open_tournaments if phase == "open" else [],
    }


def should_run_auto(dt: datetime) -> str:
    """
    Auto mode:
      - Sunday around 00:01 Paris -> open
      - Sunday around 23:59 Paris -> close
      - otherwise -> noop
    """
    if dt.weekday() != 6:
        return "noop"

    if dt.hour == 0 and dt.minute <= 10:
        return "open"

    if dt.hour == 23 and dt.minute >= 50:
        return "close"

    return "noop"


def clear_inscriptions(supabase_url: str, supabase_key: str, anchor_date: date) -> None:
    supabase_request(
        "DELETE",
        supabase_url,
        supabase_key,
        "inscriptions",
        params={"window_start_date": f"eq.{anchor_date.isoformat()}"},
    )

def fetch_applications(
    supabase_url: str,
    supabase_key: str,
    anchor_date: date,
) -> list[dict[str, Any]]:
    return supabase_get_all(
        supabase_url,
        supabase_key,
        "inscriptions",
        params={
            "window_start_date": f"eq.{anchor_date.isoformat()}",
            "order": "user_id.asc,preference_rank.asc,tournament_id.asc",
        },
    )


def chunked(seq: list[dict[str, Any]], size: int = 100):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def assign_next_inscriptions(
    supabase_url: str,
    supabase_key: str,
    anchor_date: date,
    open_tournaments: list[dict[str, Any]],
) -> int:
    tournaments: dict[str, dict[str, Any]] = {
        t["tournament_id"]: {**t, "remaining": int(t["draw_size"] or 0)}
        for t in open_tournaments
    }

    apps = fetch_applications(supabase_url, supabase_key, anchor_date)
    by_user: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in apps:
        by_user[str(row["user_id"])].append(row)

    def user_sort_key(rows: list[dict[str, Any]]) -> tuple[int, int, str]:
        first = rows[0]
        rank = first.get("user_world_rank")
        rank_key = rank if rank is not None else 10**9
        return (0 if rank is not None else 1, int(rank_key), str(first.get("user_name") or "").lower())

    ordered_users = sorted(by_user.items(), key=lambda kv: user_sort_key(kv[1]))
    assignments: list[dict[str, Any]] = []

    for user_id, rows in ordered_users:
        rows = sorted(rows, key=lambda r: (int(r.get("preference_rank") or 1), str(r.get("tournament_id") or "")))
        user_name = str(rows[0].get("user_name") or "")
        user_rank = rows[0].get("user_world_rank")

        chosen = None

        for row in rows:
            tournament_id = str(row.get("tournament_id") or "")
            tournament = tournaments.get(tournament_id)
            if not tournament:
                continue

            rule = tournament.get("eligibility", {})
            if rule.get("restricted"):
                min_rank_allowed = rule.get("min_rank_allowed")
                if user_rank is not None and int(user_rank) < int(min_rank_allowed):
                    continue

            if tournament["remaining"] <= 0:
                continue

            chosen = {
                "window_start_date": anchor_date.isoformat(),
                "target_start_date": tournament["start_date"],
                "user_id": user_id,
                "user_name": user_name,
                "user_world_rank": user_rank,
                "tour": tournament["tour"],
                "tournament_id": tournament["tournament_id"],
                "tournament_name": tournament["tournament_name"],
                "tournament_category": tournament["category"],
                "tournament_num_players": tournament["draw_size"],
                "assigned_preference_rank": int(row.get("preference_rank") or 1),
            }
            tournament["remaining"] -= 1
            break

        if chosen:
            assignments.append(chosen)

    # Replace prior rows for the same window.
    supabase_request(
        "DELETE",
        supabase_url,
        supabase_key,
        "next_inscriptions",
        params={"window_start_date": f"eq.{anchor_date.isoformat()}"},
    )

    if not assignments:
        return 0

    inserted = 0
    for batch in chunked(assignments, 100):
        resp = supabase_request(
            "POST",
            supabase_url,
            supabase_key,
            "next_inscriptions",
            body=batch,
            extra_headers={"Prefer": "return=representation"},
        )
        if isinstance(resp, list):
            inserted += len(resp)
        elif resp is None:
            inserted += len(batch)
        else:
            inserted += len(batch)

    return inserted


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", default="auto", choices=["auto", "open", "close"])
    parser.add_argument(
        "--anchor-date",
        default="",
        help="YYYY-MM-DD. Optional Sunday anchor for manual runs or replay.",
    )
    args = parser.parse_args()

    workspace = Path(os.environ.get("GITHUB_WORKSPACE", ".")).resolve()
    atp_path = Path(os.environ.get("ATP_TOURNAMENTS_JSON", "docs/atp_tournaments_2026.json"))
    wta_path = Path(os.environ.get("WTA_TOURNAMENTS_JSON", "docs/wta_tournaments_2026.json"))
    out_path = Path(os.environ.get("OUTPUT_JSON", "docs/bracket/open_inscriptions.json"))
    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if not atp_path.is_absolute():
        atp_path = workspace / atp_path
    if not wta_path.is_absolute():
        wta_path = workspace / wta_path
    if not out_path.is_absolute():
        out_path = workspace / out_path

    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")

    now_dt = now_paris()

    if args.anchor_date:
        anchor_date = date.fromisoformat(args.anchor_date)
        phase = args.phase if args.phase in {"open", "close"} else "open"
    else:
        anchor_date = now_dt.date()
        if args.phase == "auto":
            phase = should_run_auto(now_dt)
        else:
            phase = args.phase

    if phase == "noop":
        print("No action: not Sunday or outside the execution windows.")
        return 0

    atp_payload = load_json(atp_path)
    wta_payload = load_json(wta_path)
    open_tournaments = build_open_tournaments(atp_payload, wta_payload, anchor_date)
    previous_payload = load_json(out_path)
    previous_window = previous_payload.get("window", {}) if isinstance(previous_payload, dict) else {}

    payload = build_payload(anchor_date, phase, open_tournaments)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if phase == "open":
        same_open_window = (
            previous_window.get("phase") == "open"
            and previous_window.get("anchor_date") == anchor_date.isoformat()
        )

        if not same_open_window:
            clear_inscriptions(supabase_url, supabase_key, anchor_date)

        print(f"Wrote {out_path} with phase=open and {len(open_tournaments)} open tournament(s).")
        return 0

    if phase == "close":
        inserted = assign_next_inscriptions(supabase_url, supabase_key, anchor_date, open_tournaments)
        print(f"Wrote {out_path} with phase=close and assigned {inserted} user(s) to next_inscriptions.")
        return 0

    print(f"Wrote {out_path} with phase={phase} and {len(payload['tournaments'])} tournament(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())