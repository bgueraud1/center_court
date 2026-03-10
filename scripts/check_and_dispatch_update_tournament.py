#!/usr/bin/env python3
# check_and_dispatch_update_tournament.py
import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error
import sys

REPO = os.environ.get("GITHUB_REPOSITORY")
TOKEN_ENV_CANDIDATES = ["PAT_UPDATE_TOURNAMENT", "UPDATE_TOURNAMENT", "PAT_FOR_CI", "GITHUB_TOKEN"]

def get_token_from_env():
    for name in TOKEN_ENV_CANDIDATES:
        val = os.environ.get(name)
        if val:
            return name, val
    return None, None

if not REPO:
    print("GITHUB_REPOSITORY not set; aborting (must run inside GitHub Actions)")
    sys.exit(1)

token_name, TOKEN = get_token_from_env()
if not TOKEN:
    print("No token provided — cannot dispatch workflow. Aborting.")
    sys.exit(10)
else:
    print(f"Using token from env var: {token_name}")

WORKFLOW_FILE = "update_tournament.yaml"

# Allow two distinct tournament JSON inputs (WTA and ATP)
dicts = {
    "wta": os.environ.get("TOURNAMENT_DICT_PATH_WTA", "docs/wta_tournaments_2026.json"),
    "atp": os.environ.get("TOURNAMENT_DICT_PATH_ATP", "docs/atp_tournaments_2026.json"),
}

# ajouter en haut du fichier si non présent
import re
from datetime import datetime, timedelta, date

# remplacer la fonction collect_tids_from_json par ceci
def parse_end_date(end_date_str):
    """
    Retourne une datetime.date si on arrive à parser end_date_str,
    sinon None.
    Gère :
      - ISO 'YYYY-MM-DD'
      - formatted strings like '29 December, 2025 - 4 January, 2026'
      - ranges '19 - 25 January, 2026' or '26 January - 1 February, 2026'
    """
    if not end_date_str:
        return None
    s = str(end_date_str).strip()
    # try ISO first
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        pass

    # if there's a dash, take the rightmost part as candidate end
    if '-' in s:
        right = s.split('-', 1)[1].strip()
    else:
        right = s

    # attempt to find "day month year" pattern
    # accept month names with accents (fr/en)
    m = re.search(r"(\d{1,2})\s*([A-Za-zéèêàùûôïçÉÈÊÀÙÛÔÏÇ]+)[,]?\s*(\d{4})", right)
    if m:
        day = int(m.group(1))
        mon_word = m.group(2).lower()
        year = int(m.group(3))
        # month name -> number mapping (small subset; extend si nécessaire)
        MONTHS = {
            'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,'july':7,'august':8,'september':9,'october':10,'november':11,'december':12,
            'janvier':1,'fevrier':2,'février':2,'mars':3,'avril':4,'mai':5,'juin':6,'juillet':7,'aout':8,'août':8,'septembre':9,'octobre':10,'novembre':11,'decembre':12,'décembre':12
        }
        mon = MONTHS.get(mon_word)
        if mon:
            try:
                return date(year, mon, day)
            except Exception:
                return None

    # fallback: try to find a 4-digit year and a day elsewhere (less reliable)
    m2 = re.search(r"(\d{1,2}).*?(\d{4})", right)
    if m2:
        try:
            d = int(m2.group(1))
            y = int(m2.group(2))
            # month unknown -> skip
            return None
        except Exception:
            return None

    return None


def collect_tids_from_json(path, tour_type_label=None):
    """
    Support multiple JSON shapes:
      - legacy dict { id: [count, start_iso, end_iso, flag], ... }
      - WTA API: { 'content': [ { 'tournamentGroup':{'id':...}, 'endDate': 'YYYY-MM-DD', ...}, ... ] }
      - ATP API: { 'TournamentDates': [ { 'Tournaments': [ { 'Id': '5216', 'FormattedDate': '29 Dec - 4 Jan, 2026', ... }, ... ] }, ... ] }
    Retourne une liste de ids (strings) pour lesquels end_date + 1 == today (Europe/Paris).
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            d = json.load(fh)
    except FileNotFoundError:
        print(f"File not found: {path} -> skipping")
        return []
    except Exception as e:
        print(f"Impossible de lire {path} : {e}")
        return []

    # timezone-safe today (Europe/Paris) with fallback
    try:
        tz = ZoneInfo("Europe/Paris")
        now = datetime.now(tz).date()
    except Exception:
        now = datetime.now().date()

    to_dispatch = []

    # Case A: WTA API-like with 'content' list
    if isinstance(d, dict) and isinstance(d.get("content"), list):
        for item in d.get("content", []):
            try:
                # id might be in tournamentGroup.id
                tid = None
                tg = item.get("tournamentGroup") or {}
                if isinstance(tg, dict):
                    tid = tg.get("id") or tg.get("Id")
                # fallback: maybe top-level has an 'id' field
                if tid is None:
                    tid = item.get("id") or item.get("Id")
                end_date_str = item.get("endDate") or item.get("end_date") or item.get("end")
                end_dt = parse_end_date(end_date_str)
                if end_dt:
                    target = end_dt + timedelta(days=1)
                    if target == now and tid is not None:
                        to_dispatch.append(str(tid))
                        print(f"[select-wta] tid={tid} end={end_dt.isoformat()} -> dispatch")
                else:
                    # debug verbose
                    # print(f"[skip-wta] item id={tid} no parsable endDate ({end_date_str})")
                    pass
            except Exception as e:
                print(f"Skipping WTA item due to parse error: {e}")
        return to_dispatch

    # Case B: ATP API-like with 'TournamentDates'
    if isinstance(d, dict) and isinstance(d.get("TournamentDates"), list):
        for month_block in d.get("TournamentDates", []):
            for t in (month_block.get("Tournaments") or []):
                try:
                    tid = t.get("Id") or t.get("ID") or t.get("IdTournament")
                    formatted = t.get("FormattedDate") or t.get("Formatted") or ""
                    end_dt = parse_end_date(formatted)
                    if end_dt:
                        target = end_dt + timedelta(days=1)
                        if target == now and tid is not None:
                            to_dispatch.append(str(tid))
                            print(f"[select-atp] tid={tid} end={end_dt.isoformat()} -> dispatch")
                    else:
                        # print(f"[skip-atp] tid={tid} no parsable end in '{formatted}'")
                        pass
                except Exception as e:
                    print(f"Skipping ATP item due to parse error: {e}")
        return to_dispatch

    # Case C: legacy mapping { id: [count,start,end,flag] } or { id: {... 'end': ... } }
    if isinstance(d, dict):
        for k, v in d.items():
            try:
                if isinstance(v, list) and len(v) >= 3:
                    end_date_str = v[2]
                elif isinstance(v, dict) and ("end" in v or "end_date" in v or "endDate" in v):
                    end_date_str = v.get("end") or v.get("end_date") or v.get("endDate")
                else:
                    # cannot find end date; skip
                    # print(f"[skip-legacy] {k}: unknown entry format")
                    continue

                end_dt = parse_end_date(end_date_str)
                if end_dt:
                    target = end_dt + timedelta(days=1)
                    if target == now:
                        to_dispatch.append(str(k))
                else:
                    # debug: could not parse
                    # print(f"[skip-legacy] {k}: cannot parse end '{end_date_str}'")
                    pass
            except Exception as e:
                print(f"Skipping {k} due to parse error: {e}")
        return to_dispatch

    # fallback: nothing matched
    print(f"Unrecognized JSON structure in {path} -> no tournaments selected")
    return []
    
any_dispatched = False

for tour_type, path in dicts.items():
    if not path:
        continue
    tids = collect_tids_from_json(path)
    if not tids:
        print(f"Aucun {tour_type.upper()} tournoi à déclencher aujourd'hui ({path})")
        continue

    tids_csv = ",".join(tids)
    payload = json.dumps({
        "ref": "main",
        "inputs": {
            "tournament_ids": tids_csv,
            "tour_type": tour_type
        }
    }).encode("utf-8")

    url = f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"token {TOKEN}")
    req.add_header("Accept", "application/vnd.github.v3+json")
    req.add_header("Content-Type", "application/json")

    print(f"Dispatching workflow {WORKFLOW_FILE} for {tour_type.upper()} tids: {tids_csv}")
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.getcode()
            body = resp.read().decode()
            print("HTTP", status)
            print(body or "(no body)")
            if 200 <= status < 300:
                print("Dispatch OK")
                any_dispatched = True
            else:
                print("Dispatch failed, HTTP", status)
    except urllib.error.HTTPError as e:
        print("HTTP error:", e.code, e.read().decode())
    except Exception as e:
        print("Dispatch exception:", e)

if not any_dispatched:
    print("Aucune dispatch effectuée.")
    sys.exit(0)
else:
    sys.exit(0)