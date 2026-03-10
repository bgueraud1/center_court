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

# helper to collect tids for a given file path
def collect_tids_from_json(path, tour_type_label=None):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            d = json.load(fh)
    except FileNotFoundError:
        print(f"File not found: {path} -> skipping")
        return []
    except Exception as e:
        print(f"Impossible de lire {path} : {e}")
        return []

    tz = ZoneInfo("Europe/Paris")
    now = datetime.now(tz).date()
    to_dispatch = []
    for k, v in d.items():
        try:
            # support older list format v = [count, start, end, flag]
            # or mapping format v = {"start": "...", "end": "..."}
            if isinstance(v, list) and len(v) >= 3:
                end_date_str = v[2]
            elif isinstance(v, dict) and ("end" in v or "end_date" in v):
                end_date_str = v.get("end") or v.get("end_date")
            else:
                raise ValueError("Unknown tournament entry format")

            # Try ISO parse, otherwise try date part
            end_date = datetime.fromisoformat(end_date_str).date()
            target = end_date + timedelta(days=1)
            if target == now:
                to_dispatch.append(str(k))
        except Exception as e:
            print(f"Skipping {k} due to parse error: {e}")
    return to_dispatch

    
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