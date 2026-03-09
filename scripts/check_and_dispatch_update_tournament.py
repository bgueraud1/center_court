#!/usr/bin/env python3
# (en-tête inchangé...)
import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error
import sys

REPO = os.environ.get("GITHUB_REPOSITORY")
# try multiple env names for PAT / token (backwards compat)
TOKEN_ENV_CANDIDATES = ["GITHUB_TOKEN", "PAT_FOR_CI", "PAT_UPDATE_TOURNAMENT", "UPDATE_TOURNAMENT", "PAT"]

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
    print("No GITHUB_TOKEN or PAT_FOR_CI or other PAT provided — cannot dispatch workflow. Aborting.")
    sys.exit(10)
else:
    # Don't print the token value. Print only which env var was used.
    print(f"Using token from env var: {token_name}")

WORKFLOW_FILE = "update_tournament.yml" 

if not REPO:
    print("GITHUB_REPOSITORY not set; aborting (must run inside GitHub Actions)")
    raise SystemExit(1)

dict_path = os.environ.get("TOURNAMENT_DICT_PATH", "docs/data/tournament_player_counts_2026.json")

try:
    with open(dict_path, "r", encoding="utf-8") as fh:
        d = json.load(fh)
except Exception as e:
    print("Impossible de lire", dict_path, ":", e)
    raise SystemExit(2)

tz = ZoneInfo("Europe/Paris")
now = datetime.now(tz).date()
to_dispatch = []

for k, v in d.items():
    try:
        # v is [count, start, end, flag]
        end_date_str = v[2]
        end_date = datetime.fromisoformat(end_date_str).date()
        target = end_date + timedelta(days=1)
        if target == now:
            to_dispatch.append(str(k))
    except Exception as e:
        print(f"Skipping {k} due to parse error: {e}")

if not to_dispatch:
    print("Aucun tournoi à déclencher aujourd'hui:", now.isoformat())
    raise SystemExit(0)

# prepare payload
tids_csv = ",".join(to_dispatch)
payload = json.dumps({
    "ref": "main",
    "inputs": {"tournament_ids": tids_csv}
}).encode("utf-8")

if not TOKEN:
    print("No GITHUB_TOKEN or PAT_FOR_CI provided — cannot dispatch workflow. Aborting.")
    raise SystemExit(10)

url = f"https://api.github.com/repos/{REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
req = urllib.request.Request(url, data=payload, method="POST")
req.add_header("Authorization", f"token {TOKEN}")
req.add_header("Accept", "application/vnd.github.v3+json")
req.add_header("Content-Type", "application/json")

print("Dispatching workflow", WORKFLOW_FILE, "for tids:", tids_csv)
try:
    with urllib.request.urlopen(req) as resp:
        status = resp.getcode()
        body = resp.read().decode()
        print("HTTP", status)
        print(body or "(no body)")
        if status >= 200 and status < 300:
            print("Dispatch OK")
            raise SystemExit(0)
        else:
            print("Dispatch failed, HTTP", status)
            raise SystemExit(11)
except urllib.error.HTTPError as e:
    print("HTTP error:", e.code, e.read().decode())
    raise SystemExit(12)
except Exception as e:
    print("Dispatch exception:", e)
    raise SystemExit(13)