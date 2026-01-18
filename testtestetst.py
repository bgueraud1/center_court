import requests
import pandas as pd
import os
import time
from datetime import date

# Config
SAVE_DIR = "debug_wta_full"
os.makedirs(SAVE_DIR, exist_ok=True)
PAGE_LIMIT = 72
PAGE_SIZE = 20
ACCEPT_THRESHOLD = 350  # ton seuil
PER_PAGE_RETRIES = 4
PER_PAGE_INIT_DELAY = 1  # seconde

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; wta-scraper/1.0)"})
session.timeout = 10

def fetch_page_with_backoff(session, url, retries=PER_PAGE_RETRIES, init_delay=PER_PAGE_INIT_DELAY):
    """Retourne (json_or_none, status_code, error_str). Fait sleep sur 429 ou exception."""
    delay = init_delay
    for attempt in range(1, retries+1):
        try:
            r = session.get(url, timeout=10)
            status = r.status_code
            if status == 429:
                # Rate-limited: sleep and retry
                print(f"  429 on attempt {attempt}/{retries} for {url} -> sleeping {delay}s")
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            try:
                j = r.json()
            except Exception as e:
                # Save raw body for inspection
                path = os.path.join(SAVE_DIR, f"bad_json_{int(time.time())}.txt")
                open(path, "w", encoding="utf-8").write(r.text[:10000])
                return None, status, f"JSON decode error: {e} (saved {path})"
            return j, status, None
        except requests.RequestException as e:
            print(f"  RequestException attempt {attempt}/{retries} for {url}: {e}")
            if attempt < retries:
                time.sleep(delay)
                delay *= 2
            else:
                return None, getattr(e, 'response', None).status_code if getattr(e,'response',None) else None, str(e)
    return None, None, "unhandled"

def normalize_items(json_obj):
    """Retourne la liste d'items (ou []) quel que soit le format renvoyé."""
    if not json_obj:
        return []
    if isinstance(json_obj, list):
        return json_obj
    if isinstance(json_obj, dict):
        for k in ("players","data","items","results","entries","content"):
            v = json_obj.get(k)
            if isinstance(v, list):
                return v
        # fallback: première valeur list
        for v in json_obj.values():
            if isinstance(v, list):
                return v
    return []

def scrape_date(current_date):
    temp_rows = []
    failed_page_info = []
    for page in range(PAGE_LIMIT):
        url = (
            "https://api.wtatennis.com/tennis/players/ranked"
            f"?page={page}&pageSize={PAGE_SIZE}&type=rankSingles&sort=asc&name=&metric=SINGLES&at={current_date}&nationality="
        )
        print(f"Fetching page {page} for {current_date} ...")
        data, status, err = fetch_page_with_backoff(session, url)
        if data is None:
            print(f" Page {page} failed: status={status} err={err}")
            failed_page_info.append((page, url, status, err))
            # ne pas BREAK: on peut vouloir continuer les pages suivantes,
            # mais ici on choisit de RETENTER la page PER_PAGE_RETRIES fois dans fetch_page_with_backoff.
            # Si échec définitif, on continue (log) — tu peux remplacer par 'break' si tu préfères.
            continue

        items = normalize_items(data)
        if not items:
            print(f"  Page {page} returned empty list -> stop pagination (no more items).")
            break

        # parse items
        for item in items:
            if not isinstance(item, dict):
                continue
            player = item.get("player") or item
            if not isinstance(player, dict):
                continue
            temp_rows.append({
                "full_name": player.get("fullName") or player.get("full_name") or player.get("firstName","") + " " + player.get("lastName",""),
                "player_id": player.get("id") or player.get("playerId"),
                "ranking": item.get("ranking") or player.get("ranking"),
                "points": item.get("points") or player.get("points"),
                "movement": item.get("movement") or player.get("movement"),
                "date": str(current_date),
            })

        print(f"  Page {page}: got {len(items)} items, cumulative rows: {len(temp_rows)}")

        # defensive: si on a déjà beaucoup de lignes, on peut décider d'arrêter
        # if len(temp_rows) >= ACCEPT_THRESHOLD: break

    # sauvegarde / logs
    if len(temp_rows) >= ACCEPT_THRESHOLD:
        df = pd.DataFrame(temp_rows)
        fpath = os.path.join(SAVE_DIR, f"data_{current_date}.csv")
        df.to_csv(fpath, index=False)
        print(f"Saved {len(temp_rows)} rows for {current_date} -> {fpath}")
    else:
        print(f"Insufficient rows for {current_date}: {len(temp_rows)} rows (threshold {ACCEPT_THRESHOLD})")
        # sauve quand même pour debug
        df = pd.DataFrame(temp_rows)
        fpath = os.path.join(SAVE_DIR, f"data_partial_{current_date}.csv")
        df.to_csv(fpath, index=False)
        print(f"Saved partial CSV to {fpath}")
        # log failed pages
        if failed_page_info:
            faildf = pd.DataFrame(failed_page_info, columns=["page","url","status","error"])
            faildf.to_csv(os.path.join(SAVE_DIR, f"failed_pages_{current_date}.csv"), index=False)
            print("Saved failed page info.")

    return len(temp_rows), failed_page_info

# Exemple d'usage pour ta date problématique
if __name__ == "__main__":
    target = date(2026, 1, 12)
    rows, fails = scrape_date(target)
    print("Done:", rows, "rows ; failed pages:", len(fails))
