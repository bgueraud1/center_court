import requests
import pandas as pd
import datetime
import os
import time
from typing import Any, List

# ------------------ CONFIG ------------------
PAGE_SIZE = 20
MAX_PAGE_LIMIT = 200            # sécurité : ne pas dépasser ce nombre de pages
CONSECUTIVE_EMPTY_THRESHOLD = 5 # s'arrêter après N pages vides consécutives
PER_PAGE_RETRIES = 3            # tentatives par page
PER_PAGE_INIT_DELAY = 1         # backoff initial (s)
DATE_ACCEPT_THRESHOLD = 100     # seuil minimal pour accepter et sauvegarder une date
REQUEST_TIMEOUT = 10            # timeout request (s)
USER_AGENT = "Mozilla/5.0 (compatible; wta-scraper/1.0)"
# --------------------------------------------

def save_csv(data: List[dict], date_obj: datetime.date, save_dir: str) -> None:
    save_dir = os.fspath(save_dir)
    os.makedirs(save_dir, exist_ok=True)
    date_str = date_obj.strftime("%Y_%m_%d")
    file_path = os.path.abspath(os.path.join(save_dir, f"data_{date_str}.csv"))
    pd.DataFrame(data).to_csv(file_path, index=False)
    print(f"Saved {len(data)} rows to {file_path} (abs)")

def log_failed_urls(failed_urls: List[str], save_dir: str) -> None:
    save_dir = os.fspath(save_dir)
    os.makedirs(save_dir, exist_ok=True)
    failed_path = os.path.abspath(os.path.join(save_dir, "failed_urls.csv"))
    pd.DataFrame({"failed_urls": failed_urls}).to_csv(failed_path, index=False)
    print(f"Logged {len(failed_urls)} failed URLs to {failed_path} (abs)")

def fetch_page_with_retries(session: requests.Session, url: str,
                            retries: int = PER_PAGE_RETRIES,
                            init_delay: int = PER_PAGE_INIT_DELAY,
                            timeout: int = REQUEST_TIMEOUT) -> Any:
    """
    Retourne le JSON (ou None si échec définitif).
    Gère les 429 avec exponential backoff et les exceptions réseau.
    """
    delay = init_delay
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                print(f"    429 Rate limit on attempt {attempt}/{retries}. Sleeping {delay}s...")
                time.sleep(delay)
                delay *= 2
                continue
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as e:
                # JSON invalide -> sauvegarder un fichier pour debug et retourner None
                snippet = resp.text[:2000]
                timestamp = int(time.time())
                debug_path = f"bad_json_{timestamp}.txt"
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"    JSON decode error saved to {debug_path}: {e}")
                return None
        except requests.RequestException as e:
            print(f"    Request exception attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                print(f"    Sleeping {delay}s before retry...")
                time.sleep(delay)
                delay *= 2
            else:
                print("    Final attempt failed for this page.")
                return None
    return None

def normalize_items(json_obj: Any) -> List[Any]:
    """Normalise la réponse en liste d'items (vide si introuvable)."""
    if not json_obj:
        return []
    if isinstance(json_obj, list):
        return json_obj
    if isinstance(json_obj, dict):
        # chercher les clés communes susceptibles de contenir la liste
        for key in ("players", "data", "items", "results", "entries", "content"):
            v = json_obj.get(key)
            if isinstance(v, list):
                return v
        # fallback : prendre la première valeur qui est une liste
        for v in json_obj.values():
            if isinstance(v, list):
                return v
    return []

def parse_items_to_rows(items: List[Any], date_obj: datetime.date) -> List[dict]:
    rows = []
    for item in items:
        if not isinstance(item, dict):
            continue
        player = item.get("player") if isinstance(item.get("player"), dict) else None
        if player is None:
            # fallback : peut-être les champs sont au top-level
            player = item
        if not isinstance(player, dict):
            continue
        full_name = player.get("fullName") or (player.get("firstName", "") + " " + player.get("lastName", "")).strip()
        rows.append({
            "full_name": full_name or None,
            "player_id": player.get("id"),
            "ranking": item.get("ranking") or player.get("ranking"),
            "points": item.get("points") or player.get("points"),
            "movement": item.get("movement") or player.get("movement"),
            "date": date_obj.strftime("%Y-%m-%d"),
        })
    return rows

def scrape_data(specific_dates: List[datetime.date], save_dir: str) -> None:
    """
    Scrape les dates passées dans specific_dates.
    Pagination intelligente : s'arrête quand on rencontre CONSECUTIVE_EMPTY_THRESHOLD pages vides consécutives.
    """
    failed_urls: List[str] = []
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    for current_date in specific_dates:
        print(f"\n=== Scraping date {current_date} ===")
        page = 0
        consecutive_empty = 0
        accumulated_rows: List[dict] = []

        # On parcourt pages jusqu'à MAX_PAGE_LIMIT ou jusqu'à seuil de pages vides consécutives atteint
        while page < MAX_PAGE_LIMIT and consecutive_empty < CONSECUTIVE_EMPTY_THRESHOLD:
            url = (
                "https://api.wtatennis.com/tennis/players/ranked"
                f"?page={page}&pageSize={PAGE_SIZE}&type=rankSingles&sort=asc&name=&metric=SINGLES&at={current_date}&nationality="
            )
            print(f" Fetching page {page} ...")
            data = fetch_page_with_retries(session, url)

            if data is None:
                # Échec réseau/JSON -> log et considérer comme "vide" mais continuer (après RETRIES internes)
                print(f"  -> page {page} failed (network/JSON). Logging and treating as empty for now.")
                failed_urls.append(url)
                consecutive_empty += 1
                page += 1
                continue

            items = normalize_items(data)

            if not items:
                # page vide
                print(f"  -> page {page} empty (no items). consecutive_empty -> {consecutive_empty + 1}")
                consecutive_empty += 1
                page += 1
                # mais on n'arrête pas immédiatement : on acceptera si on voit moins de CONSECUTIVE_EMPTY_THRESHOLD vides consécutives
                continue

            # page non-vide : parse et reset counter
            rows = parse_items_to_rows(items, current_date)
            accumulated_rows.extend(rows)
            print(f"  -> page {page} returned {len(items)} items, parsed {len(rows)} rows. cumulative rows: {len(accumulated_rows)}")
            consecutive_empty = 0
            page += 1

        # Fin de pagination pour cette date
        print(f"Finished pagination for {current_date}. Total rows collected: {len(accumulated_rows)} (stopped at page {page}).")

        # Décider d'accepter ou non suivant le seuil
        if len(accumulated_rows) >= DATE_ACCEPT_THRESHOLD:
            save_csv(accumulated_rows, current_date, save_dir)
        else:
            # Si tu veux sauver partiel pour debug, on le fait (pratique)
            partial_path_dir = os.path.abspath(os.path.join(os.fspath(save_dir), "partials"))
            os.makedirs(partial_path_dir, exist_ok=True)
            partial_file = os.path.join(partial_path_dir, f"data_partial_{current_date.strftime('%Y_%m_%d')}.csv")
            pd.DataFrame(accumulated_rows).to_csv(partial_file, index=False)
            print(f"Insufficient rows ({len(accumulated_rows)} < {DATE_ACCEPT_THRESHOLD}). Saved partial CSV to {partial_file} (abs)")

        # Save failed URLs periodically
        log_failed_urls(failed_urls, save_dir)

    print("\nAll dates processed.")
