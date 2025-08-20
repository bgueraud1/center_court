import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime
import re
from time import sleep
from typing import Optional, List, Dict
import glob
import os
import unicodedata
import time

# US state abbreviations for birthplace normalization
US_STATE_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
    "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
    "Wisconsin": "WI", "Wyoming": "WY"
}

def normalize_birthplace(place: str) -> str:
    if not place or pd.isna(place):
        return None
    place = re.sub(r"\[.*?\]", "", place)
    parts = [p.strip() for p in place.split(',')]
    if len(parts) == 3 and parts[1] in US_STATE_ABBR:
        parts[1] = US_STATE_ABBR[parts[1]]
    return ", ".join(parts)

def clean_ws(text: str) -> str:
    return " ".join(text.replace('\xa0', ' ').split())

def parse_iso_date(value: str) -> str:
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None




# utils in scrape_wiki_wta.py (or a shared util module)
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_retry_session(
    total_retries=5,
    backoff_factor=1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"])
):
    """
    Return a requests.Session preconfigured with Retry logic that also honors
    429 responses and Retry-After headers.
    """
    session = requests.Session()

    session.headers.update({
        "User-Agent": "CenterCourtBot/1.0 (+https://center-court.net; contact:ben.gueraud@yahoo.com)",
    })

    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=allowed_methods,
        raise_on_status=False,  # we will handle statuses manually after retries
        respect_retry_after_header=True
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session




def scrape_wiki_player(session: requests.Session, url: str,
                       max_tries: int = 5, base_delay: float = 0.8) -> dict:
    """
    Fetch & parse a Wikipedia infobox. Retries on 429/5xx with exponential backoff.
    Raises FileNotFoundError on 404, RuntimeError on failure after retries.
    """
    attempt = 0
    last_exc = None
    while attempt < max_tries:
        attempt += 1
        try:
            resp = session.get(url, timeout=15)
        except requests.RequestException as e:
            last_exc = e
            wait = base_delay * (2 ** (attempt - 1))
            print(f"[WARN] network error fetching {url}: {e} — sleeping {wait:.2f}s (try {attempt}/{max_tries})")
            time.sleep(wait)
            continue

        # 404 -> page missing
        if resp.status_code == 404:
            raise FileNotFoundError(f"Page not found: {url}")

        # 200 -> parse
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, 'html.parser')
            infobox = soup.find('table', class_='infobox')
            if not infobox:
                # keep behavior similar to before
                raise ValueError("Infobox not found")

            out = {
                'height_ft': None, 'height_m': None,
                'plays':     None,
                'birth_date':   None,
                'birth_place':  None
            }

            for row in infobox.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if not th or not td:
                    continue
                label = clean_ws(th.get_text())
                value = clean_ws(td.get_text(" "))

                if label == 'Height':
                    m = re.match(r'(.+?)\s*\((.+?)\)', value)
                    if m:
                        a, b = m.groups()
                        if 'm' in a:
                            out['height_m'], out['height_ft'] = a.strip(), b.strip()
                        else:
                            out['height_ft'], out['height_m'] = a.strip(), b.strip()

                elif label == 'Plays':
                    out['plays'] = 'Left-Handed' if 'Left' in value else 'Right-Handed'

                elif label == 'Born':
                    span = td.find('span', class_='bday')
                    if span:
                        out['birth_date'] = span.get_text()
                    else:
                        m2 = re.search(r'([A-Za-z]+ \d{1,2}, \d{4})', value)
                        if m2:
                            out['birth_date'] = datetime.strptime(m2.group(1), '%B %d, %Y') \
                                                  .strftime('%Y-%m-%d')
                    br = td.find('br')
                    if br:
                        raw = ''.join(str(s) for s in td.contents[td.contents.index(br)+1:])
                        out['birth_place'] = clean_ws(BeautifulSoup(raw, 'html.parser').get_text())

            return out

        # non-200: handle 429 (rate limit) and 5xx (server)
        if resp.status_code == 429:
            # respect Retry-After if supplied, else exponential backoff
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra is not None and ra.strip().isdigit() else base_delay * (2 ** (attempt - 1))
            except Exception:
                wait = base_delay * (2 ** (attempt - 1))
            print(f"[WARN] 429 for {url} — sleeping {wait:.2f}s (try {attempt}/{max_tries})")
            time.sleep(wait)
            continue

        if 500 <= resp.status_code < 600:
            wait = base_delay * (2 ** (attempt - 1))
            print(f"[WARN] server {resp.status_code} for {url} — sleeping {wait:.2f}s (try {attempt}/{max_tries})")
            time.sleep(wait)
            continue

        # other unexpected status -> raise RuntimeError (enrich_csv attrape RuntimeError)
        raise RuntimeError(f"Unexpected status {resp.status_code} fetching {url}")

    # exhausted retries
    msg = f"Failed to fetch {url} after {max_tries} tries"
    if last_exc:
        msg += f": {last_exc}"
    raise RuntimeError(msg)


def format_heights(ft: str, m: str):
    # transform scraped "5 ft 11 in" or "1.80 m" into your desired format
    h_in, h_m = None, None
    if ft:
        nums = re.findall(r"(\d+)", ft)
        if len(nums) >= 2:
            h_in = f"{nums[0]}' {nums[1]}\""
    if m:
        mm = re.match(r'([\d\.]+)\s*m', m)
        if mm:
            h_m = f"{mm.group(1)}m"
    return h_in, h_m

def enrich_csv(
    session,
    input_csv: str,
    output_csv: str,
    summary_csv: str,
    rankings_dir: str,
    start_index: int = 0,
    end_index: Optional[int] = None,
    overwrite: bool = False,
    min_first_date: Optional[str] = None,
):
    """
    Enrich only *active* players.  
    0. Skip scraping if ALL target cols are already non-blank.  
    1. Do not log or overwrite when new == old.  
    2. If min_first_date is set, only overwrite existing cells for players
       whose first_appearance > min_first_date.  
    3. Never replace a non-blank cell with a blank/NA.
    """

    prev_rejects = set()
    if os.path.exists(summary_csv):
        # Lire et remplacer tous les NaN par des chaînes vides
        old = pd.read_csv(summary_csv, dtype=str).fillna('')
    
        # Assurer que la colonne 'reject' existe (sécurité)
        if 'reject' not in old.columns:
            old['reject'] = ''
    
        # Filtrer de façon vectorisée et robuste les lignes où reject == '1'
        mask = old['reject'].str.strip() == '1'
        for _, row in old.loc[mask].iterrows():
            pid_num = pd.to_numeric(row.get('player_id', ''), errors='coerce')
            if pd.isna(pid_num):
                # si player_id absent ou non convertible, ignorer
                continue
            pid = int(pid_num)
            col = row['column']
            prev_rejects.add((pid, col))
    

    # load master
    df = pd.read_csv(
        input_csv,
        parse_dates=['birth_date','first_appearance','last_appearance'],
        keep_default_na=False,
    )
    df['player_id'] = pd.to_numeric(df['player_id'], errors='coerce').dropna().astype(int)

    # find current max ranking date
    rank_files = glob.glob(os.path.join(rankings_dir, 'data*.csv'))
    ranks = pd.concat([
        pd.read_csv(f, parse_dates=['date'])
          .assign(player_id=lambda d: pd.to_numeric(d['player_id'], errors='coerce').dropna().astype(int))
        for f in rank_files
    ], ignore_index=True)
    max_date = ranks['date'].max()
    active_ids = set(ranks.loc[ranks['date'] == max_date, 'player_id'])

    # optional min_first_date threshold
    if min_first_date:
        min_first_dt = pd.to_datetime(min_first_date)

    # get indices of active players
    active_idxs = [i for i, pid in enumerate(df['player_id']) if pid in active_ids]
    if end_index is None or end_index > len(active_idxs):
        end_index = len(active_idxs)

    target_cols = ['height_inches','height_cm','plays','birth_date','birthplace']
    # ensure target cols
    for c in target_cols:
        if c not in df.columns:
            df[c] = ''

    changes: List[Dict] = []
    attempted = scraped = 0

       # --- configuration vitesse (ajustable) ---
    SLEEP_BETWEEN = float(os.getenv("WIKI_DELAY", "0.25"))  # secondes, par défaut 0.25s
    MAX_SCRAPE_TRIES = int(os.getenv("WIKI_MAX_TRIES", "5"))
    BASE_BACKOFF = float(os.getenv("WIKI_BASE_BACKOFF", "0.8"))

    for pos in range(start_index, end_index):
        idx = active_idxs[pos]
        row = df.loc[idx]
        name = row['full_name']
        pid = row['player_id']

        # 0. if all target cols non-blank, skip entire row
        if all(row[c] for c in target_cols):
            print(f"[{idx}] SKIP “{name}”: all fields already filled")
            continue

        attempted += 1

        # scrape
        wiki_url = f"https://en.wikipedia.org/wiki/{name.replace(' ', '_')}"
        try:
            info = scrape_wiki_player(session, wiki_url,
                                      max_tries=MAX_SCRAPE_TRIES,
                                      base_delay=BASE_BACKOFF)
        except FileNotFoundError as e:
            print(f"[{idx}] SKIP – {name}: {e}")
            time.sleep(SLEEP_BETWEEN)
            continue
        except (RuntimeError, ValueError) as e:
            # network/server/parse issue — log and skip this player, continue with the rest
            print(f"[{idx}] SKIP – {name}: {e}")
            time.sleep(SLEEP_BETWEEN)
            continue

        # if info is None for any reason, skip
        if not info:
            print(f"[{idx}] SKIP – {name}: empty info")
            time.sleep(SLEEP_BETWEEN)
            continue

        # format heights / build new_data (unchanged)
        ft, cm = format_heights(info['height_ft'], info['height_m'])
        new_data = {
            'height_inches': ft or '',
            'height_cm':     cm or '',
            'plays':         info['plays'] or '',
            'birth_date':    (
            lambda bd: bd.strftime('%b %d %Y').replace(' 0', ' ')
        )(pd.to_datetime(info['birth_date'])) if info['birth_date'] else '',
            'birthplace':    normalize_birthplace(info['birth_place']) or ''
        }

        # decide if this player is allowed overwrites by first date
        allow_over = overwrite and (
            not min_first_date or row['first_appearance'] > min_first_dt
        )

        # apply new_data (unchanged)
        for col, new_val in new_data.items():
            old_val = df.at[idx, col]

            if (pid, col) in prev_rejects:
                continue

            if not new_val and old_val:
                continue
            if new_val == old_val:
                continue
            if old_val and not allow_over:
                continue

            changes.append({
                'player_id': pid,
                'player_name': name,
                'column':    col,
                'row_index': idx,
                'old_value': old_val,
                'new_value': new_val
            })
            df.at[idx, col] = new_val

        scraped += 1
        pct = scraped / attempted * 100
        print(f"[{idx}] OK – {name} – {scraped}/{attempted} = {pct:.1f}%")
        time.sleep(SLEEP_BETWEEN)

    # write outputs
    df.to_csv(output_csv, index=False)
    print(f"Enriched file → {output_csv}")

    if changes:
        summary_df = pd.DataFrame(changes)

        # initialize all to blank
        summary_df['reject'] = ''

        # for any (pid,col) in prev_rejects, carry over the '1'
        for pid, col in prev_rejects:
            mask = (summary_df['player_id'] == pid) & (summary_df['column'] == col)
            summary_df.loc[mask, 'reject'] = '1'

        summary_df.to_csv(summary_csv, index=False)
        print(f"Change log with preserved rejects → {summary_csv}")
    else:
        print("No new overwrites to log.")

    # —————————————————————————————————————————————
    # 4) Finally save the enriched master
    # —————————————————————————————————————————————
    df.to_csv(output_csv, index=False)
    print(f"Enriched file → {output_csv}")


