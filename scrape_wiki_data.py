import glob
import os
import re
import time
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Set
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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

SCHEMAS = {
    "ATP": {
        "column_order": [
            "full_name", "player_id", "represented_country", "height_inches", "height_cm",
            "plays", "backhand", "birth_date", "birthplace", "first_appearance",
            "last_appearance", "highest_ranking", "prize_money", "reviewed_player",
            "date_review", "biography", "turned_pro", "retired"
        ],
        "target_cols": ["height_inches", "height_cm", "plays", "backhand", "birth_date", "birthplace"],
        "birth_date_format": "iso",
    },
    "WTA": {
        "column_order": [
            "height_inches", "height_cm", "plays", "birth_date", "birthplace",
            "player_id", "full_name", "best_rank", "first_appearance", "last_appearance",
            "represented_country", "reviewed_player", "date_review", "biography", "backhand"
        ],
        "target_cols": ["height_inches", "height_cm", "plays", "backhand", "birth_date", "birthplace"],
        "birth_date_format": "wta",
    },
}


def normalize_birthplace(place: str) -> Optional[str]:
    if not place or pd.isna(place):
        return None
    place = re.sub(r"\[.*?\]", "", str(place))
    parts = [p.strip() for p in place.split(",")]
    if len(parts) == 3 and parts[1] in US_STATE_ABBR:
        parts[1] = US_STATE_ABBR[parts[1]]
    return ", ".join(parts)


def clean_ws(text: str) -> str:
    if text is None:
        return ""
    return " ".join(str(text).replace("\xa0", " ").split())


def parse_iso_date(value: str) -> Optional[str]:
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def format_birth_date(date_value: Optional[str], mode: str) -> str:
    dt = pd.to_datetime(date_value, errors="coerce")
    if pd.isna(dt):
        return ""
    mode = mode.upper()
    if mode == "ATP":
        return dt.strftime("%Y-%m-%d")
    # WTA style: Mar 30 1999
    return dt.strftime("%b %d %Y").replace(" 0", " ")


def make_retry_session(
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
) -> requests.Session:
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
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_plays_and_backhand(value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse Wikipedia 'Plays' field into:
      - plays: Left-Handed / Right-Handed
      - backhand: One-Handed / Two-Handed (when present)
    """
    if not value:
        return None, None

    text = clean_ws(value)
    low = text.lower()

    plays = None
    if "left" in low:
        plays = "Left-Handed"
    elif "right" in low:
        plays = "Right-Handed"

    backhand = None
    if re.search(r"one[- ]handed backhand", low):
        backhand = "One-Handed"
    elif re.search(r"two[- ]handed backhand", low):
        backhand = "Two-Handed"
    elif re.search(r"left[- ]handed backhand", low):
        backhand = "One-Handed"
    elif re.search(r"right[- ]handed backhand", low):
        backhand = "One-Handed"

    return plays, backhand


def scrape_wiki_player(
    session: requests.Session,
    url: str,
    max_tries: int = 5,
    base_delay: float = 0.8,
) -> dict:
    """
    Fetch & parse a Wikipedia infobox.
    Retries on 429/5xx with exponential backoff.
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

        if resp.status_code == 404:
            raise FileNotFoundError(f"Page not found: {url}")

        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            infobox = soup.find("table", class_="infobox")
            if not infobox:
                raise ValueError("Infobox not found")

            out = {
                "height_ft": None,
                "height_m": None,
                "plays": None,
                "backhand": None,
                "birth_date": None,
                "birth_place": None,
            }

            for row in infobox.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue

                label = clean_ws(th.get_text())
                value = clean_ws(td.get_text(" "))

                if label == "Height":
                    m = re.match(r"(.+?)\s*\((.+?)\)", value)
                    if m:
                        a, b = m.groups()
                        if "m" in a.lower():
                            out["height_m"], out["height_ft"] = a.strip(), b.strip()
                        else:
                            out["height_ft"], out["height_m"] = a.strip(), b.strip()

                elif label == "Plays":
                    plays, backhand = parse_plays_and_backhand(value)
                    if plays:
                        out["plays"] = plays
                    if backhand:
                        out["backhand"] = backhand

                elif label == "Backhand":
                    _, backhand = parse_plays_and_backhand(value)
                    if backhand:
                        out["backhand"] = backhand
                    else:
                        low = value.lower()
                        if "one" in low:
                            out["backhand"] = "One-Handed"
                        elif "two" in low:
                            out["backhand"] = "Two-Handed"

                elif label == "Born":
                    span = td.find("span", class_="bday")
                    if span:
                        out["birth_date"] = span.get_text(strip=True)
                    else:
                        m2 = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", value)
                        if m2:
                            out["birth_date"] = datetime.strptime(m2.group(1), "%B %d, %Y").strftime("%Y-%m-%d")

                    br = td.find("br")
                    if br:
                        raw = "".join(str(s) for s in td.contents[td.contents.index(br) + 1 :])
                        out["birth_place"] = clean_ws(BeautifulSoup(raw, "html.parser").get_text(" "))

            return out

        if resp.status_code == 429:
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

        raise RuntimeError(f"Unexpected status {resp.status_code} fetching {url}")

    msg = f"Failed to fetch {url} after {max_tries} tries"
    if last_exc:
        msg += f": {last_exc}"
    raise RuntimeError(msg)


def format_heights(ft: str, m: str) -> Tuple[str, str]:
    """
    Transform scraped heights into:
      - height_inches: 6' 3"
      - height_cm: 1.91m
    """
    h_in, h_m = None, None

    if ft:
        nums = re.findall(r"(\d+)", str(ft))
        if len(nums) >= 2:
            h_in = f"{int(nums[0])}' {int(nums[1])}\""

    if m:
        mm = re.match(r"([\d\.]+)\s*m", str(m), flags=re.I)
        if mm:
            h_m = f"{mm.group(1)}m"

    return h_in, h_m


def build_wiki_url(full_name: str) -> str:
    """
    Build a safe Wikipedia URL from a player name.
    """
    title = str(full_name).strip().replace(" ", "_")
    safe_chars = "_()'-"
    return f"https://en.wikipedia.org/wiki/{quote(title, safe=safe_chars)}"


def enrich_csv(
    session: Optional[requests.Session],
    input_csv: str,
    output_csv: str,
    summary_csv: str,
    rankings_dir: str,
    mode: str = "WTA",
    start_index: int = 0,
    end_index: Optional[int] = None,
    overwrite: bool = False,
    min_first_date: Optional[str] = None,
):
    """
    Enrich only active players.

    Rules:
    - Skip scraping if all target columns are already non-blank.
    - Do not log or overwrite when new == old.
    - If min_first_date is set, only allow overwrites for players whose
      first_appearance > min_first_date.
    - Never replace a non-blank cell with a blank/NA.
    - Preserve the CSV layout of the selected mode.
    """
    mode = mode.upper().strip()
    if mode not in SCHEMAS:
        raise ValueError(f"Unknown mode '{mode}'. Use 'ATP' or 'WTA'.")

    schema = SCHEMAS[mode]
    column_order = schema["column_order"]
    target_cols = schema["target_cols"]
    birth_date_format = schema["birth_date_format"]

    if session is None:
        session = make_retry_session()

    prev_rejects: Set[Tuple[str, str]] = set()
    if os.path.exists(summary_csv):
        old = pd.read_csv(summary_csv, dtype=str, keep_default_na=False).fillna("")
        if "reject" not in old.columns:
            old["reject"] = ""
        if "player_id" not in old.columns:
            old["player_id"] = ""
        if "column" not in old.columns:
            old["column"] = ""

        mask = old["reject"].astype(str).str.strip() == "1"
        for _, row in old.loc[mask].iterrows():
            pid = str(row.get("player_id", "")).strip()
            col = str(row.get("column", "")).strip()
            if pid and col:
                prev_rejects.add((pid, col))

    # Load master CSV as strings to preserve formatting and ATP alphanumeric IDs
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False).fillna("")

    if "player_id" not in df.columns:
        raise ValueError("Input CSV must contain a 'player_id' column.")
    if "full_name" not in df.columns:
        raise ValueError("Input CSV must contain a 'full_name' column.")

    df["player_id"] = df["player_id"].astype(str).str.strip()
    df["full_name"] = df["full_name"].astype(str).str.strip()

    # Make sure the target columns exist
    for c in target_cols:
        if c not in df.columns:
            df[c] = ""

    # Load ranking files to identify active players
    rank_files = glob.glob(os.path.join(rankings_dir, "data*.csv"))
    if not rank_files:
        raise FileNotFoundError(f"No ranking files found in {rankings_dir!r} (expected data*.csv).")

    rank_frames = []
    for f in rank_files:
        r = pd.read_csv(f, dtype=str, keep_default_na=False).fillna("")
        if "date" not in r.columns or "player_id" not in r.columns:
            continue
        r["date"] = pd.to_datetime(r["date"], errors="coerce")
        r["player_id"] = r["player_id"].astype(str).str.strip()
        r = r[r["player_id"] != ""]
        rank_frames.append(r)

    if not rank_frames:
        raise ValueError("No valid ranking files found: each ranking file must contain 'date' and 'player_id' columns.")

    ranks = pd.concat(rank_frames, ignore_index=True)
    ranks = ranks[pd.notna(ranks["date"])]

    if ranks.empty:
        raise ValueError("Ranking files do not contain any valid dates.")

    max_date = ranks["date"].max()
    active_ids = set(ranks.loc[ranks["date"] == max_date, "player_id"].astype(str).str.strip())

    if min_first_date:
        min_first_dt = pd.to_datetime(min_first_date, errors="coerce")
        if pd.isna(min_first_dt):
            raise ValueError(f"Invalid min_first_date: {min_first_date!r}")
    else:
        min_first_dt = None

    active_idxs = [i for i, pid in enumerate(df["player_id"]) if pid in active_ids]
    if end_index is None or end_index > len(active_idxs):
        end_index = len(active_idxs)

    changes: List[Dict[str, object]] = []
    attempted = 0
    scraped = 0

    SLEEP_BETWEEN = float(os.getenv("WIKI_DELAY", "0.25"))
    MAX_SCRAPE_TRIES = int(os.getenv("WIKI_MAX_TRIES", "5"))
    BASE_BACKOFF = float(os.getenv("WIKI_BASE_BACKOFF", "0.8"))

    for pos in range(start_index, end_index):
        idx = active_idxs[pos]
        row = df.loc[idx]
        name = str(row.get("full_name", "")).strip()
        pid = str(row.get("player_id", "")).strip()

        if not name or not pid:
            continue

        # Skip whole row if everything is already filled
        if all(str(row.get(c, "")).strip() != "" for c in target_cols):
            print(f"[{idx}] SKIP “{name}”: all target fields already filled")
            continue

        attempted += 1

        wiki_url = build_wiki_url(name)

        try:
            info = scrape_wiki_player(
                session,
                wiki_url,
                max_tries=MAX_SCRAPE_TRIES,
                base_delay=BASE_BACKOFF,
            )
        except FileNotFoundError as e:
            print(f"[{idx}] SKIP – {name}: {e}")
            time.sleep(SLEEP_BETWEEN)
            continue
        except (RuntimeError, ValueError) as e:
            print(f"[{idx}] SKIP – {name}: {e}")
            time.sleep(SLEEP_BETWEEN)
            continue

        if not info:
            print(f"[{idx}] SKIP – {name}: empty info")
            time.sleep(SLEEP_BETWEEN)
            continue

        ft, cm = format_heights(info.get("height_ft"), info.get("height_m"))

        if birth_date_format == "iso":
            birth_date_out = format_birth_date(info.get("birth_date"), "ATP")
        else:
            birth_date_out = format_birth_date(info.get("birth_date"), "WTA")

        new_data = {
            "height_inches": ft or "",
            "height_cm": cm or "",
            "plays": info.get("plays") or "",
            "backhand": info.get("backhand") or "",
            "birth_date": birth_date_out or "",
            "birthplace": normalize_birthplace(info.get("birth_place")) or "",
        }

        first_app_dt = pd.to_datetime(row.get("first_appearance", ""), errors="coerce")
        allow_over = overwrite and (
            min_first_dt is None or (pd.notna(first_app_dt) and first_app_dt > min_first_dt)
        )

        for col, new_val in new_data.items():
            if col not in df.columns:
                continue

            old_val = str(df.at[idx, col]).strip()

            if (pid, col) in prev_rejects:
                continue

            if not new_val and old_val:
                continue

            if new_val == old_val:
                continue

            if old_val and not allow_over:
                continue

            changes.append({
                "player_id": pid,
                "player_name": name,
                "column": col,
                "row_index": idx,
                "old_value": old_val,
                "new_value": new_val,
            })
            df.at[idx, col] = new_val

        scraped += 1
        pct = (scraped / attempted * 100) if attempted else 0.0
        print(f"[{idx}] OK – {name} – {scraped}/{attempted} = {pct:.1f}%")
        time.sleep(SLEEP_BETWEEN)

    # Ensure all schema columns exist and preserve desired order
    for c in column_order:
        if c not in df.columns:
            df[c] = ""

    ordered_cols = [c for c in column_order if c in df.columns] + [c for c in df.columns if c not in column_order]
    df = df[ordered_cols]

    # Write main output
    df.to_csv(output_csv, index=False)
    print(f"Enriched file → {output_csv}")

    # Write change log
    if changes:
        summary_df = pd.DataFrame(changes)
        summary_df["reject"] = ""

        for pid, col in prev_rejects:
            mask = (summary_df["player_id"].astype(str) == str(pid)) & (summary_df["column"].astype(str) == str(col))
            summary_df.loc[mask, "reject"] = "1"

        summary_df.to_csv(summary_csv, index=False)
        print(f"Change log with preserved rejects → {summary_csv}")
    else:
        print("No new overwrites to log.")

    return df


if __name__ == "__main__":
    # Exemple d'utilisation :
    # session = make_retry_session()
    # enrich_csv(
    #     session=session,
    #     input_csv="player_data_atp.csv",
    #     output_csv="player_data_atp_enriched.csv",
    #     summary_csv="summary_atp.csv",
    #     rankings_dir="rankings",
    #     mode="ATP",   # ou "WTA"
    #     overwrite=False,
    #     min_first_date=None,
    # )
    pass