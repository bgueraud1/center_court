import pandas as pd
import time
import re
import unicodedata
import requests
from bs4 import BeautifulSoup
from typing import Optional

# IOC overrides for TennisEnDirect full country names
IOC_OVERRIDES = {
    "Russia":  "RUS",
    "Belarus": "BLR"
}

def slugify(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^a-z0-9\s-]", "", name.lower())
    return re.sub(r"\s+", "-", name).strip("-")

def build_wta_url(player_id: str, full_name: str) -> str:
    slug = slugify(full_name)
    return f"https://www.wtatennis.com/players/{int(player_id)}/{slug}"

def build_ted_url(full_name: str) -> str:
    slug = slugify(full_name)
    return f"https://www.tennisendirect.net/wta/{slug}/"

def get_country_code_wta(session: requests.Session, url: str) -> Optional[str]:
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    img = soup.find("img", alt=lambda alt: bool(alt and len(alt) == 3 and alt.isalpha()))
    return img["alt"] if img else None






def get_country_code_ted(session, url):
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    text = BeautifulSoup(resp.text, "html.parser").get_text(separator="\n")
    m = re.search(r"Pays:\s*([A-Za-z ]+)", text)
    if not m:
        return None
    country = m.group(1).strip()
    return IOC_OVERRIDES.get(country, country[:3].upper())

def enrich_country_codes(
    session: requests.Session,
    input_csv: str,
    output_csv: str,
    start_index: int = 0,
    end_index: Optional[int] = None,
    overwrite: bool = False
) -> str:
    # Read CSV
    df = pd.read_csv(input_csv, dtype={'player_id': 'Int64', 'represented_country': str})
    df['player_id'] = df['player_id'].astype(str)

    n = len(df)
    if end_index is None or end_index > n:
        end_index = n

    print(f"Scraping rows {start_index} through {end_index-1} of {n}")

    codes = []
    for idx in range(start_index, end_index):
        current = df.at[idx, 'represented_country']
        # 1) skip if present and not overwriting
        if pd.notna(current) and current.strip() and not overwrite:
            codes.append(current)
            print(f"[{idx}] SKIP – already has '{current}'")
            continue

        pid = df.at[idx, 'player_id']
        name = df.at[idx, 'full_name']
        # if missing player_id, skip
        if not pid or pid.upper() == "<NA>":
            codes.append(None)
            print(f"[{idx}] SKIP – no player_id")
            continue

        # --- Stage 1: WTA site ---
        wta_url = build_wta_url(pid, name)
        print(f"[{idx}] → WTA: {name} → {wta_url}")
        code = None
        try:
            code = get_country_code_wta(session, wta_url)
        except Exception as e:
            print(f"   WTA lookup failed: {e}")

        # --- Stage 2: fallback if still missing ---
        if not code:
            ted_url = build_ted_url(name)
            print(f"   → TED: {ted_url}")
            try:
                code = get_country_code_ted(session, ted_url)
            except Exception as e:
                print(f"   TED lookup failed: {e}")

        codes.append(code)
        print(f"   Found code: {code}")
        time.sleep(1)

    # assign back and write
    df.loc[start_index:end_index-1, 'represented_country'] = codes
    df.to_csv(output_csv, index=False)
    print(f"Done. Wrote enriched file to {output_csv}")
    return output_csv



