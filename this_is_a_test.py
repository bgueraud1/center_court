#!/usr/bin/env python3
# enrich_new_players.py
"""
Enrich CSV player_data_atp.csv for new players only.

Usage example:
    python enrich_new_players.py --input player_data_atp.csv --output player_data_atp_enriched.csv --start-index 10000 --delay 1.0

Behavior:
 - rows with index < start_index are copied unchanged
 - for rows >= start_index, the script tries to scrape Wikipedia and fill:
    height_inches, height_cm, plays, backhand, birth_date, birthplace, turned_pro, retired, prize_money
 - the script DOES NOT touch highest_ranking, first_appearance or last_appearance
"""
import argparse
import re
from time import sleep
from datetime import datetime
from urllib.parse import quote
import requests
from bs4 import BeautifulSoup
import pandas as pd

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
    parts = [p.strip() for p in place.split(',') if p.strip()]
    if len(parts) >= 2 and parts[1] in US_STATE_ABBR:
        parts[1] = US_STATE_ABBR[parts[1]]
    return ", ".join(parts)

def clean_ws(text: str) -> str:
    return " ".join(text.replace('\xa0', ' ').split()).strip()

def parse_iso_date(value: str) -> str:
    if not value or pd.isna(value):
        return None
    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%d %B %Y", "%Y"):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            continue
    # fallback: try pandas parsing
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.strftime("%Y-%m-%d")
    except Exception:
        return None

_money_re = re.compile(r"[\$£€]\s?[\d\.,]+")

def extract_money(value: str) -> str:
    if not value or pd.isna(value):
        return None
    m = _money_re.search(value)
    if m:
        return m.group(0).replace(' ', '').replace(',', '')
    return None

def find_infobox(soup):
    # find table whose class contains 'infobox'
    table = soup.find('table', class_=lambda c: c and 'infobox' in c)
    return table

def scrape_wiki_player(session: requests.Session, url: str, timeout=10) -> dict:
    r = session.get(url, timeout=timeout)
    if r.status_code == 404:
        raise FileNotFoundError(f"Page not found: {url}")
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')
    infobox = find_infobox(soup)
    if not infobox:
        raise ValueError("Infobox introuvable")

    data = {
        'height_ft': None, 'height_m': None,
        'plays': None, 'backhand': None,
        'birth_date': None, 'birth_place': None,
        'turned_pro': None, 'retired': None,
        'prize_money': None
    }

    for row in infobox.find_all('tr'):
        th = row.find('th')
        td = row.find('td')
        if not th or not td:
            continue
        label = clean_ws(th.get_text())
        value = clean_ws(td.get_text(" "))
        # Height: many shapes -> try to extract meters and feet
        if label.lower() == 'height':
            # look for m or cm, and for feet pattern
            m_m = re.search(r"([\d\.]+)\s*m", value)
            ft_m = re.search(r"(\d+)\s*ft(?:\.?| |)?\s*(\d+)?\s*in?", value, flags=re.IGNORECASE)
            if m_m:
                data['height_m'] = f"{m_m.group(1)}m"
            if ft_m:
                if ft_m.group(2):
                    data['height_ft'] = f"{ft_m.group(1)}' {ft_m.group(2)}\""
                else:
                    data['height_ft'] = f"{ft_m.group(1)}'"
        elif label.lower() == 'plays':
            low = value.lower()
            if 'left' in low:
                data['plays'] = 'Left-Handed'
            elif 'right' in low:
                data['plays'] = 'Right-Handed'
            # backhand sometimes included in plays cell inside parentheses
            bhm = re.search(r'\(([^)]+backhand[^)]*)\)', value, flags=re.IGNORECASE)
            if bhm:
                bh = bhm.group(1).lower()
                if 'one' in bh:
                    data['backhand'] = 'One-Handed'
                elif 'two' in bh:
                    data['backhand'] = 'Two-Handed'
        elif label.lower() == 'backhand':
            lo = value.lower()
            if 'one' in lo:
                data['backhand'] = 'One-Handed'
            elif 'two' in lo:
                data['backhand'] = 'Two-Handed'
        elif label.lower().startswith('born') or label.lower() == 'born':
            # look for span.bday
            span = td.find('span', class_='bday')
            if span:
                data['birth_date'] = parse_iso_date(span.get_text())
            else:
                # try to find a date in the text
                d2 = re.search(r'([A-Za-z]+ \d{1,2}, \d{4})', value)
                if d2:
                    data['birth_date'] = parse_iso_date(d2.group(1))
            # birthplace often after a <br> tag in the same cell
            br = td.find('br')
            if br:
                # everything after the br
                try:
                    idx = list(td.contents).index(br)
                    raw = "".join(str(x) for x in td.contents[idx+1:])
                    data['birth_place'] = clean_ws(BeautifulSoup(raw, 'html.parser').get_text())
                except Exception:
                    pass
        elif label.lower().startswith('turned pro') or label.lower() == 'turned pro':
            data['turned_pro'] = parse_iso_date(value)
        elif label.lower() == 'retired':
            data['retired'] = parse_iso_date(value)
        elif 'prize money' in label.lower():
            data['prize_money'] = extract_money(value)
    return data

def format_heights(ft: str, m: str):
    h_in, h_m = None, None
    if ft:
        nums = re.findall(r"(\d+)", ft)
        if len(nums) >= 2:
            h_in = f"{nums[0]}' {nums[1]}\""
        elif len(nums) == 1:
            h_in = f"{nums[0]}'"
    if m:
        mm = re.match(r'([\d\.]+)\s*m', m)
        if mm:
            h_m = f"{mm.group(1)}m"
    return h_in, h_m

def enrich_csv(input_csv: str, output_csv: str, start_index: int = 0, delay: float = 1.0, user_agent: str = None):
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    # ensure columns exist
    target_cols = ['height_inches','height_cm','plays','backhand','birth_date','birthplace','turned_pro','retired','prize_money']
    for c in target_cols:
        if c not in df.columns:
            df[c] = ""

    # we will NOT touch these columns:
    preserved = ['highest_ranking','first_appearance','last_appearance']

    # prepare HTTP session
    sess = requests.Session()
    headers = {"User-Agent": user_agent or "Mozilla/5.0 (compatible; enrichment-bot/1.0)"}
    sess.headers.update(headers)

    total = len(df)
    found = 0
    attempted = 0

    for idx in range(start_index, total):
        name = df.at[idx, 'full_name'].strip()
        if not name:
            print(f"[{idx}] skipping empty name")
            continue
        attempted += 1

        base_name = name.replace(' ', '_')
        # ensure proper urllib quoting (handles accents & special chars)
        quoted = quote(base_name, safe='_/()')
        base_url = f"https://en.wikipedia.org/wiki/{quoted}"
        alt_url = base_url + '_(tennis)'

        info = None
        tried = []
        for url in (base_url, alt_url):
            tried.append(url)
            try:
                info = scrape_wiki_player(sess, url)
                break
            except FileNotFoundError:
                # page absent -> try next
                info = None
            except ValueError:
                # infobox not found -> try next
                info = None
            except requests.RequestException as e:
                print(f"[{idx}] HTTP error for {url}: {e}")
                info = None
            except Exception as e:
                print(f"[{idx}] Unexpected error for {url}: {e}")
                info = None

        if not info:
            pct = (found / attempted * 100) if attempted else 0.0
            print(f"[{idx}] NOT FOUND / NO INFO — {name} ({found}/{attempted} = {pct:.1f}%) tried: {tried}")
            sleep(delay)
            continue

        # Successful scrape: fill fields BUT do not touch preserved columns
        ft, cm = format_heights(info.get('height_ft'), info.get('height_m'))
        # update only if empty (safer) OR you can override unconditionally by removing the checks
        if not df.at[idx, 'height_inches']:
            df.at[idx, 'height_inches'] = ft or ""
        if not df.at[idx, 'height_cm']:
            df.at[idx, 'height_cm'] = cm or ""
        if not df.at[idx, 'plays']:
            df.at[idx, 'plays'] = info.get('plays') or ""
        if not df.at[idx, 'backhand']:
            df.at[idx, 'backhand'] = info.get('backhand') or ""
        if not df.at[idx, 'birth_date']:
            df.at[idx, 'birth_date'] = info.get('birth_date') or ""
        if not df.at[idx, 'birthplace']:
            df.at[idx, 'birthplace'] = normalize_birthplace(info.get('birth_place') or "")
        if not df.at[idx, 'turned_pro']:
            df.at[idx, 'turned_pro'] = info.get('turned_pro') or ""
        if not df.at[idx, 'retired']:
            df.at[idx, 'retired'] = info.get('retired') or ""
        if not df.at[idx, 'prize_money']:
            df.at[idx, 'prize_money'] = info.get('prize_money') or ""

        found += 1
        pct = found / attempted * 100 if attempted else 0
        print(f"[{idx}] OK — {name} ({found}/{attempted} = {pct:.1f}%)")
        sleep(delay)

    # write output
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print("Terminé, fichier écrit :", output_csv)
    print(f"Processed rows from index {start_index}..{total-1}. Found={found}, Attempted={attempted}.")

if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="Enrich new players from Wikipedia")
    ap.add_argument("--input", "-i", required=True, help="Input CSV (player_data_atp.csv)")
    ap.add_argument("--output", "-o", required=True, help="Output CSV path")
    ap.add_argument("--start-index", "-s", type=int, default=0, help="Start index (rows before this are left unchanged)")
    ap.add_argument("--delay", "-d", type=float, default=1.0, help="Delay between requests in seconds")
    ap.add_argument("--user-agent", type=str, default=None, help="Custom User-Agent header")
    args = ap.parse_args()

    enrich_csv(args.input, args.output, start_index=args.start_index, delay=args.delay, user_agent=args.user_agent)
