# add_ioc_to_player_atp.py
import pandas as pd
import time
import re
import unicodedata
import requests
from bs4 import BeautifulSoup
from typing import Optional

# Small overrides if needed
IOC_OVERRIDES = {
    "Russia":  "RUS",
    "Belarus": "BLR"
}

def slugify(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^a-z0-9\s-]", "", name.lower())
    return re.sub(r"\s+", "-", name).strip("-")

def build_atp_url(player_id: str, full_name: str) -> str:
    """
    ATP URLs sometimes look like: https://www.atptour.com/en/players/<slug>/<code>
    However many atp pages use numeric ids; sample player_id like 'S0AG' may not match.
    We try a generic player search URL first as fallback to the player page.
    """
    # If player_id seems numeric, use /en/players/<slug>/<id>
    try:
        int(player_id)
        return f"https://www.atptour.com/en/players/{slugify(full_name)}/{player_id}"
    except Exception:
        # fallback: search by name on atptour
        return f"https://www.atptour.com/en/players?search={requests.utils.quote(full_name)}"

def build_ted_url(full_name: str) -> str:
    slug = slugify(full_name)
    return f"https://www.tennisendirect.net/atp/{slug}/"

def get_country_code_atp(session: requests.Session, url: str) -> Optional[str]:
    """
    Attempt to fetch country code from ATP page. On the ATP pages the country code
    is sometimes in an <img alt="ITA"> or in a span. We attempt multiple heuristics.
    """
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # common pattern: img with alt "ITA"
    img = soup.find("img", alt=lambda alt: bool(alt and len(alt.strip()) == 3 and alt.isalpha()))
    if img and img.get("alt"):
        return img["alt"].upper()

    # sometimes country is present in a <span class="country-code"> or meta tags
    span = soup.find(lambda t: t.name in ("span","div") and 'country' in (t.get('class') or []))
    if span:
        txt = span.get_text(strip=True)
        if len(txt) <= 3 and txt.isalpha():
            return txt.upper()

    # fallback: parse textual "Country:" label
    text = soup.get_text(separator="\n")
    m = re.search(r'Country[:\s]+\b([A-Za-z ]+)\b', text)
    if m:
        country = m.group(1).strip()
        return IOC_OVERRIDES.get(country, country[:3].upper())

    return None

def get_country_code_ted(session, url):
    # reuse TED logic from original: parse "Pays:"
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    text = BeautifulSoup(resp.text, "html.parser").get_text(separator="\n")
    m = re.search(r"Pays:\s*([A-Za-z ]+)", text)
    if not m:
        return None
    country = m.group(1).strip()
    return IOC_OVERRIDES.get(country, country[:3].upper())

def enrich_country_codes_atp(
    session: requests.Session,
    input_csv: str,
    output_csv: str,
    start_index: int = 0,
    end_index: Optional[int] = None,
    overwrite: bool = False
) -> str:
    df = pd.read_csv(input_csv, dtype={'player_id': str, 'represented_country': str})
    n = len(df)
    if end_index is None or end_index > n:
        end_index = n

    print(f"Scraping rows {start_index} through {end_index-1} of {n} (ATP)")

    codes = []
    for idx in range(start_index, end_index):
        current = df.at[idx, 'represented_country'] if 'represented_country' in df.columns else None
        if pd.notna(current) and current.strip() and not overwrite:
            codes.append(current)
            print(f"[{idx}] SKIP – already has '{current}'")
            continue

        pid = str(df.at[idx, 'player_id'])
        name = df.at[idx, 'full_name']
        if not pid or pid.strip() == "":
            codes.append(None)
            print(f"[{idx}] SKIP – no player_id")
            continue

        atp_url = build_atp_url(pid, name)
        print(f"[{idx}] → ATP: {name} → {atp_url}")
        code = None
        try:
            code = get_country_code_atp(session, atp_url)
        except Exception as e:
            print(f"   ATP lookup failed: {e}")

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

    df.loc[start_index:end_index-1, 'represented_country'] = codes
    df.to_csv(output_csv, index=False)
    print(f"Done. Wrote enriched file to {output_csv}")
    return output_csv
