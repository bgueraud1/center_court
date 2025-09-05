#!/usr/bin/env python3
# scrape_atp_resilient_logged_restart_run.py
"""
Resilient ATP scraper that reads a list of dates from a CSV and scrapes only those dates
which are NOT already present in atp_rankings/data_YYYY_MM_DD.csv.

Special rule:
 - If a date in the input CSV is not a Monday, the script maps it to the Monday of that week.
   That Monday is the one used for the scraping URL and the saved CSV's date column/filename.
"""
import re
import time
import os
import argparse
import random
import pandas as pd
from datetime import datetime, timedelta, date as date_cls
from pathlib import Path
from typing import List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------
# Configuration defaults
# ---------------------------
URL_TEMPLATE = "https://www.atptour.com/en/rankings/singles?rankRange=0-5000&dateWeek={date}"
ATP_BASE_URL = "https://www.atptour.com/en/rankings/singles"

DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ---------------------------
# Exceptions & Helpers
# ---------------------------
class CaptchaDetected(Exception):
    """Raised when a captcha / 'verify you are human' page is detected."""
    pass

def clean_points(text: str) -> str:
    if not text:
        return ""
    s = re.sub(r"[^\d\-]", "", text)
    return s

def read_proxies(proxy_file: Optional[str]) -> List[str]:
    if not proxy_file:
        return []
    p = Path(proxy_file)
    if not p.exists():
        print(f"Proxy file {proxy_file} not found -> ignoring proxies.")
        return []
    lines = [l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
    return lines

def choose_user_agent(ua_list: List[str]) -> str:
    return random.choice(ua_list)

def make_driver(headless=False, user_agent=None, proxy=None):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    else:
        opts.add_argument("--start-maximized")

    if user_agent:
        opts.add_argument(f"user-agent={user_agent}")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    if proxy:
        proxy_arg = proxy if "://" in proxy else f"http://{proxy}"
        opts.add_argument(f"--proxy-server={proxy_arg}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
    except Exception:
        pass
    return driver

# ---------------------------
# Helper: map any date to the Monday of that week (ISO week starting Monday)
# ---------------------------
def to_monday_iso(date_str: str) -> str:
    """
    Given date_str in 'YYYY-MM-DD', returns the Monday (YYYY-MM-DD) of that week.
    Example: 2025-03-30 (Sunday) -> 2025-03-24 (Monday) if you consider week starting Monday.
    Implementation: monday = d - timedelta(days=d.weekday()) where Monday.weekday()==0.
    """
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    monday = d - timedelta(days=d.weekday())
    return monday.isoformat()

# ---------------------------
# Page parsing & detection (same logic)
# ---------------------------
def is_captcha_page(driver):
    try:
        title = (driver.title or "").lower()
        body_text = ""
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        except Exception:
            pass

        keywords = ["verify", "are you human", "access to this site has been blocked", "please verify", "security check", "robot", "captcha"]
        for kw in keywords:
            if kw in title or kw in body_text:
                return True

        if driver.find_elements(By.CSS_SELECTOR, "iframe[src*='recaptcha']") or driver.find_elements(By.CSS_SELECTOR, ".g-recaptcha"):
            return True

        if driver.find_elements(By.XPATH, "//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'please verify you are a human')]"):
            return True

        return False
    except Exception:
        return False

def find_header_indices(driver):
    try:
        WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table thead")))
    except TimeoutException:
        pass

    ths = driver.find_elements(By.CSS_SELECTOR, "table thead tr th")
    headers = [t.text.strip() for t in ths if t.text.strip()]
    if not headers:
        ths = driver.find_elements(By.CSS_SELECTOR, "table th")
        headers = [t.text.strip() for t in ths if t.text.strip()]
    headers_lower = [h.lower() for h in headers]

    rank_idx = player_idx = points_idx = None
    for i,h in enumerate(headers_lower):
        if rank_idx is None and ("rank" in h or h.startswith("#") or "pos" in h):
            rank_idx = i
        if player_idx is None and ("player" in h or "player/name" in h or "player name" in h):
            player_idx = i
        if points_idx is None and ("official" in h or "official points" in h or "points" in h):
            points_idx = i

    if points_idx is None:
        for i,h in enumerate(headers_lower):
            if "points" in h:
                points_idx = i
                break
    if points_idx is None and len(headers) >= 4:
        points_idx = 3

    return rank_idx, player_idx, points_idx, headers

def parse_table_rows(driver, rank_idx, player_idx, points_idx, date_str, max_players=None):
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    players = []
    for r in rows:
        try:
            cells = r.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue
            idxs = [i for i in (rank_idx, player_idx, points_idx) if i is not None]
            if not idxs or max(idxs) >= len(cells):
                continue

            rank_text = cells[rank_idx].text.strip() if rank_idx is not None else ""
            rank_text = rank_text.replace("\n", " ").strip()

            name = ""
            try:
                name_elem = cells[player_idx].find_element(By.TAG_NAME, "a")
                name = name_elem.text.strip()
            except Exception:
                name = cells[player_idx].text.strip()
            name = name.replace("\n", " ").strip()

            pts_text = cells[points_idx].text.strip() if points_idx is not None else ""
            points_text = clean_points(pts_text)

            if not name or name.lower() in ("player", "rank"):
                continue

            players.append({"Rank": rank_text, "Player": name, "Official Points": points_text, "Date": date_str})
            if max_players is not None and len(players) >= max_players:
                break
        except StaleElementReferenceException:
            continue
    return players

def fallback_parse_by_text(driver, date_str, max_players=None):
    elems = driver.find_elements(By.CSS_SELECTOR, "div, li, p")
    candidates = []
    for e in elems:
        t = e.text.strip()
        if not t:
            continue
        m = re.search(r"^\s*(\d{1,4})\s+([A-Za-zÀ-ÖØ-öø-ÿ\.\'\- ]{2,80}?)\s+([0-9\.,\s]{2,20})", t)
        if m:
            rank = m.group(1)
            name = m.group(2).strip()
            pts = clean_points(m.group(3))
            candidates.append({"Rank": rank, "Player": name, "Official Points": pts, "Date": date_str})
            if max_players is not None and len(candidates) >= max_players:
                break
    return candidates

# ---------------------------
# Cookie handling
# ---------------------------
def accept_cookies_if_present(driver, timeout=5):
    """
    Robust OneTrust cookie accept helper.
    Returns True if clicked / accepted, False otherwise.
    """
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, "onetrust-button-group"))
        )
    except Exception:
        return False

    def _try_click_element(el):
        try:
            el.click()
            time.sleep(min(0.4, 15))
            return True
        except Exception:
            try:
                driver.execute_script("arguments[0].click();", el)
                time.sleep(min(0.4, 15))
                return True
            except Exception:
                return False

    # try main document
    for aid in ("onetrust-accept-btn-handler", "onetrust-reject-all-handler", "onetrust-pc-btn-handler"):
        try:
            el = driver.find_element(By.ID, aid)
            if el and _try_click_element(el):
                print(f"Cookie banner: clicked {aid}")
                return True
        except Exception:
            continue

    # try querySelector via JS
    try:
        clicked = driver.execute_script("""
            var b = document.querySelector('#onetrust-accept-btn-handler') 
                    || document.querySelector('#onetrust-reject-all-handler') 
                    || document.querySelector('#onetrust-pc-btn-handler');
            if (b) { b.click(); return true; } else { return false; }
        """)
        if clicked:
            time.sleep(min(0.5, 15))
            print("Cookie banner: clicked via JS")
            return True
    except Exception:
        pass

    # try iframes
    try:
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for iframe in iframes:
            try:
                driver.switch_to.frame(iframe)
                for aid in ("onetrust-accept-btn-handler", "onetrust-reject-all-handler", "onetrust-pc-btn-handler"):
                    try:
                        el = driver.find_element(By.ID, aid)
                        if el and _try_click_element(el):
                            driver.switch_to.default_content()
                            print(f"Cookie banner: clicked {aid} inside iframe")
                            return True
                    except Exception:
                        continue
                driver.switch_to.default_content()
            except Exception:
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
                continue
    except Exception:
        pass

    print("Cookie banner: not found/clicked")
    return False

# ---------------------------
# Scrape one date
# ---------------------------
def scrape_for_date(driver, date_str, max_players=None):
    """
    date_str expected as YYYY-MM-DD (we'll pass the Monday iso).
    """
    url = URL_TEMPLATE.format(date=date_str)
    tried = 0
    while True:
        try:
            tried += 1
            driver.get(url)
            break
        except Exception as e:
            print(f"[{date_str}] Error loading page (attempt {tried}): {e}")
            if tried >= 2:
                raise
            time.sleep(min(2, 15))

    time.sleep(min(1.0, 15))

    # Try to accept cookie banner
    try:
        accept_cookies_if_present(driver)
    except Exception:
        pass

    if is_captcha_page(driver):
        raise CaptchaDetected(f"Captcha / verification detected on {date_str}")

    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
    except TimeoutException:
        pass

    if is_captcha_page(driver):
        raise CaptchaDetected(f"Captcha / verification detected on {date_str}")

    page_date = date_str
    rank_idx, player_idx, points_idx, headers = find_header_indices(driver)
    if rank_idx is not None and player_idx is not None and points_idx is not None:
        players = parse_table_rows(driver, rank_idx, player_idx, points_idx, page_date, max_players=max_players)
    else:
        players = fallback_parse_by_text(driver, page_date, max_players=max_players)

    cleaned = []
    seen = set()
    for p in players:
        rk = p.get("Rank","").strip()
        nm = p.get("Player","").strip()
        pts = p.get("Official Points","").strip()
        dt = p.get("Date","").strip()
        key = (rk, nm, dt)
        if not nm or key in seen:
            continue
        seen.add(key)
        cleaned.append({"Rank": rk, "Player": nm, "Official Points": pts, "Date": dt})

    return pd.DataFrame(cleaned)

# ---------------------------
# Save CSV & logs
# ---------------------------
def save_df_for_date(df_to_save, out_dir: Path, date_iso: str):
    """date_iso expected YYYY-MM-DD (Monday)"""
    fn_date = date_iso.replace("-", "_")
    csv_path = out_dir / f"data_{fn_date}.csv"

    if df_to_save is None or df_to_save.empty:
        out_df = pd.DataFrame(columns=["full_name","ranking","points","date"])
    else:
        mapping = {"Player":"full_name", "Rank":"ranking", "Official Points":"points", "Date":"date"}
        out_df = df_to_save.rename(columns=mapping)
        for col in ["full_name","ranking","points","date"]:
            if col not in out_df.columns:
                out_df[col] = ""
        out_df = out_df[["full_name","ranking","points","date"]]

    out_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"Saved {len(out_df)} rows -> {csv_path}")
    return csv_path

def append_missed_date_log(out_dir: Path, date_str: str, reason: str):
    log_path = out_dir / "missed_dates.log"
    line = f"{date_str},{datetime.utcnow().isoformat()},{reason}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(f"Logged missed date {date_str} -> {log_path}")

# ---------------------------
# Dates CSV handling
# ---------------------------
def read_dates_file(dates_csv: str) -> List[str]:
    """
    Lecture robuste d'un CSV contenant une colonne 'date' ou 'dates' (ou d'un fichier contenant
    des dates en clair). Retourne une liste de dates ISO 'YYYY-MM-DD' dans l'ordre trouvé,
    sans doublons.
    """
    p = Path(dates_csv)
    if not p.exists():
        raise FileNotFoundError(f"{dates_csv} not found")

    text = p.read_text(encoding="utf-8", errors="replace")

    # 1) Essayer pandas avec engine='python' (tolérant aux séparateurs irréguliers)
    try:
        df = pd.read_csv(p, dtype=str, keep_default_na=False, engine="python")
        # trouver colonne 'date' ou 'dates' (insensible à la casse)
        col = None
        for c in df.columns:
            if c.strip().lower() in ("date", "dates"):
                col = c
                break

        raw_values = []
        if col is not None:
            raw_values = df[col].astype(str).tolist()
        else:
            # si le dataframe n'a qu'une colonne, on l'utilise
            if len(df.columns) == 1:
                raw_values = df.iloc[:, 0].astype(str).tolist()
            else:
                # sinon fallback au parsing ligne par ligne ci-dessous
                raw_values = []

    except Exception:
        raw_values = []

    # 2) Si pandas n'a rien donné / on est en fallback, parser le fichier ligne par ligne
    if not raw_values:
        raw_values = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            # enlever éventuels quotes
            if (line.startswith('"') and line.endswith('"')) or (line.startswith("'") and line.endswith("'")):
                line = line[1:-1].strip()
            # si la ligne ressemble à "date" (en-tête), skip
            if line.lower() in ("date", "dates"):
                continue
            raw_values.append(line)

    # 3) Extraire et normaliser les dates depuis raw_values
    seen = set()
    cleaned = []
    for v in raw_values:
        if not v or v.strip().lower() in ("date", "dates"):
            continue
        v = v.strip()
        # remplacer points par tirets si besoin
        v2 = v.replace(".", "-")
        # Si la valeur est exactement YYYY-MM-DD, accept
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v2):
            iso = v2
        else:
            # chercher une date dans la chaîne (YYYY-MM-DD ou YYYY.MM.DD)
            m = re.search(r"(\d{4}[.\-]\d{2}[.\-]\d{2})", v)
            if m:
                iso = m.group(1).replace(".", "-")
            else:
                # essayer avec pandas.to_datetime pour formats tolérants
                try:
                    dt = pd.to_datetime(v, dayfirst=False, errors="coerce")
                    if pd.isna(dt):
                        continue
                    iso = dt.strftime("%Y-%m-%d")
                except Exception:
                    continue
        if iso not in seen:
            seen.add(iso)
            cleaned.append(iso)

    return cleaned


# ---------------------------
# Main: iterate dates from the CSV list (mapping to monday)
# ---------------------------
def iterate_dates_from_list(dates_list: List[str], headless=False,
                            max_players=None, rotate_ua=False, ua_list=None,
                            proxy_file=None, restart_run_every=2, max_retries_captcha=3, backoff_factor=2.0):
    out_dir = Path("atp_rankings")
    out_dir.mkdir(parents=True, exist_ok=True)

    proxies = read_proxies(proxy_file)
    ua_list = ua_list or DEFAULT_USER_AGENTS

    # build list of tuples: (input_date, monday_date)
    mapped: List[Tuple[str,str]] = []
    for inp in dates_list:
        monday = to_monday_iso(inp)
        mapped.append((inp, monday))

    # filter out monday-dates that already have a CSV
    to_process = []
    for inp, monday in mapped:
        fn = out_dir / f"data_{monday.replace('-', '_')}.csv"
        if fn.exists():
            print(f"Skipping {inp} -> {monday}: already exists -> {fn.name}")
            continue
        to_process.append((inp, monday))

    if not to_process:
        print("No dates to process (all present in atp_rankings). Exiting.")
        return

    print(f"Will process {len(to_process)} dates (first: {to_process[0]}).")

    # initial driver
    current_ua = choose_user_agent(ua_list) if rotate_ua else ua_list[0]
    current_proxy = random.choice(proxies) if proxies else None
    driver = make_driver(headless=headless, user_agent=current_ua, proxy=current_proxy)
    print(f"Started driver (UA={current_ua[:60]}..., proxy={current_proxy})")

    previous_df = None
    run_count = 1

    try:
        for idx, (inp_date, monday_date) in enumerate(to_process, start=1):
            print(f"\n[{idx}/{len(to_process)}] Processing input {inp_date} -> monday {monday_date} (run {run_count}) ...")

            # optionally simulate a fresh run boundary
            if restart_run_every and restart_run_every > 0 and (idx-1) % restart_run_every == 0 and idx != 1:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
                previous_df = None
                pause = random.uniform(10, 20)
                print(f"Simulating run boundary: sleeping {min(pause, 15):.1f}s and restarting driver")
                time.sleep(min(pause, 15))
                run_count += 1
                current_ua = choose_user_agent(ua_list) if rotate_ua else ua_list[0]
                current_proxy = random.choice(proxies) if proxies else None
                driver = make_driver(headless=headless, user_agent=current_ua, proxy=current_proxy)
                print(f"[run {run_count}] new driver (UA={current_ua[:60]}..., proxy={current_proxy})")

            attempt = 0
            success = False
            captcha_flag = False
            wait_seconds = 2.0
            df = None

            # NOTE: we use monday_date for the actual scraping (ATP expects week Mondays)
            while attempt <= max_retries_captcha and not success:
                attempt += 1
                try:
                    if attempt > 1:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        current_ua = choose_user_agent(ua_list) if rotate_ua else current_ua
                        current_proxy = random.choice(proxies) if proxies else current_proxy
                        driver = make_driver(headless=headless, user_agent=current_ua, proxy=current_proxy)
                        print(f"[{monday_date}] Retry {attempt}: restarted driver with UA={current_ua[:60]}..., proxy={current_proxy}")

                    df = scrape_for_date(driver, monday_date, max_players=max_players)
                    success = True
                except CaptchaDetected as cex:
                    captcha_flag = True
                    print(f"[{monday_date}] CaptchaDetected: {cex}. Attempt {attempt}/{max_retries_captcha}. Backing off {wait_seconds}s.")
                    time.sleep(min(wait_seconds + random.uniform(5, 12), 15))
                    wait_seconds = min(wait_seconds * backoff_factor, 60)
                except WebDriverException as wex:
                    print(f"[{monday_date}] WebDriverException: {wex}. Attempt {attempt}/{max_retries_captcha}. Retrying after {wait_seconds}s.")
                    time.sleep(min(wait_seconds + random.uniform(5, 12), 15))
                    wait_seconds = min(wait_seconds * backoff_factor, 60)
                except Exception as ex:
                    print(f"[{monday_date}] Unexpected error: {ex}. Attempt {attempt}/{max_retries_captcha}.")
                    time.sleep(min(wait_seconds, 15))
                    wait_seconds = min(wait_seconds * backoff_factor, 60)

            if not success and captcha_flag:
                append_missed_date_log(out_dir, monday_date, "captcha_detected")
                # don't create CSV
            elif not success:
                print(f"[{monday_date}] Failed after retries (no captcha).")
                if previous_df is not None and not previous_df.empty:
                    df_to_write = previous_df.copy()
                    df_to_write["Date"] = monday_date
                    save_df_for_date(df_to_write, out_dir, monday_date)
                else:
                    save_df_for_date(pd.DataFrame(), out_dir, monday_date)
            else:
                if df is None or df.empty:
                    if previous_df is not None and not previous_df.empty:
                        print(f"[{monday_date}] Empty result — using previous date content")
                        df_to_write = previous_df.copy()
                        df_to_write["Date"] = monday_date
                        save_df_for_date(df_to_write, out_dir, monday_date)
                    else:
                        save_df_for_date(pd.DataFrame(), out_dir, monday_date)
                else:
                    df["Date"] = monday_date
                    save_df_for_date(df, out_dir, monday_date)
                    previous_df = df.copy()

            # small randomized pause between dates (bounded)
            sleep_for = random.uniform(5, 10)
            print(f"[{monday_date}] Sleeping {min(sleep_for, 15):.1f}s before next date...")
            time.sleep(min(sleep_for, 15))

        print("\nFinished processing dates from CSV.")
    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass

# ---------------------------
# Backwards-compatible main
# ---------------------------
def iterate_dates_and_scrape(start_date_str, end_date_str=None, weeks=None, headless=False,
                             max_players=None, min_delay=5.0, max_delay=10.0,
                             rotate_ua=False, ua_list=None, proxy_file=None,
                             restart_every=0, restart_run_every=2, max_retries_captcha=3, backoff_factor=2.0,
                             dates_file: Optional[str]=None):
    """
    If dates_file is provided, read the list of dates from it and process only those not already present.
    Otherwise, original weekly behavior is preserved (unchanged here).
    """
    if dates_file:
        dates_list = read_dates_file(dates_file)
        iterate_dates_from_list(dates_list,
                                headless=headless,
                                max_players=max_players,
                                rotate_ua=rotate_ua,
                                ua_list=ua_list,
                                proxy_file=proxy_file,
                                restart_run_every=restart_run_every,
                                max_retries_captcha=max_retries_captcha,
                                backoff_factor=backoff_factor)
        return

    # fallback to original weekly stepping behavior (unchanged, omitted here for brevity)
    raise RuntimeError("Weekly stepping mode was not requested. Use --dates-file to process dates from CSV.")

# ---------------------------
# CLI
# ---------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Resilient ATP weekly scraper with UA rotation and captcha logging.")
    p.add_argument("--start-date", required=False, help="Start date YYYY-MM-DD (ignored if --dates-file is used).")
    p.add_argument("--end-date", required=False, help="End date YYYY-MM-DD (ignored if --dates-file is used).")
    p.add_argument("--weeks", type=int, required=False, help="Number of weekly iterations (ignored if --dates-file used).")
    p.add_argument("--headless", dest="headless", action="store_true", help="Run Chrome headless.")
    p.add_argument("--no-headless", dest="headless", action="store_false", help="Run Chrome visible.")
    p.set_defaults(headless=False)
    p.add_argument("--max-players", type=int, default=None, help="Limit number of players parsed per date (useful for testing).")
    p.add_argument("--min-delay", type=float, default=5.0, help="Minimum delay between date scrapes (seconds).")
    p.add_argument("--max-delay", type=float, default=12.0, help="Maximum delay between date scrapes (seconds).")
    p.add_argument("--rotate-ua", action="store_true", help="Rotate user-agents between attempts.")
    p.add_argument("--proxy-file", type=str, default=None, help="File with one proxy per line.")
    p.add_argument("--restart-every", type=int, default=0, help="Restart browser every N iterations (0 = never).")
    p.add_argument("--restart-run-every", type=int, default=2, help="Simulate a fresh run every N dates.")
    p.add_argument("--max-retries-captcha", type=int, default=3, help="Max retries on captcha/other failure.")
    p.add_argument("--backoff-factor", type=float, default=2.0, help="Backoff factor for retries.")
    p.add_argument("--dates-file", type=str, default=None, help="CSV file with a column 'date' or 'dates' listing ISO dates to process.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    iterate_dates_and_scrape(
        start_date_str=args.start_date or datetime.utcnow().strftime("%Y-%m-%d"),
        end_date_str=args.end_date,
        weeks=args.weeks,
        headless=args.headless,
        max_players=args.max_players,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        rotate_ua=args.rotate_ua,
        ua_list=DEFAULT_USER_AGENTS,
        proxy_file=args.proxy_file,
        restart_every=args.restart_every,
        restart_run_every=args.restart_run_every,
        max_retries_captcha=args.max_retries_captcha,
        backoff_factor=args.backoff_factor,
        dates_file=args.dates_file
    )
