# scrape_atp_with_date_and_limit.py
# Selenium-based ATP rankings scraper that writes CSV files compatible with the
# pipeline (columns: full_name, player_id, ranking, points, date).
#
# Usage:
#   from scrape_atp_with_date_and_limit import scrape_data
#   scrape_data([date1, date2], save_dir, headless=True, max_players=None)

import re
import time
import os
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.atptour.com/en/rankings/singles?rankRange=0-5000"

def make_driver(headless=False):
    opts = webdriver.ChromeOptions()
    if headless:
        # Chrome 109+ new headless mode
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    else:
        opts.add_argument("--start-maximized")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    # optional: disable automation flags that sometimes trigger blocks
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver

def clean_points(text):
    if not text:
        return ""
    s = re.sub(r"[^\d\-]", "", text)
    return s

def extract_player_id_from_href(href: str):
    """
    Try to extract atp player id from a typical href like:
    /en/players/jannik-sinner/S0AG/overview
    Returns uppercase id or None.
    """
    if not href:
        return None
    m = re.search(r"/players/[^/]+/([^/]+)/", href, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # sometimes href ends with id (no trailing slash)
    m2 = re.search(r"/players/[^/]+/([^/]+)$", href, flags=re.IGNORECASE)
    if m2:
        return m2.group(1).upper()
    return None

def get_selected_date(driver):
    """Return selected date as YYYY-MM-DD or ''."""
    try:
        sel_elem = driver.find_element(By.ID, "dateWeek-filter")
        select = Select(sel_elem)
        opt = select.first_selected_option
        val = (opt.get_attribute("value") or "").strip()
        text = (opt.text or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
            return val
        m = re.search(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", text)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        for o in sel_elem.find_elements(By.TAG_NAME, "option"):
            v = (o.get_attribute("value") or "").strip()
            if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                return v
        txt_candidate = val.replace('.', '-')
        if re.match(r"^\d{4}-\d{2}-\d{2}$", txt_candidate):
            return txt_candidate
        txt_candidate = text.replace('.', '-')
        if re.match(r"^\d{4}-\d{2}-\d{2}$", txt_candidate):
            return txt_candidate
        return ""
    except Exception:
        return ""

def set_date_select(driver, date_str: str, timeout: float = 8.0):
    """
    Try to set the ranking date selector to date_str (YYYY-MM-DD).
    If the exact value isn't present, try variations: 'YYYY.MM.DD', human text.
    Returns True if selection occurred, False otherwise.
    """
    try:
        sel_elem = driver.find_element(By.ID, "dateWeek-filter")
        select = Select(sel_elem)
        # try by value
        try:
            select.select_by_value(date_str)
            return True
        except Exception:
            pass
        # try by visible text that contains the date components
        # common ATP formats: "Aug 25 2025", "2025.08.25", "25 Aug 2025"
        Y, M, D = date_str.split("-")
        candidates = [
            date_str,
            f"{Y}.{M}.{D}",
            f"{Y}.{int(M)}.{int(D)}",
            f"{int(D)} {datetime.strptime(M, '%m').strftime('%b')} {Y}",  # e.g. "25 Aug 2025"
            f"{datetime.strptime(date_str, '%Y-%m-%d').strftime('%b %d %Y')}",  # "Aug 25 2025"
            f"{datetime.strptime(date_str, '%Y-%m-%d').strftime('%B %d %Y')}"  # "August 25 2025"
        ]
        options = sel_elem.find_elements(By.TAG_NAME, "option")
        option_texts = [ (o.get_attribute("value") or "", o.text or "") for o in options ]
        # try to match candidate string in either value or text
        for cand in candidates:
            for val, txt in option_texts:
                if cand == (val or "").strip() or cand.lower() in (txt or "").lower():
                    try:
                        select.select_by_visible_text(txt)
                        return True
                    except Exception:
                        try:
                            select.select_by_value(val)
                            return True
                        except Exception:
                            pass
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
            # Player name & optional href/id
            name = ""
            pid = ""
            try:
                name_elem = cells[player_idx].find_element(By.TAG_NAME, "a")
                name = name_elem.text.strip()
                href = name_elem.get_attribute("href") or ""
                pid = extract_player_id_from_href(href)
            except Exception:
                name = cells[player_idx].text.strip()
                pid = ""

            name = name.replace("\n", " ").strip()

            points_text = cells[points_idx].text.strip() if points_idx is not None else ""
            points_text = clean_points(points_text)

            if not name or name.lower() in ("player", "rank"):
                continue

            # convert rank to int where possible
            ranking = None
            try:
                ranking = int(re.sub(r"[^\d]", "", rank_text)) if rank_text else None
            except Exception:
                ranking = None

            players.append({
                "full_name": name,
                "player_id": pid or "",
                "ranking": ranking if ranking is not None else "",
                "points": int(points_text) if points_text and re.match(r"^\d+$", points_text) else "",
                "date": date_str
            })
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
            candidates.append({
                "full_name": name,
                "player_id": "",
                "ranking": int(rank),
                "points": int(pts) if pts.isdigit() else "",
                "date": date_str
            })
            if max_players is not None and len(candidates) >= max_players:
                break
    return candidates

def save_csv(data, date_str, save_dir):
    # save file named like data_YYYY_MM_DD.csv
    try:
        os.makedirs(save_dir, exist_ok=True)
        date_for_name = date_str.replace("-", "_")
        file_path = os.path.join(save_dir, f"data_{date_for_name}.csv")
        pd.DataFrame(data).to_csv(file_path, index=False)
        return file_path
    except Exception as e:
        print("Erreur en sauvegardant CSV:", e)
        return None

def scrape_data(specific_dates, save_dir, headless=True, max_players=None, retry_on_timeout=True):
    """
    specific_dates: iterable of date objects or strings 'YYYY-MM-DD'
    save_dir: directory where data_YYYY_MM_DD.csv will be written
    headless: run headless Chrome if True
    max_players: optional integer to limit number of players per date
    """
    failed_dates = []
    for d in specific_dates:
        if isinstance(d, (str,)):
            date_obj = datetime.strptime(d, "%Y-%m-%d")
        else:
            date_obj = d
        date_str = date_obj.strftime("%Y-%m-%d")
        print(f"--- Scraping ATP rankings for date {date_str} ---")

        driver = make_driver(headless=headless)
        try:
            tried = 0
            success = False
            players_for_date = []

            while not success and tried < 3:
                tried += 1
                try:
                    driver.get(URL)
                    # wait a bit for JS to populate the date selector and table
                    time.sleep(1.0)
                    # set selector to requested date if possible
                    ok = set_date_select(driver, date_str)
                    if ok:
                        # wait for table update
                        try:
                            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
                        except TimeoutException:
                            # proceed anyway to attempt parsing
                            pass
                    else:
                        # selection failed — still attempt parsing current page (maybe default is already the desired date)
                        pass

                    detected_date = get_selected_date(driver)
                    if detected_date and detected_date != date_str:
                        print(f"Warning: after selection detected date {detected_date} (requested {date_str})")

                    rank_idx, player_idx, points_idx, headers = find_header_indices(driver)
                    if rank_idx is not None and player_idx is not None and points_idx is not None:
                        players_for_date = parse_table_rows(driver, rank_idx, player_idx, points_idx, date_str, max_players=max_players)
                    else:
                        print("Table headers not clearly detected — fallback parsing.")
                        players_for_date = fallback_parse_by_text(driver, date_str, max_players=max_players)

                    # basic quality check: at least 50 rows (?) or at least some rows
                    if players_for_date and len(players_for_date) >= min(50, max_players or 50):
                        success = True
                    else:
                        # accept fewer if max_players small
                        if max_players and len(players_for_date) >= min(10, max_players):
                            success = True
                        else:
                            # retry once
                            print(f"Scrape attempt {tried} returned {len(players_for_date)} rows — retrying...")
                            time.sleep(2)
                except Exception as e:
                    print(f"Erreur lors du chargement/parsing (tentative {tried}): {e}")
                    time.sleep(2)

            if not players_for_date:
                print(f"Insufficient data for {date_str}, marking as failed.")
                failed_dates.append(date_str)
            else:
                # save in pipeline-friendly format similar to WTA API output
                # columns: full_name, player_id, ranking, points, date
                saved = save_csv(players_for_date, date_str, save_dir)
                if saved:
                    print(f"Saved {len(players_for_date)} rows -> {saved}")
                else:
                    print("Erreur: fichier non sauvegardé.")
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    if failed_dates:
        failed_path = os.path.join(save_dir, "failed_dates.csv")
        pd.DataFrame({"failed_dates": failed_dates}).to_csv(failed_path, index=False)
        print(f"Failed scraping for dates: {failed_dates}. Written -> {failed_path}")

    # return list of produced files (optional)
    produced = sorted([os.path.join(save_dir, f) for f in os.listdir(save_dir) if f.startswith("data_") and f.endswith(".csv")])
    return produced

if __name__ == "__main__":
    # quick debug run (change date as needed). headless False helps debug visually.
    from datetime import date
    today = date.today()
    # default behavior: scrape latest available Monday (as in main)
    scrape_data([today.strftime("%Y-%m-%d")], save_dir="rankings_atp", headless=False, max_players=500)
