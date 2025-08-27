#!/usr/bin/env python3
# scrape_atp_date_range.py
"""
Scrape ATP ranking pages for weekly dates spaced by 7 days.
Creates folder 'atp_rankings' and files named data_YYYY_MM_DD.csv
Columns in CSV: full_name,ranking,points,date

Usage examples:
    python scrape_atp_date_range.py --start-date 1973-08-23 --weeks 10 --headless
    python scrape_atp_date_range.py --start-date 1973-08-23 --end-date 1974-01-01
"""

import re
import time
import os
import argparse
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

URL_TEMPLATE = "https://www.atptour.com/en/rankings/singles?rankRange=0-5000&dateWeek={date}"

def make_driver(headless=False):
    opts = webdriver.ChromeOptions()
    if headless:
        # headless new is supported in recent Chrome
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    else:
        opts.add_argument("--start-maximized")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    # optional: reduce detection
    opts.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver

def clean_points(text):
    if not text:
        return ""
    s = re.sub(r"[^\d\-]", "", text)
    return s

def get_selected_date(driver):
    """Retourne la date sélectionnée en YYYY-MM-DD (ou '' si introuvable)."""
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

def find_header_indices(driver):
    """Retourne (rank_idx, player_idx, points_idx, headers)"""
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

            # Player
            name = ""
            try:
                name_elem = cells[player_idx].find_element(By.TAG_NAME, "a")
                name = name_elem.text.strip()
            except Exception:
                name = cells[player_idx].text.strip()
            name = name.replace("\n", " ").strip()

            # Points
            points_text = cells[points_idx].text.strip() if points_idx is not None else ""
            points_text = clean_points(points_text)

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

def scrape_for_date(driver, date_str, max_players=None, retry_on_timeout=True):
    """Scrape a single date URL and return a DataFrame with columns Rank, Player, Official Points, Date"""
    url = URL_TEMPLATE.format(date=date_str)
    tried = 0
    last_exception = None
    while True:
        try:
            tried += 1
            driver.get(url)
            break
        except Exception as e:
            last_exception = e
            print(f"[{date_str}] Erreur lors du chargement de la page (tentative {tried}): {e}")
            if not retry_on_timeout or tried >= 2:
                raise
            print("[{0}] Retrying after 2s...".format(date_str))
            time.sleep(2)

    # laisser un petit temps pour que le JS insère les éléments
    time.sleep(1.0)
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
    except TimeoutException:
        # continue anyway, fallback may find something
        pass

    # Attempt to detect date selected on page (not strictly necessary since we control date)
    page_date = get_selected_date(driver)
    if page_date:
        used_date = page_date
    else:
        used_date = date_str

    rank_idx, player_idx, points_idx, headers = find_header_indices(driver)
    if rank_idx is not None and player_idx is not None and points_idx is not None:
        players = parse_table_rows(driver, rank_idx, player_idx, points_idx, used_date, max_players=max_players)
    else:
        # fallback
        players = fallback_parse_by_text(driver, used_date, max_players=max_players)

    # dedup and minimal clean
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

    df = pd.DataFrame(cleaned)
    return df

def save_df_for_date(df_to_save, out_dir: Path, date_obj: datetime.date):
    """Rename columns and save CSV as data_YYYY_MM_DD.csv in out_dir"""
    fn_date = date_obj.strftime("%Y_%m_%d")
    csv_path = out_dir / f"data_{fn_date}.csv"

    # rename columns to: full_name,ranking,points,date
    if df_to_save is None or df_to_save.empty:
        # create empty frame with correct columns
        out_df = pd.DataFrame(columns=["full_name","ranking","points","date"])
    else:
        # handle source columns possibly present
        mapping = {"Player":"full_name", "Rank":"ranking", "Official Points":"points", "Date":"date"}
        out_df = df_to_save.rename(columns=mapping)
        # if there are columns with lowercase variants, try to adapt
        if "full_name" not in out_df.columns and "Player" in out_df.columns:
            out_df = out_df.rename(columns={"Player":"full_name"})
        # ensure columns exist and order them
        for col in ["full_name","ranking","points","date"]:
            if col not in out_df.columns:
                out_df[col] = ""
        out_df = out_df[["full_name","ranking","points","date"]]

    out_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"Saved {len(out_df)} rows -> {csv_path}")
    return csv_path

def iterate_dates_and_scrape(start_date_str, end_date_str=None, weeks=None, headless=False, max_players=None):
    """
    Iterate dates from start_date by +7 days until end_date (inclusive) or for `weeks` iterations.
    Creates atp_rankings folder and writes data_YYYY_MM_DD.csv for each date.
    """
    out_dir = Path("atp_rankings")
    out_dir.mkdir(parents=True, exist_ok=True)

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    if end_date_str:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    else:
        end_date = None

    if weeks is not None:
        if weeks <= 0:
            raise ValueError("weeks must be >= 1")
        # compute end date from weeks
        end_date = start_date + timedelta(days=7*(weeks-1))
    elif end_date is None:
        # default: just 1 week (only the start date) to avoid accidental massive scrapes
        end_date = start_date

    # ensure start <= end
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")

    total_weeks = ((end_date - start_date).days // 7) + 1
    print(f"Scraping from {start_date.isoformat()} to {end_date.isoformat()} ({total_weeks} weeks).")

    driver = None
    previous_df = None
    try:
        driver = make_driver(headless=headless)
        cur_date = start_date
        i = 0
        while cur_date <= end_date:
            i += 1
            date_str = cur_date.strftime("%Y-%m-%d")
            print(f"\n[{i}/{total_weeks}] Processing date {date_str} ...")
            try:
                df = scrape_for_date(driver, date_str, max_players=max_players)
                # if df empty -> use previous_df if exists
                if df is None or df.empty:
                    if previous_df is not None and not previous_df.empty:
                        print(f"[{date_str}] Aucun joueur trouvé, utilisation du contenu de la date précédente.")
                        # save previous_df (note: ensure date column in copy updated to this date)
                        df_to_write = previous_df.copy()
                        # update date column values to current date string
                        if "Date" in df_to_write.columns:
                            df_to_write["Date"] = date_str
                        elif "date" in df_to_write.columns:
                            df_to_write["date"] = date_str
                        else:
                            df_to_write["Date"] = date_str
                        save_df_for_date(df_to_write, out_dir, cur_date)
                    else:
                        print(f"[{date_str}] Aucun joueur trouvé et pas de date précédente. Création d'un CSV vide (entêtes).")
                        save_df_for_date(pd.DataFrame(), out_dir, cur_date)
                else:
                    # ensure Date column is the date we expect
                    if "Date" in df.columns:
                        df["Date"] = date_str
                    elif "date" in df.columns:
                        df["date"] = date_str
                    save_df_for_date(df, out_dir, cur_date)
                    previous_df = df.copy()
            except WebDriverException as e:
                print(f"[{date_str}] Erreur WebDriver: {e}")
                # fallback to previous data
                if previous_df is not None and not previous_df.empty:
                    df_to_write = previous_df.copy()
                    if "Date" in df_to_write.columns:
                        df_to_write["Date"] = date_str
                    elif "date" in df_to_write.columns:
                        df_to_write["date"] = date_str
                    else:
                        df_to_write["Date"] = date_str
                    save_df_for_date(df_to_write, out_dir, cur_date)
                else:
                    save_df_for_date(pd.DataFrame(), out_dir, cur_date)
            except Exception as e:
                print(f"[{date_str}] Erreur inattendue: {e}")
                # same fallback behavior
                if previous_df is not None and not previous_df.empty:
                    df_to_write = previous_df.copy()
                    if "Date" in df_to_write.columns:
                        df_to_write["Date"] = date_str
                    elif "date" in df_to_write.columns:
                        df_to_write["date"] = date_str
                    else:
                        df_to_write["Date"] = date_str
                    save_df_for_date(df_to_write, out_dir, cur_date)
                else:
                    save_df_for_date(pd.DataFrame(), out_dir, cur_date)

            cur_date += timedelta(days=7)

        print("\nFinished scraping.")
    finally:
        if driver:
            driver.quit()

def parse_args():
    p = argparse.ArgumentParser(description="Scrape ATP rankings weekly by dateWeek parameter.")
    p.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD (ex: 1973-08-23)")
    p.add_argument("--end-date", required=False, help="End date YYYY-MM-DD (inclusive). Alternatively use --weeks.")
    p.add_argument("--weeks", type=int, required=False, help="Number of weekly iterations (start date + 7 days * (weeks-1)).")
    p.add_argument("--headless", action="store_true", help="Run Chrome headless.")
    p.add_argument("--max-players", type=int, default=None, help="Limit number of players parsed per date (useful for testing).")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    iterate_dates_and_scrape(
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        weeks=args.weeks,
        headless=args.headless,
        max_players=args.max_players
    )
