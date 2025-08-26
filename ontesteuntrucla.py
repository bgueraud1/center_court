# scrape_atp_with_date_and_limit.py
import re
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.atptour.com/en/rankings/singles?rankRange=0-5000"

def make_driver(headless=False):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")
    else:
        opts.add_argument("--start-maximized")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    # réduire chance de blocage trop long et éviter read-timeout : set page load timeout raisonnable
    driver.set_page_load_timeout(60)  # secondes
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

        # si la value est déjà YYYY-MM-DD
        if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
            return val

        # si le texte est du type 2025.08.25 (points) -> remplacer par tirets
        m = re.search(r"(\d{4})[.\-](\d{2})[.\-](\d{2})", text)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # chercher une option ayant une value au format YYYY-MM-DD
        for o in sel_elem.find_elements(By.TAG_NAME, "option"):
            v = (o.get_attribute("value") or "").strip()
            if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                return v

        # tenter remplacer '.' par '-' dans value/text
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

    # fallback: trouver 'points' quelque part
    if points_idx is None:
        for i,h in enumerate(headers_lower):
            if "points" in h:
                points_idx = i
                break
    # autre fallback observé : index 3
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
    # heuristique textuelle si pas de table
    elems = driver.find_elements(By.CSS_SELECTOR, "div, li, p")
    candidates = []
    for e in elems:
        t = e.text.strip()
        if not t:
            continue
        # pattern : rank name ... points (relativement permissif)
        m = re.search(r"^\s*(\d{1,4})\s+([A-Za-zÀ-ÖØ-öø-ÿ\.\'\- ]{2,80}?)\s+([0-9\.,\s]{2,20})", t)
        if m:
            rank = m.group(1)
            name = m.group(2).strip()
            pts = clean_points(m.group(3))
            candidates.append({"Rank": rank, "Player": name, "Official Points": pts, "Date": date_str})
            if max_players is not None and len(candidates) >= max_players:
                break
    return candidates

def scrape(headless=False, max_players=None, retry_on_timeout=True):
    driver = make_driver(headless=headless)
    try:
        # Try/Retry driver.get to reduce chance of read-timeout
        tried = 0
        while True:
            try:
                tried += 1
                driver.get(URL)
                break
            except Exception as e:
                print(f"Erreur lors du chargement de la page (tentative {tried}): {e}")
                if not retry_on_timeout or tried >= 2:
                    raise
                print("Retrying after 2s...")
                time.sleep(2)

        # laisser un petit temps pour que le JS insère les éléments
        time.sleep(1.0)
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
        except TimeoutException:
            # si pas de table, on continue quand même (fallback)
            pass

        date_str = get_selected_date(driver)
        if not date_str:
            print("Date non détectée dans le sélecteur; la colonne Date sera vide.")
        else:
            print(f"Date détectée : {date_str}")

        rank_idx, player_idx, points_idx, headers = find_header_indices(driver)
        if rank_idx is not None and player_idx is not None and points_idx is not None:
            players = parse_table_rows(driver, rank_idx, player_idx, points_idx, date_str, max_players=max_players)
        else:
            print("Entêtes de table non détectés correctement, utilisation du fallback textuel.")
            players = fallback_parse_by_text(driver, date_str, max_players=max_players)

        # dédup et nettoyage minimal
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
        # nom de fichier informatif
        date_for_name = date_str if date_str else "no-date"
        limit_for_name = f"{max_players}" if max_players is not None else "all"
        csv_path = f"atp_rankings_{date_for_name}_{limit_for_name}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"Extrait {len(df)} lignes. Sauvegardé -> {csv_path}")
        if not df.empty:
            print(df.head(20).to_string(index=False))
        return df

    finally:
        driver.quit()

if __name__ == "__main__":
    # Exemple : headless=False pour debugger ; max_players=10 pour aller vite
    scrape(headless=False, max_players=100)
