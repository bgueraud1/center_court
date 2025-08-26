# main_atp.py
"""
Main runner ATP (mise à jour) :
 - calcule la/les dates à scraper (comme dans ton main.py WTA)
 - lance scrape_atp_with_date_and_limit.scrape_data pour obtenir data_YYYY_MM_DD.csv
 - continue avec enrichments IOC optionnels et génération de maps (main_maps_atp)
"""
import os
from pathlib import Path
import datetime
import argparse

# Config: adapt to your repo layout
from config import players_path, output_path, rankings_dir  # si tu as un config.py générique
from config_maps_atp import CACHE_FILE_MIGRATION, IOC_TO_ISO3, GEOJSON_URL, CACHE_FILE
from config_maps_atp import OUTPUT_HTML_BIRTHPLACE, OUTPUT_HTML_FROM, OUTPUT_HTML_TO, OUTPUT_HTML_PERCENTAGE

# modules ATP que nous avons préparés
from scrape_atp_with_date_and_limit import scrape_data
from add_ioc_to_player_atp import enrich_country_codes_atp
from main_maps_atp import __name__ as maps_runner_module  # import defers actual map run
import time
import requests

def compute_scrape_dates():
    # same logic as your previous main.py
    today = datetime.date.today()
    scrape_date_env = os.getenv("SCRAPE_DATE")
    if scrape_date_env:
        try:
            start_date = datetime.datetime.strptime(scrape_date_env, "%Y-%m-%d").date()
        except Exception:
            raise SystemExit("SCRAPE_DATE mal formattée, utiliser YYYY-MM-DD")
    else:
        # monday of current week
        days_since_monday = today.weekday()  # Monday=0
        start_date = today - datetime.timedelta(days=days_since_monday)
    end_date = start_date
    specific_dates = [start_date + datetime.timedelta(weeks=i) for i in range((end_date - start_date).days // 7 + 1)]
    return specific_dates

def ensure_dirs(rankings_dir):
    Path(rankings_dir).mkdir(parents=True, exist_ok=True)
    print("Ensured rankings dir:", rankings_dir)

def run_ranking_scrape_if_requested(rankings_dir, headless=True, max_players=None):
    # Allow invoking via env var SCRAPE_RANKINGS=1 or CLI flag
    should = os.environ.get("SCRAPE_RANKINGS", "1").strip() in ("1","true","yes")
    if not should:
        print("SCRAPE_RANKINGS disabled via env var; skipping ranking scrape.")
        return []
    dates = compute_scrape_dates()
    print("Will scrape rankings for:", [d.strftime("%Y-%m-%d") for d in dates])
    produced = scrape_data(dates, rankings_dir, headless=headless, max_players=max_players)
    print("Ranking files produced:", produced)
    return produced

def run_ioc_enrich_if_requested(players_csv, start_index=0, end_index=None, overwrite=False):
    should = os.environ.get("ENRICH_IOC", "0").strip() in ("1","true","yes")
    if not should:
        print("IOC enrichment disabled via env var; skipping.")
        return None
    print("Running IOC enrichment (ATP) — be polite with remote servers.")
    session = requests.Session()
    tmp_out = Path(players_csv).parent / "players_ioc_enriched_atp.csv"
    enrich_country_codes_atp(session=session, input_csv=str(players_csv), output_csv=str(tmp_out),
                             start_index=start_index, end_index=end_index, overwrite=overwrite)
    return tmp_out

def run_maps():
    # import the module that contains orchestration. we expect main_maps_atp to run on import
    import main_maps_atp
    # if main_maps_atp defines a function to run maps, prefer it
    if hasattr(main_maps_atp, "build_all_maps"):
        main_maps_atp.build_all_maps()
    else:
        print("main_maps_atp imported — it should run maps at module-level if designed so.")

def main():
    parser = argparse.ArgumentParser(description="ATP pipeline runner")
    parser.add_argument("--no-scrape", dest="no_scrape", action="store_true", help="Skip ranking scrape")
    parser.add_argument("--headless", dest="headless", action="store_true", help="Run headless browser for scraping")
    parser.add_argument("--max-players", dest="max_players", type=int, default=None, help="Limit players per date (for faster runs)")
    parser.add_argument("--enrich-ioc", dest="enrich_ioc", action="store_true", help="Run IOC enrichment before maps")
    args = parser.parse_args()

    ensure_dirs(rankings_dir)

    if not args.no_scrape:
        produced = run_ranking_scrape_if_requested(rankings_dir, headless=args.headless, max_players=args.max_players)
    else:
        print("Skipping ranking scrape (--no-scrape).")

    if args.enrich_ioc:
        run_ioc_enrich_if_requested(players_path)

    # Run maps generation (this will use the produced ranking files if needed)
    run_maps()
    print("All done.")

if __name__ == "__main__":
    main()
