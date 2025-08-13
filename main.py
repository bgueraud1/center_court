import requests
import pandas as pd
import datetime
import os
import time

from config import players_path, output_path, rankings_dir, min_first_date, overwrite_wiki, overwrite_ioc, begin_index_ioc, begin_index_wiki, end_index_wiki, end_index_ioc
from scrape_player_ranking_wta import scrape_data
from rankings_to_player_base import load_players, load_rankings, find_new_ids, summarize_new_players, save_players, update_last_appearances
from add_ioc_to_player import enrich_country_codes
from scrape_wiki_wta import enrich_csv, make_retry_session

# This pipeline is meant to 
# 1- scrape new ranking data from WTA website
# 2- Incorporate new players in the players database and enrich their data with ranking and IOC informations
# 3- Enrich data of any player (new or old) with data from Wikipedia

# As of Aug 7 2025, the whole pipeline without any index selection or overwriting runs in 30 minutes for about 1400 players


### Call the scraping code for the WTA rankings of the desired weeks

# Initialize variables

os.makedirs(rankings_dir, exist_ok=True)

# Generate all Mondays in 2023
start_date = datetime.date(2025, 8, 11)  # First Monday in 2023
end_date = datetime.date(2025, 8, 11)  # Last Monday in 2023
specific_dates = [start_date + datetime.timedelta(weeks=i) for i in range((end_date - start_date).days // 7 + 1)]

# Run the scraper
scrape_data(specific_dates, rankings_dir)

### Transfer the new players to the player_data_wta base 
### + Enrich the new players' data with best, first, last rankings


# --- Load data ---
players_df = load_players(players_path)
ranks_df = load_rankings(rankings_dir)

# --- Find new players ---
new_ids = find_new_ids(players_df, ranks_df)

if new_ids:
    new_players_df = summarize_new_players(ranks_df, new_ids, list(players_df.columns))
    players_df = pd.concat([players_df, new_players_df], ignore_index=True)
    print(f"Added {len(new_ids)} new players.")
else:
    print("No new players to add.")

# --- Update existing players' last appearance ---
players_df = update_last_appearances(players_df, ranks_df)

# --- Save updated master file ---
save_players(players_df, output_path)
print(f"Player data refreshed and written to → {output_path}")



### Scrape Wikipedia to get the info of players that are either new to the base or active with unknown infos

if __name__ == '__main__':
    session = make_retry_session(
        total_retries=3,
        backoff_factor=0.5,
        status_forcelist=[500,502,503,504]
    )
    enrich_csv(
        session=session,
        input_csv=players_path,
        output_csv=output_path,
        summary_csv='player_base_and_maps/overwrite_changes.csv',
        rankings_dir=rankings_dir,
        start_index=begin_index_wiki,
        end_index=end_index_wiki,
        overwrite=overwrite_wiki,
        min_first_date=min_first_date
    )




### Scrape WTA player page to get the IOC
### WARNING : RUS and BLR are currently erased from WTA databases !!!



with requests.Session() as sess:
    enrich_country_codes(
        session=sess,
        input_csv=players_path,
        output_csv=output_path,
        start_index=begin_index_ioc,
        end_index=end_index_ioc,
        overwrite=overwrite_ioc
    )


print("Please, run in the shell: \nplayer_base_and_maps\revert_overwrites.py player_base_and_maps\overwrite_changes.csv player_base_and_maps\player_data_wta.csv")