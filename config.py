# Configuration file for the player_data_wta enrichment Pipeline
# Created Aug 7 2025
# Ran to end


players_path: str = 'player_base_and_maps\player_data_wta.csv'
output_path: str = 'player_base_and_maps\player_data_wta.csv'
rankings_dir: str = 'player_base_and_maps\wta_rankings'


min_first_date = '2015-01-01' # date under which player's data won't be overwritten if overwriting activated



overwrite_wiki = False
overwrite_ioc = False

begin_index_wiki = 0  # index under which player won't be scraped for wiki data
end_index_wiki = None # index above which player won't be scraped for wiki data

begin_index_ioc = 0  # index under which player won't be scraped for ioc
end_index_ioc = None # index above which player won't be scraped for ioc




