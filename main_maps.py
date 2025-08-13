from map_birth_place import load_and_clean, geocode_with_cache, normalize_dates_and_heights, build_and_save_map

from config_maps import INPUT_CSV, CACHE_FILE, OUTPUT_HTML_BIRTHPLACE, OUTPUT_HTML_FROM, OUTPUT_HTML_PERCENTAGE, OUTPUT_HTML_TO, IOC_TO_ISO3, geolocator, CACHE_FILE_MIGRATION, GEOJSON_URL, OUTPUT_HTML_FALSE
from migration_map_from import (
    load_and_normalize, load_cache, build_points_and_migrations,
    build_and_save_map_migration 
)

from migration_map_to import (
    load_and_normalize_to, load_cache_to, build_points_and_migrations_to,
    build_and_save_map_migration_to
)


from map_percentage import prepare_players, build_and_save_presence_map, load_and_normalize_percentage
from migration_map_from import load_and_normalize 

import pandas as pd

# Map for birthplaces

# 1) Load & clean
df = load_and_clean(INPUT_CSV)
# 2) Geocode (with caching!)
df = geocode_with_cache(df, CACHE_FILE)
# 3) Normalize dates & heights
all_pts = normalize_dates_and_heights(df)
# 4) Build map and save
build_and_save_map(all_pts, OUTPUT_HTML_BIRTHPLACE)



# Map for migrations FROM



cache = load_cache(CACHE_FILE_MIGRATION)
df = load_and_normalize(IOC_TO_ISO3, INPUT_CSV)
all_pts, migrations = build_points_and_migrations(CACHE_FILE_MIGRATION, geolocator, df, cache)
print(f"✅ {len(all_pts)} players loaded.")
print(f"🚀 {len(migrations)} migration records built.")
build_and_save_map_migration(all_pts, migrations, OUTPUT_HTML_FROM)








# Map for migrations TO


##### TOCHANGE
cache = load_cache_to(CACHE_FILE_MIGRATION)
df = load_and_normalize_to(IOC_TO_ISO3, INPUT_CSV)
all_pts, migrations = build_points_and_migrations_to(CACHE_FILE_MIGRATION, geolocator, df, cache)
print(f"✅ {len(all_pts)} players loaded.")
print(f"🚀 {len(migrations)} migration records built.")
build_and_save_map_migration_to(all_pts, migrations, OUTPUT_HTML_TO)




# Map for percentages 



df = load_and_normalize_percentage(IOC_TO_ISO3, INPUT_CSV)   # df has represented_country ISO3
players = prepare_players(df)

build_and_save_presence_map(players, OUTPUT_HTML_PERCENTAGE, GEOJSON_URL)




# False map birthplace



from map_birth_place import build_and_save_map  # your existing map builder
from false_birthplace_map import apply_cache_coords, create_false_all_pts_from_df

# 1) load whole CSV (do NOT call load_and_clean because that drops missing birthplace rows)
df_all = pd.read_csv(INPUT_CSV)

# 2) apply your cache (no network)
df_with_coords = apply_cache_coords(df_all, CACHE_FILE)

# 3) create the false all_pts (deterministic with seed)
all_pts_false, stats = create_false_all_pts_from_df(df_with_coords, seed=42)

print("False-map stats:", stats)

# 4) render map with your existing map builder
build_and_save_map(all_pts_false, OUTPUT_HTML_FALSE)
