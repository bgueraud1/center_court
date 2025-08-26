# main_maps_atp.py
"""
Orchestre la génération des maps ATP:
 - birthplace map
 - migration map FROM
 - migration map TO
 - percentage presence map

Assume:
 - map_birthplace_atp.py
 - migration_map_from_atp.py
 - migration_map_to_atp.py
 - map_percentage_atp.py
 - scripts/geocode_utils_atp.py
are présents et importables.
"""
import os
from pathlib import Path
import pandas as pd

from config_maps_atp import (
    INPUT_CSV, CACHE_FILE, OUTPUT_HTML_BIRTHPLACE,
    OUTPUT_HTML_FROM, OUTPUT_HTML_TO, OUTPUT_HTML_PERCENTAGE,
    IOC_TO_ISO3, geolocator, CACHE_FILE_MIGRATION, GEOJSON_URL
)

# ATP-adapted map modules (fichiers fournis précédemment/ci-dessus)
from map_birthplace_atp import (
    load_and_clean_atp, geocode_with_cache_atp,
    normalize_dates_and_heights_atp, build_and_save_map_atp
)

from migration_map_from_atp import (
    load_and_normalize_from_atp, build_points_and_migrations_from_atp,
    build_and_save_map_migration_from_atp
)

from migration_map_to_atp import (
    load_and_normalize_to, build_points_and_migrations_to,
    build_and_save_map_migration_to
)

from map_percentage_atp import (
    load_and_normalize_percentage_atp, prepare_players_atp,
    build_and_save_presence_map_atp
)

from scripts.geocode_utils_atp import load_cache, bulk_geocode

# ---------- 1) Birthplace map ----------
print("→ BUILDING birthplace map (ATP)")
df_birth = load_and_clean_atp(INPUT_CSV)
# geocode with cache: this will call bulk_geocode internally (and honor SKIP_GEOCODE)
df_geo = geocode_with_cache_atp(df_birth, CACHE_FILE)
all_pts = normalize_dates_and_heights_atp(df_geo)
build_and_save_map_atp(all_pts, OUTPUT_HTML_BIRTHPLACE)
print("  saved:", OUTPUT_HTML_BIRTHPLACE)

# ---------- 2) Migration map FROM ----------
print("→ BUILDING migration map FROM (ATP)")
# load cache & normalize CSV
cache = load_cache(CACHE_FILE_MIGRATION)
df_from = load_and_normalize_from_atp(IOC_TO_ISO3, INPUT_CSV)

# prepare places to bulk geocode (birthplaces not in cache)
places = [p for p in df_from['birthplace'].dropna().unique() if p not in cache.get('geocode', {})]
if places:
    # temporarily allow network geocoding for this step if SKIP_GEOCODE set in CI
    old = os.environ.get("SKIP_GEOCODE")
    os.environ["SKIP_GEOCODE"] = os.environ.get("SKIP_GEOCODE", "0")
    try:
        # bulk_geocode writes to cache file
        cache = bulk_geocode(places, CACHE_FILE_MIGRATION, user_agent="center-court-atp", delay=1.2, timeout=10)
    finally:
        # restore previous SKIP_GEOCODE (if any)
        if old is None:
            os.environ.pop("SKIP_GEOCODE", None)
        else:
            os.environ["SKIP_GEOCODE"] = old

all_pts_from, migrations_from = build_points_and_migrations_from_atp(CACHE_FILE_MIGRATION, geolocator, df_from, cache)
print(f"  players: {len(all_pts_from)}, migrations: {len(migrations_from)}")
build_and_save_map_migration_from_atp(all_pts_from, migrations_from, OUTPUT_HTML_FROM)
print("  saved:", OUTPUT_HTML_FROM)

# ---------- 3) Migration map TO ----------
print("→ BUILDING migration map TO (ATP)")
cache_to = load_cache(CACHE_FILE_MIGRATION)
df_to = load_and_normalize_to(IOC_TO_ISO3, INPUT_CSV)
all_pts_to, migrations_to = build_points_and_migrations_to(CACHE_FILE_MIGRATION, geolocator, df_to, cache_to)
print(f"  players: {len(all_pts_to)}, migrations: {len(migrations_to)}")
build_and_save_map_migration_to(all_pts_to, migrations_to, OUTPUT_HTML_TO)
print("  saved:", OUTPUT_HTML_TO)

# ---------- 4) Percentage presence map ----------
print("→ BUILDING percentage presence map (ATP)")
df_pct = load_and_normalize_percentage_atp(IOC_TO_ISO3, INPUT_CSV)
players = prepare_players_atp(df_pct)
build_and_save_presence_map_atp(players, OUTPUT_HTML_PERCENTAGE, GEOJSON_URL)
print("  saved:", OUTPUT_HTML_PERCENTAGE)

print("Maps generation complete.")
