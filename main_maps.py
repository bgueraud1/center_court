# main_maps.py
"""
Orchestre la génération des maps WTA:
 - birthplace map
 - migration map FROM
 - migration map TO
 - percentage presence map

Attendu:
 - map_birth_place.py
 - migration_map_from.py
 - migration_map_to.py
 - map_percentage.py
 - scripts/geocode_utils.py
 - config_maps.py
"""
import os
from pathlib import Path

import pandas as pd

from config_maps import (
    INPUT_CSV,
    CACHE_FILE,
    OUTPUT_HTML_BIRTHPLACE,
    OUTPUT_HTML_FROM,
    OUTPUT_HTML_TO,
    OUTPUT_HTML_PERCENTAGE,
    OUTPUT_HTML_FALSE,
    IOC_TO_ISO3,
    geolocator,
    CACHE_FILE_MIGRATION,
    GEOJSON_URL,
)

from map_birth_place import (
    load_and_clean,
    geocode_with_cache,
    normalize_dates_and_heights,
    build_and_save_map,
)

from scripts.geocode_utils import (
    geocode_place,
    should_skip_geocode,
    load_cache as geo_load_cache,
    save_cache as geo_save_cache,
    bulk_geocode,
)

from migration_map_from import (
    load_and_normalize,
    build_points_and_migrations,
    build_and_save_map_migration,
)

from migration_map_to import (
    load_and_normalize_to,
    load_cache_to,
    build_points_and_migrations_to,
    build_and_save_map_migration_to,
)

from map_percentage import (
    load_and_normalize_percentage,
    prepare_players,
    build_and_save_presence_map,
)


def _load_cache_safe(path: str) -> dict:
    try:
        return geo_load_cache(path)
    except Exception:
        return {"geocode": {}, "reverse": {}}


def _save_cache_safe(cache: dict, path: str) -> None:
    try:
        geo_save_cache(cache, path)
    except Exception:
        pass


def build_birthplace_map():
    print("→ BUILDING birthplace map (WTA)")
    df = load_and_clean(INPUT_CSV)
    df = geocode_with_cache(df, CACHE_FILE)
    all_pts = normalize_dates_and_heights(df)
    build_and_save_map(all_pts, OUTPUT_HTML_BIRTHPLACE)
    print("  saved:", OUTPUT_HTML_BIRTHPLACE)


def build_migration_from_map():
    print("→ BUILDING migration map FROM (WTA)")

    cache = _load_cache_safe(CACHE_FILE_MIGRATION)
    df = load_and_normalize(IOC_TO_ISO3, INPUT_CSV)

    cache = _load_cache_safe(CACHE_FILE_MIGRATION)
    places = [p for p in df["birthplace"].dropna().unique() if p not in cache.get("geocode", {})]

    if places:
        old = os.environ.get("SKIP_GEOCODE")
        os.environ["SKIP_GEOCODE"] = "0"
        try:
            cache = bulk_geocode(
                places,
                CACHE_FILE_MIGRATION,
                user_agent="center-court-bot",
                delay=1.2,
                timeout=10,
            )
        finally:
            if old is None:
                os.environ.pop("SKIP_GEOCODE", None)
            else:
                os.environ["SKIP_GEOCODE"] = old

    all_pts, migrations = build_points_and_migrations(CACHE_FILE_MIGRATION, geolocator, df, cache)
    print(f"✅ {len(all_pts)} players loaded.")
    print(f"🚀 {len(migrations)} migration records built.")
    build_and_save_map_migration(all_pts, migrations, OUTPUT_HTML_FROM)
    print("  saved:", OUTPUT_HTML_FROM)


def build_migration_to_map():
    print("→ BUILDING migration map TO (WTA)")

    cache = load_cache_to(CACHE_FILE_MIGRATION)
    df = load_and_normalize_to(IOC_TO_ISO3, INPUT_CSV)
    all_pts, migrations = build_points_and_migrations_to(CACHE_FILE_MIGRATION, geolocator, df, cache)
    print(f"✅ {len(all_pts)} players loaded.")
    print(f"🚀 {len(migrations)} migration records built.")
    build_and_save_map_migration_to(all_pts, migrations, OUTPUT_HTML_TO)
    print("  saved:", OUTPUT_HTML_TO)


def build_percentage_map():
    print("→ BUILDING percentage presence map (WTA)")
    df = load_and_normalize_percentage(IOC_TO_ISO3, INPUT_CSV)
    players = prepare_players(df)
    build_and_save_presence_map(players, OUTPUT_HTML_PERCENTAGE, GEOJSON_URL)
    print("  saved:", OUTPUT_HTML_PERCENTAGE)


def main():
    print("=== WTA maps generation start ===")
    build_birthplace_map()
    build_migration_from_map()
    build_migration_to_map()
    build_percentage_map()
    print("Maps generation complete.")


if __name__ == "__main__":
    main()