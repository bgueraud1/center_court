from geopy.geocoders import Nominatim
from pathlib import Path

# Use paths relative to repo root (do NOT prefix with repo dirname)
CACHE_FILE = Path("docs") / "coords_cache.json"

INPUT_CSV = Path("player_data_wta.csv")
OUTPUT_HTML_BIRTHPLACE = Path("docs") / "birthplace_map.html"

OUTPUT_HTML_FROM = Path("docs") / "migration_map_from.html"
OUTPUT_HTML_TO = Path("docs") / "migration_map_to.html"
OUTPUT_HTML_PERCENTAGE = Path("docs") / "map_percentage.html"
OUTPUT_HTML_FALSE = Path("docs") / "map_birthplace_false.html"

# Use posix paths via Path — avoid backslash literals
CACHE_FILE_MIGRATION = Path("docs") / "coords_cache_migrations.json"

IOC_TO_ISO3 = {
    "RSA": "ZAF", "GER": "DEU", "NED": "NLD", "INA": "IDN",
    "PHI": "PHL", "POR": "PRT", "GRE": "GRC", "BUL": "BGR",
    "LAT": "LVA", "MAD": "MDG", "ALG": "DZA", "CHI": "CHL",
    "GUA": "GTM", "ESA": "SLV", "SUI": "CHE", "SLO": "SVN",
    "CRO": "HRV", "URU": "URY", "PAR": "PRY", "NGR": "NGA",
    "DEN": "DNK", "NEP": "NPL", "VIE": "VNM", "HAI": "HTI"
}

# ── GEOCODER SETUP ──────────────────────────────────────────
geolocator = Nominatim(user_agent="tennis-migrations", timeout=10)

GEOJSON_URL = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
