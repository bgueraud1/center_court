# config_maps_atp.py
# Configuration pour la génération de cartes ATP
from geopy.geocoders import Nominatim
from pathlib import Path

# Cache coords (relative to repo root)
CACHE_FILE = Path("maps_html") / "coords_cache_atp.json"

# Input CSV ATP
INPUT_CSV = Path("player_data_atp.csv")



# Output HTML files (maps)
OUTPUT_HTML_BIRTHPLACE = Path("maps_html") / "birthplace_map_atp.html"
OUTPUT_HTML_FROM = Path("maps_html") / "migration_map_from_atp.html"
OUTPUT_HTML_TO = Path("maps_html") / "migration_map_to_atp.html"
OUTPUT_HTML_PERCENTAGE = Path("maps_html") / "map_percentage_atp.html"
OUTPUT_HTML_FALSE = Path("maps_html") / "map_birthplace_false_atp.html"

# migration cache (separate to avoid conflicts)
CACHE_FILE_MIGRATION = Path("maps_html") / "coords_cache_migrations_atp.json"

# IOC -> ISO3 common overrides (reuse/extend your existing map if needed)
IOC_TO_ISO3 = {
    "RSA": "ZAF", "GER": "DEU", "NED": "NLD", "INA": "IDN",
    "PHI": "PHL", "POR": "PRT", "GRE": "GRC", "BUL": "BGR",
    "LAT": "LVA", "MAD": "MDG", "ALG": "DZA", "CHI": "CHL",
    "GUA": "GTM", "ESA": "SLV", "SUI": "CHE", "SLO": "SVN",
    "CRO": "HRV", "URU": "URY", "PAR": "PRY", "NGR": "NGA",
    "DEN": "DNK", "NEP": "NPL", "VIE": "VNM", "HAI": "HTI"
}

# GEOCODER setup (used by migration scripts)
geolocator = Nominatim(user_agent="tennis-migrations-atp", timeout=10)

# world geojson used by maps
GEOJSON_URL = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
