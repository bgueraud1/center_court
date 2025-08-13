from geopy.geocoders import Nominatim

CACHE_FILE = "player_base_and_maps\maps_html\coords_cache.json"
INPUT_CSV  = "player_base_and_maps\player_data_wta.csv"
OUTPUT_HTML_BIRTHPLACE = r"player_base_and_maps\maps_html\birthplace_map_wta.html"

OUTPUT_HTML_FROM   = r"player_base_and_maps\maps_html\migration_map_from.html"
OUTPUT_HTML_TO   = r"player_base_and_maps\maps_html\migration_map_to.html"
OUTPUT_HTML_PERCENTAGE   = r"player_base_and_maps\maps_html\map_percentage.html"
OUTPUT_HTML_FALSE   = r"player_base_and_maps\maps_html\map_birthplace_false.html"


CACHE_FILE_MIGRATION = "player_base_and_maps\maps_html\coords_cache_migrations.json"

IOC_TO_ISO3   = {
    "RSA":"ZAF","GER":"DEU","NED":"NLD","INA":"IDN",
    "PHI":"PHL","POR":"PRT","GRE":"GRC","BUL":"BGR",
    "LAT":"LVA","MAD":"MDG","ALG":"DZA","CHI":"CHL",
    "GUA":"GTM","ESA":"SLV","SUI":"CHE","SLO":"SVN",
    "CRO":"HRV","URU":"URY","PAR":"PRY","NGR":"NGA",
    "DEN":"DNK", "NEP":"NPL", "VIE":"VNM", "HAI":"HTI"
}

# ── GEOCODER SETUP ──────────────────────────────────────────
geolocator = Nominatim(user_agent="tennis-migrations", timeout=10)

GEOJSON_URL = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
