# scripts/geocode_utils_atp.py
"""
Utilities de geocodage adaptés pour l'ATP (utilisé par migration_map_from_atp.py).

Fournit :
 - normalize_place(place)
 - is_skip_geocode()
 - load_cache(path), save_cache(path)
 - get_geolocator(user_agent, timeout)
 - geocode_place(place, cache, cache_file, ...)
 - reverse_to_iso3(lat, lon, cache, cache_file, ...)
 - bulk_geocode(places, cache_file, ...)

Remarque : dépend de geopy (Nominatim). Pense à configurer SKIP_GEOCODE=1 en CI si nécessaire.
"""
import os
import json
import time
import logging
from typing import Optional, Tuple, Dict, Any

import pycountry
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeopyError

DEFAULT_TIMEOUT = 10
DEFAULT_DELAY = 1.0
MAX_RETRIES = 2

_log = logging.getLogger("geocode_utils_atp")
logging.basicConfig(level=logging.INFO)

import re
def normalize_place(place: str) -> str:
    """
    Normalise la chaîne 'place' utilisée comme clé de cache :
    - strip (retire espaces en tête/fin)
    - retire virgules ou espaces en trop au début/fin
    - remplace séquences de virgules/espaces par une virgule + espace simple
    - retire caractères de contrôle
    - collapse espaces multiples
    """
    if not place or not isinstance(place, str):
        return ""
    s = place
    # remove control chars
    s = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', s)
    # trim and remove leading/trailing commas/spaces
    s = s.strip()
    s = re.sub(r'^[,;\s]+|[,;\s]+$', '', s)
    # replace sequences like ",  ," or ", ," with ", "
    s = re.sub(r'\s*,\s*', ', ', s)
    # collapse multiple spaces
    s = re.sub(r'\s+', ' ', s)
    # final trim
    s = s.strip()
    return s


def is_skip_geocode() -> bool:
    v = os.getenv("SKIP_GEOCODE", "0")
    return str(v).strip().lower() in ("1", "true", "yes")


# ── CACHE HANDLING (robuste pour encodages & écritures atomiques) ──
import tempfile

def load_cache(path: str) -> dict:
    """
    Charge le cache JSON en essayant UTF-8, puis latin-1, puis nettoyage
    des caractères de contrôle. Si tout échoue, retourne la structure vide.
    Structure renvoyée: {"geocode": {place_key: [lat,lon] or None}, "reverse": { "lat,lon": "ISO3" or None }}
    """
    if not os.path.exists(path):
        return {"geocode": {}, "reverse": {}}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except UnicodeDecodeError:
        try:
            with open(path, 'r', encoding='latin-1') as f:
                raw = f.read()
            return json.loads(raw)
        except Exception:
            try:
                with open(path, 'r', encoding='latin-1') as f:
                    raw = f.read()
                cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', raw)
                return json.loads(cleaned)
            except Exception as e:
                _log.warning("failed to parse cache %s: %s", path, e)
                return {"geocode": {}, "reverse": {}}
    except json.JSONDecodeError as e:
        _log.warning("JSON decode error for cache %s: %s", path, e)
        return {"geocode": {}, "reverse": {}}
    except Exception as e:
        _log.warning("unexpected error reading cache %s: %s", path, e)
        return {"geocode": {}, "reverse": {}}


def save_cache(cache: dict, path: str):
    """
    Sauvegarde le cache en UTF-8 JSON, atomiquement.
    """
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="cache_", suffix=".json", dir=parent or ".")
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)
    except Exception as e:
        _log.warning("Failed to save cache %s: %s", path, e)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def get_geolocator(user_agent: str = "migration-atp", timeout: int = DEFAULT_TIMEOUT):
    """
    Retourne une instance Nominatim. user_agent doit être spécifique à ton app.
    """
    return Nominatim(user_agent=user_agent, timeout=timeout)


def _do_geocode(geolocator, place: str, timeout: int) -> Optional[Tuple[float, float]]:
    try:
        loc = geolocator.geocode(place)
        if loc:
            return (float(loc.latitude), float(loc.longitude))
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        raise
    except Exception as e:
        _log.debug("Non fatal geocode error for %r: %s", place, e)
    return None


# --- utilité: charger/save cache simple (dict place -> [lat, lon] or None) ---
def load_coords_cache(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def save_coords_cache(path: str, cache: dict):
    try:
        parent = os.path.dirname(path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        with open(path, "w", encoding="utf8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        _log.debug("Failed to save coords cache to %s", path)


def should_skip_geocode(place: str, cache_path: Optional[str] = None) -> bool:
    """
    Retourne True si le geocoding doit être sauté pour 'place'.
    Conditions :
      - SKIP_GEOCODE env var = '1'
      - place déjà présent dans cache_path (et non-null)
    """
    if os.environ.get("SKIP_GEOCODE", "") == "1":
        return True
    if not place:
        return True
    if cache_path:
        cache = load_coords_cache(cache_path)
        if place in cache:
            return cache.get(place) is not None
    return False


def geocode_place(place: str,
                  cache: Dict[str, Any],
                  cache_file: str,
                  user_agent: str = "migration-atp",
                  delay: float = DEFAULT_DELAY,
                  timeout: int = DEFAULT_TIMEOUT,
                  max_retries: int = MAX_RETRIES,
                  save_each_time: bool = True) -> Optional[Tuple[float, float]]:
    """
    Retourne (lat, lon) ou None. Mutates cache dict ('geocode' key).
    Si SKIP_GEOCODE=1 -> retourne None (mais s'il existe en cache on renvoie la valeur cache).
    """
    if not place or not isinstance(place, str):
        return None

    place_key = normalize_place(place)
    cache_geo = cache.setdefault("geocode", {})

    if place_key in cache_geo:
        v = cache_geo[place_key]
        return None if v is None else tuple(v)

    if is_skip_geocode():
        _log.info("SKIP_GEOCODE enabled -> not geocoding %r", place)
        return None

    geolocator = get_geolocator(user_agent=user_agent, timeout=timeout)

    for attempt in range(max_retries + 1):
        if delay:
            time.sleep(delay)
        try:
            coords = _do_geocode(geolocator, place, timeout)
            cache_geo[place_key] = list(coords) if coords else None
            if save_each_time:
                save_cache(cache, cache_file)
            return coords
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            _log.warning("geocode timeout/unavailable for %r (attempt %d/%d): %s", place, attempt, max_retries, e)
            if attempt == max_retries:
                cache_geo[place_key] = None
                if save_each_time:
                    save_cache(cache, cache_file)
                return None
            time.sleep(1 + attempt)
        except GeopyError as e:
            _log.warning("geopy error for %r: %s", place, e)
            cache_geo[place_key] = None
            if save_each_time:
                save_cache(cache, cache_file)
            return None
        except Exception as e:
            _log.warning("Unexpected error while geocoding %r: %s", place, e)
            cache_geo[place_key] = None
            if save_each_time:
                save_cache(cache, cache_file)
            return None


def reverse_to_iso3(lat: float,
                    lon: float,
                    cache: Dict[str, Any],
                    cache_file: str,
                    user_agent: str = "migration-atp",
                    delay: float = DEFAULT_DELAY,
                    timeout: int = DEFAULT_TIMEOUT,
                    max_retries: int = MAX_RETRIES,
                    save_each_time: bool = True) -> Optional[str]:
    """
    Retourne ISO3 (ex: 'FRA') ou None. Mutates cache['reverse'].
    Key: 'lat,lon' with 5 decimals.
    """
    try:
        key = f"{float(lat):.5f},{float(lon):.5f}"
    except Exception:
        return None
    cache_rev = cache.setdefault("reverse", {})
    if key in cache_rev:
        return cache_rev[key]

    if is_skip_geocode():
        _log.info("SKIP_GEOCODE enabled -> not reverse geocoding %s", key)
        return None

    geolocator = get_geolocator(user_agent=user_agent, timeout=timeout)

    for attempt in range(max_retries + 1):
        if delay:
            time.sleep(delay)
        try:
            loc = geolocator.reverse((lat, lon), language="en")
            iso3 = None
            if loc and isinstance(loc.raw, dict):
                ccode = loc.raw.get('address', {}).get('country_code')
                if ccode:
                    try:
                        iso2 = ccode.upper()
                        country = pycountry.countries.get(alpha_2=iso2)
                        if country:
                            iso3 = country.alpha_3
                    except Exception:
                        iso3 = None
            cache_rev[key] = iso3
            if save_each_time:
                save_cache(cache, cache_file)
            return iso3
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            _log.warning("reverse geocode timeout/unavailable for %s (attempt %d/%d): %s", key, attempt, max_retries, e)
            if attempt == max_retries:
                cache_rev[key] = None
                if save_each_time:
                    save_cache(cache, cache_file)
                return None
            time.sleep(1 + attempt)
        except Exception as e:
            _log.warning("Unexpected error in reverse geocode %s: %s", key, e)
            cache_rev[key] = None
            if save_each_time:
                save_cache(cache, cache_file)
            return None


def bulk_geocode(places, cache_file: str, user_agent: str = "migration-atp",
                 delay: float = DEFAULT_DELAY, timeout: int = DEFAULT_TIMEOUT):
    """
    Assure que chaque place est présente dans le cache. Renvoie le cache chargé.
    """
    cache = load_cache(cache_file)
    for p in sorted(set(p for p in places if p)):
        place_key = normalize_place(p)
        if place_key in cache.get("geocode", {}):
            continue
        geocode_place(p, cache, cache_file, user_agent=user_agent, delay=delay, timeout=timeout, save_each_time=True)
    return cache
