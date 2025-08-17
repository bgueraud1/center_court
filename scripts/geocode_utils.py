# scripts/geocode_utils.py
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

_log = logging.getLogger("geocode_utils")
logging.basicConfig(level=logging.INFO)


def is_skip_geocode() -> bool:
    v = os.getenv("SKIP_GEOCODE", "0")
    return str(v).strip().lower() in ("1", "true", "yes")


def load_cache(cache_file: str) -> Dict[str, Dict[str, Any]]:
    try:
        with open(cache_file, "r", encoding="utf8") as f:
            data = json.load(f)
            # ensure expected shape
            if not isinstance(data, dict):
                return {"geocode": {}, "reverse": {}}
            data.setdefault("geocode", {})
            data.setdefault("reverse", {})
            return data
    except FileNotFoundError:
        return {"geocode": {}, "reverse": {}}
    except Exception as e:
        _log.warning("Could not load cache %s: %s", cache_file, e)
        return {"geocode": {}, "reverse": {}}


def save_cache(cache: Dict[str, Any], cache_file: str):
    try:
        with open(cache_file, "w", encoding="utf8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        _log.warning("Failed to save cache %s: %s", cache_file, e)


def get_geolocator(user_agent: str = "center_court_build", timeout: int = DEFAULT_TIMEOUT):
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


def geocode_place(place: str,
                  cache: Dict[str, Any],
                  cache_file: str,
                  user_agent: str = "center_court_build",
                  delay: float = DEFAULT_DELAY,
                  timeout: int = DEFAULT_TIMEOUT,
                  max_retries: int = MAX_RETRIES,
                  save_each_time: bool = True) -> Optional[Tuple[float, float]]:
    """
    Return (lat,lon) or None. Mutates cache dict ('geocode' key).
    If SKIP_GEOCODE=1 in env, returns None and does NOT call network.
    """
    if not place or not isinstance(place, str):
        return None

    if is_skip_geocode():
        _log.info("SKIP_GEOCODE enabled -> not geocoding %r", place)
        # do not add to cache here; caller can decide
        return None

    cache_geo = cache.setdefault("geocode", {})

    # if cached already
    if place in cache_geo:
        v = cache_geo[place]
        return None if v is None else tuple(v)

    geolocator = get_geolocator(user_agent=user_agent, timeout=timeout)

    for attempt in range(max_retries + 1):
        if delay:
            time.sleep(delay)
        try:
            coords = _do_geocode(geolocator, place, timeout)
            # store standardized: list [lat,lon] or None
            cache_geo[place] = list(coords) if coords else None
            if save_each_time:
                save_cache(cache, cache_file)
            return coords
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            _log.warning("geocode timeout/unavailable for %r (attempt %d/%d): %s", place, attempt, max_retries, e)
            if attempt == max_retries:
                cache_geo[place] = None
                if save_each_time:
                    save_cache(cache, cache_file)
                return None
            # exponential-ish backoff
            time.sleep(1 + attempt)
        except GeopyError as e:
            _log.warning("geopy error for %r: %s", place, e)
            cache_geo[place] = None
            if save_each_time:
                save_cache(cache, cache_file)
            return None
        except Exception as e:
            _log.warning("Unexpected error while geocoding %r: %s", place, e)
            cache_geo[place] = None
            if save_each_time:
                save_cache(cache, cache_file)
            return None


def reverse_to_iso3(lat: float,
                    lon: float,
                    cache: Dict[str, Any],
                    cache_file: str,
                    user_agent: str = "center_court_build",
                    delay: float = DEFAULT_DELAY,
                    timeout: int = DEFAULT_TIMEOUT,
                    max_retries: int = MAX_RETRIES,
                    save_each_time: bool = True) -> Optional[str]:
    """
    Return ISO3 (e.g. 'FRA') or None. Mutates cache['reverse'] with key "lat,lon" (5 decimals).
    """
    key = f"{float(lat):.5f},{float(lon):.5f}"
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


def bulk_geocode(places, cache_file: str, user_agent: str = "center_court_build",
                 delay: float = DEFAULT_DELAY, timeout: int = DEFAULT_TIMEOUT):
    """
    Ensure each place in iterable 'places' is in the cache (reads and writes cache_file).
    Returns loaded cache (dict).
    """
    cache = load_cache(cache_file)
    for p in sorted(set(p for p in places if p)):
        # skip if already cached
        if p in cache.get("geocode", {}):
            continue
        geocode_place(p, cache, cache_file, user_agent=user_agent, delay=delay, timeout=timeout, save_each_time=True)
    return cache
