# debug_migrations_normalized.py
import csv
import re
from collections import Counter
try:
    import pycountry
except Exception:
    pycountry = None

CSV = "player_data_wta.csv"

# mapping manuelle pour cas fréquents / ambigus
FALLBACK = {
    "england": "GBR",
    "scotland": "GBR",
    "wales": "GBR",
    "northern ireland": "GBR",
    "uk": "GBR",
    "united kingdom": "GBR",
    "usa": "USA",
    "us": "USA",
    "united states": "USA",
    "united states of america": "USA",
    "america": "USA",
    "czech republic": "CZE",
    "czechia": "CZE",
    "russia": "RUS",
    "moldova": "MDA",
    "bosnia": "BIH",
    "bosnia and herzegovina": "BIH",
    "south korea": "KOR",
    "north korea": "PRK",
    "ivory coast": "CIV",
    "côte d'ivoire": "CIV",
    "holland": "NLD",
    "netherlands": "NLD",
    "england": "GBR",
    "hong kong": "HKG",
    "macau": "MAC",
    "taiwan": "TWN",
    "vietnam": "VNM",
    "laos": "LAO",
    "republic of ireland": "IRL",
    "ireland": "IRL",
    # ajoute au besoin
}

def normalize_iso3(country_like):
    if not country_like: 
        return None
    s = str(country_like).strip()
    if not s: 
        return None
    # si c'est déjà un code alpha-3
    if re.fullmatch(r'^[A-Za-z]{3}$', s):
        return s.upper()
    # si c'est alpha-2 code (FR, US, GB) -> convertir via pycountry
    if re.fullmatch(r'^[A-Za-z]{2}$', s):
        if pycountry:
            try:
                c = pycountry.countries.get(alpha_2=s.upper())
                if c and getattr(c, 'alpha_3', None):
                    return c.alpha_3
            except Exception:
                pass
        # fallback best-effort:
        two = s.upper()
        # quelques cas manuels
        if two == "UK": return "GBR"
        if two == "GB": return "GBR"
        if two == "US": return "USA"
        return two
    # normaliser le texte (en minuscule, enlever accents légers si besoin)
    key = s.lower().strip()
    key = key.replace(".", "").replace("'", "").strip()
    # direct fallback dict
    if key in FALLBACK:
        return FALLBACK[key]
    # tenter pycountry (nom complet ou common name)
    if pycountry:
        try:
            # search_fuzzy peut lever, on l'encapsule
            candidates = None
            try:
                candidates = pycountry.countries.search_fuzzy(s)
            except Exception:
                candidates = None
            if candidates:
                c = candidates[0]
                return getattr(c, "alpha_3", None)
            # try by name attribute
            for c in pycountry.countries:
                if (getattr(c, "name", "") or "").lower() == key:
                    return getattr(c, "alpha_3", None)
                if key in (getattr(c, "official_name", "") or "").lower():
                    return getattr(c, "alpha_3", None)
        except Exception:
            pass
    # containment-based heuristics
    for k, v in FALLBACK.items():
        if k in key:
            return v
    # try last token splitting by space/ comma
    last = re.split(r'[,\-\/]', s)[-1].strip().lower()
    if last in FALLBACK:
        return FALLBACK[last]
    # give raw uppercased last 3 chars as worst fallback (NOT ideal)
    return None

def extract_country_from_birthplace(birthplace):
    if not birthplace: return None
    # often form: "City, Province, Country" -> take last comma piece
    parts = [p.strip() for p in birthplace.split(",") if p.strip()]
    if not parts:
        return None
    cand = parts[-1]
    # sometimes country is "USA" or "United States" or "England"
    return cand

# main
rows = []
with open(CSV, encoding="utf-8", newline='') as f:
    rdr = csv.DictReader(f)
    for r in rdr:
        rows.append(r)

print("CSV columns:", rdr.fieldnames)
count_has_birth = sum(1 for r in rows if (r.get("birthplace") or "").strip())
count_has_rep = sum(1 for r in rows if (r.get("represented_country") or "").strip())
print("Non-empty counts: birthplace:", count_has_birth, "represented_country:", count_has_rep)

migs = []
same = 0
unknown_birth = 0
unknown_rep = 0
for r in rows:
    birth_raw = (r.get("birthplace") or "").strip()
    rep_raw   = (r.get("represented_country") or "").strip()
    if not birth_raw or not rep_raw: 
        if not birth_raw: unknown_birth += 1
        if not rep_raw: unknown_rep += 1
        continue
    birth_country_token = extract_country_from_birthplace(birth_raw)
    iso_birth = normalize_iso3(birth_country_token)
    iso_rep   = normalize_iso3(rep_raw)
    if iso_birth is None:
        # try to map some multi-word tokens
        iso_birth = normalize_iso3(birth_country_token.lower())
    if iso_rep is None:
        iso_rep = normalize_iso3(rep_raw.lower())
    if iso_birth and iso_rep:
        if iso_birth != iso_rep:
            migs.append((r.get("player") or r.get("player_id") or "??", r.get("full_name") or r.get("name") or "", birth_raw, rep_raw, iso_birth, iso_rep))
        else:
            same += 1
    else:
        # can't determine
        if iso_birth is None:
            unknown_birth += 1
        if iso_rep is None:
            unknown_rep += 1

print("Unknown birth count (couldn't extract/normalize):", unknown_birth)
print("Unknown rep count (couldn't normalize):", unknown_rep)
print("Same-country (after normalization):", same)
print("Migrations detected (after normalization):", len(migs))
print("Examples (first 30):")
import json
print(json.dumps(migs[:30], ensure_ascii=False, indent=2))
