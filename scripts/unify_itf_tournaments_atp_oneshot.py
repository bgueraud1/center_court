#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Transforme un calendrier ITF en JSON au schéma cible.

Usage : python reformer_tournois.py entree.json sortie.json
Option : --flat écrit une liste directement, sans l'enveloppe {"url", "items"}.
Les IDs générés sont synthétiques, pas des IDs officiels ITF.
"""

import argparse
import json
import re
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

ID_START = 900_000  # Début de la plage réservée aux IDs synthétiques
SURFACE_CODES = {"C": "Clay", "H": "Hard", "HC": "Hard", "G": "Grass", "CA": "Carpet"}
CURRENCY_SYMBOLS = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "₹": "INR"}
COUNTRY_CODES = {
    "morocco": "MAR", "united states": "USA", "united states of america": "USA",
    "usa": "USA", "france": "FRA", "spain": "ESP", "italy": "ITA",
    "germany": "GER", "united kingdom": "GBR", "great britain": "GBR",
    "australia": "AUS", "canada": "CAN", "china": "CHN", "japan": "JPN",
    "india": "IND", "brazil": "BRA", "argentina": "ARG", "portugal": "POR",
    "switzerland": "SUI", "netherlands": "NED", "belgium": "BEL",
    "south africa": "RSA", "egypt": "EGY", "tunisia": "TUN", "turkey": "TUR",
    "kazakhstan": "KAZ", "uzbekistan": "UZB", "new zealand": "NZL",
    "mexico": "MEX", "thailand": "THA", "sweden": "SWE", "serbia": "SRB",
}


def text(value):
    return "" if value is None else str(value).strip()


def iso_date(value):
    """Convertit une date ou datetime ISO en YYYY-MM-DD."""
    value = text(value)
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value).date().isoformat()
    except ValueError:
        try:
            return date.fromisoformat(value[:10]).isoformat()
        except ValueError:
            return None


def dates_from_label(value):
    """Parse, par exemple, '29 Dec to 04 Jan 2026', y compris le passage d'année."""
    match = re.search(
        r"(\d{1,2}\s+[A-Za-z]{3,9})\s+to\s+(\d{1,2}\s+[A-Za-z]{3,9})\s+(\d{4})",
        text(value), re.I,
    )
    if not match:
        return None, None
    start_part, end_part, end_year = match.groups()
    for fmt in ("%d %b", "%d %B"):
        try:
            start_month = datetime.strptime(start_part, fmt).month
            end_date = datetime.strptime(f"{end_part} {end_year}", fmt + " %Y").date()
            start_year = int(end_year) - 1 if start_month > end_date.month else int(end_year)
            start_date = datetime.strptime(f"{start_part} {start_year}", fmt + " %Y").date()
            return start_date.isoformat(), end_date.isoformat()
        except ValueError:
            continue
    return None, None


def get_dates(item):
    start, end = iso_date(item.get("startDate")), iso_date(item.get("endDate"))
    if not start or not end:
        label_start, label_end = dates_from_label(item.get("dates"))
        start, end = start or label_start, end or label_end
    if not start or not end:
        name = text(item.get("tournamentName") or item.get("name")) or "Tournoi inconnu"
        raise ValueError(f"dates illisibles pour {name!r} (startDate/endDate/dates).")
    return start, end


def get_category(item):
    category = text(item.get("category"))
    if category:
        return category
    name = text(item.get("tournamentName") or item.get("name"))
    match = re.match(r"^(WTT\s+)?([MW]\s?\d{2,3})\b", name, re.I)
    if match:
        prefix, tier = match.groups()
        tier = tier.upper().replace(" ", "")
        return f"WTT {tier}" if prefix else tier
    return name.split()[0] if name else "ITF"


def get_location(item):
    for key in ("location", "venue", "city"):
        value = text(item.get(key))
        if value:
            return value
    name = text(item.get("tournamentName") or item.get("name"))
    category = text(item.get("category"))
    if category:
        name = re.sub(r"^" + re.escape(category) + r"\s*[-–:]?\s*", "", name, flags=re.I)
    return name or "UNKNOWN"


def get_country_code(item):
    for key in ("hostNationCode", "countryCode", "country"):
        value = text(item.get(key))
        if re.fullmatch(r"[A-Za-z]{3}", value):
            return value.upper()
        if re.fullmatch(r"[A-Za-z]{2}", value):
            try:
                import pycountry
                found = pycountry.countries.get(alpha_2=value.upper())
                if found:
                    return found.alpha_3.upper()
            except (ImportError, AttributeError):
                pass
    name = text(item.get("hostNation") or item.get("country"))
    if not name:
        return ""
    try:
        import pycountry
        return pycountry.countries.lookup(name).alpha_3.upper()
    except (ImportError, LookupError, AttributeError):
        return COUNTRY_CODES.get(name.casefold(), name.upper())


def get_surface(item):
    return text(item.get("surfaceDesc")) or SURFACE_CODES.get(text(item.get("surfaceCode")).upper(), "")


def get_indoor_outdoor(item):
    value = text(item.get("indoorOrOutDoor") or item.get("indoorOrOutdoor") or item.get("inOutdoor")).casefold()
    if value.startswith("out") or value == "o":
        return "O"
    if value.startswith("in") or value == "i":
        return "I"
    return ""


def get_prize_money(value):
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return int(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return None
    match = re.search(r"[-+]?\d[\d\s.,]*", text(value))
    if not match:
        return None
    number = match.group().replace(" ", "")
    if "," in number and "." in number:
        number = number.replace(".", "").replace(",", ".") if number.rfind(",") > number.rfind(".") else number.replace(",", "")
    elif "," in number:
        number = number.replace(",", "") if len(number.rsplit(",", 1)[1]) == 3 else number.replace(",", ".")
    elif "." in number and len(number.rsplit(".", 1)[1]) == 3:
        number = number.replace(".", "")
    try:
        return int(Decimal(number))
    except (InvalidOperation, ValueError):
        return None


def get_currency(item):
    explicit = text(item.get("prizeMoneyCurrency") or item.get("currency"))
    if explicit:
        return explicit.upper()
    value = text(item.get("prizeMoney"))
    upper = value.upper()
    for code in ("USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY", "INR"):
        if code in upper:
            return code
    for symbol, code in CURRENCY_SYMBOLS.items():
        if symbol in value:
            return code
    return "USD"  # Valeur par défaut pour les montants ITF sans devise explicite.


def draw_size(item, keys, default):
    for key in keys:
        value = item.get(key)
        try:
            if value is not None and str(value).strip():
                return int(value)
        except (TypeError, ValueError):
            pass
    return default


def transform(item, synthetic_id):
    start, end = get_dates(item)
    location = get_location(item)
    category = get_category(item)
    country = get_country_code(item)
    prefix = category if category.upper().startswith("WTT ") else f"WTT {category}"
    destination = location.upper()
    if country and country not in {part.strip().upper() for part in destination.split(",")}:
        destination = f"{destination}, {country}" if destination else country

    year = item.get("year")
    try:
        year = int(year) if year is not None and str(year).strip() else int(start[:4])
    except (TypeError, ValueError):
        year = int(start[:4])

    link = item.get("tournamentLink")
    live_id = item.get("liveScoringId")
    return {
        "tournamentGroup": {
            "id": synthetic_id,
            "name": location.upper(),
            "level": "ITF",
            "metadata": {},
        },
        "year": year,
        "title": f"{prefix} - {destination}".strip(" -"),
        "startDate": start,
        "endDate": end,
        "surface": get_surface(item),
        "inOutdoor": get_indoor_outdoor(item),
        "city": text(item.get("city")),
        "country": country,
        "singlesDrawSize": draw_size(item, ("singlesDrawSize", "singles_draw_size", "singlesDraw"), 32),
        "doublesDrawSize": draw_size(item, ("doublesDrawSize", "doubles_draw_size", "doublesDraw"), 16),
        "prizeMoney": get_prize_money(item.get("prizeMoney")),
        "prizeMoneyCurrency": get_currency(item),
        "liveScoringId": "" if live_id is None else str(live_id),
        # La valeur source est conservée strictement : pas de reconstruction ou de changement d'URL.
        "tournamentLink": "" if link is None else str(link),
    }


def main():
    parser = argparse.ArgumentParser(description="Convertit un JSON de tournois ITF au schéma cible.")
    parser.add_argument("input", help="Fichier JSON source")
    parser.add_argument("output", help="Fichier JSON de sortie")
    parser.add_argument("--flat", action="store_true", help="Écrit seulement la liste des tournois transformés.")
    parser.add_argument("--id-start", type=int, default=ID_START, help=f"Premier ID synthétique (défaut : {ID_START}).")
    args = parser.parse_args()

    try:
        with Path(args.input).open("r", encoding="utf-8-sig") as f:
            source = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Erreur de lecture du JSON source : {exc}", file=sys.stderr)
        return 1

    wrapped = isinstance(source, dict) and isinstance(source.get("items"), list)
    if wrapped:
        items = source["items"]
    elif isinstance(source, list):
        items = source
    else:
        print("Erreur : le JSON doit être une liste ou un objet avec une liste 'items'.", file=sys.stderr)
        return 1

    result = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            print(f"Erreur : l'élément {index + 1} n'est pas un objet JSON.", file=sys.stderr)
            return 1
        try:
            result.append(transform(item, args.id_start + index))
        except ValueError as exc:
            print(f"Erreur à l'élément {index + 1} : {exc}", file=sys.stderr)
            return 1

    output = result if args.flat or not wrapped else {**source, "items": result}
    try:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
            f.write("\n")
    except OSError as exc:
        print(f"Erreur d'écriture du JSON de sortie : {exc}", file=sys.stderr)
        return 1

    print(f"OK : {len(result)} tournoi(s) écrit(s) dans {args.output}.")
    print(f"IDs synthétiques : {args.id_start} à {args.id_start + len(result) - 1 if result else args.id_start}.")
    if wrapped and not args.flat:
        print("L'enveloppe d'origine et son champ 'url' ont été conservés.")
    if any(t["prizeMoney"] is None for t in result):
        print("Attention : au moins un montant n'a pas pu être lu et vaut null.")
    if any(not t["tournamentLink"] for t in result):
        print("Attention : au moins un tournoi n'avait pas de tournamentLink dans la source.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
