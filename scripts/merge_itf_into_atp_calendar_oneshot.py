#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Retire les événements Type=FU et ajoute TOUS les tournois du JSON ITF.

Usage :
    python merge_itf_into_atp_calendar.py calendrier_atp.json tournois_itf.json calendrier_final.json

Le calendrier ATP source doit être le calendrier original (non fusionné).
Aucune bibliothèque externe n'est nécessaire.

Règles d'intégrité :
- aucune déduplication : chaque élément de itf_json["items"] est ajouté une fois ;
- aucun ID de tournoi ITF n'est régénéré ou remplacé ;
- le titre est repris tel quel dans Name (repli sur tournamentGroup.name si
  title est absent) ;
- tournamentLink est recopié tel quel dans TournamentOverviewUrl ;
- les dates proviennent de startDate/endDate et sont seulement formatées pour
  FormattedDate ;
- si un champ essentiel manque, le script s'arrête avec l'index de l'entrée au
  lieu de l'ignorer silencieusement.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

COUNTRY_NAMES = {
    "ARG": "Argentina", "AUS": "Australia", "AUT": "Austria", "BEL": "Belgium",
    "BRA": "Brazil", "BUL": "Bulgaria", "CAN": "Canada", "CHL": "Chile",
    "CHN": "China", "COL": "Colombia", "CRO": "Croatia", "CZE": "Czech Republic",
    "DEN": "Denmark", "DOM": "Dominican Republic", "ECU": "Ecuador", "EGY": "Egypt",
    "ESP": "Spain", "EST": "Estonia", "FIN": "Finland", "FRA": "France",
    "GBR": "United Kingdom", "GEO": "Georgia", "GER": "Germany", "GRE": "Greece",
    "HUN": "Hungary", "INA": "Indonesia", "IND": "India", "IRL": "Ireland",
    "ISR": "Israel", "ITA": "Italy", "JPN": "Japan", "KAZ": "Kazakhstan",
    "KEN": "Kenya", "KOR": "South Korea", "LAT": "Latvia", "LTU": "Lithuania",
    "LUX": "Luxembourg", "MAR": "Morocco", "MEX": "Mexico", "NED": "Netherlands",
    "NOR": "Norway", "NZL": "New Zealand", "PER": "Peru", "PHI": "Philippines",
    "POL": "Poland", "POR": "Portugal", "PUR": "Puerto Rico", "ROU": "Romania",
    "RSA": "South Africa", "SRB": "Serbia", "SUI": "Switzerland", "SVK": "Slovakia",
    "SWE": "Sweden", "THA": "Thailand", "TPE": "Chinese Taipei", "TUN": "Tunisia",
    "TUR": "Turkey", "UKR": "Ukraine", "URU": "Uruguay", "USA": "USA",
    "UZB": "Uzbekistan", "VEN": "Venezuela", "VIE": "Vietnam",
}

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
MONTH_NAME_TO_NUMBER = {name.lower(): n for n, name in enumerate(MONTH_NAMES, start=1)}
CURRENCY_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "£", "AUD": "A$", "CAD": "C$",
    "CHF": "CHF ", "JPY": "¥", "CNY": "¥", "INR": "₹", "TND": "TND ",
    "MAD": "MAD ",
}


def load_json(path: Path) -> Any:
    try:
        with path.open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    except FileNotFoundError as exc:
        raise ValueError(f"Fichier introuvable : {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"JSON invalide dans {path} (ligne {exc.lineno}, colonne {exc.colno}) : {exc.msg}"
        ) from exc


def require_text(value: Any, field: str, index: int) -> str:
    """Valide une valeur textuelle sans en modifier le contenu renvoyé."""
    if value is None or not isinstance(value, (str, int)) or not str(value).strip():
        raise ValueError(f"items[{index}] : champ essentiel '{field}' absent ou vide.")
    return str(value)


def parse_iso_date(value: Any, field: str, index: int, tournament_name: str) -> date:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"items[{index}] : date essentielle '{field}' absente pour {tournament_name!r}.")
    try:
        # La source peut être YYYY-MM-DD ou une date ISO avec heure.
        return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError(
            f"items[{index}] : date '{field}' invalide ({value!r}) pour {tournament_name!r}."
        ) from exc


def formatted_date(start: date, end: date) -> str:
    """Convertit les dates en format d'affichage ATP, sans changer les jours."""
    if end < start:
        raise ValueError(f"Date de fin {end} antérieure à la date de début {start}.")
    if start.year == end.year and start.month == end.month:
        return f"{start.day} - {end.day} {MONTH_NAMES[end.month - 1]}, {end.year}"
    if start.year == end.year:
        return (
            f"{start.day} {MONTH_NAMES[start.month - 1]} - "
            f"{end.day} {MONTH_NAMES[end.month - 1]}, {end.year}"
        )
    return (
        f"{start.day} {MONTH_NAMES[start.month - 1]}, {start.year} - "
        f"{end.day} {MONTH_NAMES[end.month - 1]}, {end.year}"
    )


def display_location_piece(value: str) -> str:
    parts = []
    for word in value.strip().split():
        if word.isalpha() and word.isupper() and len(word) <= 3:
            parts.append(word)
        else:
            parts.append(word[:1].upper() + word[1:].lower() if word else word)
    return " ".join(parts)


def build_location(item: dict[str, Any]) -> str:
    group = item.get("tournamentGroup") or {}
    city_value = item.get("city")
    if not isinstance(city_value, str) or not city_value.strip():
        city_value = group.get("name") or ""
    city = ", ".join(
        display_location_piece(part) for part in str(city_value).split(",") if part.strip()
    )
    country_code = str(item.get("country") or "").strip().upper()
    country_name = COUNTRY_NAMES.get(country_code, country_code)
    if city and country_name:
        if city.casefold().endswith(country_name.casefold()):
            return city
        return f"{city}, {country_name}"
    return city or country_name


def get_group_date(start: date, end: date, tournament_year: Any) -> tuple[int, int]:
    try:
        year = int(tournament_year)
    except (TypeError, ValueError):
        year = end.year
    if start.year == year:
        return year, start.month
    if end.year == year:
        return year, end.month
    return end.year, end.month


def month_key(display_date: Any) -> tuple[int, int] | None:
    if not isinstance(display_date, str):
        return None
    match = re.fullmatch(r"\s*([A-Za-z]+),\s*(\d{4})\s*", display_date)
    if not match:
        return None
    month = MONTH_NAME_TO_NUMBER.get(match.group(1).lower())
    return (int(match.group(2)), month) if month else None


def get_or_create_month(
    tournament_dates: list[dict[str, Any]], year: int, month: int
) -> dict[str, Any]:
    for month_group in tournament_dates:
        if month_key(month_group.get("DisplayDate")) == (year, month):
            month_group.setdefault("Tournaments", [])
            month_group.setdefault("IsExpanded", False)
            return month_group
    month_group = {
        "DisplayDate": f"{MONTH_NAMES[month - 1]}, {year}",
        "IsExpanded": False,
        "NoEvents": 0,
        "Tournaments": [],
    }
    tournament_dates.append(month_group)
    return month_group


def money_as_string(item: dict[str, Any]) -> str:
    amount = item.get("prizeMoney")
    currency = str(item.get("prizeMoneyCurrency") or "USD").upper()
    if amount in (None, ""):
        return ""
    try:
        amount_text = f"{float(amount):,.0f}"
    except (TypeError, ValueError):
        amount_text = str(amount)
    return f"{CURRENCY_SYMBOLS.get(currency, currency + ' ')}{amount_text}"


def make_atp_calendar_entry(
    item: dict[str, Any], index: int
) -> tuple[dict[str, Any], int, int]:
    group = item.get("tournamentGroup")
    if not isinstance(group, dict):
        raise ValueError(f"items[{index}] : 'tournamentGroup' absent ou invalide.")

    # Ne jamais fabriquer, dédupliquer ou remplacer ces données sources.
    raw_id = require_text(group.get("id"), "tournamentGroup.id", index)
    raw_title = item.get("title")
    if isinstance(raw_title, str) and raw_title.strip():
        name = raw_title  # reprise exacte, sans .strip() ni reconstruction
    else:
        name = require_text(group.get("name"), "tournamentGroup.name/title", index)

    tournament_link = require_text(item.get("tournamentLink"), "tournamentLink", index)
    start = parse_iso_date(item.get("startDate"), "startDate", index, name)
    end = parse_iso_date(item.get("endDate"), "endDate", index, name)
    if end < start:
        raise ValueError(f"items[{index}] : endDate antérieure à startDate pour {name!r}.")

    surface = str(item.get("surface") or "")
    indoor_outdoor_code = str(item.get("inOutdoor") or "").upper()
    indoor_outdoor = {"I": "Indoor", "O": "Outdoor"}.get(indoor_outdoor_code, "")
    prize_money = money_as_string(item)

    entry = {
        "Id": raw_id,
        "Name": name,
        "Location": build_location(item),
        "FormattedDate": formatted_date(start, end),
        "IsLive": False,
        "IsPastEvent": end < date.today(),
        "ScoresUrl": "",
        "DrawsUrl": "",
        "TournamentSiteUrl": "",
        "ScheduleUrl": "",
        "Type": "ITF",
        "SinglesDrawPrintUrl": "",
        "DoublesDrawPrintUrl": "",
        "QualySinglesDrawPrintUrl": "",
        "SchedulePrintUrl": "",
        "CountryFlagUrl": "",
        "BadgeUrl": "",
        # C'est le champ de destination disponible pour le lien /en/tournament/...
        # La valeur recopiée est strictement celle du JSON intermédiaire.
        "TournamentOverviewUrl": tournament_link,
        "TicketHotline": None,
        "TicketsUrl": "",
        "TicketsPackageUrl": None,
        "PhoneNumber": "",
        "Email": None,
        "EventTypeDetail": 0,
        "TotalFinancialCommitment": prize_money,
        "PrizeMoneyDetails": prize_money,
        "Surface": surface,
        "IndoorOutdoor": indoor_outdoor,
        "SglDrawSize": item.get("singlesDrawSize"),
        "DblDrawSize": item.get("doublesDrawSize"),
        "EventType": "Tour",
        "ChallengerCategory": None,
    }
    try:
        tournament_year = int(item.get("year"))
    except (TypeError, ValueError):
        tournament_year = end.year
    year, month = get_group_date(start, end, tournament_year)
    return entry, year, month


def merge_calendars(
    atp_data: dict[str, Any], itf_data: dict[str, Any]
) -> tuple[dict[str, Any], int, int]:
    tournament_dates = atp_data.get("TournamentDates")
    if not isinstance(tournament_dates, list):
        raise ValueError("Le premier JSON doit contenir une liste 'TournamentDates'.")
    itf_items = itf_data.get("items")
    if not isinstance(itf_items, list):
        raise ValueError("Le JSON intermédiaire doit contenir une liste 'items'.")

    # Prépare/valide TOUTES les entrées avant de modifier le calendrier. Aucune
    # entrée ne peut être écartée discrètement en raison d'un doublon d'ID/URL.
    new_entries: list[tuple[dict[str, Any], int, int]] = []
    source_ids: set[str] = set()
    for index, item in enumerate(itf_items):
        if not isinstance(item, dict):
            raise ValueError(f"items[{index}] n'est pas un objet JSON ; aucune sortie n'a été écrite.")
        entry, year, month = make_atp_calendar_entry(item, index)
        if entry["Id"] in source_ids:
            raise ValueError(
                f"ID source en double {entry['Id']!r} à items[{index}]. "
                "Le script ne modifiera pas cet ID : vérifie le fichier intermédiaire."
            )
        source_ids.add(entry["Id"])
        new_entries.append((entry, year, month))

    # Supprime strictement les événements FU, sans toucher aux autres entrées ATP.
    removed_fu = 0
    for month_group in tournament_dates:
        if not isinstance(month_group, dict):
            raise ValueError("Une entrée de 'TournamentDates' n'est pas un objet JSON.")
        original = month_group.get("Tournaments", [])
        if not isinstance(original, list):
            raise ValueError(f"La liste 'Tournaments' est invalide dans {month_group.get('DisplayDate')!r}.")
        kept = []
        for tournament in original:
            if isinstance(tournament, dict) and str(tournament.get("Type") or "").strip().upper() == "FU":
                removed_fu += 1
            else:
                kept.append(tournament)
        month_group["Tournaments"] = kept

    # Ajoute chaque entrée source exactement une fois. Aucune recherche de doublons.
    for entry, year, month in new_entries:
        month_group = get_or_create_month(tournament_dates, year, month)
        month_group["Tournaments"].append(entry)

    for month_group in tournament_dates:
        month_group["NoEvents"] = len(month_group.get("Tournaments", []))

    def sort_key(month_group: dict[str, Any]) -> tuple[int, int, str]:
        key = month_key(month_group.get("DisplayDate"))
        return (key[0], key[1], "") if key else (9999, 13, str(month_group.get("DisplayDate", "")))

    tournament_dates.sort(key=sort_key)

    # Contrôle final : chaque entrée intermédiaire est bien présente avec son ID,
    # son nom et son URL source ; en cas d'anomalie, le programme échoue.
    found_by_id: dict[str, dict[str, Any]] = {}
    for month_group in tournament_dates:
        for tournament in month_group.get("Tournaments", []):
            if isinstance(tournament, dict) and tournament.get("Type") == "ITF":
                found_by_id[str(tournament.get("Id"))] = tournament
    for item_index, item in enumerate(itf_items):
        group = item["tournamentGroup"]
        source_id = str(group["id"])
        output_entry = found_by_id.get(source_id)
        if output_entry is None:
            raise ValueError(f"Contrôle final échoué : ID {source_id!r} (items[{item_index}]) introuvable.")
        source_name = item.get("title") if isinstance(item.get("title"), str) and item.get("title").strip() else group.get("name")
        if output_entry.get("Name") != source_name:
            raise ValueError(f"Contrôle final échoué : le nom de l'ID {source_id!r} a changé.")
        if output_entry.get("TournamentOverviewUrl") != item.get("tournamentLink"):
            raise ValueError(f"Contrôle final échoué : l'URL de l'ID {source_id!r} a changé.")

    return atp_data, removed_fu, len(new_entries)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Supprime les tournois FU et ajoute toutes les entrées ITF au calendrier ATP."
    )
    parser.add_argument("atp_json", type=Path, help="JSON ATP original avec TournamentDates")
    parser.add_argument("itf_json", type=Path, help="JSON intermédiaire ITF avec items")
    parser.add_argument("output_json", type=Path, help="Chemin du calendrier final")
    args = parser.parse_args()

    try:
        if args.atp_json.resolve() == args.output_json.resolve():
            raise ValueError("Le fichier de sortie doit être différent du fichier ATP source.")
        if args.itf_json.resolve() == args.output_json.resolve():
            raise ValueError("Le fichier de sortie doit être différent du fichier ITF source.")

        atp_data = load_json(args.atp_json)
        itf_data = load_json(args.itf_json)
        if not isinstance(atp_data, dict) or not isinstance(itf_data, dict):
            raise ValueError("Les deux fichiers JSON doivent avoir un objet JSON à leur racine.")

        result, removed_fu, added = merge_calendars(atp_data, itf_data)
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        with args.output_json.open("w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
            f.write("\n")

        expected = len(itf_data["items"])
        print(f"Fichier créé : {args.output_json}")
        print(f"Tournois FU supprimés : {removed_fu}")
        print(f"Tournois ITF présents dans la source : {expected}")
        print(f"Tournois ITF ajoutés au calendrier : {added}")
        print(f"Contrôle : {added}/{expected} entrées ITF intégrées avec ID, nom, dates et URL validés.")
        return 0
    except (ValueError, OSError) as exc:
        print(f"Erreur : {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
