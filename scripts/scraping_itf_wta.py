#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
import sys
import unicodedata
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip().lower()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = re.sub(r"[^a-z0-9]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def load_player_ids(csv_path: str) -> dict:
    lookup = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            full_name = (row.get("full_name") or "").strip()
            player_id = (row.get("player_id") or "").strip()
            if not full_name or not player_id:
                continue
            key = normalize_name(full_name)
            if key and key not in lookup:
                lookup[key] = player_id
    return lookup


def extract_team_info(team_wrapper):
    team_info = team_wrapper.select_one(".drawsheet-widget__team-info")
    is_winner = team_info is not None and "is-winner" in (team_info.get("class") or [])

    first = team_wrapper.select_one(".drawsheet-widget__first-name")
    last = team_wrapper.select_one(".drawsheet-widget__last-name")
    country = team_wrapper.select_one(".drawsheet-widget__nationality")

    first_name = first.get_text(" ", strip=True) if first else ""
    last_name = last.get_text(" ", strip=True) if last else ""
    player_name = f"{first_name} {last_name}".strip()

    country_code = country.get_text(" ", strip=True) if country else ""
    country_code = country_code.replace(" ", "").strip()

    return {
        "player_name": player_name,
        "country": country_code,
        "is_winner": is_winner,
    }

def scrape_from_html(html: str, player_lookup: dict, start_match_id: int = 16):
    soup = BeautifulSoup(html, "html.parser")
    widgets = soup.select(".drawsheet-widget-spacer .drawsheet-widget")

    # On ne garde que les 16 premiers matchs : LS016 -> LS031
    widgets = widgets[:16]

    print(f"[debug] widgets pris en compte: {len(widgets)}")

    results = []
    for idx, widget in enumerate(widgets):
        team_wrappers = widget.select(".drawsheet-widget__team-info-wrapper")
        if len(team_wrappers) < 2:
            print(f"[debug] widget {idx}: structure incomplète")
            continue

        team1 = extract_team_info(team_wrappers[0])
        team2 = extract_team_info(team_wrappers[1])

        if team1["is_winner"] and not team2["is_winner"]:
            winner, loser = team1, team2
        elif team2["is_winner"] and not team1["is_winner"]:
            winner, loser = team2, team1
        else:
            winner, loser = team1, team2

        match_id = f"LS{start_match_id + idx:03d}"

        results.append(
            {
                "match_id": match_id,
                "winner_player_name": winner["player_name"],
                "winner_player_id": player_lookup.get(normalize_name(winner["player_name"]), ""),
                "loser_player_name": loser["player_name"],
                "loser_player_id": player_lookup.get(normalize_name(loser["player_name"]), ""),
                "winner_country": winner["country"],
                "loser_country": loser["country"],
                "score_string": "",
            }
        )

        print(
            f"[debug] {match_id}: "
            f"{winner['player_name']} ({winner['country']}) "
            f"vs {loser['player_name']} ({loser['country']})"
        )

    return results
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL de la page à scraper")
    parser.add_argument("--csv", default="player_data_wta.csv", help="Chemin du CSV joueur")
    parser.add_argument("--output", default="matches.json", help="Fichier JSON de sortie")
    parser.add_argument("--start-match-id", type=int, default=16, help="Départ pour LS016")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV introuvable: {csv_path}", file=sys.stderr)
        sys.exit(1)

    player_lookup = load_player_ids(str(csv_path))

    with sync_playwright() as p:
        print("[debug] lancement du navigateur visible...")
        browser = p.chromium.launch(
            headless=False,
            slow_mo=100,
            args=["--start-maximized"],
        )

        context = browser.new_context(no_viewport=True)
        page = context.new_page()

        print("[debug] ouverture de la page...")
        page.goto(args.url, wait_until="domcontentloaded", timeout=120000)

        print("[debug] attente du chargement réseau...")
        page.wait_for_load_state("networkidle", timeout=120000)

        page.wait_for_timeout(3000)

        widget_count = page.locator(".drawsheet-widget-spacer .drawsheet-widget").count()
        print(f"[debug] widgets visibles dans le navigateur: {widget_count}")

        html = page.content()
        matches = scrape_from_html(html, player_lookup, start_match_id=args.start_match_id)

        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(matches, f, ensure_ascii=False, indent=2)

        print(f"{len(matches)} matchs écrits dans {args.output}")

        context.close()
        browser.close()


if __name__ == "__main__":
    main()