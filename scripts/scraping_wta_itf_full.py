#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from pathlib import Path

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


ROUND_LABELS_TO_VISIT = ["Quarter-finals", "Final"]

CONSENT_BUTTON_LABELS = [
    "Autoriser",
    "Tout autoriser",
    "Accepter",
    "Tout accepter",
    "J’accepte",
    "J'accepte",
    "Accept all",
    "Allow",
    "I agree",
    "OK",
    "D'accord",
]


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip().lower()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = re.sub(r"[^a-z0-9]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def load_player_ids(csv_path: str) -> dict:
    lookup = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            full_name = clean_text(row.get("full_name") or "")
            player_id = clean_text(row.get("player_id") or "")
            if not full_name or not player_id:
                continue
            key = normalize_name(full_name)
            if key and key not in lookup:
                lookup[key] = player_id
    return lookup


def extract_country_code(team_wrapper) -> str:
    """
    Extrait le code pays de manière robuste.
    Selon les matchs, il peut être présent en texte ('SLO') ou seulement
    dans la classe CSS du drapeau ('itf-flags--SLO').
    """
    nationality = team_wrapper.select_one(".drawsheet-widget__nationality")
    if not nationality:
        return ""

    text = clean_text(nationality.get_text(" ", strip=True))
    if re.fullmatch(r"[A-Z]{3}", text):
        return text

    flag = nationality.select_one("[class*='itf-flags--']")
    if flag:
        for cls in flag.get("class", []):
            m = re.search(r"itf-flags--([A-Z]{3})", cls)
            if m:
                return m.group(1)

    return text.replace(" ", "").upper()


def parse_score_cell(score_span):
    """
    Retourne:
      - le score principal
      - le score de tie-break s'il existe
    """
    if score_span is None:
        return "", ""

    main_parts = []
    tiebreak = ""

    for child in score_span.contents:
        if getattr(child, "name", None) == "span" and "losing-score" in (child.get("class") or []):
            tiebreak = clean_text(child.get_text(" ", strip=True))
        else:
            if hasattr(child, "get_text"):
                main_parts.append(child.get_text(" ", strip=True))
            else:
                main_parts.append(str(child))

    main_score = clean_text("".join(main_parts))
    return main_score, tiebreak


def format_set_score(score_a_span, score_b_span) -> str:
    a, tb_a = parse_score_cell(score_a_span)
    b, tb_b = parse_score_cell(score_b_span)

    if not a and not b:
        return ""

    if a.isdigit() and b.isdigit():
        ai = int(a)
        bi = int(b)
        tb = tb_a or tb_b

        if ai > bi:
            base = f"{ai}-{bi}"
        elif bi > ai:
            base = f"{bi}-{ai}"
        else:
            base = f"{a}-{b}"

        return f"{base}({tb})" if tb else base

    raw = f"{a}-{b}".strip("-")
    return raw


def extract_team_info(team_wrapper):
    team_info = team_wrapper.select_one(".drawsheet-widget__team-info")
    is_winner = team_info is not None and "is-winner" in (team_info.get("class") or [])

    first = team_wrapper.select_one(".drawsheet-widget__first-name")
    last = team_wrapper.select_one(".drawsheet-widget__last-name")

    first_name = clean_text(first.get_text(" ", strip=True)) if first else ""
    last_name = clean_text(last.get_text(" ", strip=True)) if last else ""
    player_name = clean_text(f"{first_name} {last_name}")

    country_code = extract_country_code(team_wrapper)

    scores = [
        clean_text(span.get_text(" ", strip=True))
        for span in team_wrapper.select(".drawsheet-widget__scores .drawsheet-widget__score")
    ]

    return {
        "player_name": player_name,
        "country": country_code,
        "is_winner": is_winner,
        "scores": scores,
    }


def extract_match_from_widget(widget, player_lookup: dict, match_id: str, round_name: str):
    team_wrappers = widget.select(".drawsheet-widget__team-info-wrapper")
    if len(team_wrappers) < 2:
        return None

    team1 = extract_team_info(team_wrappers[0])
    team2 = extract_team_info(team_wrappers[1])

    if not team1["player_name"] and not team2["player_name"]:
        return None

    if team1["is_winner"] and not team2["is_winner"]:
        winner, loser = team1, team2
        winner_wrapper, loser_wrapper = team_wrappers[0], team_wrappers[1]
    elif team2["is_winner"] and not team1["is_winner"]:
        winner, loser = team2, team1
        winner_wrapper, loser_wrapper = team_wrappers[1], team_wrappers[0]
    else:
        winner, loser = team1, team2
        winner_wrapper, loser_wrapper = team_wrappers[0], team_wrappers[1]

    winner_score_spans = winner_wrapper.select(".drawsheet-widget__scores .drawsheet-widget__score")
    loser_score_spans = loser_wrapper.select(".drawsheet-widget__scores .drawsheet-widget__score")
    set_count = min(len(winner_score_spans), len(loser_score_spans))

    raw_sets = [
        format_set_score(winner_score_spans[i], loser_score_spans[i])
        for i in range(set_count)
    ]
    raw_sets = [s for s in raw_sets if s]
    score_string = " ".join(raw_sets)

    if not score_string:
        winner_scores = winner.get("scores") or []
        loser_scores = loser.get("scores") or []
        set_count = min(len(winner_scores), len(loser_scores))
        score_string = " ".join(
            f"{winner_scores[i]}-{loser_scores[i]}"
            for i in range(set_count)
            if winner_scores[i] and loser_scores[i]
        )

    return {
        "match_id": match_id,
        "round_name": round_name,
        "winner_player_name": winner["player_name"],
        "winner_player_id": player_lookup.get(normalize_name(winner["player_name"]), ""),
        "loser_player_name": loser["player_name"],
        "loser_player_id": player_lookup.get(normalize_name(loser["player_name"]), ""),
        "winner_country": winner["country"],
        "loser_country": loser["country"],
        "score_string": score_string,
    }


def scrape_from_html(html: str, player_lookup: dict, start_match_id: int = 16, already_seen_rounds=None):
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen_rounds = already_seen_rounds if already_seen_rounds is not None else set()
    current_match_id = start_match_id

    round_containers = soup.select(".drawsheet-round-container")
    print(f"[debug] rounds trouvés dans le HTML: {len(round_containers)}")

    for round_container in round_containers:
        title_el = round_container.select_one(".drawsheet-round-container__round-title")
        round_name = clean_text(title_el.get_text(" ", strip=True)) if title_el else "Unknown round"

        if round_name in seen_rounds:
            continue
        seen_rounds.add(round_name)

        widgets = round_container.select(".drawsheet-widget-spacer .drawsheet-widget")
        print(f"[debug] {round_name}: {len(widgets)} widgets")

        for idx, widget in enumerate(widgets):
            match_id = f"LS{current_match_id:03d}"
            match = extract_match_from_widget(widget, player_lookup, match_id, round_name)
            if match is None:
                print(f"[debug] {round_name} / widget {idx}: ignoré")
                continue

            results.append(match)
            print(
                f"[debug] {match_id} [{round_name}]: "
                f"{match['winner_player_name']} ({match['winner_country']}) "
                f"vs {match['loser_player_name']} ({match['loser_country']}) "
                f"=> {match['score_string']}"
            )
            current_match_id += 1

    return results, current_match_id, seen_rounds


def wait_until_draw_is_ready(page, timeout_ms: int = 60000):
    deadline = time.time() + timeout_ms / 1000.0
    last_count = 0

    while time.time() < deadline:
        try:
            widgets = page.locator(".drawsheet-widget-spacer .drawsheet-widget")
            count = widgets.count()
            last_count = count

            if count > 0:
                sample_count = min(count, 10)
                for i in range(sample_count):
                    widget = widgets.nth(i)
                    try:
                        first_name = (widget.locator(".drawsheet-widget__first-name").first.text_content() or "").strip()
                        last_name = (widget.locator(".drawsheet-widget__last-name").first.text_content() or "").strip()
                    except Exception:
                        first_name = ""
                        last_name = ""

                    if first_name or last_name:
                        return count
        except Exception:
            pass

        page.wait_for_timeout(1000)

    print(f"[warn] drawsheet non confirmé après {timeout_ms} ms, on continue quand même ({last_count} widgets vus).")
    return last_count


def click_anywhere_in_page_or_frames(page, locator_builder):
    # page principale
    try:
        loc = locator_builder(page)
        if loc.count() > 0:
            loc.first.click(timeout=3000)
            return True
    except Exception:
        pass

    # iframes
    for frame in page.frames:
        try:
            loc = locator_builder(frame)
            if loc.count() > 0:
                loc.first.click(timeout=3000)
                return True
        except Exception:
            continue

    return False


def dismiss_consent_popups(page, timeout_ms: int = 12000):
    """
    Essaie de fermer/valider les deux popups de consentement :
    - consentement de site / autorisation
    - cookies
    Le tout peut apparaître dans la page principale ou dans un iframe.
    """
    deadline = time.time() + timeout_ms / 1000.0
    attempts = 0

    while time.time() < deadline and attempts < 20:
        attempts += 1
        clicked_any = False

        for label in CONSENT_BUTTON_LABELS:
            def builder(scope, text=label):
                return scope.locator(
                    'button:has-text("{0}"), [role="button"]:has-text("{0}"), input[type="button"][value="{0}"]'.format(text)
                )

            if click_anywhere_in_page_or_frames(page, builder):
                clicked_any = True
                page.wait_for_timeout(1000)

        if not clicked_any:
            fallback_selectors = [
                'button:has-text("Allow all cookies")',
                'button:has-text("Accept all cookies")',
                'button:has-text("Accept")',
                'button:has-text("I agree")',
            ]
            for selector in fallback_selectors:
                if click_anywhere_in_page_or_frames(page, lambda scope, sel=selector: scope.locator(sel)):
                    clicked_any = True
                    page.wait_for_timeout(1000)
                    break

        if not clicked_any:
            return

    page.wait_for_timeout(500)


def click_round(page, label: str):
    """
    Clique sur un onglet du carrousel par son libellé.
    """
    selectors = [
        f'.carousel-pager__item-wrapper:has-text("{label}")',
        f'.carousel-pager__label:has-text("{label}")',
    ]

    for selector in selectors:
        try:
            loc = page.locator(selector).first
            if loc.count() > 0:
                loc.click(timeout=10000)
                page.wait_for_timeout(1500)
                return
        except Exception:
            continue

    raise RuntimeError(f"Impossible de cliquer sur le round: {label}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL de la page à scraper")
    parser.add_argument("--csv", default="player_data_wta.csv", help="Chemin du CSV joueur")
    parser.add_argument("--output", default="matches.json", help="Fichier JSON de sortie")
    parser.add_argument("--start-match-id", type=int, default=16, help="Départ pour LS016")
    parser.add_argument("--event-id", type=int, required=True, help="ID événement à écrire dans le JSON")
    parser.add_argument("--event-year", type=int, required=True, help="Année événement à écrire dans le JSON")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSV introuvable: {csv_path}", file=sys.stderr)
        sys.exit(1)

    player_lookup = load_player_ids(str(csv_path))

    with sync_playwright() as p:
        print("[debug] lancement du navigateur...")
        browser = p.chromium.launch(
            headless=True,
            args=["--start-maximized"],
        )
        context = browser.new_context(no_viewport=True)
        page = context.new_page()

        try:
            print("[debug] ouverture de la page...")
            page.goto(args.url, wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(2000)

            # Les popups peuvent apparaître immédiatement après le chargement.
            dismiss_consent_popups(page)

            seen_rounds = set()
            current_match_id = args.start_match_id
            all_matches = []

            # État initial : rounds 1 et 2 visibles par défaut
            wait_until_draw_is_ready(page, timeout_ms=60000)
            html = page.content()
            matches, current_match_id, seen_rounds = scrape_from_html(
                html,
                player_lookup,
                start_match_id=current_match_id,
                already_seen_rounds=seen_rounds,
            )
            all_matches.extend(matches)

            # On clique sur Quarter-finals pour faire apparaître les rounds 3 et 4
            print("[debug] clic sur Quarter-finals...")
            dismiss_consent_popups(page)
            click_round(page, "Quarter-finals")
            dismiss_consent_popups(page)
            wait_until_draw_is_ready(page, timeout_ms=60000)
            html = page.content()
            matches, current_match_id, seen_rounds = scrape_from_html(
                html,
                player_lookup,
                start_match_id=current_match_id,
                already_seen_rounds=seen_rounds,
            )
            all_matches.extend(matches)

            # On clique sur Final pour faire apparaître le dernier round
            print("[debug] clic sur Final...")
            dismiss_consent_popups(page)
            click_round(page, "Final")
            dismiss_consent_popups(page)
            wait_until_draw_is_ready(page, timeout_ms=60000)
            html = page.content()
            matches, current_match_id, seen_rounds = scrape_from_html(
                html,
                player_lookup,
                start_match_id=current_match_id,
                already_seen_rounds=seen_rounds,
            )
            all_matches.extend(matches)

            if not all_matches:
                raise RuntimeError("Aucun match valide n'a été extrait. Le JSON ne sera pas écrit.")

            payload = {
                "event_id": args.event_id,
                "event_year": args.event_year,
                "matches": all_matches,
            }

            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            print(f"{len(all_matches)} matchs écrits dans {args.output}")

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()