#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


CSV_HEADERS = [h.strip() for h in """
tourney_id,tourney_year,tourney_name,level,start_date,end_date,surface,city,country,singles_draw_size,prize_money,prize_money_currency,match_id,date,round,winner,loser,winner_country,loser_country,winner_seed,loser_seed,set1_score,set2_score,set3_score,indoor_outdoor,player_id_winner,player_id_loser,event_id,event_year,player_a,player_b,country_a,country_b,seed_a,seed_b,winner_flag_raw,winner_player_name,loser_player_name,num_sets,score_string,match_timestamp,match_date,match_time_total,tournament_name,tournament_title,liveScoringId,venue_id,venue_name,PlayerIDA,PlayerIDA2,PlayerIDB,PlayerIDB2,settime_set1,winner_score_set1,loser_score_set1,winner_scoret_set1,loser_scoret_set1,winner_aces_set1,loser_aces_set1,winner_dblflt_set1,loser_dblflt_set1,winner_ptswon1stserv_set1,loser_ptswon1stserv_set1,winner_ptsplayed1stserv_set1,loser_ptsplayed1stserv_set1,winner_ptstotwonserv_set1,loser_ptstotwonserv_set1,winner_totservplayed_set1,loser_totservplayed_set1,winner_breakptsconv_set1,loser_breakptsconv_set1,winner_breakptsplayed_set1,loser_breakptsplayed_set1,winner_servgamesplayed_set1,loser_servgamesplayed_set1,winner_pts1stservlost_set1,loser_pts1stservlost_set1,winner_totptswon_set1,loser_totptswon_set1,winner_acesss_set1,loser_acesss_set1,settime_set2,winner_score_set2,loser_score_set2,winner_scoret_set2,loser_scoret_set2,winner_aces_set2,loser_aces_set2,winner_dblflt_set2,loser_dblflt_set2,winner_ptswon1stserv_set2,loser_ptswon1stserv_set2,winner_ptsplayed1stserv_set2,loser_ptsplayed1stserv_set2,winner_ptstotwonserv_set2,loser_ptstotwonserv_set2,winner_totservplayed_set2,loser_totservplayed_set2,winner_breakptsconv_set2,loser_breakptsconv_set2,winner_breakptsplayed_set2,loser_breakptsplayed_set2,winner_servgamesplayed_set2,loser_servgamesplayed_set2,winner_pts1stservlost_set2,loser_pts1stservlost_set2,winner_totptswon_set2,loser_totptswon_set2,winner_acesss_set2,loser_acesss_set2,settime_set3,winner_score_set3,loser_score_set3,winner_scoret_set3,loser_scoret_set3,winner_aces_set3,loser_aces_set3,winner_dblflt_set3,loser_dblflt_set3,winner_ptswon1stserv_set3,loser_ptswon1stserv_set3,winner_ptsplayed1stserv_set3,loser_ptsplayed1stserv_set3,winner_ptstotwonserv_set3,loser_ptstotwonserv_set3,winner_totservplayed_set3,loser_totservplayed_set3,winner_breakptsconv_set3,loser_breakptsconv_set3,winner_breakptsplayed_set3,loser_breakptsplayed_set3,winner_servgamesplayed_set3,loser_servgamesplayed_set3,winner_pts1stservlost_set3,loser_pts1stservlost_set3,winner_totptswon_set3,loser_totptswon_set3,winner_acesss_set3,loser_acesss_set3,match_message,match_status,set4_score,set5_score,winner_flag,doublefaults_tot_winner,doublefaults_tot_loser,aces_tot_winner,aces_tot_loser,firstserve_dividend_tot_winner,firstserve_dividend_tot_loser,firstserve_divisor_tot_winner,firstserve_divisor_tot_loser,firstserve_percent_tot_winner,firstserve_percent_tot_loser,firstservepointswon_dividend_tot_winner,firstservepointswon_dividend_tot_loser,firstservepointswon_divisor_tot_winner,firstservepointswon_divisor_tot_loser,firstservepointswon_percent_tot_winner,firstservepointswon_percent_tot_loser,secondservepointswon_percent_tot_winner,secondservepointswon_dividend_tot_winner,secondservepointswon_divisor_tot_winner,secondservepointswon_percent_tot_loser,secondservepointswon_dividend_tot_loser,secondservepointswon_divisor_tot_loser,breakpointssaved_dividend_tot_winner,breakpointssaved_divisor_tot_winner,breakpointssaved_percent_tot_winner,servicegamesplayed_tot_winner,servicegamesplayed_tot_loser,serverating_tot_winner,serverating_tot_loser,totalservicepointswon_dividend_tot_winner,totalservicepointswon_percent_tot_winner,totalreturnpointswon_percent_tot_winner,totalpointswon_percent_tot_winner,tiebreak_set1_winner,tiebreak_set1_loser,tiebreak_set2_winner,tiebreak_set2_loser,tiebreak_set3_winner,tiebreak_set3_loser,player_winner,player_loser,country_winner,country_loser,seed_winner,seed_loser,serveratinglink_tot_winner,serveratinglink_tot_loser,breakpointssaved_percent_tot_loser,breakpointssaved_dividend_tot_loser,breakpointssaved_divisor_tot_loser
""".strip().split(",")]

ROUND_PRIORITY = {
    "final": 0,
    "semi finals": 1,
    "semi final": 1,
    "semifinals": 1,
    "semifinal": 1,
    "sf": 1,
    "quarter finals": 2,
    "quarter final": 2,
    "quarterfinals": 2,
    "quarterfinal": 2,
    "qf": 2,
    "2nd round": 3,
    "second round": 3,
    "round 2": 3,
    "2r": 3,
    "1st round": 4,
    "first round": 4,
    "round 1": 4,
    "1r": 4,
}

CONSENT_BUTTON_LABELS = [
    "Autoriser",
    "Tout autoriser",
    "Accepter",
    "Tout accepter",
    "J’accepte",
    "J'accepte",
    "Accept all",
    "Allow all",
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


def normalize_round_name(round_name: str) -> str:
    return normalize_name(round_name).replace("-", " ")


def round_priority(round_name: str) -> int:
    n = normalize_round_name(round_name)
    for key, prio in ROUND_PRIORITY.items():
        if key in n:
            return prio
    return 99


def round_code(round_name: str) -> str:
    n = normalize_round_name(round_name)
    if "final" in n and "semi" not in n and "quarter" not in n:
        return "F"
    if "semi" in n:
        return "S"
    if "quarter" in n:
        return "Q"
    if "2nd round" in n or "second round" in n or "round 2" in n or n == "2r":
        return "2R"
    if "1st round" in n or "first round" in n or "round 1" in n or n == "1r":
        return "1R"
    return clean_text(round_name)


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def parse_seed_text(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    m = re.search(r"\[(\d+)\]", value)
    if m:
        return m.group(1)
    m = re.search(r"(\d+)", value)
    if m:
        return m.group(1)
    return ""


def split_score_string(score_string: str):
    score_string = clean_text(score_string)
    if not score_string:
        return []
    return [s for s in score_string.split(" ") if s]


def parse_date(value: str):
    value = clean_text(value)
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def format_date(value):
    if not value:
        return ""
    return value.strftime("%Y-%m-%d")


def compute_match_date(match: dict, start_date_str: str, end_date_str: str) -> str:
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)
    rn = normalize_round_name(match.get("round_name", ""))

    if "final" in rn and "semi" not in rn and "quarter" not in rn:
        d = end_date
    elif "semi" in rn:
        d = (end_date - timedelta(days=1)) if end_date else None
    elif "quarter" in rn:
        d = (end_date - timedelta(days=2)) if end_date else None
    elif "2nd round" in rn or "second round" in rn or "round 2" in rn or rn == "2r":
        d = (end_date - timedelta(days=4)) if end_date else None
    elif "1st round" in rn or "first round" in rn or "round 1" in rn or rn == "1r":
        d = start_date or end_date
    else:
        d = None

    return format_date(d)


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


def extract_seed_from_team(team_wrapper) -> str:
    seed_el = team_wrapper.select_one(".drawsheet-widget__seeding")
    if not seed_el:
        return ""
    return parse_seed_text(seed_el.get_text(" ", strip=True))


def parse_score_cell(score_span):
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

    return f"{a}-{b}".strip("-")


def extract_team_info(team_wrapper):
    team_info = team_wrapper.select_one(".drawsheet-widget__team-info")
    is_winner = team_info is not None and "is-winner" in (team_info.get("class") or [])

    first = team_wrapper.select_one(".drawsheet-widget__first-name")
    last = team_wrapper.select_one(".drawsheet-widget__last-name")

    first_name = clean_text(first.get_text(" ", strip=True)) if first else ""
    last_name = clean_text(last.get_text(" ", strip=True)) if last else ""
    player_name = clean_text(f"{first_name} {last_name}")

    country_code = extract_country_code(team_wrapper)
    seed = extract_seed_from_team(team_wrapper)

    return {
        "player_name": player_name,
        "country": country_code,
        "seed": seed,
        "is_winner": is_winner,
    }


def extract_match_from_widget(widget, player_lookup: dict, round_name: str, round_position: int):
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

    score_parts = []
    for i in range(set_count):
        score = format_set_score(winner_score_spans[i], loser_score_spans[i])
        if score:
            score_parts.append(score)

    score_string = " ".join(score_parts)

    return {
        "round_name": round_name,
        "round_position": round_position,
        "winner_player_name": winner["player_name"],
        "winner_player_id": player_lookup.get(normalize_name(winner["player_name"]), ""),
        "loser_player_name": loser["player_name"],
        "loser_player_id": player_lookup.get(normalize_name(loser["player_name"]), ""),
        "winner_country": winner["country"],
        "loser_country": loser["country"],
        "winner_seed": winner["seed"],
        "loser_seed": loser["seed"],
        "score_string": score_string,
    }


def extract_matches_from_html(html: str, player_lookup: dict, wanted_rounds=None):
    soup = BeautifulSoup(html, "html.parser")
    results = []

    wanted_norm = None
    if wanted_rounds is not None:
        wanted_norm = {normalize_round_name(r) for r in wanted_rounds}

    round_containers = soup.select(".drawsheet-round-container")
    print(f"[debug] rounds trouvés dans le HTML: {len(round_containers)}")

    for round_container in round_containers:
        title_el = round_container.select_one(".drawsheet-round-container__round-title")
        round_name = clean_text(title_el.get_text(" ", strip=True)) if title_el else "Unknown round"

        if wanted_norm is not None and normalize_round_name(round_name) not in wanted_norm:
            continue

        widgets = round_container.select(".drawsheet-widget-spacer .drawsheet-widget")
        print(f"[debug] {round_name}: {len(widgets)} widgets")

        for idx, widget in enumerate(widgets):
            match = extract_match_from_widget(widget, player_lookup, round_name, idx)
            if match is None:
                print(f"[debug] {round_name} / widget {idx}: ignoré")
                continue

            results.append(match)
            print(
                f"[debug] {round_name} / {idx}: "
                f"{match['winner_player_name']} ({match['winner_country']}) "
                f"vs {match['loser_player_name']} ({match['loser_country']}) "
                f"=> {match['score_string']}"
            )

    return results


def deduplicate_matches(matches):
    seen = set()
    deduped = []

    for match in matches:
        p1 = normalize_name(match["winner_player_name"])
        p2 = normalize_name(match["loser_player_name"])
        pair_key = tuple(sorted([p1, p2]))
        sig = (
            normalize_round_name(match["round_name"]),
            pair_key[0],
            pair_key[1],
            match.get("score_string", ""),
        )
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(match)

    return deduped


def sort_matches_for_ls(matches):
    return sorted(
        matches,
        key=lambda m: (
            round_priority(m["round_name"]),
            m.get("round_position", 9999),
            normalize_name(m["winner_player_name"]),
            normalize_name(m["loser_player_name"]),
        ),
    )


def assign_ls_ids(matches, start_match_id: int = 1):
    sorted_matches = sort_matches_for_ls(deduplicate_matches(matches))
    for offset, match in enumerate(sorted_matches, start=start_match_id):
        match["match_id"] = f"LS{offset:03d}"
    return sorted_matches


def click_anywhere_in_page_or_frames(page, locator_builder):
    try:
        loc = locator_builder(page)
        if loc.count() > 0:
            loc.first.click(timeout=3000)
            return True
    except Exception:
        pass

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
                page.wait_for_timeout(800)

        if not clicked_any:
            return

    page.wait_for_timeout(300)


def wait_for_draw_ready(page, timeout_ms: int = 60000):
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


def get_round_wrapper(page, label: str):
    xpath = (
        "//div[contains(@class,'carousel-pager__item-wrapper')]"
        f"[.//span[contains(@class,'carousel-pager__label') and normalize-space(.)='{label}']]"
    )
    return page.locator(f"xpath={xpath}").first


def wait_for_round_active(page, label: str, timeout_ms: int = 20000) -> bool:
    deadline = time.time() + timeout_ms / 1000.0
    wrapper = get_round_wrapper(page, label)

    while time.time() < deadline:
        try:
            if wrapper.count() > 0 and wrapper.locator("div.carousel-pager__item.active").count() > 0:
                return True
        except Exception:
            pass

        page.wait_for_timeout(250)

    return False


def click_round(page, label: str):
    """
    Clique sur le vrai bouton du carrousel.
    On cible le wrapper contenant le label exact, puis on clique son .carousel-pager__item.
    La validation se fait via l'état actif du pager.
    """
    dismiss_consent_popups(page)

    wrapper = get_round_wrapper(page, label)
    if wrapper.count() == 0:
        raise RuntimeError(f"Bouton introuvable: {label}")

    item = wrapper.locator("div.carousel-pager__item").first

    try:
        item.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass

    click_attempts = [
        lambda: item.click(timeout=10000),
        lambda: item.click(timeout=10000, force=True),
        lambda: wrapper.click(timeout=10000),
        lambda: wrapper.click(timeout=10000, force=True),
        lambda: item.evaluate("(el) => el.click()"),
        lambda: wrapper.evaluate("(el) => el.click()"),
        lambda: page.evaluate(
            """
            (label) => {
                const wrappers = [...document.querySelectorAll('.carousel-pager__item-wrapper')];
                const wrapper = wrappers.find(w =>
                    [...w.querySelectorAll('.carousel-pager__label')]
                        .some(s => s.textContent.trim() === label)
                );
                if (!wrapper) return false;

                const item = wrapper.querySelector('.carousel-pager__item');
                if (!item) return false;

                const ev = new MouseEvent('click', { bubbles: true, cancelable: true, view: window });
                item.dispatchEvent(ev);
                wrapper.dispatchEvent(ev);
                return true;
            }
            """,
            label,
        ),
    ]

    last_error = None
    for attempt in click_attempts:
        try:
            attempt()
            page.wait_for_timeout(1000)

            if wait_for_round_active(page, label, timeout_ms=15000):
                return

        except Exception as e:
            last_error = e

    try:
        box = item.bounding_box()
        if box:
            page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            page.wait_for_timeout(1000)

            if wait_for_round_active(page, label, timeout_ms=15000):
                return
    except Exception as e:
        last_error = e

    raise RuntimeError(f"Impossible de cliquer sur le round: {label}") from last_error


def safe_goto(page, url: str, retries: int = 3, timeout_ms: int = 120000):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[debug] goto tentative {attempt}/{retries}...")
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(2000)
            return
        except Exception as e:
            last_error = e
            print(f"[warn] goto échoue ({attempt}/{retries}): {e}", file=sys.stderr)
            if attempt < retries:
                page.wait_for_timeout(2000)
            else:
                break
    raise last_error


def snapshot(page, player_lookup: dict, wanted_rounds=None):
    dismiss_consent_popups(page)
    wait_for_draw_ready(page, timeout_ms=60000)
    html = page.content()
    return extract_matches_from_html(html, player_lookup, wanted_rounds=wanted_rounds)


def load_tournaments(tournaments_path: str):
    with open(tournaments_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("tournaments", "data", "items", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]

    return []


def normalize_tournament_link(value: str) -> str:
    if not value:
        return ""
    value = clean_text(value)
    if value.startswith("http://") or value.startswith("https://"):
        value = urlparse(value).path
    value = re.sub(r"/+", "/", value)
    value = value.rstrip("/")
    return value.lower()


def strip_draws_and_results(path: str) -> str:
    path = normalize_tournament_link(path)
    path = re.sub(r"/draws-and-results$", "", path)
    return path


def tournament_slug_from_path(path: str) -> str:
    path = strip_draws_and_results(path)
    return path.rstrip("/").split("/")[-1] if path else ""


def iter_tournament_records(obj):
    if isinstance(obj, dict):
        if "tournamentGroup" in obj or "tournamentLink" in obj or "title" in obj:
            yield obj
        for v in obj.values():
            yield from iter_tournament_records(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_tournament_records(item)


def find_tournament_record(tournaments_data, page_url: str, event_year: int, event_id: str):
    target_path = strip_draws_and_results(page_url)
    target_slug = tournament_slug_from_path(page_url)
    target_year = str(event_year)
    target_event_id = clean_text(event_id)

    candidates = []

    for rec in iter_tournament_records(tournaments_data):
        if not isinstance(rec, dict):
            continue

        rec_year = str(rec.get("year", "")).strip()
        if rec_year != target_year:
            continue

        link = rec.get("tournamentLink") or rec.get("tournament_link") or ""
        link_path = strip_draws_and_results(link)
        link_slug = tournament_slug_from_path(link)

        if link_path and link_path == target_path:
            return rec

        if link_slug and link_slug == target_slug:
            return rec

        if target_event_id:
            if link_slug.endswith(target_event_id):
                candidates.append(rec)
                continue
            if f"w-itf-srb-{target_year}-{target_event_id}" in link_slug:
                candidates.append(rec)
                continue

    return candidates[0] if candidates else None


def tournament_id_from_record(record: dict, fallback_event_id: str) -> str:
    group = record.get("tournamentGroup") or {}
    group_id = group.get("id")
    if group_id is not None and str(group_id).strip():
        return str(group_id).strip()

    link = record.get("tournamentLink") or record.get("tournament_link") or ""
    link = normalize_tournament_link(link)
    if link:
        last = link.rstrip("/").split("/")[-1]
        if last:
            return last.split("-")[-1] if re.search(r"-\d+$", last) else last

    return clean_text(fallback_event_id)


def tournament_short_name(record: dict) -> str:
    group = record.get("tournamentGroup") or {}
    name = clean_text(group.get("name") or "")
    if name:
        return name.upper()
    title = clean_text(record.get("title") or "")
    if " - " in title:
        return title.split(" - ", 1)[1].split(",")[0].strip().upper()
    return title.upper()


def tournament_city(record: dict) -> str:
    city = clean_text(record.get("city") or "")
    if city:
        return city.upper()

    title = clean_text(record.get("title") or "")
    if " - " in title:
        candidate = title.split(" - ", 1)[1].split(",")[0].strip()
        if candidate:
            return candidate.upper()

    group = record.get("tournamentGroup") or {}
    return clean_text(group.get("name") or "").upper()


def tournament_level(record: dict) -> str:
    group = record.get("tournamentGroup") or {}
    level = clean_text(group.get("level") or "")
    if level:
        return level
    title = clean_text(record.get("title") or "")
    return title.split(" - ", 1)[0].strip() if " - " in title else title


def build_empty_row():
    return {h: "" for h in CSV_HEADERS}


def build_csv_row(match: dict, tournament: dict, tourney_id: str, tourney_year: str):
    row = build_empty_row()

    group = tournament.get("tournamentGroup") or {}

    tourney_name = tournament_short_name(tournament)
    tournament_title = clean_text(tournament.get("title") or "")
    level = tournament_level(tournament)
    start_date = clean_text(tournament.get("startDate") or "")
    end_date = clean_text(tournament.get("endDate") or "")
    surface = clean_text(tournament.get("surface") or "")
    city = tournament_city(tournament)
    country = clean_text(tournament.get("country") or "")
    singles_draw_size = tournament.get("singlesDrawSize", "")
    prize_money = tournament.get("prizeMoney", "")
    prize_money_currency = clean_text(tournament.get("prizeMoneyCurrency") or "")
    indoor_outdoor = clean_text(tournament.get("inOutdoor") or "")

    score_tokens = split_score_string(match.get("score_string", ""))
    set_scores = score_tokens[:5]
    num_sets = len(score_tokens)
    match_date = compute_match_date(match, start_date, end_date)

    winner_name = clean_text(match.get("winner_player_name") or "")
    loser_name = clean_text(match.get("loser_player_name") or "")
    winner_country = clean_text(match.get("winner_country") or "")
    loser_country = clean_text(match.get("loser_country") or "")
    winner_seed = clean_text(match.get("winner_seed") or "")
    loser_seed = clean_text(match.get("loser_seed") or "")
    winner_id = clean_text(match.get("winner_player_id") or "")
    loser_id = clean_text(match.get("loser_player_id") or "")

    row.update({
        "tourney_id": clean_text(tourney_id),
        "tourney_year": str(tourney_year),
        "tourney_name": tourney_name,
        "level": level,
        "start_date": start_date,
        "end_date": end_date,
        "surface": surface,
        "city": city,
        "country": country,
        "singles_draw_size": str(singles_draw_size),
        "prize_money": str(prize_money),
        "prize_money_currency": prize_money_currency,

        "match_id": match.get("match_id", ""),
        "date": match_date,
        "round": round_code(match.get("round_name", "")),

        "winner": winner_name,
        "loser": loser_name,
        "winner_country": winner_country,
        "loser_country": loser_country,
        "winner_seed": winner_seed,
        "loser_seed": loser_seed,

        "set1_score": set_scores[0] if len(set_scores) > 0 else "",
        "set2_score": set_scores[1] if len(set_scores) > 1 else "",
        "set3_score": set_scores[2] if len(set_scores) > 2 else "",
        "indoor_outdoor": indoor_outdoor,

        "player_id_winner": winner_id,
        "player_id_loser": loser_id,

        "event_id": clean_text(tourney_id),
        "event_year": str(tourney_year),

        "player_a": winner_name,
        "player_b": loser_name,
        "country_a": winner_country,
        "country_b": loser_country,
        "seed_a": winner_seed,
        "seed_b": loser_seed,

        "winner_flag_raw": winner_country,
        "winner_player_name": winner_name,
        "loser_player_name": loser_name,

        "num_sets": str(num_sets),
        "score_string": ",".join(score_tokens),

        "match_timestamp": "",
        "match_date": match_date,
        "match_time_total": "",

        "tournament_name": tourney_name,
        "tournament_title": tournament_title,
        "liveScoringId": clean_text(tournament.get("liveScoringId") or ""),
        "venue_id": "",
        "venue_name": "",

        "PlayerIDA": winner_id,
        "PlayerIDA2": "",
        "PlayerIDB": loser_id,
        "PlayerIDB2": "",

        "winner_score_set1": "",
        "loser_score_set1": "",
        "winner_scoret_set1": "",
        "loser_scoret_set1": "",
        "winner_score_set2": "",
        "loser_score_set2": "",
        "winner_scoret_set2": "",
        "loser_scoret_set2": "",
        "winner_score_set3": "",
        "loser_score_set3": "",
        "winner_scoret_set3": "",
        "loser_scoret_set3": "",

        "match_message": "",
        "match_status": "",

        "set4_score": "",
        "set5_score": "",

        "winner_flag": winner_country,

        "player_winner": winner_name,
        "player_loser": loser_name,
        "country_winner": winner_country,
        "country_loser": loser_country,
        "seed_winner": winner_seed,
        "seed_loser": loser_seed,
    })

    for idx, token in enumerate(set_scores[:3], start=1):
        m = re.fullmatch(r"(\d+)-(\d+)(?:\((\d+)\))?", token or "")
        if m:
            row[f"winner_score_set{idx}"] = m.group(1)
            row[f"loser_score_set{idx}"] = m.group(2)
            if m.group(3):
                row[f"winner_scoret_set{idx}"] = "7"
                row[f"loser_scoret_set{idx}"] = m.group(3)
                row[f"tiebreak_set{idx}_winner"] = "7"
                row[f"tiebreak_set{idx}_loser"] = m.group(3)

    return row


def resolve_output_path(tourney_id: str, tourney_year: str) -> Path:
    return Path("docs/matches/wta_matches") / f"wta_{tourney_id}_{tourney_year}.csv"


def write_csv(rows, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_HEADERS})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL de la page à scraper")
    parser.add_argument("--csv", default="player_data_wta.csv", help="Chemin du CSV joueur")
    parser.add_argument("--tournaments", default="docs/wta_tournaments_2026.json", help="JSON des tournois")
    parser.add_argument("--output", default=None, help="Conservé pour compatibilité, mais ignoré")
    parser.add_argument("--start-match-id", type=int, default=1, help="Départ pour LS001")
    parser.add_argument("--event-id", required=True, help="ID événement à écrire dans le CSV")
    parser.add_argument("--event-year", type=int, required=True, help="Année événement à écrire dans le CSV")
    args = parser.parse_args()

    player_csv_path = Path(args.csv)
    if not player_csv_path.exists():
        print(f"CSV joueurs introuvable: {player_csv_path}", file=sys.stderr)
        sys.exit(1)

    tournaments_path = Path(args.tournaments)
    if not tournaments_path.exists():
        print(f"JSON tournois introuvable: {tournaments_path}", file=sys.stderr)
        sys.exit(1)

    player_lookup = load_player_ids(str(player_csv_path))
    tournaments = load_tournaments(str(tournaments_path))
    tournament = find_tournament_record(tournaments, args.url, args.event_year, args.event_id)

    if not tournament:
        print(
            f"[debug] page_url normalisée : {strip_draws_and_results(args.url)}",
            file=sys.stderr,
        )
        print(
            f"Tournoi introuvable dans {tournaments_path} pour l'URL {args.url} "
            f"(year={args.event_year}, event_id={args.event_id})",
            file=sys.stderr,
        )
        sys.exit(1)

    tourney_id = tournament_id_from_record(tournament, args.event_id)
    tourney_year = str(tournament.get("year") or args.event_year)
    output_path = resolve_output_path(tourney_id, tourney_year)

    with sync_playwright() as p:
        print("[debug] lancement du navigateur...")
        browser = p.chromium.launch(
            headless=False,
            args=["--start-maximized"],
        )
        context = browser.new_context(no_viewport=True)
        page = context.new_page()
        page.set_default_timeout(10000)

        try:
            print("[debug] ouverture de la page...")
            safe_goto(page, args.url, retries=3, timeout_ms=120000)

            all_matches = []

            print("[debug] snapshot initial...")
            all_matches.extend(snapshot(page, player_lookup, wanted_rounds={"1st Round", "2nd Round"}))

            print("[debug] clic sur Quarter-finals...")
            click_round(page, "Quarter-finals")
            all_matches.extend(snapshot(page, player_lookup, wanted_rounds={"Quarter-finals", "Semi-finals"}))

            print("[debug] clic sur Final...")
            click_round(page, "Final")
            all_matches.extend(snapshot(page, player_lookup, wanted_rounds={"Semi-finals", "Final"}))

            if not all_matches:
                raise RuntimeError("Aucun match valide n'a été extrait. Le CSV ne sera pas écrit.")

            final_matches = assign_ls_ids(all_matches, start_match_id=args.start_match_id)

            csv_rows = [
                build_csv_row(match, tournament, tourney_id=tourney_id, tourney_year=tourney_year)
                for match in final_matches
            ]

            write_csv(csv_rows, output_path)

            print(f"{len(csv_rows)} matchs écrits dans {output_path}")

        finally:
            context.close()
            browser.close()


if __name__ == "__main__":
    main()