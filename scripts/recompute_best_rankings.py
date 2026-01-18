#!/usr/bin/env python3
"""
Recompute best rankings for active players and update player_data CSV(s) AND player HTML files
in docs/players (WTA) or docs/players_atp (ATP).

Behavior summary (high level):
 - Read all CSV files data_*.csv from --rankings-dir and concat them.
 - Detect whether ranking rows expose a player id or only names. Group by id or by normalized name
   and compute the best (min) rank observed for each player across all ranking files.
 - Detect the latest date present among ranking files; if found, only players present on that latest
   date are considered "active" (unless --include-inactive is passed).
 - Update player CSV (--csv) the same way as before (keeps existing behavior), and also update
   corresponding player HTML files under a matching docs directory (auto-detected, or overridden
   with --html-dir).

Matching HTML files:
 - We attempt to match by player_id first (if ranking rows provide ids). The script scans each
   HTML for an embedded 'player=' query parameter (e.g. in the "Suggérer une modification" link) and
   uses that token to map file -> id.
 - If no id match is found, the script falls back to matching by normalized player name extracted from
   the <h1 class="card-title"> or <title> tag.

HTML update rules:
 - Finds a <dt>...</dt> whose text contains the words "best" or "highest" (case-insensitive)
   and replaces the following <dd>...</dd> content with the computed value (or empty string when sentinel).
 - If no suitable dt/dd pair is found we log a warning and skip that file.

Options:
  --rankings-dir : directory with data_YYYY_MM_DD.csv files (required)
  --csv : path to player_data CSV to update (optional - it will try update if given)
  --sentinel : sentinel value for missing ranks (default 9999999)
  --dry-run : do not write CSV or HTML, just print summary
  --html-dir : override HTML players directory (optional); otherwise auto-detects:
       if 'wta' in rankings-dir name -> docs/players
       elif 'atp' in rankings-dir name -> docs/players_atp
  --include-inactive : update players even if they're not in the latest ranking date
"""

import argparse
from pathlib import Path
import pandas as pd
import sys
import logging
import re
from typing import Tuple, Dict, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

RANK_COL_CANDIDATES = ["rank", "ranking", "position", "pos", "best_rank", "highest_ranking", "highest_rank", "rank_value"]
PID_CANDIDATES = ["player_id", "player id", "playerid", "id", "pid"]
NAME_CANDIDATES = ["full_name", "fullname", "full name", "name", "player_name", "player name"]


def detect_rank_col(df: pd.DataFrame):
    for c in RANK_COL_CANDIDATES:
        if c in df.columns:
            return c
    for c in df.columns:
        if "rank" in c.lower() or "position" in c.lower():
            return c
    return None


def detect_pid_or_name_col(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    for c in PID_CANDIDATES:
        if c in df.columns:
            return ("id", c)
    for c in NAME_CANDIDATES:
        if c in df.columns:
            return ("name", c)
    cols_lower = {col.lower(): col for col in df.columns}
    for c in PID_CANDIDATES:
        if c.lower() in cols_lower:
            return ("id", cols_lower[c.lower()])
    for c in NAME_CANDIDATES:
        if c.lower() in cols_lower:
            return ("name", cols_lower[c.lower()])
    return (None, None)


def normalize_text(s):
    if pd.isna(s) or s is None:
        return ""
    return re.sub(r"\s+", " ", str(s).strip()).casefold()


# HTML helpers
PLAYER_QUERY_RE = re.compile(r"[?&]player=([^&'\"]+)")
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
# dt/dd pair where dt contains 'best' or 'highest'
BEST_DTPLAIN_RE = re.compile(r"(<dt[^>]*>\s*([^<]*?)\s*</dt>\s*<dd[^>]*>)([^<]*)(</dd>)", re.IGNORECASE | re.DOTALL)


def build_html_mappings(html_dir: Path) -> Tuple[Dict[str, Path], Dict[str, Path]]:
    """Scan html_dir and build two mappings:
       - id_to_path: token after 'player=' -> Path
       - name_to_path: normalized h1/title -> Path
    """
    id_map = {}
    name_map = {}
    if not html_dir.exists() or not html_dir.is_dir():
        logging.warning("HTML dir %s does not exist; will skip HTML updates", html_dir)
        return id_map, name_map

    for p in sorted(html_dir.glob("*.html")):
        try:
            txt = p.read_text(encoding="utf-8")
        except Exception:
            try:
                txt = p.read_text(encoding="latin-1")
            except Exception:
                logging.warning("Could not read %s", p)
                continue
        # try extract player= token
        m = PLAYER_QUERY_RE.search(txt)
        if m:
            token = m.group(1)
            if token:
                id_map[token] = p
        # extract h1
        mh = H1_RE.search(txt)
        name = None
        if mh:
            name = mh.group(1).strip()
        else:
            mt = TITLE_RE.search(txt)
            if mt:
                # often title like 'Name — Player Profile'
                # split on '—' or '-' and take first part
                t = mt.group(1).strip()
                name = re.split(r"[\u2013\u2014\-–—]", t)[0].strip()
        if name:
            name_map[normalize_text(name)] = p
    return id_map, name_map


def replace_best_in_html(path: Path, new_val: Optional[int], sentinel: int) -> bool:
    """Replace the best/highest rank value in a single HTML file. Returns True if modified.
    If new_val is None or >= sentinel, we write an empty string into the dd.
    """
    try:
        txt = path.read_text(encoding="utf-8")
    except Exception:
        try:
            txt = path.read_text(encoding="latin-1")
        except Exception:
            logging.warning("Failed to read %s for updating", path)
            return False

    # search for a dt/dd pair where the dt contains 'best' or 'highest'
    def _dt_repl(m):
        dt_full = m.group(1)
        dt_label = m.group(2) or ""
        # decide if this is the field we want
        if re.search(r"\b(best|highest)\b", dt_label, re.IGNORECASE):
            new_txt = "" if new_val is None or int(new_val) >= sentinel else str(int(new_val))
            return f"{m.group(1)}{new_txt}{m.group(4)}"
        return m.group(0)

    new_txt, nsubs = BEST_DTPLAIN_RE.subn(_dt_repl, txt, count=1)
    if nsubs == 0:
        # fallback: try to find explicit labels in html (e.g. '>Best rank<')
        # naive insertion approach: look for 'Best rank' or 'Highest ranking' text and replace following <dd>...
        pattern = re.compile(
            r"(<dt[^>]*>\s*(?:Best rank|Highest ranking|best rank|highest ranking)[^<]*</dt>\s*<dd[^>]*>)([^<]*)(</dd>)",
            re.IGNORECASE,
        )

        # safe repl function to avoid f-string quoting issues
        def _fallback_repl(m):
            val = "" if new_val is None or int(new_val) >= sentinel else str(int(new_val))
            return f"{m.group(1)}{val}{m.group(3)}"

        new_txt, nsubs = pattern.subn(_fallback_repl, txt, count=1)
    if nsubs == 0:
        logging.warning("No best/highest field found in %s; skipping HTML update", path)
        return False

    try:
        path.write_text(new_txt, encoding="utf-8")
        return True
    except Exception as e:
        logging.error("Failed to write updated HTML %s: %s", path, e)
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rankings-dir", required=True, help="Directory containing data_YYYY_MM_DD.csv files")
    p.add_argument("--csv", required=False, help="player_data csv to update (optional)")
    p.add_argument("--sentinel", type=int, default=9999999, help="sentinel value for missing ranks")
    p.add_argument("--dry-run", action="store_true", help="do not write CSV or HTML, just print summary")
    p.add_argument("--html-dir", required=False, help="override html players dir (optional)")
    p.add_argument("--include-inactive", action="store_true", help="also update players not present on latest ranking date")
    args = p.parse_args()

    rankings_dir = Path(args.rankings_dir)
    if not rankings_dir.exists() or not rankings_dir.is_dir():
        logging.error("rankings_dir %s does not exist or is not a directory", rankings_dir)
        sys.exit(1)

    files = sorted(rankings_dir.glob("data_*.csv"))
    if not files:
        logging.warning("No ranking files found in %s", rankings_dir)
        sys.exit(0)

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str).fillna("")
            df["__src_file"] = f.name
            frames.append(df)
        except Exception as e:
            logging.warning("Could not read %s: %s", f, e)
    if not frames:
        logging.warning("No readable ranking frames in %s", rankings_dir)
        sys.exit(0)

    ranks_df = pd.concat(frames, ignore_index=True)
    rank_col = detect_rank_col(ranks_df)
    id_type, id_col = detect_pid_or_name_col(ranks_df)

    if id_col is None:
        logging.error("Could not detect player id or name column in ranking files (tried common names). Columns: %s", list(ranks_df.columns))
        sys.exit(1)
    logging.info("Detected id column type=%s col=%s", id_type, id_col)

    if rank_col is None:
        logging.error("Could not detect numeric rank column in ranking files. Columns: %s", list(ranks_df.columns))
        sys.exit(1)
    logging.info("Detected rank column: %s", rank_col)

    ranks_df["__rank_num"] = pd.to_numeric(ranks_df[rank_col].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    valid_ranks = ranks_df.dropna(subset=["__rank_num"]).copy()
    valid_ranks["__rank_num"] = valid_ranks["__rank_num"].astype(int)

    if id_type == "id":
        group_key = id_col
        best = valid_ranks.groupby(group_key)["__rank_num"].min().rename("best_rank_computed").reset_index()
        logging.info("Computed best ranks (by id) for %d players from %d ranking rows", best.shape[0], len(valid_ranks))
    else:
        valid_ranks["_norm_name"] = valid_ranks[id_col].apply(normalize_text)
        best = valid_ranks.groupby("_norm_name")["__rank_num"].min().rename("best_rank_computed").reset_index()
        logging.info("Computed best ranks (by name) for %d name-keys from %d ranking rows", best.shape[0], len(valid_ranks))

    # detect latest date (if present)
    if "date" in ranks_df.columns:
        ranks_df["__date_parsed"] = pd.to_datetime(ranks_df["date"], errors="coerce")
    else:
        ranks_df["__date_parsed"] = pd.to_datetime(ranks_df["__src_file"].str.extract(r"data_(\d{4}_\d{2}_\d{2})")[0].str.replace("_","-"), errors="coerce")
    latest_date = ranks_df["__date_parsed"].max()
    if pd.isna(latest_date):
        logging.warning("Could not determine latest ranking date; will treat all players as potentially active")
        active_keys = None
    else:
        latest_mask = ranks_df["__date_parsed"] == latest_date
        if id_type == "id":
            active_keys = set(ranks_df.loc[latest_mask, id_col].astype(str))
        else:
            active_keys = set(ranks_df.loc[latest_mask, id_col].apply(normalize_text).astype(str))
        logging.info("Latest ranking date detected: %s -> active keys: %d", latest_date.date(), len(active_keys))

    # Prepare HTML directory mapping
    html_dir = None
    if args.html_dir:
        html_dir = Path(args.html_dir)
    else:
        name_lower = rankings_dir.name.lower()
        if "wta" in name_lower:
            html_dir = Path("docs/players")
        elif "atp" in name_lower:
            html_dir = Path("docs/players_atp")

    if html_dir is None:
        logging.info("Could not auto-detect html target dir (rankings-dir name not containing 'wta' or 'atp'); HTML updates will be skipped unless --html-dir provided")

    # Build HTML mappings if needed
    id_to_html = {}
    name_to_html = {}
    if html_dir is not None:
        id_to_html, name_to_html = build_html_mappings(html_dir)
        logging.info("Discovered %d html files (name keys=%d, id keys=%d)", len(list(html_dir.glob('*.html'))) if html_dir.exists() else 0, len(name_to_html), len(id_to_html))

    # Load players CSV if provided
    players_df = None
    player_id_col = None
    player_name_col = None
    player_rank_col = None
    if args.csv:
        players_path = Path(args.csv)
        if not players_path.exists():
            logging.error("Players CSV not found: %s", players_path)
            sys.exit(1)
        players_df = pd.read_csv(players_path, dtype=str).fillna("")
        # find id/name columns in players_df
        for c in ("player_id","player id","id","pid"):
            if c in players_df.columns:
                player_id_col = c
                break
        for c in ("full_name","fullname","name","player_name","player name"):
            if c in players_df.columns:
                player_name_col = c
                break
        logging.info("Players CSV columns: id_col=%s name_col=%s", player_id_col, player_name_col)
        # detect column to write best ranking
        candidate_player_rank_cols = ["highest_ranking","best_rank","best rank","bestRank","highest_rank"]
        for c in candidate_player_rank_cols:
            if c in players_df.columns:
                player_rank_col = c
                break
        if player_rank_col is None:
            player_rank_col = "best_rank"
            players_df[player_rank_col] = ""

    updated_csv_rows = 0
    updated_html_files = 0
    skipped_html = 0

    # Build helper maps from ranking frames to map id -> normalized name (useful if we need fallback)
    id_to_norm_name = {}
    if id_type == "id":
        name_col_candidates = [c for c in ranks_df.columns if c.lower() in ("full_name","fullname","player","player_name","player name")] 
        name_col = name_col_candidates[0] if name_col_candidates else None
        if name_col:
            for _, r in ranks_df.iterrows():
                pid = str(r.get(id_col, "")).strip()
                if pid and name_col in r and r[name_col]:
                    id_to_norm_name.setdefault(pid, normalize_text(r[name_col]))

    # Build mapping dicts for computed best ranks
    if id_type == "id":
        best_dict = {str(r[id_col]): int(r["best_rank_computed"]) for _, r in best.iterrows()}
    else:
        best_dict = {str(r["_norm_name"]): int(r["best_rank_computed"]) for _, r in best.iterrows()}

    # Update players_df if present
    if players_df is not None:
        for idx, row in players_df.iterrows():
            key_id = str(row.get(player_id_col, "")).strip() if player_id_col else ""
            key_name = normalize_text(row.get(player_name_col, "")) if player_name_col else ""

            # choose whether to update this row (active only unless --include-inactive)
            if not args.include_inactive and (active_keys is not None):
                if id_type == "id":
                    if not key_id or key_id not in active_keys:
                        continue
                else:
                    if not key_name or key_name not in active_keys:
                        continue

            computed = None
            if id_type == "id":
                computed = best_dict.get(key_id)
                # fallback: try mapping using ranks_df name map
                if computed is None and key_id in id_to_norm_name:
                    computed = best_dict.get(id_to_norm_name[key_id])
            else:
                computed = best_dict.get(key_name)

            if computed is None:
                computed = args.sentinel

            cur_raw = str(row.get(player_rank_col, "")).strip()
            cur_digits = re.sub(r"[^\d]", "", cur_raw) if cur_raw else ""
            try:
                cur_val = int(cur_digits) if cur_digits else args.sentinel
            except Exception:
                cur_val = args.sentinel
            if int(computed) != cur_val:
                # update dataframe
                players_df.at[idx, player_rank_col] = "" if int(computed) >= args.sentinel else str(int(computed))
                updated_csv_rows += 1

    # Update HTML files: iterate best_dict items and map to HTML files
    if html_dir is not None and best_dict:
        for key, computed_val in best_dict.items():
            # if active filtering is on
            if not args.include_inactive and active_keys is not None:
                if id_type == "id":
                    if key not in active_keys:
                        continue
                else:
                    if key not in active_keys:
                        continue

            target_path = None
            if id_type == "id":
                # direct id match
                if key in id_to_html:
                    target_path = id_to_html[key]
                else:
                    # fallback: try map id -> normalized name -> name_to_html
                    nm = id_to_norm_name.get(key)
                    if nm and nm in name_to_html:
                        target_path = name_to_html[nm]
            else:
                # name-based key already normalized
                if key in name_to_html:
                    target_path = name_to_html[key]

            if not target_path:
                skipped_html += 1
                logging.debug("No html file found for key %s", key)
                continue

            if args.dry_run:
                logging.info("DRY RUN: would update HTML %s -> %s", target_path, computed_val)
                updated_html_files += 1
            else:
                ok = replace_best_in_html(target_path, computed_val, args.sentinel)
                if ok:
                    updated_html_files += 1

    logging.info("Summary: updated_csv_rows=%d updated_html_files=%d skipped_html=%d", updated_csv_rows, updated_html_files, skipped_html)

    # write CSV if requested
    if not args.dry_run and players_df is not None and args.csv:
        players_path = Path(args.csv)
        backup = players_path.with_suffix(players_path.suffix + ".bak")
        try:
            if backup.exists():
                backup.unlink()
            players_path.rename(backup)
            players_df.to_csv(players_path, index=False)
            logging.info("Wrote updated players CSV %s (backup at %s)", players_path, backup)
        except Exception as e:
            logging.error("Failed to write players CSV: %s", e)
            if backup.exists():
                backup.rename(players_path)
            sys.exit(1)

    if args.dry_run:
        logging.info("Dry-run: not writing CSV/HTML")


if __name__ == "__main__":
    main()
