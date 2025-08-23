#!/usr/bin/env python3
"""
Generate only changed player pages between old_csv and new_csv.
Usage:
  python scripts/generate_players_incremental.py --old old.csv --new player_data_wta.csv
If --old is missing, it will generate all pages (fallback).
"""
import argparse
from pathlib import Path
import pandas as pd
import html, re
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs" / "players"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def safe_slug(name: str) -> str:
    s = (name or "unknown").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s or "player"

def parse_date_only(s):
    if not s:
        return ""
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return s.split()[0]
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return s.split()[0] if isinstance(s, str) else ""

def parse_height_cm(row):
    hc = row.get("height_cm", "")
    if isinstance(hc, str) and hc.strip().endswith("m"):
        try:
            return float(hc.strip().replace("m","")) * 100
        except:
            pass
    if pd.notna(hc) and hc != "-":
        try:
            return float(hc)
        except:
            pass
    hi = row.get("height_inches", "")
    if isinstance(hi, str) and hi.strip():
        m = re.search(r"(\d+)\D+(\d+)", hi)
        if m:
            feet = int(m.group(1)); inches = int(m.group(2))
            total_in = feet*12 + inches
            return round(total_in * 2.54, 1)
    return None

PLAYER_TMPL = """(copie ta template PLAYER_TMPL depuis generate_players.py)"""
INDEX_TOP = """(copie INDEX_TOP depuis generate_players.py)"""
INDEX_BOTTOM = """(copie INDEX_BOTTOM depuis generate_players.py)"""

def esc(s):
    if pd.isna(s) or s is None:
        return ""
    return html.escape(str(s))

def build_page(row, slug):
    name = (row.get("full_name","") or "").strip()
    birthplace = row.get("birthplace", "") or ""
    birth_date = parse_date_only(row.get("birth_date",""))
    plays = row.get("plays","")
    best_rank = row.get("best_rank","")
    first_app = parse_date_only(row.get("first_appearance",""))
    last_app = parse_date_only(row.get("last_appearance",""))
    country = row.get("represented_country","")
    hcm = parse_height_cm(row)
    if hcm:
        htxt = f"{hcm:.1f} cm"
    else:
        htxt = row.get("height_inches","") or row.get("height_cm","") or ""

    content = PLAYER_TMPL.format(
        esc_name = esc(name),
        esc_country = esc(country),
        birth_date = esc(birth_date),
        esc_birthplace = esc(birthplace),
        height = esc(htxt),
        plays = esc(plays),
        best_rank = esc(best_rank),
        first_appearance = esc(first_app),
        last_appearance = esc(last_app)
    )
    out_file = OUT_DIR / f"{slug}.html"
    out_file.write_text(content, encoding="utf-8")
    return out_file

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--old", default=None)
    p.add_argument("--new", required=True)
    args = p.parse_args()

    new_df = pd.read_csv(args.new, dtype=str).fillna("")
    new_df['player_id'] = new_df['player_id'].astype(str)

    if args.old and Path(args.old).exists():
        old_df = pd.read_csv(args.old, dtype=str).fillna("")
        old_df['player_id'] = old_df['player_id'].astype(str)
        # merge on player_id if present, otherwise on full_name
        key = 'player_id' if 'player_id' in old_df.columns else 'full_name'
        merged = new_df.merge(old_df, how='left', on=key, suffixes=('', '_old'), indicator=True)
        changed = []
        for _, r in merged.iterrows():
            pid = r.get('player_id') or r.get('full_name')
            # if new row (_merge == 'left_only') or any column differs -> mark changed
            if r['_merge'] == 'left_only':
                changed.append(pid)
                continue
            # compare significant columns
            cols = ['full_name','birth_date','birthplace','height_inches','height_cm','plays','best_rank','first_appearance','last_appearance','represented_country']
            diff = False
            for c in cols:
                a = (r.get(c) or "").strip()
                b = (r.get(c + '_old') or "").strip()
                if a != b:
                    diff = True; break
            if diff:
                changed.append(pid)
        # filter new_df for changed rows
        if not changed:
            print("No changed players detected.")
            changed_rows = new_df.iloc[0:0]
        else:
            # select rows corresponding to changed player_ids
            changed_rows = new_df[new_df['player_id'].isin(changed)]
            print(f"Detected {len(changed_rows)} changed/new players.")
    else:
        print("No old CSV provided or not found: generating all pages.")
        changed_rows = new_df
    # build unique slugs similarly to generate_players
    seen_slugs = set([p.stem for p in OUT_DIR.glob("*.html")])
    index_entries = []
    # load full new_df to build index (we need index update)
    full = new_df
    for _, row in changed_rows.iterrows():
        name = (row.get("full_name","") or "").strip()
        if not name:
            continue
        base = safe_slug(name)
        slug = base
        n = 1
        while slug in seen_slugs:
            n += 1
            slug = f"{base}-{n}"
        seen_slugs.add(slug)
        build_page(row, slug)
        # index entry will be regenerated below using full DF
    # regenerate index for ALL players (safe)
    players_index_lines = []
    seen_slugs = set()
    for _, row in full.iterrows():
        name = (row.get("full_name","") or "").strip()
        if not name:
            continue
        base = safe_slug(name)
        slug = base
        n = 1
        while slug in seen_slugs or (OUT_DIR / f"{slug}.html").exists() is False:
            # if page doesn't exist for this slug, keep suffixing until a present file is found
            if (OUT_DIR / f"{slug}.html").exists():
                if slug in seen_slugs:
                    n += 1
                    slug = f"{base}-{n}"
                else:
                    seen_slugs.add(slug)
                    break
            else:
                # try to find a unique slug name (best-effort)
                n += 1
                slug = f"{base}-{n}"
        seen_slugs.add(slug)
        country = row.get("represented_country","")
        entry = f'<a class="list-group-item list-group-item-action" href="{slug}.html" data-name="{html.escape(name)}">{html.escape(name)} <small class="text-muted">({html.escape(country)})</small></a>'
        players_index_lines.append(entry)

    index_html = INDEX_TOP + "\n".join(players_index_lines) + INDEX_BOTTOM
    (OUT_DIR / "index.html").write_text(index_html, encoding="utf-8")
    print("Index regenerated.")

if __name__ == "__main__":
    main()
