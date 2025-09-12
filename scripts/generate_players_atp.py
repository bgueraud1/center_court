#!/usr/bin/env python3
# scripts/generate_players_atp.py
"""
Generate one HTML page per ATP player from player_data_atp.csv
Outputs to docs/players_atp/<player_id>-<slug>.html and docs/players_atp/index.html
"""
from pathlib import Path
import pandas as pd
import html
import re
from datetime import datetime
import shutil
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parents[1]
CSV = ROOT / "player_data_atp.csv"
OUT_DIR = ROOT / "docs" / "players_atp"

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
            return str(s).split()[0]
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(s).split()[0] if isinstance(s, str) else ""

def parse_height_cm(row):
    # ATP CSV may have height_inches and height_cm similar to WTA; attempt same heuristics
    hc = row.get("height_cm", "")
    if isinstance(hc, str) and hc.strip().endswith("m"):
        try:
            return float(hc.strip().replace("m","")) * 100
        except:
            pass
    try:
        if pd.notna(hc) and hc != "-":
            return float(hc)
    except:
        pass
    hi = row.get("height_inches", "")
    if isinstance(hi, str) and hi.strip():
        m = re.search(r"(\d+)[^\d]+(\d+)", hi)
        if m:
            feet = int(m.group(1)); inches = int(m.group(2))
            total_in = feet*12 + inches
            return round(total_in * 2.54, 1)
    return None

PLAYER_TMPL = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{esc_name} — Player Profile</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Center Court</a>
      <a class="navbar-text text-white" href="../players_atp/index.html">Players (ATP)</a>
    </div>
  </nav>

  <main class="container py-4">
    <div class="card mb-4">
      <div class="card-body">
        <h1 class="card-title">{esc_name}</h1>
        <p class="text-muted">{esc_country}</p>
        <div class="row">
          <div class="col-md-8">
            <dl class="row">
              <dt class="col-sm-4">Birth date</dt><dd class="col-sm-8">{birth_date}</dd>
              <dt class="col-sm-4">Birth place</dt><dd class="col-sm-8">{esc_birthplace}</dd>
              <dt class="col-sm-4">Height</dt><dd class="col-sm-8">{height}</dd>
              <dt class="col-sm-4">Hand</dt><dd class="col-sm-8">{plays}</dd>
              <dt class="col-sm-4">Backhand</dt><dd class="col-sm-8">{backhand}</dd>
              <dt class="col-sm-4">Highest ranking</dt><dd class="col-sm-8">{highest_ranking}</dd>
              <dt class="col-sm-4">First appearance</dt><dd class="col-sm-8">{first_appearance}</dd>
              <dt class="col-sm-4">Last appearance</dt><dd class="col-sm-8">{last_appearance}</dd>
              <dt class="col-sm-4">Prize money</dt><dd class="col-sm-8">{prize_money}</dd>
            </dl>
          </div>
          <div class="col-md-4">
            <div class="border rounded p-3 text-center">
              <p class="mb-0"><small>Picture non available</small></p>
            </div>
          </div>
        </div>

        <!-- Elements of Biography -->
        {bio_block}

        <!-- Link to propose edits -->
        <p class="mt-3">
        <a class="me-3" href="index.html">&larr; Back to the player index</a>
        <a class="btn btn-sm btn-outline-primary" href="../edit_atp.html?player={slug}&name={url_name}">Suggérer une modification</a>
      </p>

      </div>
    </div>
  </main>

  <footer class="text-center py-3">
    <small>© Center Court</small>
  </footer>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""


INDEX_TOP = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Players — Center Court (ATP)</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark mb-3">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Center Court</a>
      <span class="navbar-text text-white">Players (ATP)</span>
      <span class="navbar-text text-white">Players</span>
      <a class="btn btn-sm btn-outline-light ms-2" href="../tools/birthdate_search.html">Search players by birthdate</a>

    </div>
  </nav>

  <main class="container py-4">
    <h1>Players list (ATP)</h1>
    <p class="lead">Search by name :</p>
    <input id="search" class="form-control mb-3" placeholder="Enter a name...">
    <div class="list-group" id="players-list">
"""

INDEX_BOTTOM = """
    </div>
  </main>

  <footer class="text-center py-3">
    <small>© Center Court</small>
  </footer>

  <script>
    const list = document.getElementById('players-list');
    const items = Array.from(list.querySelectorAll('.list-group-item'));
    document.getElementById('search').addEventListener('input', function(e){
      const q = e.target.value.toLowerCase();
      items.forEach(it => {
        const txt = it.dataset.name.toLowerCase();
        it.style.display = txt.includes(q) ? '' : 'none';
      });
    });
  </script>
</body>
</html>
"""

def esc(s):
    if pd.isna(s) or s is None:
        return ""
    return html.escape(str(s))

def main():
    if not CSV.exists():
        print(f"CSV not found at {CSV}. Run script from repository root.")
        return

    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(CSV, dtype=str).fillna("")
    players_index_lines = []

    for _, row in df.iterrows():
        name = (row.get("full_name","") or "").strip()
        if not name:
            continue

        pid_raw = row.get("player_id", "")
        pid = pid_raw or ""
        base_slug = safe_slug(name)
        if pid:
            filename_stem = f"{pid}-{base_slug}"
        else:
            filename_stem = base_slug

        slug = filename_stem
        birthplace = row.get("birthplace", "") or ""
        birth_date = parse_date_only(row.get("birth_date",""))
        plays = row.get("plays","")
        backhand = row.get("backhand","")
        highest_ranking = row.get("highest_ranking","") or row.get("highest_rank","")
        prize_money = row.get("prize_money","")
        country = row.get("represented_country","")
        hcm = parse_height_cm(row)
        if hcm:
            htxt = f"{hcm:.1f} cm"
        else:
            htxt = row.get("height_inches","") or row.get("height_cm","") or ""

        url_name = quote_plus(name)

        # biography (existing)
        biography = row.get("biography", "") or ""
        esc_bio = esc(biography)
        if esc_bio.strip():
            bio_block = (
                '<div class="card mb-3">'
                '<div class="card-body">'
                '<h5 class="card-title">Elements of Biography</h5>'
                f'<div style="white-space:pre-wrap;">{esc_bio}</div>'
                '</div></div>'
            )
        else:
            bio_block = ''

        # --- NEW: first_appearance / last_appearance parsing and sentinel handling ---
        # sentinel constants (display-only; CSV values remain unchanged)
        SENTINEL_DATE = "1870-09-04"
        SENTINEL_RANK_NUM = 9999999

        # parse date fields (normalized) using helper parse_date_only
        fa_raw = row.get("first_appearance", "") or row.get("first_appear", "") or ""
        la_raw = row.get("last_appearance", "") or row.get("last_appear", "") or ""
        fa_parsed = parse_date_only(fa_raw)
        la_parsed = parse_date_only(la_raw)

        # hide if sentinel date or empty
        first_appearance = fa_parsed if (fa_parsed and fa_parsed != SENTINEL_DATE) else ""
        last_appearance = la_parsed if (la_parsed and la_parsed != SENTINEL_DATE) else ""

        # Highest ranking: hide if sentinel numeric (like 9999999)
        hr_raw = row.get("highest_ranking", "") or row.get("highest_rank", "") or ""
        highest_ranking = ""
        if hr_raw:
            # extract digits (handles strings like "1", "124", "$124" etc.)
            digits = re.sub(r"[^\d]", "", str(hr_raw))
            try:
                if digits:
                    nr = int(digits)
                    if nr >= SENTINEL_RANK_NUM:
                        highest_ranking = ""
                    else:
                        # keep the original representation if it looks numeric, else keep original raw
                        highest_ranking = str(int(nr)) if digits == str(nr) else str(hr_raw)
                else:
                    # no digits -> keep original raw text
                    highest_ranking = str(hr_raw).strip()
            except Exception:
                highest_ranking = str(hr_raw).strip()
        else:
            highest_ranking = ""

        # prize money (existing)
        prize_money = row.get("prize_money","")

        # country already exists
        country = row.get("represented_country","")

        # parse height as before
        hcm = parse_height_cm(row)
        if hcm:
            htxt = f"{hcm:.1f} cm"
        else:
            htxt = row.get("height_inches","") or row.get("height_cm","") or ""

        # plays/backhand already defined earlier in your file; ensure they exist here
        plays = row.get("plays","")
        backhand = row.get("backhand","")

        # --- finally build the page content ---
        content = PLAYER_TMPL.format(
          esc_name = esc(name),
          esc_country = esc(country),
          birth_date = esc(parse_date_only(row.get("birth_date",""))),
          esc_birthplace = esc(birthplace),
          height = esc(htxt),
          plays = esc(plays),
          backhand = esc(backhand),
          highest_ranking = esc(highest_ranking),
          first_appearance = esc(first_appearance),
          last_appearance = esc(last_appearance),
          prize_money = esc(prize_money),
          slug = slug,
          url_name = url_name,
          bio_block = bio_block
      )


        out_file = OUT_DIR / f"{filename_stem}.html"
        out_file.write_text(content, encoding="utf-8")

        entry = f'<a class="list-group-item list-group-item-action" href="{filename_stem}.html" data-name="{html.escape(name)}">{html.escape(name)} <small class="text-muted">({html.escape(country)})</small></a>'
        players_index_lines.append(entry)

    index_html = INDEX_TOP + "\n".join(players_index_lines) + INDEX_BOTTOM

    import subprocess, sys
    # --- update birthdate JSON for the client tool (non-fatal) ---
    try:
        gen_script = ROOT / "scripts" / "generate_birthdate_index.py"
        if gen_script.exists():
            print("Updating docs/tools/players_by_birth.json ...")
            subprocess.check_call([sys.executable, str(gen_script)])
        else:
            print("generate_birthdate_index.py not found; skipping birthdate JSON generation.")
    except Exception as e:
      print("Warning: failed to run generate_birthdate_index.py:", e)

    (OUT_DIR / "index.html").write_text(index_html, encoding="utf-8")
    print(f"Generated {len(players_index_lines)} player pages to {OUT_DIR}")

if __name__ == "__main__":
    main()
