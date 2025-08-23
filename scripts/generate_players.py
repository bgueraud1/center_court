#!/usr/bin/env python3
"""
Generate one HTML page per player from player_data_wta.csv
Outputs to docs/players/<player_id>-<slug>.html and docs/players/index.html
Overwrites existing pages (clean build).
"""
from pathlib import Path
import pandas as pd
import html
import re
from datetime import datetime
import shutil

ROOT = Path(__file__).resolve().parents[1]
CSV = ROOT / "player_data_wta.csv"
OUT_DIR = ROOT / "docs" / "players"

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
            feet = int(m.group(1))
            inches = int(m.group(2))
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
      <a class="navbar-text text-white" href="../players/index.html">Players</a>
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
              <dt class="col-sm-4">Best rank</dt><dd class="col-sm-8">{best_rank}</dd>
              <dt class="col-sm-4">First appearance</dt><dd class="col-sm-8">{first_appearance}</dd>
              <dt class="col-sm-4">Last appearance</dt><dd class="col-sm-8">{last_appearance}</dd>
            </dl>
          </div>
          <div class="col-md-4">
            <div class="border rounded p-3 text-center">
              <p class="mb-0"><small>Picture non available</small></p>
            </div>
          </div>
        </div>

        <!-- Link to propose edits -->
        <p class="mt-3">
          <a class="btn btn-outline-primary" href="edit.html?player_id={player_id}">Proposer une modification</a>
        </p>

        <p class="mt-3"><a href="index.html">&larr; Back to the player index</a></p>
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
  <title>Players — Center Court</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark mb-3">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Central Court</a>
      <span class="navbar-text text-white">Players</span>
    </div>
  </nav>

  <main class="container py-4">
    <h1>Players list</h1>
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

    # Clean output dir to avoid duplicates / suffixes
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(CSV, dtype=str).fillna("")
    players_index_lines = []

    for _, row in df.iterrows():
        name = (row.get("full_name","") or "").strip()
        if not name:
            continue

        # Prefer stable filename using player_id if available
        pid_raw = row.get("player_id", "")
        try:
            pid = str(int(float(pid_raw))) if pid_raw not in ("", None) else ""
        except Exception:
            pid = pid_raw or ""
        base_slug = safe_slug(name)
        if pid:
            filename_stem = f"{pid}-{base_slug}"
        else:
            filename_stem = base_slug

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
            last_appearance = esc(last_app),
            player_id = esc(pid)
        )
        out_file = OUT_DIR / f"{filename_stem}.html"
        out_file.write_text(content, encoding="utf-8")

        entry = f'<a class="list-group-item list-group-item-action" href="{filename_stem}.html" data-name="{html.escape(name)}">{html.escape(name)} <small class="text-muted">({html.escape(country)})</small></a>'
        players_index_lines.append(entry)

    index_html = INDEX_TOP + "\n".join(players_index_lines) + INDEX_BOTTOM
    (OUT_DIR / "index.html").write_text(index_html, encoding="utf-8")
    print(f"Generated {len(players_index_lines)} player pages to {OUT_DIR}")

if __name__ == "__main__":
    main()
