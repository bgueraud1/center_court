#!/usr/bin/env python3
"""
Generate one HTML page per player from player_data_wta.csv
Outputs to docs/players/<slug>.html and docs/players/index.html
"""
from pathlib import Path
import pandas as pd
import html
import re
from datetime import datetime

CSV = Path("player_data_wta.csv")
OUT_DIR = Path("docs/players")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def slugify(name: str, pid: str) -> str:
    # lower, keep letters/numbers/-, replace spaces -> -, append id to ensure uniqueness
    s = name.lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "-", s.strip())
    return f"{s}-{pid}"

def parse_date(s):
    try:
        return pd.to_datetime(s, errors="coerce")
    except Exception:
        return None

def format_date(s):
    dt = parse_date(s)
    if pd.isnull(dt) or dt is None:
        return ""
    return dt.strftime("%Y-%m-%d")

def parse_height_cm(row):
    # prefer height_cm if given like "1.75m" or numeric
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
    # fallback: parse height_inches like 5' 9" or 5' 9''
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
  <title>{esc_name} — Fiche joueuse</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Central Court</a>
      <a class="navbar-text text-white" href="../players/index.html">Joueurs</a>
    </div>
  </nav>

  <main class="container py-4">
    <div class="card mb-4">
      <div class="card-body">
        <h1 class="card-title">{esc_name}</h1>
        <p class="text-muted">ID: {player_id} — {esc_country}</p>
        <div class="row">
          <div class="col-md-8">
            <dl class="row">
              <dt class="col-sm-4">Date de naissance</dt><dd class="col-sm-8">{birth_date}</dd>
              <dt class="col-sm-4">Lieu de naissance</dt><dd class="col-sm-8">{esc_birthplace}</dd>
              <dt class="col-sm-4">Taille</dt><dd class="col-sm-8">{height}</dd>
              <dt class="col-sm-4">Main</dt><dd class="col-sm-8">{plays}</dd>
              <dt class="col-sm-4">Meilleur classement</dt><dd class="col-sm-8">{best_rank}</dd>
              <dt class="col-sm-4">Première apparition</dt><dd class="col-sm-8">{first_appearance}</dd>
              <dt class="col-sm-4">Dernière apparition</dt><dd class="col-sm-8">{last_appearance}</dd>
            </dl>
          </div>
          <div class="col-md-4">
            <!-- Espace pour photo/avatar futur -->
            <div class="border rounded p-3 text-center">
              <p class="mb-0"><small>Photo non fournie</small></p>
            </div>
          </div>
        </div>
        <p class="mt-3"><a href="index.html">&larr; Retour à la liste des joueuses</a></p>
      </div>
    </div>
  </main>

  <footer class="text-center py-3">
    <small>© Central Court</small>
  </footer>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

INDEX_TMPL_TOP = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Joueuses — Central Court</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .player-row { cursor: pointer; }
  </style>
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark mb-3">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Central Court</a>
      <span class="navbar-text text-white">Joueurs</span>
    </div>
  </nav>

  <main class="container py-4">
    <h1>Liste des joueuses</h1>
    <p class="lead">Recherche rapide par nom :</p>
    <input id="search" class="form-control mb-3" placeholder="Tapez un nom...">
    <div class="list-group" id="players-list">
"""

INDEX_TMPL_BOTTOM = """
    </div>
  </main>

  <footer class="text-center py-3">
    <small>© Central Court</small>
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
    df = pd.read_csv(CSV, dtype=str).fillna("")
    players_index_lines = []
    for _, row in df.iterrows():
        pid = row.get("player_id", "") or ""
        name = row.get("full_name", "Unknown") or "Unknown"
        slug = slugify(name, pid)
        birthplace = row.get("birthplace", "") or ""
        birth_date = format_date(row.get("birth_date",""))
        plays = row.get("plays","")
        best_rank = row.get("best_rank","")
        first_app = row.get("first_appearance","")
        last_app = row.get("last_appearance","")
        country = row.get("represented_country","")
        # height
        hcm = parse_height_cm(row)
        if hcm:
            htxt = f"{hcm:.1f} cm"
        else:
            htxt = row.get("height_inches","") or row.get("height_cm","") or ""
        # create player page
        content = PLAYER_TMPL.format(
            esc_name = esc(name),
            player_id = esc(pid),
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
        # prepare index entry
        entry = f'<a class="list-group-item list-group-item-action" href="{slug}.html" data-name="{html.escape(name)}">{html.escape(name)} <small class="text-muted">({html.escape(country)})</small></a>'
        players_index_lines.append(entry)

    # write index
    index_html = INDEX_TMPL_TOP + "\n".join(players_index_lines) + INDEX_TMPL_BOTTOM
    (OUT_DIR / "index.html").write_text(index_html, encoding="utf-8")
    print(f"Generated {len(players_index_lines)} player pages to {OUT_DIR}")

if __name__ == "__main__":
    main()
