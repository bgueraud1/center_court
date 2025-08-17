# scripts/build_all.py
import subprocess
import sys
from pathlib import Path
import shutil
import os
import glob

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

# 0) ensure docs dir is fresh
if DOCS.exists():
    shutil.rmtree(DOCS)
DOCS.mkdir(parents=True, exist_ok=True)

# 1) Option: set SKIP_GEOCODE env so main_maps will not call network in CI.
# If your main_maps.py knows how to honor this env var (see note below), it will use cached coords.
env = os.environ.copy()
env["SKIP_GEOCODE"] = env.get("SKIP_GEOCODE", "1")  # CI default: skip network geocoding

# 2) Run your main script that creates the HTML maps (use Path join for cross-platform)
print("Running main build script (main_maps.py)...")
# --- find main_maps.py robustly (it may be at repo root or inside player_base_and_maps/)
candidates_main = [
    ROOT / "main_maps.py",
    ROOT / "player_base_and_maps" / "main_maps.py",
    ROOT / "player_base_and_maps.py",
]
MAIN_MAP = next((p for p in candidates_main if p.exists()), None)

if MAIN_MAP is None:
    print("⚠️ main_maps.py not found in expected locations. Skipping main_maps run.")
    rc = subprocess.CompletedProcess(args=[], returncode=0)
else:
    print(f"Running main build script ({MAIN_MAP})...")
    rc = subprocess.run([sys.executable, str(MAIN_MAP)], cwd=str(ROOT), env=env)
    if rc.returncode != 0:
        print("⚠️ main_maps.py failed (exit code {})".format(rc.returncode))

if rc.returncode != 0:
    print("⚠️ main_maps.py failed (exit code {})".format(rc.returncode))

# 3) Collect HTML outputs (maps)
candidates = []
candidates += list(ROOT.glob("*.html"))
candidates += list((ROOT / "maps_html").glob("*.html")) if (ROOT / "maps_html").exists() else []
candidates += list(ROOT.glob("*map*.html"))

moved = 0
for p in sorted(set(candidates)):
    try:
        dest = DOCS / p.name
        shutil.copy2(p, dest)
        moved += 1
        print(f"Copied {p} -> {dest}")
    except Exception as e:
        print("Could not copy", p, e)
print(f"✅ {moved} fichier(s) HTML copiés dans {DOCS}")

# 4) Generate neighbors FIRST so generate_players can embed them
print("Generating neighbors (embeddings / knn)...")
subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_neighbors.py")],
               cwd=str(ROOT), env=env, check=True)

# Copy neighbor/embedding files into docs/ (so they are served)
for pattern in ("node_knn_top10.csv","graphsage_knn_top10.csv","node_embeddings*.csv","players_graphsage_embeddings.csv"):
    for f in ROOT.glob(pattern):
        try:
            shutil.copy2(f, DOCS / f.name)
            print(f"Copied data file {f} -> {DOCS / f.name}")
        except Exception as e:
            print("Could not copy data file", f, e)

# 5) Now generate player pages (they can read the neighbor CSV now)
print("Generating player pages...")
subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_players.py")],
               cwd=str(ROOT), env=env, check=True)


# 5) Build a nicer index.html (Bootstrap) that includes maps + link to players
maps_links = []
for p in sorted(DOCS.glob("*.html")):
    if p.name == "index.html":
        continue
    maps_links.append(f'          <a class="list-group-item list-group-item-action" href="{p.name}">{p.stem}</a>')

# if players index exists, add a link to the players directory
players_index_rel = "players/index.html"
players_link_html = ""
if (DOCS / "players" / "index.html").exists():
    players_link_html = '          <a class="list-group-item list-group-item-action" href="players/index.html">Joueurs</a>'

INDEX_HTML = f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Center Court — Cartes Tennis</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark">
    <div class="container">
      <a class="navbar-brand" href="#">Center Court</a>
    </div>
  </nav>

  <main class="container py-4">
    <div class="row">
      <div class="col-lg-8">
        <h1>Cartes Tennis</h1>
        <p class="lead">Cartes interactives générées à partir des données.</p>

        <div class="list-group">
{players_link_html if players_link_html else ""}
{chr(10).join(maps_links)}
        </div>
      </div>

      <aside class="col-lg-4">
        <div class="card mb-3">
          <div class="card-body">
            <h5 class="card-title">Recherche joueurs</h5>
            <p class="card-text">Utilise la page <a href="players/index.html">Joueurs</a> (si disponible).</p>
          </div>
        </div>
      </aside>
    </div>
  </main>

  <footer class="text-center py-3">
    <small>© Central Court</small>
  </footer>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

# write the index (always overwrite with the nicer template)
index_path = DOCS / "index.html"
index_path.write_text(INDEX_HTML, encoding="utf-8")
print(f"✅ index.html créé/écrasé dans {index_path}")
