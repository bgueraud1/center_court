# scripts/build_all.py
import subprocess
import sys
from pathlib import Path
import shutil
import os
import json

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"



# Safety: if nested player_base_and_maps/player_base_and_maps exists (old bug), move its files out
# If a nested directory is present, warn but DO NOT auto-move files (we want deterministic behavior)
nested = ROOT / "player_base_and_maps" / "player_base_and_maps"
if nested.exists() and nested.is_dir():
    print("WARNING(build_all): Found nested player_base_and_maps at", nested)
    print("Please remove nested folder manually and ensure canonical CSV and cache live at:", ROOT / "player_base_and_maps")





# -------------------------
# Helpers / config loaders
# -------------------------
def load_json_optional(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"⚠️ Could not parse JSON {path}: {e}")
        return {}

# load optional map display names mapping
MAPS_METADATA = load_json_optional(ROOT / "maps_metadata.json")
# load optional site-wide config (contact, copyright, title)
SITE_CONFIG = load_json_optional(ROOT / "site_config.json")

SITE_TITLE = SITE_CONFIG.get("site_title") or os.getenv("SITE_TITLE") or "Center Court"
CONTACT_EMAIL = SITE_CONFIG.get("contact_email") or os.getenv("SITE_CONTACT_EMAIL") or "contact@example.com"
COPYRIGHT = SITE_CONFIG.get("copyright") or os.getenv("SITE_COPYRIGHT") or "© Center Court"

# -------------------------
# Utility pretty name
# -------------------------
def pretty_name_from_stem(stem: str) -> str:
    # remove common suffixes/prefixes used in filenames
    s = stem
    # remove common map suffixes
    for suffix in ("_map_wta", "_map", "-map", "_maphtml", "map_"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    # replace underscores / dashes with spaces
    s = s.replace("_", " ").replace("-", " ").strip()
    # small cleanup for repeated words
    s = " ".join(s.split())
    # title-case but keep some acronyms uppercase (WTA, ATP)
    words = []
    for w in s.split():
        up = w.upper()
        if up in ("WTA", "ATP"):
            words.append(up)
        else:
            words.append(w.capitalize())
    return " ".join(words) if words else stem

# -------------------------
# 0) ensure docs dir is fresh
# -------------------------
if DOCS.exists():
    shutil.rmtree(DOCS)
DOCS.mkdir(parents=True, exist_ok=True)
# -------------------------
# Copy repo-provided static site assets (durable files)
# -------------------------
STATIC_DIR = ROOT / "site_static"
if STATIC_DIR.exists() and STATIC_DIR.is_dir():
    try:
        # dirs_exist_ok requires Python 3.8+. CI uses Python 3.10.
        # instead of shutil.copytree(STATIC_DIR, DOCS, dirs_exist_ok=True)
        for root, dirs, files in os.walk(STATIC_DIR):
            rel = os.path.relpath(root, STATIC_DIR)
            target_dir = DOCS / rel
            target_dir.mkdir(parents=True, exist_ok=True)
            for fn in files:
                if fn in ('edit.html', 'some_other_file.html'):   # <- ignore these
                    continue
                src = Path(root) / fn
                dst = target_dir / fn
                shutil.copy2(src, dst)
        
        print(f"Copied durable static files from {STATIC_DIR} -> {DOCS}")
    except Exception as e:
        print(f"Could not copy static dir {STATIC_DIR} into docs/: {e}")
else:
    print("No site_static directory found — skipping copy of durable static files.")

# -------------------------
# Copy repo-provided static site assets (durable files)
# -------------------------
STATIC_DIR = ROOT / "site_static"
if STATIC_DIR.exists() and STATIC_DIR.is_dir():
    try:
        # Python 3.8+: dirs_exist_ok=True to merge into DOCS
        shutil.copytree(STATIC_DIR, DOCS, dirs_exist_ok=True)
        print(f"Copied durable static files from {STATIC_DIR} -> {DOCS}")
    except Exception as e:
        print(f"Could not copy static dir {STATIC_DIR} into docs/: {e}")
else:
    print("No site_static directory found — skipping copy of durable static files.")


# -------------------------
# copy logo if present (logo.png search in several places)
# -------------------------
LOGO_CANDIDATES = [ROOT / "logo.png", ROOT / "assets" / "logo.png", ROOT / "static" / "logo.png"]
logo_path = None
for cand in LOGO_CANDIDATES:
    if cand.exists():
        try:
            shutil.copy2(cand, DOCS / "logo.png")
            logo_path = "logo.png"
            print("Copied logo -> docs/logo.png")
            break
        except Exception as e:
            print("Could not copy logo", cand, e)

# -------------------------
# 1) Option: set SKIP_GEOCODE env so main_maps will not call network in CI.
# -------------------------
print("SKIP_GEOCODE =", repr(os.getenv("SKIP_GEOCODE")))
env = os.environ.copy()
env["SKIP_GEOCODE"] = env.get("SKIP_GEOCODE", "1")  # déjà présent
# forcer utf-8 pour les sous-processus et Python stdio
env["PYTHONIOENCODING"] = "utf-8"
env.setdefault("LANG", "en_US.UTF-8")





# -------------------------
# 2) Run your main script that creates the HTML maps
# -------------------------
print("Running main build script (main_maps.py)...")
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

# -------------------------
# 3) Collect HTML outputs (maps)
# -------------------------
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

# -------------------------
# 4) Generate neighbors FIRST so generate_players can embed them
# -------------------------
print("Generating neighbors (embeddings / knn)...")
try:
    # capture output en UTF-8 pour éviter erreurs d'encodage lors de la lecture des pipes
    res = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_neighbors.py")],
        cwd=str(ROOT),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=600
    )
    if res.stdout:
        print(res.stdout)
    if res.stderr:
        print("STDERR from generate_neighbors:", res.stderr)
except subprocess.CalledProcessError as e:
    print("generate_neighbors failed, returncode:", e.returncode)
    if e.output:
        print("OUTPUT:", e.output)
    if e.stderr:
        print("STDERR:", e.stderr)
    raise
except Exception as e:
    print("Unexpected error running generate_neighbors:", e)
    raise


# Copy neighbor/embedding files into docs/ (so they are served)
for pattern in ("node_knn_top10.csv","graphsage_knn_top10.csv","node_embeddings*.csv","players_graphsage_embeddings.csv"):
    for f in ROOT.glob(pattern):
        try:
            shutil.copy2(f, DOCS / f.name)
            print(f"Copied data file {f} -> {DOCS / f.name}")
        except Exception as e:
            print("Could not copy data file", f, e)

# -------------------------
# 5) Now generate player pages
# -------------------------
print("Generating player pages...")
try:
    res = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_players.py")],
        cwd=str(ROOT),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=900
    )
    if res.stdout:
        print(res.stdout)
    if res.stderr:
        print("STDERR from generate_players:", res.stderr)
except subprocess.CalledProcessError as e:
    print("generate_players failed, returncode:", e.returncode)
    if e.output:
        print("OUTPUT:", e.output)
    if e.stderr:
        print("STDERR:", e.stderr)
    raise
except Exception as e:
    print("Unexpected error running generate_players:", e)
    raise


# -------------------------
# 6) Build the nicer index.html with sections, custom names, header logo, footer
# -------------------------
# collect maps (exclude index.html)
map_files = [p for p in sorted((DOCS).glob("*.html")) if p.name != "index.html"]

# build maps HTML list using metadata mapping or pretty name
maps_entries_html = []
for p in map_files:
    stem = p.stem
    display = MAPS_METADATA.get(stem) or MAPS_METADATA.get(p.name) or pretty_name_from_stem(stem)
    # small hint showing filename in muted text
    maps_entries_html.append(f'<a class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" href="{p.name}"><div><strong>{display}</strong><div class="small text-muted">{p.name}</div></div><span class="badge bg-secondary rounded-pill">Map</span></a>')

# players block (link to players if exists)
players_link_html = ""
players_exist = (DOCS / "players" / "index.html").exists()
if players_exist:
    # don't duplicate: add a header link card separately
    players_link_html = f'<a class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" href="players/index.html"><div><strong>Player directory</strong><div class="small text-muted">Individual Profile</div></div><span class="badge bg-primary rounded-pill">Players</span></a>'

# Construct the final HTML using Bootstrap, with separate sections and footer
logo_img_html = f'<img src="{logo_path}" alt="logo" height="38" class="me-2"/>' if logo_path else ""
INDEX_HTML = f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{SITE_TITLE}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    /* small custom tweaks */
    .maps-section .card {{ min-height: 200px; }}
    .players-section .card {{ min-height: 120px; }}
    .site-footer {{ background:#f8f9fa; border-top:1px solid #e9ecef; }}
    .map-filename {{ font-family: monospace; font-size: .85rem; }}
  </style>
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark">
    <div class="container d-flex align-items-center">
      <a class="navbar-brand d-flex align-items-center" href="#">
        {logo_img_html}
        <span>Center Court</span>
      </a>
      <div class="ms-auto text-muted small">Maps and Player Profile</div>
    </div>
  </nav>

  <main class="container py-4">
    <header class="mb-4">
      <h1 class="h3">{SITE_TITLE}</h1>
      <p class="lead">Interactive Maps and Player Profile</p>
    </header>

    <div class="row g-4">
      <div class="col-lg-8">
        <div class="card maps-section shadow-sm">
          <div class="card-body">
            <h4 class="card-title">Maps</h4>
            <p class="card-text text-muted">Click a map to open it.</p>
            <div class="list-group">
{chr(10).join(maps_entries_html) if maps_entries_html else '              <div class="text-muted">No map found</div>'}
            </div>
          </div>
        </div>
      </div>

      <div class="col-lg-4">
        <div class="card players-section shadow-sm mb-3">
          <div class="card-body">
            <h5 class="card-title">Players</h5>
            <p class="card-text">Access the player directory and individual profiles.</p>
            <div class="list-group">
{players_link_html if players_link_html else '              <div class="small text-muted">Player directory not available.</div>'}
            </div>
          </div>
        </div>

        <div class="card shadow-sm">
          <div class="card-body">
            <h6 class="card-title">About</h6>
            <p class="card-text small text-muted">This site presents maps and player profile generated from hand-collected data of the internet</p>
          </div>
        </div>

      </div>
    </div>
  </main>

  <footer class="site-footer py-4">
    <div class="container d-flex justify-content-between align-items-center">
      <div>
        <strong>Contact</strong> — <a href="mailto:{CONTACT_EMAIL}">{CONTACT_EMAIL}</a><br>
        <small class="text-muted">{COPYRIGHT}</small>
      </div>
      <div class="text-end small text-muted">Automatically generated</div>
    </div>
  </footer>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

# write the index (always overwrite with the nicer template)
index_path = DOCS / "index.html"
index_path.write_text(INDEX_HTML, encoding="utf-8")
print(f"✅ index.html créé/écrasé dans {index_path}")

