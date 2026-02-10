# scripts/build_all.py
import subprocess
import sys
from pathlib import Path
import shutil
import os
import json

import argparse

ROOT = Path(__file__).resolve().parents[1]

#DOCS = ROOT / "docs"



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
# parse optional --out-dir, fallback to env BUILD_OUT_DIR, or to ROOT/"docs_build"
parser = argparse.ArgumentParser(description="Build static site outputs into an output directory")
parser.add_argument("--out-dir", default=os.getenv("BUILD_OUT_DIR", str(ROOT / "docs_build")), help="Directory to write generated site files into (default: docs_build)")
args = parser.parse_args()
BUILD_DIR = Path(args.out_dir).resolve()

# Don't touch source `docs/` — instead ensure a fresh build dir
DOCS_SRC = ROOT / "docs"
if BUILD_DIR.exists():
    shutil.rmtree(BUILD_DIR)
BUILD_DIR.mkdir(parents=True, exist_ok=True)
print(f"Using build output directory: {BUILD_DIR} (source docs if present: {DOCS_SRC})")
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
            target_dir = BUILD_DIR / rel
            target_dir.mkdir(parents=True, exist_ok=True)
            for fn in files:
                src = Path(root) / fn
                dst = target_dir / fn
                shutil.copy2(src, dst)
        

        print(f"Copied durable static files from {STATIC_DIR} -> {BUILD_DIR}")
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
            shutil.copy2(cand, BUILD_DIR / "logo.png")
            logo_path = "logo.png"
            logo_img_html = ""
            if logo_path:
                # Use a small inline <img> so the template can render the logo left of site title
                logo_img_html = f'<img src="{logo_path}" alt="logo" style="height:28px;margin-right:8px;"/>'
            print("Copied logo -> docs/logo.png")
            break
        except Exception as e:
            print("Could not copy logo", cand, e)

# -------------------------
# 1) Option: set SKIP_GEOCODE env so main_maps will not call network in CI.
# -------------------------
print("SKIP_GEOCODE =", repr(os.getenv("SKIP_GEOCODE")))
env = os.environ.copy()
# 1) Option: set SKIP_GEOCODE env so main_maps will not call network in CI.
print("SKIP_GEOCODE (before) =", repr(os.getenv("SKIP_GEOCODE")))
env = os.environ.copy()
# If we're running in CI (or Netlify/GitHub Actions), default to skipping geocode.
# Otherwise keep current value or default to "0" for local runs.
if os.getenv("CI") or os.getenv("NETLIFY") or os.getenv("GITHUB_ACTIONS"):
    env["SKIP_GEOCODE"] = env.get("SKIP_GEOCODE", "1")
else:
    env["SKIP_GEOCODE"] = env.get("SKIP_GEOCODE", "0")
# ensure UTF-8 for subprocesses
env["PYTHONIOENCODING"] = "utf-8"
env.setdefault("LANG", "en_US.UTF-8")
print("SKIP_GEOCODE (used for subprocesses) =", repr(env["SKIP_GEOCODE"]))
# forcer utf-8 pour les sous-processus et Python stdio
env["PYTHONIOENCODING"] = "utf-8"
env.setdefault("LANG", "en_US.UTF-8")





# -------------------------
# 2) Run your main script that creates the HTML maps
# -------------------------
# ---------- RUN both WTA and ATP map builders ----------
print("Running WTA maps (main_maps.py)...")
candidates_main_wta = [
    ROOT / "main_maps.py",
    ROOT / "player_base_and_maps" / "main_maps.py",
    ROOT / "player_base_and_maps.py",
]
MAIN_MAP_WTA = next((p for p in candidates_main_wta if p.exists()), None)
if MAIN_MAP_WTA:
    print(f"Running WTA main build script ({MAIN_MAP_WTA})...")
    rc_wta = subprocess.run([sys.executable, str(MAIN_MAP_WTA)], cwd=str(ROOT), env=env)
    if rc_wta.returncode != 0:
        print("⚠️ WTA main build failed (exit code {})".format(rc_wta.returncode))
else:
    print("⚠️ WTA main_maps.py not found — skipping WTA maps.")

# ---------- ATP (if module exists) ----------
print("Running ATP maps (main_maps_atp.py)...")
MAIN_MAP_ATP = ROOT / "main_maps_atp.py"
if MAIN_MAP_ATP.exists():
    rc_atp = subprocess.run([sys.executable, str(MAIN_MAP_ATP)], cwd=str(ROOT), env=env)
    if rc_atp.returncode != 0:
        print("⚠️ ATP main build failed (exit code {})".format(rc_atp.returncode))
else:
    print("No main_maps_atp.py found — skipping ATP maps.")

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
        dest = BUILD_DIR / p.name
        shutil.copy2(p, dest)
        moved += 1
        print(f"Copied {p} -> {dest}")
    except Exception as e:
        print("Could not copy", p, e)
print(f"✅ {moved} fichier(s) HTML copiés dans {BUILD_DIR}")

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
            shutil.copy2(f, BUILD_DIR / f.name)
            print(f"Copied data file {f} -> {BUILD_DIR / f.name}")
        except Exception as e:
            print("Could not copy data file", f, e)

# -------------------------
# 5) Now generate player pages (WTA + ATP)
# -------------------------
print("Generating WTA player pages (if matches dir exists)...")
wta_matches = ROOT / "matches" / "wta_matches"
gen_wta = ROOT / "scripts" / "generate_players.py"

if gen_wta.exists() and wta_matches.exists():
    try:
        res = subprocess.run(
            [sys.executable, str(gen_wta), "--matches-dir", str(wta_matches)],
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
            print("STDERR from generate_players (WTA):", res.stderr)
    except subprocess.CalledProcessError as e:
        print("generate_players (WTA) failed, returncode:", e.returncode)
        if e.output:
            print("OUTPUT:", e.output)
        if e.stderr:
            print("STDERR:", e.stderr)
        raise
    except Exception as e:
        print("Unexpected error running generate_players (WTA):", e)
        raise
else:
    if not gen_wta.exists():
        print(f"No {gen_wta} found — skipping WTA player generation.")
    else:
        print(f"No WTA matches dir found at {wta_matches} — skipping WTA player generation.")


# === Generate ATP player pages (if script exists) ===
print("Generating ATP player pages (if available)...")
gen_atp = ROOT / "scripts" / "generate_players_atp.py"
atp_matches = ROOT / "matches" / "atp_matches"

if gen_atp.exists():
    if atp_matches.exists():
        atp_args = [sys.executable, str(gen_atp), "--matches-dir", str(atp_matches)]
    else:
        print(f"No ATP matches dir found at {atp_matches} — skipping ATP generation.")
        atp_args = None

    if atp_args:
        try:
            res = subprocess.run(
                atp_args,
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
                print("STDERR from generate_players_atp:", res.stderr)
        except subprocess.CalledProcessError as e:
            print("generate_players_atp failed, returncode:", e.returncode)
            if e.output:
                print("OUTPUT:", e.output)
            if e.stderr:
                print("STDERR:", e.stderr)
            raise
        except Exception as e:
            print("Unexpected error running generate_players_atp:", e)
            raise
else:
    print("No generate_players_atp.py found — skipping ATP player pages.")

# copy players directories into docs (if present)
for src_dir, dst_dir in [
    (ROOT / "players", BUILD_DIR / "players"),
    (ROOT / "players_atp", BUILD_DIR / "players_atp")
]:
    if src_dir.exists() and src_dir.is_dir():
        try:
            shutil.copytree(src_dir, dst_dir, dirs_exist_ok=True)
            print(f"Copied player dir {src_dir} -> {dst_dir}")
        except Exception as e:
            print(f"Could not copy player dir {src_dir} -> {dst_dir}: {e}")


# -------------------------
# 6) Build the nicer index.html with sections, custom names, header logo, footer
# -------------------------
# collect maps (exclude index.html and edit pages)
EXCLUDE_HTML = {"index.html", "edit.html", "404.html", "edit_atp.html", "some_other_file.html"}
# -------------------------
# BUILD MAPS LIST (WTA / ATP buckets)
# -------------------------
map_files = [p for p in sorted(BUILD_DIR.glob("*.html")) if p.name not in EXCLUDE_HTML]

wta_items = []
atp_items = []
for p in map_files:
    stem = p.stem
    display = MAPS_METADATA.get(stem) or MAPS_METADATA.get(p.name) or pretty_name_from_stem(stem)
    item_html = (
        f'<a class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" '
        f'href="{p.name}"><div><strong>{display}</strong><div class="small text-muted">{p.name}</div></div>'
    )
    # bucket by filename: *_atp.* -> ATP
    if '_atp' in p.name.lower() or 'atp' in p.name.lower().split('_') :
        atp_items.append(item_html + '<span class="badge bg-primary rounded-pill">Map</span></a>')
    else:
        wta_items.append(item_html + '<span class="badge" style="background:#9b59b6;color:#fff;">Map</span></a>')

# small helper HTML for empty buckets
wta_list_html = "\n".join(wta_items) if wta_items else '<div class="text-muted">No WTA maps found</div>'
atp_list_html = "\n".join(atp_items) if atp_items else '<div class="text-muted">No ATP maps found</div>'


# players block (link to players if exists) — include both WTA and ATP directories
players_links = []
if (BUILD_DIR / "players" / "index.html").exists():
    players_links.append(
        '<a class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" '
        'href="players/index.html"><div><strong>Player directory (WTA)</strong>'
        '<div class="small text-muted">Individual Profile</div></div>'
        '<span class="badge bg-primary rounded-pill">Players</span></a>'
    )
if (BUILD_DIR / "players_atp" / "index.html").exists():
    players_links.append(
        '<a class="list-group-item list-group-item-action d-flex justify-content-between align-items-start" '
        'href="players_atp/index.html"><div><strong>Player directory (ATP)</strong>'
        '<div class="small text-muted">Individual Profile</div></div>'
        '<span class="badge bg-info text-white rounded-pill">Players</span></a>'
    )

# fallback if none present
players_link_html = "\n".join(players_links) if players_links else '<div class="small text-muted">Player directory not available.</div>'


# New TEMPLATE with two colored cards stacked
TEMPLATE = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>__SITE_TITLE__</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .maps-section .card {{ min-height: 200px; }}
    .players-section .card {{ min-height: 120px; }}
    .site-footer {{ background:#f8f9fa; border-top:1px solid #e9ecef; }}
    .map-filename {{ font-family: monospace; font-size: .85rem; }}
    /* color accents */
    .card-wta {{ border-left: 6px solid #8e44ad; }}
    .card-atp {{ border-left: 6px solid #0d6efd; }}
  </style>
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark">
    <div class="container d-flex align-items-center">
      <a class="navbar-brand d-flex align-items-center" href="#">
        __LOGO_IMG_HTML__
        <span>__SITE_TITLE__</span>
      </a>
      <div class="ms-auto text-muted small">Maps and Player Profile</div>
    </div>
  </nav>

  <main class="container py-4">
    <header class="mb-4">
      <h1 class="h3">__SITE_TITLE__</h1>
      <p class="lead">Interactive Maps and Player Profile</p>
    </header>

    <div class="row g-4">
      <div class="col-lg-12">
        <div class="card card-wta shadow-sm mb-3">
          <div class="card-body">
            <h4 class="card-title">WTA maps</h4>
            <p class="card-text text-muted">Maps related to WTA data.</p>
            <div class="list-group">
__WTA_MAPS__
            </div>
          </div>
        </div>

        <div class="card card-atp shadow-sm mb-3">
          <div class="card-body">
            <h4 class="card-title">ATP maps</h4>
            <p class="card-text text-muted">Maps related to ATP data.</p>
            <div class="list-group">
__ATP_MAPS__
            </div>
          </div>
        </div>
      </div>

      <div class="col-lg-12">
        <div class="card players-section shadow-sm mb-3">
          <div class="card-body">
            <h5 class="card-title">Players</h5>
            <p class="card-text">Access the player directory and individual profiles.</p>
            <div class="list-group">
__PLAYERS_LINK__
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
        <strong>Contact</strong> — <a href="mailto:__CONTACT_EMAIL__">__CONTACT_EMAIL__</a><br>
        <small class="text-muted">__COPYRIGHT__</small>
      </div>
      <div class="text-end small text-muted">Automatically generated</div>
    </div>
  </footer>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

INDEX_HTML = TEMPLATE.replace("__SITE_TITLE__", SITE_TITLE)\
    .replace("__LOGO_IMG_HTML__", logo_img_html or "")\
    .replace("__WTA_MAPS__", wta_list_html)\
    .replace("__ATP_MAPS__", atp_list_html)\
    .replace("__PLAYERS_LINK__", players_link_html if players_link_html else '<div class="small text-muted">Player directory not available.</div>')\
    .replace("__CONTACT_EMAIL__", CONTACT_EMAIL)\
    .replace("__COPYRIGHT__", COPYRIGHT)

# write the index (always overwrite with the nicer template)
index_path = BUILD_DIR / "index.html"
index_path.write_text(INDEX_HTML, encoding="utf-8")
print(f"✅ index.html créé/écrasé dans {index_path}")
