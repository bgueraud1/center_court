#!/usr/bin/env python3
"""
Generate one HTML page per player from player_data_wta.csv
Outputs to docs/players/<player_id>-<slug>.html and docs/players/index.html
Overwrites existing pages (clean build).

This version checks Cloudinary for existing images under public_id "players/<slug>"
and embeds responsive Cloudinary URLs if present. Requires CLOUDINARY_CLOUD_NAME,
CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET to be set in env.
"""
from pathlib import Path
import pandas as pd
import html
import re
from datetime import datetime
import shutil
from urllib.parse import quote_plus
import os
import sys

# Cloudinary SDK
try:
    import cloudinary
    import cloudinary.api
    import cloudinary.utils
    import cloudinary.exceptions
except Exception:
    cloudinary = None

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
              <dt class="col-sm-4">Backhand</dt><dd class="col-sm-8">{backhand}</dd>
              <dt class="col-sm-4">Best rank</dt><dd class="col-sm-8">{best_rank}</dd>
              <dt class="col-sm-4">First appearance</dt><dd class="col-sm-8">{first_appearance}</dd>
              <dt class="col-sm-4">Last appearance</dt><dd class="col-sm-8">{last_appearance}</dd>
            </dl>
          </div>
          <div class="col-md-4">
            {image_block}
          </div>
        </div>

        <!-- Elements of Biography -->
        {bio_block}

        <!-- Link to propose edits -->
        <p class="mt-3">
        <a class="me-3" href="index.html">&larr; Back to the player index</a>
        <a class="btn btn-sm btn-outline-primary" href="../edit.html?player={slug}&name={url_name}">Suggérer une modification</a>
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
  <title>Players — Center Court</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark mb-3">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Central Court</a>
      <span class="navbar-text text-white">Players</span>

      <span class="navbar-text text-white">Players</span>
      <a class="btn btn-sm btn-outline-light ms-2" href="../tools/birthdate_search.html">Search players by birthdate</a>

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

def fetch_cloudinary_player_public_ids(prefix="players/"):
    """
    Fetch all Cloudinary public_ids under given prefix.
    Returns a set of public_id strings (without file extension).
    Uses pagination with next_cursor.
    """
    if not cloudinary:
        return set()
    try:
        found = set()
        next_cursor = None
        while True:
            # max_results can be up to 500
            if next_cursor:
                resp = cloudinary.api.resources(type='upload', prefix=prefix, max_results=500, next_cursor=next_cursor)
            else:
                resp = cloudinary.api.resources(type='upload', prefix=prefix, max_results=500)
            resources = resp.get("resources", [])
            for r in resources:
                pid = r.get("public_id")
                if pid:
                    found.add(pid)
            next_cursor = resp.get("next_cursor")
            if not next_cursor:
                break
        return found
    except cloudinary.exceptions.Error as e:
        print("Warning: failed to list Cloudinary resources:", e, file=sys.stderr)
        return set()
    except Exception as e:
        print("Warning: unexpected error listing Cloudinary resources:", e, file=sys.stderr)
        return set()

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

    # Configure cloudinary from env (if available)
    cloud_name = os.environ.get("CLOUDINARY_CLOUD_NAME", "").strip()
    api_key = os.environ.get("CLOUDINARY_API_KEY", "").strip()
    api_secret = os.environ.get("CLOUDINARY_API_SECRET", "").strip()

    if cloud_name and api_key and api_secret and cloudinary:
        cloudinary.config(cloud_name=cloud_name, api_key=api_key, api_secret=api_secret, secure=True)
        print("Cloudinary configured. Fetching list of player images (prefix 'players/') ...")
        cloud_public_ids = fetch_cloudinary_player_public_ids(prefix="players/")
        print(f"Found {len(cloud_public_ids)} images on Cloudinary under 'players/'.")
    else:
        cloud_public_ids = set()
        if not cloudinary:
            print("cloudinary SDK not installed; skipping Cloudinary checks.")
        else:
            print("Cloudinary credentials missing; skipping Cloudinary checks.")

    for _, row in df.iterrows():
        name = (row.get("full_name","") or "").strip()
        if not name:
            continue

        # Prefer stable filename using player_id if available
        pid_raw = row.get("player_id", "")
        try:
            pid = str(int(float(pid_raw))) if pid_raw not in ("", None, "") else ""
        except Exception:
            pid = pid_raw or ""
        base_slug = safe_slug(name)
        if pid:
            filename_stem = f"{pid}-{base_slug}"
        else:
            filename_stem = base_slug

        # === HERE: slug used in URLs ===
        slug = filename_stem

        birthplace = row.get("birthplace", "") or ""
        birth_date = parse_date_only(row.get("birth_date",""))
        plays = row.get("plays","")
        backhand = row.get("backhand","") 
        best_rank = row.get("best_rank","")
        first_app = parse_date_only(row.get("first_appearance",""))
        last_app = parse_date_only(row.get("last_appearance",""))
        country = row.get("represented_country","")
        hcm = parse_height_cm(row)
        if hcm:
            htxt = f"{hcm:.1f} cm"
        else:
            htxt = row.get("height_inches","") or row.get("height_cm","") or ""

        url_name = quote_plus(name)

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

        # ===== Cloudinary image handling =====
        public_id = f"players/{slug}"   # convention: players/<slug>
        if public_id in cloud_public_ids and cloudinary:
            # build responsive Cloudinary URLs (format auto, quality auto)
            try:
                # --- Build Cloudinary URLs manually (stable & avoids .auto suffix problems) ---
                # cloud_name doit déjà être défini plus haut (env CLOUDINARY_CLOUD_NAME)
                base = f"https://res.cloudinary.com/{cloud_name}/image/upload"
                
                # transformation string: format auto (f_auto) and quality auto (q_auto)
                # crop fill + gravity face for portraits
                t300  = "f_auto,q_auto,w_300,c_fill,g_face"
                t600  = "f_auto,q_auto,w_600,c_fill,g_face"
                t1200 = "f_auto,q_auto,w_1200,c_fill,g_face"
                
                url300  = f"{base}/{t300}/{public_id}"
                url600  = f"{base}/{t600}/{public_id}"
                url1200 = f"{base}/{t1200}/{public_id}"

                image_block = f'''
                <picture>
                  <source srcset="{url1200} 1200w, {url600} 600w, {url300} 300w" sizes="(max-width:768px) 90vw, 300px">
                  <img src="{url300}" srcset="{url300} 300w, {url600} 600w, {url1200} 1200w"
                       sizes="(max-width:768px) 90vw, 300px"
                       alt="{esc(name)} — portrait" loading="lazy" class="img-fluid rounded"/>
                </picture>
                '''
            except Exception as e:
                print(f"Warning: failed to build Cloudinary URLs for {public_id}: {e}", file=sys.stderr)
                image_block = '<div class="border rounded p-3 text-center"><p class="mb-0"><small>Picture non available</small></p></div>'
        else:
            # No image found -> placeholder
            image_block = '<div class="border rounded p-3 text-center"><p class="mb-0"><small>Picture non available</small></p></div>'

        content = PLAYER_TMPL.format(
          esc_name = esc(name),
          esc_country = esc(country),
          birth_date = esc(birth_date),
          esc_birthplace = esc(birthplace),
          height = esc(htxt),
          plays = esc(plays),
          backhand = esc(backhand), 
          best_rank = esc(best_rank),
          first_appearance = esc(first_app),
          last_appearance = esc(last_app),
          slug = slug,
          url_name = url_name,
          bio_block = bio_block,
          image_block = image_block
      )

        out_file = OUT_DIR / f"{filename_stem}.html"
        out_file.write_text(content, encoding="utf-8")

        entry = f'<a class="list-group-item list-group-item-action" href="{filename_stem}.html" data-name="{html.escape(name)}">{html.escape(name)} <small class="text-muted">({html.escape(country)})</small></a>'
        players_index_lines.append(entry)

    index_html = INDEX_TOP + "\n".join(players_index_lines) + INDEX_BOTTOM
    import subprocess
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
