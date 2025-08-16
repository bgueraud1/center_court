#!/usr/bin/env python3
"""
Génère:
 - docs/data/players_meta.json   : { player_id_str: {name,slug} }
 - docs/data/players_neighbors.json : 
      { player_id_str: { name, slug, top: [{id,slug,name,score},...], bottom: [...] } }

Utilise un fichier d'embeddings (players_graphsage_embeddings.csv ou node_embeddings_node2vec.csv)
et le fichier players.csv (ou player_data_wta.csv) pour les noms.
"""
import os, json
import numpy as np
import pandas as pd
from sklearn.preprocessing import normalize
from sklearn.neighbors import NearestNeighbors
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS_DATA = ROOT / "docs" / "data"
DOCS_DATA.mkdir(parents=True, exist_ok=True)

# --- CONFIG: change si besoin ---
EMB_PATHS = [
    ROOT / "players_graphsage_embeddings.csv",
    ROOT / "node_embeddings_node2vec.csv",
    ROOT / "triplet_finetuned_embeddings.csv",
    ROOT / "node_embeddings_by_surface_node2vec.csv"
]
PLAYERS_CSV = ROOT / "player_data_wta.csv"  # or other
TOP_K = 10
BOTTOM_K = 10

# --- helper slugify ---
def slugify(s):
    if s is None: return ""
    s = str(s).strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")

# 1) load embeddings (pick the first existing)
emb_df = None
for p in EMB_PATHS:
    if p.exists():
        try:
            emb_df = pd.read_csv(p)
            print("Loaded embeddings from", p)
            break
        except Exception as e:
            print("Cannot read", p, e)
if emb_df is None:
    raise FileNotFoundError("Aucun fichier d'embeddings trouvé. Regarde EMB_PATHS dans le script.")

# find embedding columns
emb_cols = [c for c in emb_df.columns if c.startswith('emb_') or c.startswith('gs_emb_') or c.startswith('surf_emb_') or c.startswith('trip_emb_')]
if len(emb_cols) == 0:
    # fallback: all numeric except player_id
    emb_cols = [c for c in emb_df.columns if np.issubdtype(emb_df[c].dtype, np.number) and c not in ('player_id',)]
    if len(emb_cols)==0:
        raise RuntimeError("Aucune colonne d'embedding détectée dans " + str(p))

# ensure player id column exists
if 'player_id' not in emb_df.columns:
    # try variations
    for cand in ['id','player','pid']:
        if cand in emb_df.columns:
            emb_df = emb_df.rename(columns={cand: 'player_id'})
            break
if 'player_id' not in emb_df.columns:
    raise RuntimeError("Le CSV d'embeddings doit contenir une colonne 'player_id'.")

# cast ids to str for robust keys
emb_df['player_id'] = emb_df['player_id'].apply(lambda x: str(int(x)) if (not pd.isna(x) and float(x).is_integer()) else str(x) if not pd.isna(x) else "")

# load player names mapping if available
players_map = {}
if PLAYERS_CSV.exists():
    mp = pd.read_csv(PLAYERS_CSV, dtype=str)
    # guess id & name columns
    id_col = None
    name_col = None
    for c in mp.columns:
        if c.lower().endswith('player_id') or c.lower()=='player_id' or c.lower()=='id':
            id_col = c
        if c.lower() in ('full_name','player_name','name','full name'):
            name_col = c
    if id_col is None:
        # try 'player_id' explicitly
        if 'player_id' in mp.columns: id_col = 'player_id'
    if name_col is None:
        # try heuristic
        for c in mp.columns:
            if 'name' in c.lower():
                name_col = c; break
    if id_col and name_col:
        for _, r in mp.iterrows():
            pid = r[id_col]
            if pd.isna(pid): continue
            pid = str(int(float(pid))) if str(pid).strip().isdigit() else str(pid)
            players_map[str(pid)] = {'name': r[name_col], 'slug': slugify(r[name_col])}
    else:
        print("Warning: impossible d'identifier id/name dans", PLAYERS_CSV)
else:
    print("Warning: players CSV not found at", PLAYERS_CSV)

# Any missing names from players_map -> try to infer from emb_df if name column present
if 'player_name' in emb_df.columns or 'player' in emb_df.columns:
    namecol = 'player_name' if 'player_name' in emb_df.columns else ('player' if 'player' in emb_df.columns else None)
    if namecol:
        for _, r in emb_df.iterrows():
            pid = str(r['player_id'])
            if pid not in players_map:
                players_map[pid] = {'name': r.get(namecol, str(pid)), 'slug': slugify(r.get(namecol, str(pid)))}

# default for any embedding id without name
for pid in emb_df['player_id'].unique():
    if pid not in players_map:
        players_map[pid] = {'name': pid, 'slug': slugify(pid)}

# build embeddings matrix (rows aligned to emb_df order)
X = emb_df[emb_cols].fillna(0.0).to_numpy(dtype=float)
# normalize rows (cosine)
Xn = normalize(X, axis=1)

# KNN search (cosine distance). sklearn NearestNeighbors with metric='cosine' returns distances in [0,2]
nn = NearestNeighbors(n_neighbors=TOP_K + 1, metric='cosine', algorithm='brute').fit(Xn)
distances, indices = nn.kneighbors(Xn, return_distance=True)

# build results
results = {}
player_ids = list(emb_df['player_id'].astype(str))
id_to_idx = {pid: i for i,pid in enumerate(player_ids)}

for i, pid in enumerate(player_ids):
    # distances[i,0] is self (0), indices too
    neighs = []
    for dist, idx in zip(distances[i,1:TOP_K+1], indices[i,1:TOP_K+1]):
        other_id = player_ids[idx]
        sim = 1.0 - float(dist)  # approximate cosine similarity
        neighs.append({
            "id": other_id,
            "slug": players_map.get(other_id,{}).get('slug', str(other_id)),
            "name": players_map.get(other_id,{}).get('name', str(other_id)),
            "score": round(sim, 6)
        })
    # bottom = farthest: compute using dot product on normalized vectors
    sims_all = (Xn @ Xn[i]).flatten()
    # get indices sorted ascending (smallest similarity = farthest)
    far_idx = np.argsort(sims_all)[:BOTTOM_K + 1]  # may include self if self is min
    bottom = []
    for idx in far_idx:
        other_id = player_ids[idx]
        if other_id == pid: 
            continue
        bottom.append({
            "id": other_id,
            "slug": players_map.get(other_id,{}).get('slug', str(other_id)),
            "name": players_map.get(other_id,{}).get('name', str(other_id)),
            "score": round(float(sims_all[idx]), 6)
        })
        if len(bottom) >= BOTTOM_K:
            break

    results[pid] = {
        "id": pid,
        "slug": players_map[pid]['slug'],
        "name": players_map[pid]['name'],
        "top": neighs,
        "bottom": bottom
    }

# write meta + neighbors
with open(DOCS_DATA / "players_meta.json", "w", encoding="utf8") as f:
    json.dump(players_map, f, ensure_ascii=False, indent=2)

with open(DOCS_DATA / "players_neighbors.json", "w", encoding="utf8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

print("Wrote", DOCS_DATA / "players_meta.json", "and", DOCS_DATA / "players_neighbors.json")


# --- append at end of scripts/generate_neighbors.py or in a new script ---
from pathlib import Path
TEMPL_DIR = Path(__file__).resolve().parents[1] / "docs" / "players"
TEMPL_DIR.mkdir(parents=True, exist_ok=True)

# small bootstrap HTML generator
page_template = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{name} — Central Court</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <nav class="navbar navbar-dark bg-dark mb-3">
    <div class="container">
      <a class="navbar-brand" href="../index.html">Central Court</a>
      <a class="nav-link text-white" href="index.html">Joueuses</a>
    </div>
  </nav>
  <main class="container py-4">
    <h1>{name}</h1>
    <p><strong>ID</strong> : {id}</p>
    <h3>Joueuses les plus proches</h3>
    <ul>
      {top_html}
    </ul>
    <h3>Joueuses les plus éloignées</h3>
    <ul>
      {bottom_html}
    </ul>
  </main>
  <footer class="text-center py-3"><small>© Central Court</small></footer>
</body>
</html>"""

for pid, rec in results.items():
    slug = rec.get("slug") or slugify(rec.get("name") or pid)
    top_html = "\n".join(f'<li><a href="{players_map.get(x["id"],{{}}).get("slug",x["id"])}.html">{x["name"]} <small>({x["score"]})</small></a></li>' for x in rec["top"])
    bottom_html = "\n".join(f'<li><a href="{players_map.get(x["id"],{{}}).get("slug",x["id"])}.html">{x["name"]} <small>({x["score"]})</small></a></li>' for x in rec["bottom"])
    outpath = TEMPL_DIR / f"{slug}.html"
    outpath.write_text(page_template.format(name=rec["name"], id=pid, top_html=top_html, bottom_html=bottom_html), encoding="utf8")
print("Wrote per-player pages in", TEMPL_DIR)
