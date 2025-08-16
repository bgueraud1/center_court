# scripts/make_quick_embeddings.py
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.decomposition import PCA
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLAYERS_CSV = ROOT /"player_data_wta.csv"
OUT_CSV = ROOT / "node_embeddings_node2vec.csv"   # nom attendu par generate_neighbors.py
N_COMPONENTS = 64

if not PLAYERS_CSV.exists():
    raise FileNotFoundError(f"Fichier introuvable: {PLAYERS_CSV}")

# read as strings to be safe
df = pd.read_csv(PLAYERS_CSV, dtype=str)

# create ids: use player_id if present, else index
if 'player_id' in df.columns:
    # ensure we produce a Series aligned with df.index
    ids = df['player_id'].where(df['player_id'].notna(), pd.Series(df.index.astype(str), index=df.index)).astype(str)
else:
    ids = pd.Series(df.index.astype(str), index=df.index).astype(str)





# choose features to build a simple numeric vector
feat = pd.DataFrame(index=df.index)

# best_rank -> numeric (lower is better)
if 'best_rank' in df.columns:
    feat['best_rank'] = pd.to_numeric(df['best_rank'], errors='coerce')

# height parsing: try to extract cm if provided, else inches
def parse_height(s):
    if pd.isna(s) or s is None:
        return np.nan
    s = str(s).strip()
    # already cm like "1.75m" or "175 cm"
    if 'm' in s and any(ch.isdigit() for ch in s):
        try:
            # formats like "1.75m" -> 1.75 * 100
            return float(s.replace('m','').strip()) * 100
        except:
            pass
    if 'cm' in s:
        try:
            return float(s.replace('cm','').strip())
        except:
            pass
    # format like 5' 11" or 5'11"
    if "'" in s:
        try:
            parts = s.split("'")
            feet = float(parts[0].strip())
            inches = 0.0
            if len(parts) > 1:
                rest = parts[1].replace('"','').strip()
                if rest:
                    inches = float(rest)
            return (feet * 12 + inches) * 2.54
        except:
            pass
    try:
        return float(s)
    except:
        return np.nan

if 'height_cm' in df.columns:
    feat['height_cm'] = pd.to_numeric(df['height_cm'], errors='coerce')
elif 'height_inches' in df.columns:
    feat['height_cm'] = pd.to_numeric(df['height_inches'], errors='coerce') * 2.54
elif 'height' in df.columns:
    feat['height_cm'] = df['height'].apply(parse_height)
else:
    # no height info
    pass

# plays (handedness) and country -> one-hot
cat_cols = []
if 'plays' in df.columns:
    cat_cols.append('plays')
if 'represented_country' in df.columns:
    cat_cols.append('represented_country')

if cat_cols:
    cats = df[cat_cols].fillna("NA")
    # One-hot encoder — compatible sklearn old/new
    try:
        # sklearn >= 1.2 uses sparse_output
        enc = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    except TypeError:
        try:
            # older sklearn uses sparse
            enc = OneHotEncoder(sparse=False, handle_unknown='ignore')
        except TypeError:
            # last-resort: construct with default args and rely on dense transform later
            enc = OneHotEncoder(handle_unknown='ignore')

    catmat = enc.fit_transform(cats)
    cat_cols_names = enc.get_feature_names_out(cat_cols)
    cat_df = pd.DataFrame(catmat, index=df.index, columns=[f"{c}" for c in cat_cols_names])
    feat = pd.concat([feat, cat_df], axis=1)

# Fill missing numeric columns with median (or 0 if median cannot be computed)
if feat.shape[1] > 0:
    feat = feat.apply(pd.to_numeric, errors='coerce')
    med = feat.median().fillna(0.0)
    feat = feat.fillna(med)
else:
    # fallback: nothing numeric -> create small random features
    feat = pd.DataFrame(np.random.RandomState(42).rand(len(df), 4), index=df.index, columns=[f'rand_{i}' for i in range(4)])

# Build matrix for PCA
Xraw = feat.values.astype(float)

# PCA to N_COMPONENTS (or pad with zeros)
n_comp = min(N_COMPONENTS, Xraw.shape[1])
pca = PCA(n_components=n_comp, random_state=42)
Xp = pca.fit_transform(Xraw)

if Xp.shape[1] < N_COMPONENTS:
    pad = np.zeros((Xp.shape[0], N_COMPONENTS - Xp.shape[1]))
    X = np.hstack([Xp, pad])
else:
    X = Xp

# write CSV with columns emb_0..emb_{N-1} and player_id
out_cols = [f"emb_{i}" for i in range(N_COMPONENTS)]
out_df = pd.DataFrame(X, columns=out_cols, index=df.index)
out_df.insert(0, 'player_id', ids.values)
out_df.to_csv(OUT_CSV, index=False)
print("Embeddings rapides générés ->", OUT_CSV)
