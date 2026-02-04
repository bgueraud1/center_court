#!/usr/bin/env python3
"""
split_tournaments.py

Script temporaire pour lire un ou plusieurs CSV (chemins fournis explicitement)
et écrire un fichier CSV final par tournoi dans matches/wta_matches/ nommé
wta_<tourney_id>_<tourney_year>.csv

Usage:
    python split_tournaments.py --files path/to/a.csv path/to/b.csv --verbose
"""
from __future__ import annotations
import os
import argparse
import pandas as pd
import numpy as np
import re
from typing import List, Optional, Set

OUT_DIR = os.path.join("matches", "wta_matches")

def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)

def sanitize_id(x) -> str:
    """Convertit l'id en chaîne sûre pour le nom de fichier."""
    if pd.isna(x) or x is None:
        return ""
    s = str(int(x)) if (isinstance(x, (float, int)) and not np.isnan(x) and float(x).is_integer()) else str(x)
    # n'autoriser que chiffres, lettres, underscore et tiret
    s = re.sub(r"[^\w\-\.]", "_", s)
    return s

def sanitize_year(y, fallback_row: Optional[pd.Series] = None) -> Optional[str]:
    """Normalise tourney_year (ex: 2025.0 -> '2025'). Fallback sur event_year/year si absent."""
    if y is not None and not (isinstance(y, float) and np.isnan(y)):
        try:
            if isinstance(y, float) and float(y).is_integer():
                return str(int(y))
            return str(y).strip()
        except Exception:
            pass
    # try fallback from row
    if fallback_row is not None:
        for alt in ("event_year", "year", "tourney_year"):
            if alt in fallback_row and fallback_row[alt] not in (None, "", float("nan")):
                v = fallback_row[alt]
                try:
                    if isinstance(v, float) and float(v).is_integer():
                        return str(int(v))
                    return str(v).strip()
                except Exception:
                    continue
    return None

def read_csv_robust(path: str) -> pd.DataFrame:
    """Lit le CSV avec des options robustes."""
    # try with low_memory False to avoid dtype guessing issues
    return pd.read_csv(path, low_memory=False)

def existing_match_ids(outfile: str) -> Set[str]:
    """
    Lit rapidement la colonne match_id du fichier déjà existant (pour éviter d'ajouter des doublons).
    Si le fichier n'existe pas, retourne set() vide.
    """
    if not os.path.isfile(outfile):
        return set()
    try:
        # read only match_id column if present
        df = pd.read_csv(outfile, usecols=[c for c in ["match_id"] if c in pd.read_csv(outfile, nrows=0).columns], low_memory=True)
        if "match_id" in df.columns:
            return set([str(x) for x in df["match_id"].dropna().astype(str).tolist()])
        return set()
    except Exception:
        # fallback: scan full file, then extract match_id if present
        try:
            df = pd.read_csv(outfile, low_memory=True)
            if "match_id" in df.columns:
                return set([str(x) for x in df["match_id"].dropna().astype(str).tolist()])
            return set()
        except Exception:
            return set()

def write_or_append(outfile: str, df: pd.DataFrame, verbose: bool = True):
    """Ecrit df dans outfile. Si outfile existe, n'ajoute que les lignes avec match_id non présentes."""
    if df is None or df.empty:
        if verbose:
            print(f"[SKIP] Aucun enregistrement à écrire pour {outfile}")
        return

    # normaliser match_id to string for dedupe
    if "match_id" in df.columns:
        df["match_id"] = df["match_id"].apply(lambda x: "" if pd.isna(x) else str(x))
    else:
        # create empty match_id column to avoid KeyErrors later
        df["match_id"] = [""] * len(df)

    if os.path.exists(outfile):
        existing_ids = existing_match_ids(outfile)
        if not existing_ids:
            # existing file but no match_id column or couldn't read — safe append all lines
            try:
                df.to_csv(outfile, mode="a", index=False, header=False)
                if verbose:
                    print(f"[APPEND] Ajouté {len(df)} lignes à {outfile} (header preserved).")
            except Exception as e:
                print(f"[ERR] Impossible d'append à {outfile}: {e}")
            return
        # filter out rows whose match_id already present
        mask_new = ~df["match_id"].isin(existing_ids)
        new_df = df[mask_new]
        if new_df.empty:
            if verbose:
                print(f"[SKIP] Aucun nouvel match à ajouter pour {outfile} (détectés par match_id).")
            return
        try:
            new_df.to_csv(outfile, mode="a", index=False, header=False)
            if verbose:
                print(f"[APPEND] Ajouté {len(new_df)} nouvelles lignes à {outfile}.")
        except Exception as e:
            print(f"[ERR] Impossible d'append à {outfile}: {e}")
    else:
        # write new file (with header)
        try:
            df.to_csv(outfile, index=False, header=True)
            if verbose:
                print(f"[WRITE] Créé {outfile} avec {len(df)} lignes.")
        except Exception as e:
            print(f"[ERR] Impossible d'écrire {outfile}: {e}")

def split_files_to_tournaments(file_paths: List[str], verbose: bool = True):
    ensure_out_dir()
    summary = {}  # (tourney_id, tourney_year) -> count written

    for path in file_paths:
        if not os.path.isfile(path):
            print(f"[WARN] Fichier introuvable, skip: {path}")
            continue
        if verbose:
            print(f"[INFO] Lecture de {path} ...")
        try:
            df = read_csv_robust(path)
        except Exception as e:
            print(f"[ERR] Impossible de lire {path}: {e}")
            continue

        if df.empty:
            if verbose:
                print(f"[INFO] {path} est vide, skip.")
            continue

        # ensure columns exist in lowercase mapping to original names if necessary
        # but we'll access columns by exact names given by user (tourney_id,tourney_year)
        # fallback if tourney_year missing: try event_year or year
        # process grouping by tourney_id + tourney_year
        # if tourney_id missing for a row, we skip it with warning
        # For performance, groupby on (tourney_id, tourney_year) using df.fillna approach

        # Normalize column names existence
        cols = set(df.columns.tolist())
        # We'll attempt to coerce tourney_id and tourney_year from alternatives
        if "tourney_id" not in cols and "event_id" in cols:
            df["tourney_id"] = df["event_id"]
        if "tourney_year" not in cols:
            if "event_year" in cols:
                df["tourney_year"] = df["event_year"]
            elif "year" in cols:
                df["tourney_year"] = df["year"]
        if "match_id" not in cols:
            # create match_id column if not exists to help dedupe later
            df["match_id"] = ["" for _ in range(len(df))]

        # ensure tourney_id present as string/int
        df["__tourney_id_raw"] = df.get("tourney_id", pd.Series([""] * len(df))).apply(lambda x: x if not (pd.isna(x)) else None)

        # create normalized year column (string)
        def _norm_year_row(row):
            y = row.get("tourney_year", None)
            yr = sanitize_year(y, fallback_row=row)
            return yr
        df["__tourney_year_norm"] = df.apply(_norm_year_row, axis=1)

        # iterate unique pairs
        pairs = df[["__tourney_id_raw", "__tourney_year_norm"]].drop_duplicates().values.tolist()
        for raw_tid, year_norm in pairs:
            if raw_tid is None or raw_tid == "" or year_norm is None or year_norm == "":
                # skip rows without proper id/year — but there may be rows with missing id/year; skip them
                if verbose:
                    # only warn once per file for missing
                    pass
                continue
            tid_s = sanitize_id(raw_tid)
            year_s = year_norm
            outfile = os.path.join(OUT_DIR, f"wta_{tid_s}_{year_s}.csv")

            # select rows matching this pair
            sel = df[(df["__tourney_id_raw"].notna()) & (df["__tourney_id_raw"].astype(str).str.strip() == str(raw_tid).strip()) & (df["__tourney_year_norm"].astype(str).str.strip() == year_s)]
            if sel.empty:
                continue

            # drop helper cols before writing
            to_write = sel.drop(columns=[c for c in ["__tourney_id_raw", "__tourney_year_norm"] if c in sel.columns])

            # write or append safely
            write_or_append(outfile, to_write, verbose=verbose)

            # update summary
            key = (tid_s, year_s)
            summary[key] = summary.get(key, 0) + len(to_write)

        # optional: report rows that couldn't be assigned to a tournament
        unassigned = df[(df["__tourney_id_raw"].isna()) | (df["__tourney_year_norm"].isna())]
        if not unassigned.empty and verbose:
            print(f"[WARN] {len(unassigned)} lignes dans {path} sans (tourney_id,tourney_year) valides -> ignorées.")

    # print summary
    if verbose:
        print("\n=== Résumé d'écriture ===")
        if not summary:
            print("Aucun tournoi écrit.")
        else:
            for (tid, year), cnt in summary.items():
                print(f"  - wta_{tid}_{year}.csv : ~{cnt} lignes traitées (peut inclure appends)")
    return summary

def parse_args():
    p = argparse.ArgumentParser(description="Sépare un ou plusieurs CSV en fichiers par tournoi (wta_<ID>_<YEAR>.csv) dans matches/wta_matches/")
    p.add_argument("--files", nargs="+", required=True, help="Chemins vers les fichiers CSV à traiter.")
    p.add_argument("--verbose", action="store_true", help="Affiche logs détaillés.")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    split_files_to_tournaments(args.files, verbose=args.verbose)
