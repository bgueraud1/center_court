from __future__ import annotations

import re
import shutil
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd


INPUT_DIR = Path("court_vision_player_json")
OUTPUT_DIR = Path("court_vision_post_treated")
WTA_CSV = Path("player_data_wta_.csv")
ATP_CSV = Path("player_data_atp.csv")


# -----------------------------
# Normalisation / utilitaires
# -----------------------------
def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(c for c in text if not unicodedata.combining(c))


def normalize_text(text: str) -> str:
    """
    Normalise pour comparer proprement:
    - minuscules
    - suppression accents
    - caractères non alphanumériques -> espaces
    - espaces multiples compressés
    """
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = strip_accents(str(text)).lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def slugify_name_part(text: str) -> str:
    """
    Pour les noms de fichiers:
    - minuscules
    - suppression accents
    - espaces / ponctuation -> underscore
    """
    text = strip_accents(str(text)).lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def first_last_from_full_name(full_name: str) -> tuple[str, str]:
    """
    Extrait prénom = premier mot, nom = dernier mot.
    Exemple: 'Novak Djokovic' -> ('Novak', 'Djokovic')
    """
    parts = str(full_name).strip().split()
    if not parts:
        return ("unknown", "unknown")
    if len(parts) == 1:
        return (parts[0], parts[0])
    return (parts[0], parts[-1])


def parse_filename(path: Path) -> Optional[tuple[str, str]]:
    """
    Attend un nom du type:
      a_anisimova_court_vision.json
    Retourne (initiale, nom_slug)
    """
    stem = path.stem
    m = re.match(r"^(?P<initial>[a-z])_(?P<last>.+?)_court_vision$", stem.lower())
    if not m:
        return None
    return m.group("initial"), m.group("last")


# -----------------------------
# Chargement des données joueurs
# -----------------------------
def load_players() -> pd.DataFrame:
    frames = []

    for csv_path, tour in [(WTA_CSV, "WTA"), (ATP_CSV, "ATP")]:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV introuvable: {csv_path}")

        df = pd.read_csv(csv_path, dtype=str).copy()
        if "full_name" not in df.columns or "player_id" not in df.columns:
            raise ValueError(f"{csv_path} doit contenir au minimum les colonnes full_name et player_id.")

        df["tour"] = tour
        df["full_name"] = df["full_name"].fillna("").astype(str)
        df["player_id"] = df["player_id"].fillna("").astype(str)

        df["full_name_norm"] = df["full_name"].map(normalize_text)
        df["player_id_norm"] = df["player_id"].map(normalize_text)

        split_name = df["full_name"].str.strip().str.split()
        df["first_name"] = split_name.str[0].fillna("")
        df["last_name"] = split_name.str[-1].fillna("")
        df["first_initial"] = df["first_name"].str[:1].str.lower().fillna("")
        df["last_name_norm"] = df["last_name"].map(normalize_text)

        frames.append(df)

    all_players = pd.concat(frames, ignore_index=True)

    # On garde seulement les colonnes utiles pour la résolution
    cols = [
        "tour", "full_name", "player_id", "first_name", "last_name",
        "first_initial", "full_name_norm", "player_id_norm", "last_name_norm"
    ]
    return all_players[cols].copy()


# -----------------------------
# Recherche / résolution joueur
# -----------------------------
def search_player(players: pd.DataFrame, query: str) -> pd.DataFrame:
    """
    Recherche un joueur à partir d'un nom complet ou d'un ID.
    """
    q_norm = normalize_text(query)

    # 1) match exact sur player_id
    exact_id = players[players["player_id_norm"] == q_norm]

    # 2) match exact sur full_name
    exact_name = players[players["full_name_norm"] == q_norm]

    # 3) match partiel sur full_name / player_id
    partial = players[
        players["full_name_norm"].str.contains(re.escape(q_norm), na=False)
        | players["player_id_norm"].str.contains(re.escape(q_norm), na=False)
    ]

    out = pd.concat([exact_id, exact_name, partial], ignore_index=True).drop_duplicates()
    return out.reset_index(drop=True)


def prompt_choose_candidate(candidates: pd.DataFrame) -> pd.Series:
    """
    Affiche une liste de candidats et demande à l'utilisateur d'en choisir un.
    """
    print("\nPlusieurs joueurs correspondent :")
    display_cols = ["tour", "full_name", "player_id"]
    for idx, row in candidates[display_cols].reset_index(drop=True).iterrows():
        print(f"  [{idx}] {row['full_name']} | id={row['player_id']} | tour={row['tour']}")

    while True:
        choice = input("Choisis l'index du bon joueur : ").strip()
        if choice.isdigit():
            i = int(choice)
            if 0 <= i < len(candidates):
                return candidates.iloc[i]
        print("Choix invalide. Réessaie.")


def resolve_player_from_filename(players: pd.DataFrame, json_path: Path) -> Optional[pd.Series]:
    """
    Résout le joueur à partir du nom du fichier.
    Exemple:
      a_anisimova_court_vision.json -> Amanda Anisimova
    """
    parsed = parse_filename(json_path)
    if parsed is None:
        return None

    initial, surname = parsed
    surname_norm = normalize_text(surname)

    # Candidats: même initiale + même nom de famille
    candidates = players[
        (players["first_initial"] == initial)
        & (
            (players["last_name_norm"] == surname_norm)
            | (players["full_name_norm"].str.contains(rf"\b{re.escape(surname_norm)}\b", regex=True, na=False))
        )
    ].copy()

    if len(candidates) == 1:
        return candidates.iloc[0]

    if len(candidates) > 1:
        return prompt_choose_candidate(candidates)

    # Aucun candidat trouvé: on demande à l'utilisateur un nom complet ou un ID
    print(f"\nAucun joueur trouvé pour le fichier: {json_path.name}")
    while True:
        query = input("Entre le nom complet du joueur ou son ID (ou vide pour passer) : ").strip()
        if not query:
            return None

        found = search_player(players, query)
        if len(found) == 1:
            return found.iloc[0]
        elif len(found) > 1:
            return prompt_choose_candidate(found)
        else:
            print("Aucun résultat dans les deux CSV. Réessaie.")


def output_filename(player_row: pd.Series) -> str:
    """
    Génère le nom final:
      prenom_nom_id_court_vision.json
    """
    first_name, last_name = first_last_from_full_name(player_row["full_name"])
    first_slug = slugify_name_part(first_name)
    last_slug = slugify_name_part(last_name)
    player_id = slugify_name_part(player_row["player_id"])

    return f"{first_slug}_{last_slug}_{player_id}_court_vision.json"


# -----------------------------
# Traitement principal
# -----------------------------
def main() -> None:
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Dossier introuvable: {INPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    players = load_players()

    json_files = sorted(INPUT_DIR.glob("*_court_vision.json"))
    if not json_files:
        print(f"Aucun fichier JSON trouvé dans {INPUT_DIR}")
        return

    print(f"{len(json_files)} fichier(s) JSON trouvé(s).")
    print("Traitement en cours...\n")

    renamed = 0
    skipped = 0

    for json_path in json_files:
        player_row = resolve_player_from_filename(players, json_path)

        if player_row is None:
            print(f"-> Ignoré: {json_path.name}")
            skipped += 1
            continue

        new_name = output_filename(player_row)
        dest_path = OUTPUT_DIR / new_name

        # Si le fichier existe déjà, on ajoute un suffixe numérique
        if dest_path.exists():
            base = dest_path.stem
            suffix = dest_path.suffix
            i = 1
            while True:
                candidate = OUTPUT_DIR / f"{base}_{i}{suffix}"
                if not candidate.exists():
                    dest_path = candidate
                    break
                i += 1

        shutil.copy2(json_path, dest_path)
        print(f"-> {json_path.name}  -->  {dest_path.name}")
        renamed += 1

    print("\nTerminé.")
    print(f"Renommés: {renamed}")
    print(f"Ignorés:   {skipped}")
    print(f"Sortie:    {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()