#!/usr/bin/env python3
"""
Synchronise les métadonnées des joueurs depuis les CSV WTA/ATP vers les JSON.

Règles :
- JSON absent  -> création d'un JSON avec métadonnées uniquement, sans matchs.
- JSON présent -> le CSV est autoritaire pour les métadonnées biographiques.
- Les données de matchs, summary, image et les éventuelles clés JSON
  supplémentaires sont conservées.
- Le script est idempotent : un second passage ne réécrit rien si les données
  sont déjà synchronisées.

Architecture attendue :
project/
├── player_data_wta.csv
├── player_data_atp.csv
├── docs/
│   ├── players/data/
│   └── players_atp/data/
└── scripts/
    └── sync_player_metadata.py

Exécution depuis la racine du projet :
    python scripts/sync_player_metadata.py

Ou depuis n'importe où :
    python /chemin/vers/scripts/sync_player_metadata.py --root /chemin/du/projet
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any


DATASETS = (
    {
        "tour": "WTA",
        "csv": Path("player_data_wta.csv"),
        "data_dir": Path("docs/players/data"),
        "rank_column": "best_rank",
    },
    {
        "tour": "ATP",
        "csv": Path("player_data_atp.csv"),
        "data_dir": Path("docs/players_atp/data"),
        "rank_column": "highest_ranking",
    },
)


# Champs considérés comme des métadonnées joueur.
# "age" n'est volontairement pas stocké : il évolue chaque jour et se déduit
# de birthdate au moment où il est nécessaire.
METADATA_FIELDS = (
    "player_id",
    "name",
    "slug",
    "country",
    "birthdate",
    "birthplace",
    "height_cm",
    "hand",
    "backhand",
    "best_rank",
    "first_appearance",
    "last_appearance",
    # Métadonnées supplémentaires présentes dans les CSV.
    "reviewed_player",
    "date_review",
    "biography",
    "turned_pro",
    "retired",
    "prize_money",
)


def clean_text(value: Any) -> str | None:
    """Retourne un texte propre ou None pour les valeurs vides/NaN."""
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text


def normalize_bool(value: Any) -> bool | None:
    """Convertit les représentations CSV courantes d'un booléen."""
    if value is None:
        return None

    text = str(value).strip().lower()
    if text in {"", "nan", "none", "null"}:
        return None
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False

    return bool(value)


def normalize_number(value: Any) -> int | float | None:
    """Convertit un nombre CSV en int/float/None."""
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None

    try:
        number = float(text)
    except ValueError:
        return None

    return int(number) if number.is_integer() else number


def normalize_height_cm(value: Any) -> float | None:
    """
    Normalise une valeur telle que 1.82m ou 182cm.

    IMPORTANT : dans les JSON fournis en exemple, height_cm contient en fait
    la valeur en mètres (1.82 pour 1.82m). On conserve donc cette convention
    pour rester compatible avec les fichiers existants.
    """
    text = clean_text(value)
    if text is None:
        return None

    match = re.search(r"-?\d+(?:[.,]\d+)?", text)
    if not match:
        return None

    number = float(match.group(0).replace(",", "."))

    # Le projet stocke 1.82 et non 182 pour "1.82m".
    return number


def normalize_date(value: Any) -> str | None:
    """Normalise les dates/datetimes courants vers YYYY-MM-DD."""
    text = clean_text(value)
    if text is None:
        return None

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%B %d %Y",
        "%b %d %Y",
        "%B %d, %Y",
        "%b %d, %Y",
    )

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        pass

    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if match:
        return match.group(1)

    raise ValueError(f"Format de date inconnu : {value!r}")


def normalize_date_or_text(value: Any) -> str | None:
    """Normalise une date si possible, sinon conserve la valeur textuelle."""
    text = clean_text(value)
    if text is None:
        return None

    try:
        return normalize_date(text)
    except ValueError:
        return text


def slugify_name(name: str) -> str:
    """Transforme un nom en slug stable pour le nom du fichier."""
    normalized = unicodedata.normalize("NFKD", name)
    normalized = "".join(
        char for char in normalized if not unicodedata.combining(char)
    )
    normalized = normalized.lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


def build_slug(player_id: str, full_name: str) -> str:
    return f"{player_id.lower()}-{slugify_name(full_name)}"


def expected_filename(player_id: str, full_name: str) -> str:
    return f"{build_slug(player_id, full_name)}.json"


def normalize_for_compare(value: Any) -> Any:
    """Normalise les valeurs uniquement pour la comparaison CSV <-> JSON."""
    if isinstance(value, float) and value != value:  # NaN
        return None

    if isinstance(value, str):
        text = value.strip()
        if text.lower() in {"", "nan", "none", "null"}:
            return None
        return text

    if isinstance(value, dict):
        return {key: normalize_for_compare(val) for key, val in value.items()}

    if isinstance(value, list):
        return [normalize_for_compare(item) for item in value]

    return value


def parse_json_constant(value: str) -> None:
    """
    Permet de lire d'anciens JSON contenant NaN/Infinity, qui ne sont pas
    du JSON standard, en les transformant en None.
    """
    if value in {"NaN", "Infinity", "-Infinity"}:
        return None
    raise ValueError(f"Constante JSON non supportée : {value}")


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle, parse_constant=parse_json_constant)

    if not isinstance(data, dict):
        raise ValueError(f"La racine du JSON doit être un objet : {path}")

    return data


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """Ecrit le JSON de façon atomique pour éviter un fichier tronqué."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    with tmp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
        handle.write("\n")

    tmp_path.replace(path)


def build_existing_file_index(data_dir: Path) -> dict[str, Path]:
    """
    Construit un index ID -> fichier à partir de : ID_firstname_lastname.json.

    On se base sur le nom de fichier pour éviter d'ouvrir potentiellement des
    milliers de JSON complets contenant chacun beaucoup de matchs.
    """
    index: dict[str, Path] = {}

    for path in data_dir.glob("*.json"):
        player_id = path.stem.split("_", 1)[0].strip()
        if player_id:
            index[player_id.lower()] = path

    return index


def build_metadata(row: dict[str, str], rank_column: str) -> dict[str, Any]:
    """Construit les métadonnées canoniques à partir d'une ligne CSV."""
    player_id = clean_text(row.get("player_id"))
    full_name = clean_text(row.get("full_name"))

    if not player_id:
        raise ValueError("player_id manquant")
    if not full_name:
        raise ValueError(f"full_name manquant pour player_id={player_id}")

    return {
        "player_id": player_id,
        "name": full_name,
        "slug": build_slug(player_id, full_name),
        "country": clean_text(row.get("represented_country")),
        "birthdate": normalize_date(row.get("birth_date")),
        "birthplace": clean_text(row.get("birthplace")),
        "height_cm": normalize_height_cm(row.get("height_cm")),
        "hand": clean_text(row.get("plays")),
        "backhand": clean_text(row.get("backhand")),
        "best_rank": normalize_number(row.get(rank_column)),
        "first_appearance": normalize_date(row.get("first_appearance")),
        "last_appearance": normalize_date(row.get("last_appearance")),
        "reviewed_player": normalize_bool(row.get("reviewed_player")),
        "date_review": normalize_date(row.get("date_review")),
        "biography": clean_text(row.get("biography")),
        "turned_pro": normalize_date_or_text(row.get("turned_pro")),
        "retired": normalize_date_or_text(row.get("retired")),
        "prize_money": clean_text(row.get("prize_money")),
    }


def create_player_json(metadata: dict[str, Any]) -> dict[str, Any]:
    """Crée le JSON minimal d'un joueur absent."""
    return {
        **metadata,
        "image": None,
        "summary": {
            "matches_played": 0,
            "matches_won": 0,
            "matches_lost": 0,
        },
        "matches": [],
    }


def sync_one_csv(
    root: Path,
    csv_path: Path,
    data_dir: Path,
    rank_column: str,
) -> dict[str, int]:
    """Synchronise un CSV complet vers son répertoire JSON."""
    full_csv_path = root / csv_path
    full_data_dir = root / data_dir

    if not full_csv_path.exists():
        raise FileNotFoundError(f"CSV introuvable : {full_csv_path}")

    full_data_dir.mkdir(parents=True, exist_ok=True)
    existing_files = build_existing_file_index(full_data_dir)

    stats = {
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
    }

    with full_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(f"CSV vide ou sans en-tête : {full_csv_path}")

        for line_number, row in enumerate(reader, start=2):
            # Ignore les lignes totalement vides.
            if not any((value or "").strip() for value in row.values()):
                continue

            try:
                metadata = build_metadata(row, rank_column)
                player_id = metadata["player_id"]
                full_name = metadata["name"]

                expected_path = full_data_dir / expected_filename(player_id, full_name)

                # Le nom attendu a priorité. Si un ancien fichier porte encore
                # le même ID avec un ancien nom, on le réutilise pour éviter un
                # doublon et on conserve son nom de fichier.
                existing_path = expected_path
                if not existing_path.exists():
                    existing_path = existing_files.get(
                        player_id.lower(),
                        expected_path,
                    )

                if existing_path.exists():
                    current = load_json(existing_path)

                    before = {
                        field: normalize_for_compare(current.get(field))
                        for field in METADATA_FIELDS
                    }
                    after = {
                        field: normalize_for_compare(metadata.get(field))
                        for field in METADATA_FIELDS
                    }

                    if before != after:
                        # Mise à jour ciblée : les matchs et autres champs
                        # historiques/custom restent inchangés.
                        current.update(metadata)
                        write_json_atomic(existing_path, current)
                        stats["updated"] += 1
                    else:
                        stats["unchanged"] += 1
                else:
                    new_data = create_player_json(metadata)
                    write_json_atomic(expected_path, new_data)
                    existing_files[player_id.lower()] = expected_path
                    stats["created"] += 1

            except Exception as exc:
                stats["errors"] += 1
                print(
                    f"[ERROR] {full_csv_path.name}:{line_number}: {exc}",
                    file=sys.stderr,
                )

    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Synchronise les métadonnées WTA/ATP des CSV vers les JSON."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help=(
            "Racine du projet contenant les CSV et docs/ "
            "(par défaut : dossier parent de scripts/)"
        ),
    )
    args = parser.parse_args()
    root = args.root.resolve()

    print(f"Project root: {root}")

    total = {
        "created": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
    }

    for dataset in DATASETS:
        print(f"\n[{dataset['tour']}] {dataset['csv']} -> {dataset['data_dir']}")

        try:
            stats = sync_one_csv(
                root=root,
                csv_path=dataset["csv"],
                data_dir=dataset["data_dir"],
                rank_column=dataset["rank_column"],
            )
        except Exception as exc:
            print(f"[FATAL] {dataset['tour']}: {exc}", file=sys.stderr)
            total["errors"] += 1
            continue

        for key, value in stats.items():
            total[key] += value

        print(
            "  "
            f"created={stats['created']} "
            f"updated={stats['updated']} "
            f"unchanged={stats['unchanged']} "
            f"errors={stats['errors']}"
        )

    print(
        "\nTotal: "
        f"created={total['created']} "
        f"updated={total['updated']} "
        f"unchanged={total['unchanged']} "
        f"errors={total['errors']}"
    )

    return 1 if total["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
