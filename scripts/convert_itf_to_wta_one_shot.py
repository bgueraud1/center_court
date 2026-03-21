import json
from pathlib import Path
import os
import tempfile
import shutil

# À modifier
INPUT_PATH = Path(r"docs/wta_tournaments_2026.json")
OUTPUT_PATH = Path(r"docs/wta_tournaments_clean_2026.json")
# Si tu veux écraser le fichier d'origine, mets OUTPUT_PATH = INPUT_PATH

def remove_itf_tournaments(data: dict) -> dict:
    content = data.get("content", [])

    if not isinstance(content, list):
        raise ValueError("Le champ 'content' doit être une liste.")

    filtered_content = []
    for tournament in content:
        # On filtre si le level est ITF au niveau racine
        level = tournament.get("level")

        # Sécurité supplémentaire : si jamais le level est porté par tournamentGroup
        group_level = tournament.get("tournamentGroup", {}).get("level")

        if level == "ITF" or group_level == "ITF":
            continue

        filtered_content.append(tournament)

    data["content"] = filtered_content

    # Optionnel : mettre à jour numEntries si présent
    if "pageInfo" in data and isinstance(data["pageInfo"], dict):
        data["pageInfo"]["numEntries"] = len(filtered_content)

    return data

def main():
    with INPUT_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned_data = remove_itf_tournaments(data)

    # Écriture sûre
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, ensure_ascii=False, indent=2)

    print(f"Terminé : {len(data.get('content', []))} tournois lus, {len(cleaned_data.get('content', []))} conservés.")
    print(f"Fichier écrit ici : {OUTPUT_PATH}")

if __name__ == "__main__":
    main()