import csv

csv_file = "C:/Users/bengu/Documents/tennis/player_base_and_maps/docs/games/blur_game/questions.csv"
tags_column = "tags"

unique_tags = set()

with open(csv_file, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        cell = row.get(tags_column, "")
        if not cell:
            continue

        # On découpe sur les virgules puis on nettoie les espaces
        tags = [tag.strip() for tag in cell.split(",")]

        # On ignore les éléments vides éventuels
        unique_tags.update(tag for tag in tags if tag)

# Liste triée des tags uniques
unique_tags_list = sorted(unique_tags)

print(unique_tags_list)