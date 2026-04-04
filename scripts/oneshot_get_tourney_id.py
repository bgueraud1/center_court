import json
from pathlib import Path
from datetime import date

json_path = Path(r"docs/wta_tournaments_2026.json")

start_limit = date(2026, 1, 1)
end_limit = date(2026, 4, 3)

allowed_levels = {
    "WTA 125",
    "WTA 250",
    "WTA 500",
    "WTA 1000",
    "Grand Slam",
}

with json_path.open("r", encoding="utf-8") as f:
    data = json.load(f)

ids = []

for tournoi in data.get("content", []):
    level = tournoi.get("level")
    end_date_str = tournoi.get("endDate")
    if not level or not end_date_str:
        continue

    if level not in allowed_levels:
        continue

    end_date = date.fromisoformat(end_date_str)

    if start_limit <= end_date <= end_limit:
        # Id du tournoi :
        ids.append(tournoi["tournamentGroup"]["id"])
        # Si tu préfères l'id live scoring, remplace la ligne au-dessus par :
        # ids.append(tournoi["liveScoringId"])

ids = sorted(set(ids))
print(ids)