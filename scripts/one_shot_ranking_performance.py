import json
from pathlib import Path

def rank_players_by_circuit(data, circuit):
    players = data.get("players", [])

    filtered = [
        p for p in players
        if p.get("circuit") == circuit and p.get("performance_index") is not None
    ]

    ranked = sorted(filtered, key=lambda p: p["performance_index"], reverse=True)

    # Ne garder que les champs utiles
    compact_ranked = []
    for rank, player in enumerate(ranked, start=1):
        compact_ranked.append({
            "player_name": player.get("player_name"),
            "id": player.get("player_id"),
            "performance_index_rank": rank,
            "performance_index": player.get("performance_index"),
        })

    return compact_ranked


with open("docs/generated/weekly_update/players/current_year_players.json", "r", encoding="utf-8") as f:
    data = json.load(f)

atp_ranked = rank_players_by_circuit(data, "ATP")
wta_ranked = rank_players_by_circuit(data, "WTA")

output_dir = Path("docs/tools")
output_dir.mkdir(parents=True, exist_ok=True)

with open(output_dir / "atp_ranking_performance.json", "w", encoding="utf-8") as f:
    json.dump(atp_ranked, f, ensure_ascii=False, indent=2)

with open(output_dir / "wta_ranking_performance.json", "w", encoding="utf-8") as f:
    json.dump(wta_ranked, f, ensure_ascii=False, indent=2)

print("Fichiers générés :")
print(output_dir / "atp_ranking_performance.json")
print(output_dir / "wta_ranking_performance.json")