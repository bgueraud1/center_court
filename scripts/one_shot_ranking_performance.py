import json

def rank_players_by_circuit(data, circuit):
    players = data.get("players", [])

    # Filtrer le circuit demandé et garder seulement ceux qui ont un performance_index
    filtered = [
        p for p in players
        if p.get("circuit") == circuit and p.get("performance_index") is not None
    ]

    # Tri décroissant sur performance_index
    ranked = sorted(filtered, key=lambda p: p["performance_index"], reverse=True)

    # Ajout du rang
    for rank, player in enumerate(ranked, start=1):
        player["performance_index_rank"] = rank

    return ranked


# --- Exemple d'utilisation ---

# Si ton JSON est dans un fichier
with open("docs/generated/weekly_update/players/current_year_players.json", "r", encoding="utf-8") as f:
    data = json.load(f)

atp_ranked = rank_players_by_circuit(data, "ATP")
wta_ranked = rank_players_by_circuit(data, "WTA")

print("Classement ATP :")
for p in atp_ranked:
    print(p["performance_index_rank"], p["player_name"], p["performance_index"])

print("\nClassement WTA :")
for p in wta_ranked:
    print(p["performance_index_rank"], p["player_name"], p["performance_index"])