# scripts/build_players_jinja.py
import csv
import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parents[1]
CSV = ROOT / "player_data_wta.csv"
OUT_DIR = ROOT / "docs" / "players"
TEMPLATES = ROOT / "templates"

OUT_DIR.mkdir(parents=True, exist_ok=True)

env = Environment(
    loader=FileSystemLoader(str(TEMPLATES)),
    autoescape=select_autoescape(['html','xml'])
)

template = env.get_template("player.html")
players_index = []

with CSV.open(encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # adapter les noms de colonnes selon ton CSV
        pid = row.get("id") or row.get("player_id") or row.get("wta_id") or row.get("name")
        safe_id = "".join(c for c in str(pid) if c.isalnum() or c in "-_").lower()
        player = {
            "id": safe_id,
            "name": row.get("name","Nom inconnu"),
            "country": row.get("country") or row.get("nation"),
            "birth_date": row.get("birthdate") or row.get("dob",""),
            "rank": row.get("rank",""),
            "extra": {k:v for k,v in row.items() if k not in ("id","player_id","wta_id","name","country","nation","birthdate","dob","rank")}
        }

        # si tu as un fichier de carte associé, indique le chemin relatif ici (optionnel)
        # ex: if Path(ROOT/"maps_html"/f"{safe_id}.html").exists(): player['map_path'] = f"../maps_html/{safe_id}.html"
        # pour l'instant on laisse vide sauf si tu veux automatiser l'association
        player["map_path"] = None

        # rendre la page
        out = OUT_DIR / f"{safe_id}.html"
        out.write_text(template.render(player=player, title=f"{player['name']} — Fiche joueur"), encoding="utf-8")

        players_index.append({"id": player["id"], "name": player["name"], "country": player["country"], "path": f"players/{player['id']}.html"})

# écrire index JSON pour recherche côté client
(ROOT / "docs" / "players.json").write_text(json.dumps(players_index, ensure_ascii=False), encoding="utf-8")
print("Generated", len(players_index), "player pages")
