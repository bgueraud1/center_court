# run_wta_dates.py
from datetime import date, timedelta
from pathlib import Path
import scrape_player_ranking_wta as scraper   # <-- adapte le module si ton fichier a un autre nom/chemin

# Paramètres : plage
start = date(2025, 8, 18)
end   = date(2026, 1, 12)

# Générer tous les lundis entre start et end inclus
# (si start est déjà lundi on le garde)
d = start
mondays = []
# s'assurer que d est lundi (le script original attend des datetime.date)
if d.weekday() != 0:  # 0 == Monday
    d = d - timedelta(days=d.weekday())  # pour être conforme au mapping Monday (mais ici start est un lundi dans ton exemple)

while d <= end:
    if d.weekday() == 0:
        mondays.append(d)
    d = d + timedelta(days=7)

# Répertoire de sauvegarde (le scraper créera ce dossier si besoin)
save_dir = "wta_rankings"
Path(save_dir).mkdir(parents=True, exist_ok=True)

# Appel du scraper (le script que tu as fourni expose scrape_data(specific_dates, save_dir))
print(f"Launching scrape for {len(mondays)} dates: {mondays[0]} -> {mondays[-1]}")
scraper.scrape_data(mondays, save_dir)
print("Finished.")
