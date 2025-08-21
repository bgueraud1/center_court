# Configuration file for the player_data_wta enrichment Pipeline
# Created Aug 7 2025
# Ran to end

# config.py (remplacement robuste)
from pathlib import Path

# point de départ : ce fichier
THIS = Path(__file__).resolve()

# remonter pour trouver la racine du repo (là où .git existe), limiter la recherche
REPO_ROOT = THIS
for _ in range(8):
    if (REPO_ROOT / ".git").exists():
        break
    if REPO_ROOT.parent == REPO_ROOT:
        break
    REPO_ROOT = REPO_ROOT.parent

# fallback: si .git non trouvé, utiliser deux niveaux au-dessus (comportement précédent)
if not (REPO_ROOT / ".git").exists():
    REPO_ROOT = THIS.parents[1]

# candidates possibles pour le dossier de données (ajoute d'autres chemins si nécessaire)
CANDIDATES = [
    REPO_ROOT / "player_base_and_maps",
    REPO_ROOT / "center_court" / "player_base_and_maps",
    REPO_ROOT / "data" / "player_base_and_maps",
    REPO_ROOT / "player_base_and_maps"  # redondant mais sûr
]

DATA_DIR = next((p for p in CANDIDATES if p.exists()), REPO_ROOT / "player_base_and_maps")
DATA_DIR = DATA_DIR.resolve()

# chemins utilisés ailleurs
players_path = DATA_DIR / "player_data_wta.csv"
output_path  = DATA_DIR / "player_data_wta.csv"
rankings_dir = REPO_ROOT / "wta_rankings"

# debug utile (sera imprimé lors du run)
print("DEBUG(config): REPO_ROOT =", REPO_ROOT)
print("DEBUG(config): DATA_DIR  =", DATA_DIR)
print("DEBUG(config): players_path =", players_path)
print("DEBUG(config): rankings_dir =", rankings_dir)





min_first_date = '2015-01-01' # date under which player's data won't be overwritten if overwriting activated



overwrite_wiki = False
overwrite_ioc = False

begin_index_wiki = 0  # index under which player won't be scraped for wiki data
end_index_wiki = None # index above which player won't be scraped for wiki data

begin_index_ioc = 0  # index under which player won't be scraped for ioc
end_index_ioc = None # index above which player won't be scraped for ioc




