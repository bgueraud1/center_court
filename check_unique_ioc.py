import pandas as pd

f1 = "player_base_and_maps\player_data_wta.csv"
f2 = "player_base_and_maps\player_data_atp.csv"

s = pd.concat([pd.read_csv(f1)["represented_country"],
               pd.read_csv(f2)["represented_country"]])
# supprimer les NaN, enlever espaces, obtenir uniques triés
unique_countries = sorted(s.dropna().astype(str).str.strip().unique())

print(unique_countries)
