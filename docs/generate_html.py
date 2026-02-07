#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Génère un HTML de bracket identique au style fourni, avec un tour supplémentaire MS064..MS127.
Produit ./template_7_generated.html
"""
from pathlib import Path
from math import floor

OUT = Path("template_7_generated.html")

# --- Réglages visuels (tu peux ajuster si besoin) ---
player_width = 180      # largeur colonne players (top+bottom)
winner_width = 130      # largeur div winner (avec border-left/bottom)
score_width = 150       # largeur div score (s'étend à droite)
row_height = 20         # hauteur d'une ligne (top / bottom / winner)
leaf_spacing = 60       # distance verticale entre match i et match i+1 (top of match)
leaf_min = 64
leaf_max = 127

# --- Construction des niveaux (niveau 0 = feuilles MS064..MS127) ---
levels = []
low = leaf_min
high = leaf_max + 1
while low >= 1:
    levels.append(list(range(low, high)))
    high = low
    low = low // 2
# levels[0] = [64..127], levels[1] = [32..63], ..., levels[-1] = [1]

# --- Calcul des centres verticaux (px) ---
centers = {}
# feuilles : positions séquentielles
for i, m in enumerate(levels[0]):
    center = i * leaf_spacing + row_height / 2.0
    centers[m] = center

# parents : centre = moyenne des deux enfants (2*m, 2*m+1)
for lvl in range(1, len(levels)):
    for m in levels[lvl]:
        left_child = centers.get(m * 2)
        right_child = centers.get(m * 2 + 1)
        if left_child is None or right_child is None:
            raise RuntimeError(f"Enfants manquants pour {m}: {left_child}, {right_child}")
        centers[m] = (left_child + right_child) / 2.0

# --- calcul largeur canvas / positions horizontales ---
# Lefts:
# players (level0) -> left = 0
# winner(level k) -> left = player_width + winner_width * k
max_winner_columns = len(levels)  # we'll use winners for all levels (including level0)
canvas_width = player_width + winner_width * max_winner_columns + 50
canvas_height = int(max(centers.values()) + 200)

def mid_str(n):
    return f"MS{n:03d}"

# --- Build HTML ---
parts = []
parts.append(f"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bracket généré - template_7</title>
<style>
  body{{font-family: Arial, Helvetica, sans-serif; font-size:12px;}}
  .container{{}}
  .wrapper{{}}
  .canvas{{position:relative; width:{canvas_width}px; min-height:{canvas_height}px; margin:10px;}}
  .flag{{display:inline-block; width:18px; margin-right:4px;}}
  .player-text, .winner-text, .score-text{{display:inline-block; vertical-align:middle;}}
  /* styles pour garder le rendu très proche de ton extrait */
</style>
</head>
<body>
<div class="container">
  <div class="wrapper">
    <div style="width:300px;height:62px;position:absolute;top:0;right:350px;"></div>
    <div class="canvas" id="canvas">
""")

# --- 1) Colonne players : uniquement pour levels[0] (MS064..MS127) ---
players_left = 0
for m in levels[0]:
    c = centers[m]
    top_top = int(round(c - row_height/2.0))   # top of top-player line
    top_bottom = int(round(c + row_height/2.0))# top of bottom-player line
    mid = mid_str(m)

    # top player (with border-top)
    parts.append(
        f'      <div style="position: absolute; top: {top_top}px; left: {players_left}px; height: {row_height}px; width: {player_width}px; border-top: 1px solid rgb(121, 121, 121); text-align: left;">\n'
        f'          <div style="float:left;width:22px;font-size:11px;padding:2px;">&nbsp;</div>\n'
        f'          <span class="flag" aria-hidden="true" data-match="{mid}"></span>\n'
        f'          <span class="player-text" data-match="{mid}" data-side="top" data-field="player">Player{mid}</span>\n'
        f'      </div>\n'
    )
    # bottom player (with border-bottom)
    parts.append(
        f'      <div style="position:absolute; top:{top_bottom}px; left:{players_left}px; height:{row_height}px; width:{player_width}px; border-bottom:1px solid #797979; text-align:left;">\n'
        f'          <div style="float:left;width:22px;font-size:11px;padding:2px;">&nbsp;</div>\n'
        f'          <span class="flag" aria-hidden="true" data-match="{mid}"></span>\n'
        f'          <span class="player-text" data-match="{mid}" data-side="bottom" data-field="player">Player{mid}</span>\n'
        f'      </div>\n'
    )

# --- 2) Colonnes winners : pour chaque niveau k produce winner+score at left = player_width + winner_width * k ---
# This creates winner col for level0 (next to players), then for level1, level2, etc.
for k, level_matches in enumerate(levels):
    left = player_width + winner_width * k
    for m in level_matches:
        c = centers[m]
        top_winner = int(round(c - row_height/2.0))   # top for winner line (same formula as sample)
        top_score = int(round(c + row_height/2.0))    # top for score line

        mid = mid_str(m)
        # winner row (border-bottom + border-left)
        parts.append(
            f'      <div style="position: absolute; top: {top_winner}px; left: {left}px; height: {row_height}px; width: {winner_width}px; border-bottom: 1px solid rgb(121, 121, 121); border-left: 1px solid rgb(121, 121, 121); text-align: left;">\n'
            f'          <span class="flag" aria-hidden="true" data-match="{mid}"></span>\n'
            f'          <span class="winner-text" data-match="{mid}" data-field="winner">Vainqueur{mid}</span>\n'
            f'      </div>\n'
        )
        # score row (border-left only, width score_width to match sample)
        parts.append(
            f'      <div style="position:absolute; top:{top_score}px; left:{left}px; height:{row_height+1}px; width:{score_width}px; border-left:1px solid #797979; text-align:left;">\n'
            f'          <span class="score-text" data-match="{mid}" data-field="score">Score{mid}</span>\n'
            f'      </div>\n'
        )

# --- Fin de document ---
parts.append("""    </div>
  </div>
</div>
</body>
</html>
""")

OUT.write_text("".join(parts), encoding="utf-8")
print("Fichier écrit :", OUT.resolve())
print(f"Canvas: {canvas_width}px x {canvas_height}px")
print(f"Niveaux générés (du plus bas au plus haut) :")
for i, lvl in enumerate(levels):
    print(f"  niveau {i}: {min(lvl)}..{max(lvl)} ({len(lvl)} matches)")
# Exemples de centres pour vérification
for m in (16, 32, 33, 8):
    if m in centers:
        print(f"  MS{m:03d} center = {centers[m]:.1f}px")
