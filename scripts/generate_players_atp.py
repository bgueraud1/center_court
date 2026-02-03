#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
generate_atp_players.py - Orchestrateur minimal
Usage:
  python generate_atp_players.py --matches-dir /path/to/matches --out-dir ./dist --modules meta,maps --limit-players 200

Ce script appelle les modules (par import) en fonction de l'option --modules.
Modules supportés (par défaut): meta,maps
- meta : appelle generate_player_meta (module 1) pour produire metadata & matches index
- maps : appelle generate_maps (module 5) pour produire cartes (per-player)

Si les modules ne sont pas importables, le script propose une erreur explicite.
"""

import argparse
import importlib
import os
import sys
import json
from datetime import datetime

# helper to attempt to import module by name (module file should be in same folder or pythonpath)
def import_module_safe(modname):
    try:
        return importlib.import_module(modname)
    except Exception as e:
        print(f"[orchestrator] Warning: failed to import module '{modname}': {e}")
        return None

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def main(matches_dir, out_dir, modules, limit_players=None):
    modules = [m.strip().lower() for m in modules.split(',') if m.strip()]
    print(f"[orchestrator] modules requested: {modules}")
    ensure_dir(out_dir)
    # default subdirs
    idx_dir = os.path.join(out_dir, "index")
    players_dir = os.path.join(out_dir, "players")
    ensure_dir(idx_dir); ensure_dir(players_dir)

    # Try to import modules
    mod_meta = import_module_safe('generate_player_meta') if 'meta' in modules else None
    mod_maps = import_module_safe('generate_maps') if 'maps' in modules else None

    # If meta requested but not importable -> error (meta module is foundational)
    if 'meta' in modules and mod_meta is None:
        print("[orchestrator] ERROR: module 'generate_player_meta' required but not importable. Place generate_player_meta.py next to this script.")
        sys.exit(1)

    # If meta present, call its main to produce index & per-player matches/meta files.
    if 'meta' in modules and mod_meta:
        print("[orchestrator] Running generate_player_meta.main() ...")
        # call main of generate_player_meta with same args
        # signature: main(matches_dir, out_dir, limit_players)
        try:
            # If the module defines a top-level main as before:
            mod_meta.main(matches_dir, out_dir, limit_players)
        except Exception as e:
            print(f"[orchestrator] ERROR: generate_player_meta.main failed: {e}")
            sys.exit(2)

    # Load players_index from index/players_index.json (produced by module meta) if exists
    players_index_path = os.path.join(idx_dir, "players_index.json")
    players = []
    if os.path.exists(players_index_path):
        with open(players_index_path, 'r', encoding='utf8') as f:
            try:
                pj = json.load(f)
                players = pj.get('players', [])
                print(f"[orchestrator] Found players_index with {len(players)} players")
            except Exception as e:
                print(f"[orchestrator] Warning: failed to read players_index.json: {e}")
                players = []
    else:
        # fallback: try listing files in out_dir/players
        pdir = players_dir
        if os.path.isdir(pdir):
            for fname in os.listdir(pdir):
                if fname.endswith('.meta.json'):
                    pid = os.path.basename(fname).split('.')[0]
                    players.append({'player_id': pid, 'meta_path': f"players/{fname}"})
            print(f"[orchestrator] Fallback: discovered {len(players)} players from {pdir}")

    # apply limit_players if given
    if limit_players:
        players = players[:int(limit_players)]

    # Run maps module per player if requested
    if 'maps' in modules:
        if mod_maps is None:
            print("[orchestrator] ERROR: module 'generate_maps' requested but not importable. Place generate_maps.py next to this script.")
            sys.exit(3)
        print("[orchestrator] Generating maps for players ...")
        # call mod_maps.main(matches_dir, out_dir, players_list) or iterate per player:
        try:
            # try if module exposes a main to handle all players
            if hasattr(mod_maps, 'main'):
                mod_maps.main(matches_dir, out_dir, [p.get('player_id') for p in players if p.get('player_id')])
            else:
                # otherwise iterate and call build_maps_for_player
                from importlib import import_module
                for i, p in enumerate(players, start=1):
                    pid = p.get('player_id')
                    if not pid:
                        continue
                    print(f"[orchestrator] [{i}/{len(players)}] maps for {pid}")
                    try:
                        maps_obj = mod_maps.build_maps_for_player_from_matches_dir(matches_dir, pid) if hasattr(mod_maps, 'build_maps_for_player_from_matches_dir') else mod_maps.build_maps_for_player(None, pid)
                        # If build_maps_for_player expects matches_df, it's module-specific - prefer the module's own main
                        # Write output to dist/players/{pid}.maps.json
                        out_path = os.path.join(out_dir, 'players_atp', f"{pid}.maps.json")
                        with open(out_path, 'w', encoding='utf8') as f:
                            json.dump(maps_obj, f, ensure_ascii=False, indent=2)
                    except Exception as e:
                        print(f"[orchestrator] Warning: failed to build maps for {pid}: {e}")
        except Exception as e:
            print(f"[orchestrator] ERROR while running maps module: {e}")
            sys.exit(4)

    print("[orchestrator] Done.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Minimal orchestrator for ATP player data generation (module runner)")
    ap.add_argument("--matches-dir", required=True, help="Directory containing matches CSV files")
    ap.add_argument("--out-dir", default="./dist", help="Output directory for generated artifacts")
    ap.add_argument("--modules", default="meta,maps", help="Comma-separated modules to run: meta,maps,...")
    ap.add_argument("--limit-players", type=int, default=None, help="Limit number of players to process (testing)")
    args = ap.parse_args()
    main(args.matches_dir, args.out_dir, args.modules, args.limit_players)
