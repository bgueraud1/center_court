#!/usr/bin/env python3
"""
scripts/build_all_tournament_winners_from_tree.py

Parcourt récursivement les répertoires json de tournois et construit
docs/wta_tournaments_winners.json et docs/atp_tournaments_winners.json
en agrégeant tous les tournament.json trouvés.
Usage: python3 scripts/build_all_tournament_winners_from_tree.py
"""
import json
from pathlib import Path
import sys

def load_json(p):
    try:
        return json.loads(p.read_text(encoding='utf-8'))
    except Exception as e:
        print(f"[WARN] cannot load {p}: {e}")
        return None

def find_all_tournament_jsons(bases):
    out = []
    for b in bases:
        p = Path(b)
        if not p.exists():
            continue
        for f in p.rglob("tournament.json"):
            out.append(f)
    return out

def get_winner(obj):
    matches = obj.get("matches", [])
    for pref in ("MS001","LS001"):
        for m in matches:
            if m.get("match_id") == pref:
                return m.get("player_id_winner") or "", m.get("winner_player_name") or ""
    for m in matches:
        if (m.get("round","")).strip().upper() in ("F","FINAL"):
            return m.get("player_id_winner") or "", m.get("winner_player_name") or ""
    return None, None

def main():
    bases = [
        "docs/data/tournaments/json_by_tournaments",
        "docs/data/tournaments/tournaments_by_json",
        "docs/data/tournaments"
    ]
    files = find_all_tournament_jsons(bases)
    print(f"[INFO] found {len(files)} tournament.json files")
    winners_atp = {}
    winners_wta = {}
    for f in files:
        obj = load_json(f)
        if not obj:
            continue
        meta = obj.get("meta", {})
        source = (meta.get("source") or "").upper()
        tid = str(meta.get("tourney_id") or "")
        year = meta.get("year")
        pid, pname = get_winner(obj)
        if not (pid or pname):
            continue
        entry = {
            "source": source,
            "tourney_id": tid,
            "year": int(year) if str(year).isdigit() else year,
            "player_id_winner": pid,
            "winner_player_name": pname
        }
        key = f"{tid}_{year}"
        if source == "ATP":
            winners_atp[key] = entry
        else:
            # treat anything else as WTA by default
            winners_wta[key] = entry
    # write outputs
    outwta = Path("docs/wta_tournaments_winners.json")
    outatp = Path("docs/atp_tournaments_winners.json")
    outwta.write_text(json.dumps(sorted(winners_wta.values(), key=lambda e: (str(e.get("tourney_id")), e.get("year"))), ensure_ascii=False, indent=2), encoding='utf-8')
    outatp.write_text(json.dumps(sorted(winners_atp.values(), key=lambda e: (str(e.get("tourney_id")), e.get("year"))), ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"[OK] wrote {outwta} ({len(winners_wta)} entries) and {outatp} ({len(winners_atp)} entries)")

if __name__ == "__main__":
    main()