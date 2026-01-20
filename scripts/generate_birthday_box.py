#!/usr/bin/env python3
"""
Génère birthday_today.json contenant jusqu'à 5 joueurs/joueuses dont
c'est l'anniversaire aujourd'hui (Europe/Paris).

Logique :
- Filtrer players_by_birth.json pour avoir ceux dont day/month == aujourd'hui.
- Pour chaque candidat, retrouver son classement actuel dans latest_atp_ranking.json
  ou latest_wta_ranking.json (recherche par player_id puis par full_name).
- Si >5 candidats : garder les 5 avec classement numérique le plus bas (1 meilleur).
  Les joueurs sans classement numérique sont considérés après ceux qui ont un classement.
  Les égalités sont résolues par ordre stable et par un tie-break aléatoire déterministe.
- Si <5 : compléter aléatoirement (seed déterministe par date) parmi ceux non choisis.
- Écriture du JSON de sortie (UTF-8).
"""
from pathlib import Path
import argparse
import json
import random
from datetime import datetime
import hashlib

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# --- ISO3 -> alpha2 mapping (partial, compléter si nécessaire) ---
ISO3_TO_ALPHA2 = {
    "ARG":"AR","AUS":"AU","AUT":"AT","BEL":"BE","BGR":"BG","BRA":"BR","CAN":"CA","CHN":"CN",
    "COL":"CO","CZE":"CZ","CRO":"HR","ESP":"ES","EST":"EE","FRA":"FR","GBR":"GB","GER":"DE",
    "DEU":"DE","ITA":"IT","JPN":"JP","KOR":"KR","KAZ":"KZ","NLD":"NL","NZL":"NZ",
    "POL":"PL","PRT":"PT","ROU":"RO","RUS":"RU","SRB":"RS","SLO":"SI","SWE":"SE","SUI":"CH",
    "TPE":"TW","UKR":"UA","USA":"US","URU":"UY","MEX":"MX","IND":"IN","IRL":"IE","ISR":"IL",
    "ZAF":"ZA","DNK":"DK","HUN":"HU","NOR":"NO","BLR":"BY","VEN":"VE","CHI":"CL","ECU":"EC",
    "PER":"PE","DOM":"DO","PAN":"PA","CYP":"CY","GRC":"GR","GRE":"GR","LUX":"LU","LTU":"LT","LVA":"LV",
    "MYS":"MY","PHL":"PH","SGP":"SG","THA":"TH","VIE":"VN","ALG":"DZ","MAR":"MA","TUN":"TN","EGY":"EG",
    "POR":"PT","NIG":"NG","KEN":"KE"
}

def iso3_to_alpha2(code: str) -> str:
    if not code:
        return ""
    c = str(code).strip().upper()
    if len(c) == 2 and c.isalpha():
        return c
    if len(c) == 3 and c.isalpha():
        return ISO3_TO_ALPHA2.get(c, "")
    cand = ''.join(ch for ch in c if ch.isalpha())[:2]
    return cand.upper() if len(cand) == 2 else ""

def emoji_from_alpha2(alpha2: str) -> str:
    """
    Convertit 'US' -> 🇺🇸 en utilisant les Regional Indicator symbols.
    Retourne '' si impossible.
    """
    if not alpha2:
        return ""
    s = str(alpha2).strip().upper()
    if len(s) != 2 or not s.isalpha():
        return ""
    try:
        return ''.join(chr(ord(ch) + 127397) for ch in s)
    except Exception:
        return ""

def looks_like_emoji(s: str) -> bool:
    if not s or not isinstance(s, str):
        return False
    s = s.strip()
    # quick heuristics: emoji flags are made of two regional indicators (surrogate pairs)
    # check for any char in the supplementary plane
    for ch in s:
        if ord(ch) > 0xFFFF:
            return True
    # also allow common flag glyphs (two-letter emoji may appear as single codepoints on some systems)
    if any(substr in s for substr in ['\U0001F1E6','\U0001F1FF']) :
        return True
    # fallback: if it contains an emoji-like glyph (not perfect)
    return any(0x1F300 <= ord(ch) <= 0x1FAFF for ch in s)

def ensure_flag_emoji_for_player(p: dict):
    """
    Ensure p['flag_emoji'] contains a real emoji when possible.
    Mutates p in-place and returns the emoji (or empty string).
    """
    if not isinstance(p, dict):
        return ''
    fe = p.get('flag_emoji') or ''
    if isinstance(fe, str) and fe.strip():
        if looks_like_emoji(fe):
            p['flag_emoji'] = fe
            return fe
        # sometimes stored as escaped sequences; keep and try other fallbacks
    # try several candidate fields for alpha2/iso3
    cand_fields = [
        p.get('country_alpha2'), p.get('country_iso2'), p.get('country_code'),
        p.get('country'), p.get('country_alpha3'), p.get('country_iso3'), p.get('country_code3')
    ]
    for cand in cand_fields:
        if not cand:
            continue
        cs = str(cand).strip().upper()
        if len(cs) == 2 and cs.isalpha():
            em = emoji_from_alpha2(cs)
            if em:
                p['flag_emoji'] = em
                return em
        if len(cs) == 3 and cs.isalpha():
            a2 = iso3_to_alpha2(cs)
            if a2:
                em = emoji_from_alpha2(a2)
                if em:
                    p['flag_emoji'] = em
                    return em
    # fallback: if p has country name like 'United States' -> we don't map here
    # leave empty string explicitly
    p['flag_emoji'] = ''
    return ''

# -------------------------
# Helpers
# -------------------------
def load_json(path: Path):
    try:
        with path.open('r', encoding='utf-8') as fh:
            return json.load(fh)
    except FileNotFoundError:
        return []
    except Exception:
        return []

def lookup_rank_for_player(player, atp_rows, wta_rows):
    """
    Retourne (circuit, rank_int_or_None).
    Cherche d'abord par player_id ensuite par full_name.
    """
    pid = str(player.get('player_id') or '').strip()
    name = (player.get('full_name') or '').strip().lower()

    def find_rank_in(rows):
        if not isinstance(rows, list):
            rows = rows.get('rows') if isinstance(rows, dict) and 'rows' in rows else []
        # first pass: player_id
        for r in rows:
            rid = str(r.get('player_id') or r.get('player_id') or '').strip()
            if pid and rid and pid == rid:
                try:
                    v = r.get('ranking')
                    return int(v) if v not in (None, '') else None
                except Exception:
                    return None
        # second pass: match by name
        for r in rows:
            rname = (r.get('full_name') or r.get('player_name') or '').strip().lower()
            if rname and name and rname == name:
                try:
                    v = r.get('ranking')
                    return int(v) if v not in (None, '') else None
                except Exception:
                    return None
        return None

    rank = find_rank_in(atp_rows)
    if rank is not None:
        return ('ATP', rank)
    rank = find_rank_in(wta_rows)
    if rank is not None:
        return ('WTA', rank)
    circuit = (player.get('circuit') or '').upper()
    return (circuit or None, None)

def is_birthday_today(player, today_day, today_month):
    bd = player.get('birth_day') or player.get('birth_day_str') or ''
    bm = player.get('birth_month') or player.get('birth_month_str') or ''
    try:
        d = int(str(bd).lstrip('0') or 0)
        m = int(str(bm).lstrip('0') or 0)
        return (d == today_day and m == today_month)
    except Exception:
        bd_raw = player.get('birth_date') or ''
        try:
            parts = str(bd_raw).split('-')
            if len(parts) >= 3:
                y, mm, dd = parts[0], parts[1], parts[2]
                return (int(dd.lstrip('0')) == today_day and int(mm.lstrip('0')) == today_month)
        except Exception:
            pass
    return False

# -------------------------
# Main
# -------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument('--players-by-birth', default='docs/tools/players_by_birth.json')
    p.add_argument('--latest-atp', default='docs/tools/latest_atp_ranking.json')
    p.add_argument('--latest-wta', default='docs/tools/latest_wta_ranking.json')
    p.add_argument('--out', default=None)
    p.add_argument('--count', type=int, default=5)
    p.add_argument('--seed', default=None, help='force seed (useful for testing)')
    args = p.parse_args()

    players_by_birth = load_json(Path(args.players_by_birth))
    atp = load_json(Path(args.latest_atp))
    wta = load_json(Path(args.latest_wta))

    # determine today's date in Europe/Paris
    if ZoneInfo is not None:
        tz = ZoneInfo('Europe/Paris')
        now = datetime.now(tz)
    else:
        now = datetime.utcnow()
    today_day = now.day
    today_month = now.month

    # deterministic per-day seed
    if args.seed:
        seed = args.seed
    else:
        seed = now.strftime('%Y-%m-%d')
    rnd = random.Random(seed)

    # filter candidates whose birthday is today
    birthday_candidates = [p for p in players_by_birth if is_birthday_today(p, today_day, today_month)]

    # Annotate each candidate with rank info
    annotated = []
    for cand in birthday_candidates:
        circuit, rank = lookup_rank_for_player(cand, atp, wta)
        annotated.append({
            'src': cand,
            'circuit': circuit,
            'rank': rank if rank is not None else None
        })

    # sort key: numeric ranks first (ascending), None at end; deterministic tie-break via hash
    def sort_key(a):
        r = a['rank']
        primary = r if (isinstance(r, int)) else 10**9
        key_str = str((a['src'].get('player_id') or a['src'].get('full_name') or '')) + seed
        h = hashlib.sha1(key_str.encode('utf-8')).hexdigest()
        tie_num = int(h[:8], 16)
        return (primary, tie_num)

    if len(annotated) > args.count:
        annotated_sorted = sorted(annotated, key=sort_key)
        chosen_ann = annotated_sorted[:args.count]
    else:
        chosen_ann = annotated[:]  # all candidates

    chosen_keys = set()
    result = []
    for a in chosen_ann:
        src = a['src']
        pid = str(src.get('player_id') or '').strip()
        key = pid or (src.get('full_name') or '')
        chosen_keys.add(key)
        result.append({
            'full_name': src.get('full_name') or '',
            'player_id': pid,
            'circuit': (a['circuit'] or src.get('circuit') or '').upper(),
            'birth_date': src.get('birth_date') or None,
            'flag_emoji': src.get('flag_emoji') or '',
            'current_rank': a['rank'] if isinstance(a['rank'], int) else None
        })

    # If fewer than needed, fill randomly from remaining players_by_birth (excluding chosen_keys)
    if len(result) < args.count:
        remaining = []
        for pbb in players_by_birth:
            pid = str(pbb.get('player_id') or '').strip()
            key = pid or (pbb.get('full_name') or '')
            if key in chosen_keys:
                continue
            remaining.append(pbb)
        rnd.shuffle(remaining)
        for pbb in remaining:
            if len(result) >= args.count:
                break
            pid = str(pbb.get('player_id') or '').strip()
            circuit, rank = lookup_rank_for_player(pbb, atp, wta)
            result.append({
                'full_name': pbb.get('full_name') or '',
                'player_id': pid,
                'circuit': (circuit or pbb.get('circuit') or '').upper(),
                'birth_date': pbb.get('birth_date') or None,
                'flag_emoji': pbb.get('flag_emoji') or '',
                'current_rank': rank if isinstance(rank, int) else None
            })
            chosen_keys.add(pid or (pbb.get('full_name') or ''))

    # trim to requested count
    result = result[:args.count]

    # ensure valid flag emoji for every entry (server-side authoritative)
    for r in result:
        # Prefer existing emoji; otherwise try to synthesize from country codes
        ensure_flag_emoji_for_player(r)

    # determine out path
    if args.out:
        out_path = Path(args.out)
    else:
        out_path = Path('docs') / 'tools' / 'birthday_today.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # write JSON (UTF-8)
    with out_path.open('w', encoding='utf-8') as fh:
        json.dump(result, fh, ensure_ascii=False, indent=2)

    print(f"Wrote {len(result)} entries to {out_path} (seed={seed}, date={now.date()})")


if __name__ == '__main__':
    main()
