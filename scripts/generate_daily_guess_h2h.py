#!/usr/bin/env python3
"""
scripts/generate_daily_guess_h2h.py

Version corrigée : normalisation CONSISTANTE des colonnes + logs supplémentaires.
"""
import csv
import os
import json
import random
from datetime import datetime, timezone
import re

ATP_CSV = "player_data_atp.csv"
WTA_CSV = "player_data_wta.csv"
OUT_DIR = "docs"
OUT_JSON = os.path.join(OUT_DIR, "selected_players.json")

BORN_AFTER_YEAR = 1980  # garder >= 1981

def normalize_colname(h):
    if h is None:
        return ""
    # lower, replace any sequence of non-alnum by underscore, strip leading/trailing underscores
    s = re.sub(r'[^0-9a-z]+', '_', h.strip().lower())
    return s.strip('_')

def read_csv_rows(path):
    if not os.path.exists(path):
        print(f"[WARN] CSV introuvable: {path}")
        return []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            # normalized-key dictionary
            norm_row = {}
            # also keep original keyed copy under key 'orig__{normalized}' in case needed
            for k, v in r.items():
                nk = normalize_colname(k or "")
                val = v.strip() if isinstance(v, str) else v
                norm_row[nk] = val
                # keep original label also accessible if needed
                norm_row[f"orig__{nk}"] = val
            rows.append(norm_row)
    return rows

def pick_candidate_columns(normalized_headers):
    # normalized_headers: list of normalized column names (lowercase, underscores)
    rank_patterns = ["highestranking", "highestrank", "bestrank", "_rank", "rank"]
    birth_patterns = ["birthdate", "birth_date", "birth", "birth_year", "birthyear", "dob"]
    rank_cols = []
    birth_cols = []
    for nk in normalized_headers:
        nk_compact = nk.replace('_', '')
        for p in rank_patterns:
            if p in nk_compact and nk not in rank_cols:
                rank_cols.append(nk)
        for p in birth_patterns:
            if p in nk_compact and nk not in birth_cols:
                birth_cols.append(nk)
    return rank_cols, birth_cols

def parse_rank_value(val):
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return None
    # remove non digits/dot
    s2 = re.sub(r'[^\d\.]', '', s)
    if s2 == "":
        return None
    try:
        return int(float(s2))
    except:
        return None

def extract_year_from_string(s):
    if not s:
        return None
    m = re.search(r'(\d{4})', str(s))
    if m:
        try:
            return int(m.group(1))
        except:
            return None
    return None

def filter_top20_and_born_after(rows, rank_cols_candidates, birth_cols_candidates):
    out = []
    stats = {"no_rank":0, "rank_gt_20":0, "no_birth":0, "birth_too_old":0, "ok":0}
    for idx, r in enumerate(rows):
        found_rank = None
        found_rank_col = None
        # 1) try candidates first (preferred)
        for c in rank_cols_candidates:
            if c in r and r[c] != "" and r[c] is not None:
                rank = parse_rank_value(r[c])
                if rank is not None:
                    found_rank = rank
                    found_rank_col = c
                    break
        # 2) fallback: scan all normalized columns for a plausible rank (<=20)
        if found_rank is None:
            for k,v in r.items():
                # skip orig__ copies in fallback to avoid duplication
                if k.startswith("orig__"):
                    continue
                if v is None or v == "":
                    continue
                rank = parse_rank_value(v)
                if rank is not None and rank <= 20:
                    found_rank = rank
                    found_rank_col = k
                    break
        if found_rank is None:
            stats["no_rank"] += 1
            continue
        if found_rank > 20:
            stats["rank_gt_20"] += 1
            continue

        # now birth year
        found_birth = None
        found_birth_col = None
        for c in birth_cols_candidates:
            if c in r and r[c]:
                y = extract_year_from_string(r[c])
                if y:
                    found_birth = y
                    found_birth_col = c
                    break
        # fallback: scan all normalized columns for a 4-digit year
        if found_birth is None:
            for k,v in r.items():
                if k.startswith("orig__"):
                    continue
                if not v:
                    continue
                y = extract_year_from_string(v)
                if y:
                    found_birth = y
                    found_birth_col = k
                    break
        if found_birth is None:
            stats["no_birth"] += 1
            continue
        if found_birth <= BORN_AFTER_YEAR:
            stats["birth_too_old"] += 1
            continue

        # ensure name/id exist (normalized keys)
        name = r.get("full_name") or r.get("fullname") or r.get("full") or r.get("name") or ""
        pid = r.get("player_id") or r.get("playerid") or r.get("player") or ""
        if not name or not pid:
            # skip lines lacking identity
            continue

        # success
        stats["ok"] += 1
        rec = dict(r)  # keep normalized keys
        rec["_parsed_rank"] = found_rank
        rec["_parsed_rank_col"] = found_rank_col
        rec["_birth_year"] = found_birth
        rec["_birth_col"] = found_birth_col
        # make sure canonical fields exist too
        rec["full_name"] = name
        rec["player_id"] = pid
        out.append(rec)

    # debug summary
    print(f"[DEBUG-FILTER] rows scanned: {len(rows)}; kept: {len(out)}; stats: {stats}")
    return out

def choose_three_with_levels(records, rnd):
    recs = sorted(records, key=lambda r: r.get("_parsed_rank", 999))
    n = len(recs)
    if n == 0:
        return []
    if n <= 3:
        chosen = recs[:n]
    else:
        chosen = rnd.sample(recs, 3)
        chosen = sorted(chosen, key=lambda r: r.get("_parsed_rank", 999))
    res = []
    if len(chosen) == 1:
        res.append({"level":"medium", "player": chosen[0]})
    elif len(chosen) == 2:
        res.append({"level":"hard", "player": chosen[0]})
        res.append({"level":"easy", "player": chosen[1]})
    else:
        res.append({"level":"hard", "player": chosen[0]})
        res.append({"level":"medium", "player": chosen[1]})
        res.append({"level":"easy", "player": chosen[2]})
    return res

def compact_player_object(record):
    rec = dict(record)
    # stringify non-int fields, keep debug fields
    for k,v in list(rec.items()):
        if isinstance(v, (int, float, bool)) or v is None:
            continue
        try:
            rec[k] = str(v)
        except:
            rec[k] = ""
    rec["birth_year"] = int(record.get("_birth_year")) if record.get("_birth_year") else None
    rec["parsed_rank"] = int(record.get("_parsed_rank")) if record.get("_parsed_rank") else None
    rec["_parsed_rank_col"] = record.get("_parsed_rank_col", "")
    rec["_birth_col"] = record.get("_birth_col", "")
    return rec

def main():
    seed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seed_int = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
    rnd = random.Random(seed_int)

    atp_rows = read_csv_rows(ATP_CSV)
    wta_rows = read_csv_rows(WTA_CSV)
    print(f"[INFO] ATP rows read: {len(atp_rows)}; WTA rows read: {len(wta_rows)}")

    atp_headers = list(atp_rows[0].keys()) if atp_rows else []
    wta_headers = list(wta_rows[0].keys()) if wta_rows else []

    # we only want the normalized headers (exclude the 'orig__' helper keys)
    atp_norm_headers = [h for h in atp_headers if not h.startswith("orig__")]
    wta_norm_headers = [h for h in wta_headers if not h.startswith("orig__")]

    atp_rank_cols, atp_birth_cols = pick_candidate_columns(atp_norm_headers)
    wta_rank_cols, wta_birth_cols = pick_candidate_columns(wta_norm_headers)

    print(f"[DEBUG] ATP candidate rank cols: {atp_rank_cols}; birth cols: {atp_birth_cols}")
    print(f"[DEBUG] WTA candidate rank cols: {wta_rank_cols}; birth cols: {wta_birth_cols}")

    atp_filtered = filter_top20_and_born_after(atp_rows, atp_rank_cols, atp_birth_cols)
    wta_filtered = filter_top20_and_born_after(wta_rows, wta_rank_cols, wta_birth_cols)

    print(f"[INFO] ATP after filter (rank<=20 & born>1980): {len(atp_filtered)}")
    print(f"[INFO] WTA after filter (rank<=20 & born>1980): {len(wta_filtered)}")

    if len(atp_filtered) > 0:
        print("[TRACE] ATP candidates (sample up to 10):")
        for rec in atp_filtered[:10]:
            print("  -", rec.get("full_name"), "pid=", rec.get("player_id"),
                  "rank=", rec.get("_parsed_rank"), "rank_col=", rec.get("_parsed_rank_col"),
                  "birth_year=", rec.get("_birth_year"), "birth_col=", rec.get("_birth_col"))
    else:
        print("[WARN] Aucun joueur ATP retenu : le script a échoué à trouver des lignes satisfaisantes. Vérifie les colonnes et/ou fournis un extrait CSV pour debug.")

    # choose 3 each side
    atp_selected = choose_three_with_levels(atp_filtered, rnd)
    wta_selected = choose_three_with_levels(wta_filtered, rnd)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat() + "Z",
        "seed_date": seed_date,
        "notes": f"Players born after {BORN_AFTER_YEAR}, best rank <= 20. Selection deterministic by seed {seed_int}.",
        "atp": [],
        "wta": []
    }

    for item in atp_selected:
        out["atp"].append({
            "level": item["level"],
            "player": compact_player_object(item["player"])
        })
    for item in wta_selected:
        out["wta"].append({
            "level": item["level"],
            "player": compact_player_object(item["player"])
        })

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_JSON, "w", encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[OK] Wrote {OUT_JSON} — ATP:{len(out['atp'])} WTA:{len(out['wta'])}")

if __name__ == "__main__":
    main()
