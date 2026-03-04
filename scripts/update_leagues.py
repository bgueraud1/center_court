# scripts/update_leagues.py
"""
Weekly league updater.

Usage examples:
  python3 scripts/update_leagues.py \
    --users-csv data/users.csv \
    --scores-csv data/scores.csv \
    --week-date 2026-03-03 \
    --out-csv data/users_updated.csv \
    --out-json data/league_users.json \
    --sql-file tmp/update_leagues.sql

Notes:
 - Input users.csv should contain at least: id,user_id,pseudo,league
 - Input scores.csv should have created_day (YYYY-MM-DD) or created_at; we filter by created_day if present.
"""
import argparse
import datetime
import json
import math
import os
import sys

import pandas as pd

LEAGUE_ORDER = [
    "Future F15",
    "Future F25",
    "Challenger C80",
    "Challenger C100",
    "Challenger C125",
    "ATP250",
    "ATP500",
    "Masters1000",
    "Grand Slam"
]

# capacity per league
LEAGUE_CAPACITY = {l: 20 for l in LEAGUE_ORDER}

# promotion / relegation pct (fraction of league size). Tune as you want.
PROMOTE_PCT = {
    "Future F15": 0.20,
    "Future F25": 0.18,
    "Challenger C80": 0.15,
    "Challenger C100": 0.12,
    "Challenger C125": 0.10,
    "ATP250": 0.08,
    "ATP500": 0.06,
    "Masters1000": 0.04,
    "Grand Slam": 0.0
}
RELEGATE_PCT = PROMOTE_PCT.copy()  # symmetric by default

def monday_of(date):
    return date - datetime.timedelta(days=date.weekday())

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--users-csv", default="data/users.csv")
    p.add_argument("--scores-csv", default="data/scores.csv")
    p.add_argument("--week-date", default="")  # YYYY-MM-DD ; if empty, use this week's Monday
    p.add_argument("--out-csv", default="data/users_updated.csv")
    p.add_argument("--out-json", default="data/league_users.json")
    p.add_argument("--sql-file", default="")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0

def main():
    args = parse_args()

    if args.week_date:
        week_date = datetime.datetime.strptime(args.week_date, "%Y-%m-%d").date()
    else:
        week_date = monday_of(datetime.date.today())
    week_date_str = week_date.isoformat()

    print(f"[INFO] Week date: {week_date_str}")

    if not os.path.exists(args.users_csv):
        print(f"[ERROR] users csv not found at {args.users_csv}", file=sys.stderr)
        sys.exit(2)
    users = pd.read_csv(args.users_csv, dtype=str).fillna("")
    # ensure required columns
    if "id" not in users.columns:
        print("[ERROR] users CSV must contain 'id' column (primary key)", file=sys.stderr); sys.exit(2)
    # ensure league column
    if "league" not in users.columns:
        users["league"] = ""

    # normalise leagues -> if empty, put lowest league
    users["league"] = users["league"].apply(lambda x: x if x in LEAGUE_ORDER else (LEAGUE_ORDER[0] if not x else x))

    # read scores (if present)
    scores = pd.DataFrame()
    if os.path.exists(args.scores_csv):
        scores = pd.read_csv(args.scores_csv, dtype=str).fillna("")
        # parse points numeric
        if "points" in scores.columns:
            scores["points"] = scores["points"].apply(lambda v: float(v) if v not in (None, "", "nan") else 0.0)
        else:
            scores["points"] = 0.0
        # prefer created_day if present
        date_col = None
        if "created_day" in scores.columns:
            date_col = "created_day"
        elif "created_at" in scores.columns:
            date_col = "created_at"
            # extract date part
            scores["created_day"] = scores["created_at"].apply(lambda s: s.split(" ")[0] if isinstance(s, str) and s else "")
            date_col = "created_day"
        else:
            scores["created_day"] = ""
            date_col = "created_day"

        # filter for the requested week_date
        week_scores = scores[scores["created_day"] == week_date_str].copy()
        print(f"[INFO] Loaded {len(scores)} score rows; {len(week_scores)} rows for {week_date_str}")
    else:
        print(f"[WARN] scores file '{args.scores_csv}' not found. All weekly points = 0")
        week_scores = pd.DataFrame(columns=["user_id", "points"])

    # map user_id or pseudo to users.id
    # prefer user_id column in users
    users_index = users.set_index("id", drop=False)

    # compute weekly points per pseudo or user_id
    # try to join via user_id first, then pseudo
    week_scores["user_id"] = week_scores.get("user_id", "")  # ensure column exists
    week_scores["pseudo"] = week_scores.get("pseudo", "")

    # aggregate weekly points by pseudo/user_id
    agg_by_userid = week_scores[week_scores["user_id"].astype(bool)].groupby("user_id")["points"].sum().to_dict()
    agg_by_pseudo = week_scores[~week_scores["user_id"].astype(bool) & week_scores["pseudo"].astype(bool)].groupby("pseudo")["points"].sum().to_dict()

    # attach weekly_points to users
    def points_for_row(row):
        uid = row.get("user_id", "") or ""
        pseudo = row.get("pseudo", "") or ""
        pts = 0.0
        if uid and uid in agg_by_userid:
            pts = agg_by_userid[uid]
        elif pseudo and pseudo in agg_by_pseudo:
            pts = agg_by_pseudo[pseudo]
        else:
            # also try matching by pseudo column in users
            if pseudo and pseudo in agg_by_pseudo:
                pts = agg_by_pseudo[pseudo]
        return float(pts)

    # but users may not have user_id populated; safer: sum by users.pseudo
    users["weekly_points"] = users["pseudo"].map(lambda p: float(agg_by_pseudo.get(p, 0.0)))
    # if users have user_id match, override
    if "user_id" in users.columns:
        users["weekly_points"] = users.apply(lambda r: float(agg_by_userid.get(str(r["user_id"]), users.loc[r.name, "weekly_points"])), axis=1)

    # Build current league member lists
    league_members = {l: [] for l in LEAGUE_ORDER}
    for _, row in users.iterrows():
        league = row["league"] if row["league"] in LEAGUE_ORDER else LEAGUE_ORDER[0]
        league_members[league].append(row.to_dict())

    # sort members inside each league by weekly_points desc, tie-breaker: existing total points or id
    for l in LEAGUE_ORDER:
        league_members[l] = sorted(league_members[l], key=lambda r: (-float(r.get("weekly_points", 0.0)), r.get("pseudo",""), r.get("id","")))

    # determine promotions/relegations
    promote_candidates = []  # tuples (user_id, old_league, new_league)
    relegate_candidates = []

    for idx, league in enumerate(LEAGUE_ORDER):
        members = league_members[league]
        n = len(members)
        if n == 0:
            continue
        # compute counts
        promote_count = 0
        relegate_count = 0
        pct_up = PROMOTE_PCT.get(league, 0.0)
        pct_down = RELEGATE_PCT.get(league, 0.0)
        if pct_up > 0 and idx < len(LEAGUE_ORDER)-1:
            promote_count = max(1, math.floor(pct_up * n)) if n>1 else 0
        if pct_down > 0 and idx > 0:
            relegate_count = max(1, math.floor(pct_down * n)) if n>1 else 0

        # top promote_count go up
        if promote_count > 0:
            for r in members[:promote_count]:
                promote_candidates.append((r["id"], league, LEAGUE_ORDER[idx+1]))
        # bottom relegate_count go down
        if relegate_count > 0:
            for r in members[-relegate_count:]:
                relegate_candidates.append((r["id"], league, LEAGUE_ORDER[idx-1]))

    # Apply moves simultaneously: build new assignment map starting from current
    new_league_map = {row["id"]: row["league"] for _, row in users.iterrows()}

    for uid, old, new in promote_candidates:
        new_league_map[uid] = new
    for uid, old, new in relegate_candidates:
        # do not overwrite a promotion with a relegation if conflict (shouldn't normally happen)
        if new_league_map.get(uid) in LEAGUE_ORDER:
            # if user was promoted and also flagged for relegation (edge-case), prefer promotion
            if LEAGUE_ORDER.index(new_league_map[uid]) > LEAGUE_ORDER.index(new):
                # already at higher league, skip
                continue
        new_league_map[uid] = new

    # compute new memberships
    new_members = {l: [] for l in LEAGUE_ORDER}
    for _, row in users.iterrows():
        nid = row["id"]
        assigned = new_league_map.get(nid, LEAGUE_ORDER[0])
        if assigned not in LEAGUE_ORDER:
            assigned = LEAGUE_ORDER[0]
        new_members[assigned].append(row.to_dict())

    # now ensure capacity: if some league exceeds LEAGUE_CAPACITY, demote surplus lowest-ranked (by weekly_points)
    for i in range(len(LEAGUE_ORDER)-1, -1, -1):  # process top->bottom or bottom->top ? do top->bottom to demote surplus down
        league = LEAGUE_ORDER[i]
        members = sorted(new_members[league], key=lambda r: (-float(r.get("weekly_points",0.0)), r.get("pseudo",""), r.get("id","")))
        cap = LEAGUE_CAPACITY.get(league, 20)
        if len(members) <= cap:
            new_members[league] = members
            continue
        # surplus to demote (the lowest ones)
        surplus = members[cap:]
        new_members[league] = members[:cap]
        lower_idx = i-1
        if lower_idx < 0:
            # cannot demote lower than bottom; put them back trimmed to cap
            print(f"[WARN] league {league} exceeded capacity and cannot demote further (dropping {len(surplus)} users).")
            continue
        print(f"[INFO] league {league} exceeded cap ({len(members)} > {cap}) - demoting {len(surplus)} users to {LEAGUE_ORDER[lower_idx]}")
        new_members[LEAGUE_ORDER[lower_idx]].extend(surplus)

    # After moving surpluses down, we might have overloaded lower leagues; iterate few times to stabilize
    for _ in range(4):
        changed = False
        for i in range(len(LEAGUE_ORDER)):
            league = LEAGUE_ORDER[i]
            members = sorted(new_members[league], key=lambda r: (-float(r.get("weekly_points",0.0)), r.get("pseudo",""), r.get("id","")))
            cap = LEAGUE_CAPACITY.get(league, 20)
            if len(members) > cap:
                surplus = members[cap:]
                new_members[league] = members[:cap]
                lower_idx = i-1
                if lower_idx >= 0:
                    new_members[LEAGUE_ORDER[lower_idx]].extend(surplus)
                    changed = True
                else:
                    # drop if bottom
                    print(f"[WARN] bottom league overflow, dropping {len(surplus)} players")
                    changed = True
        if not changed:
            break

    # build final mapping and diff
    final_map = {}
    for l in LEAGUE_ORDER:
        for r in new_members[l]:
            final_map[r["id"]] = l

    # produce outputs
    users_out = users.copy()
    users_out["new_league"] = users_out["id"].map(lambda i: final_map.get(i, users_out.loc[users_out["id"]==i, "league"].iloc[0]))
    users_out["weekly_points"] = users_out["weekly_points"].astype(float)

    # list changes
    changes = users_out[users_out["league"] != users_out["new_league"]][["id","user_id","pseudo","league","new_league","weekly_points"]]
    print(f"[INFO] total changes: {len(changes)}")
    if len(changes):
        print(changes.to_string(index=False))

    if args.dry_run:
        print("[DRY RUN] no files written due to --dry-run")
        return

    # write out csv/json
    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    users_out.loc[:, users.columns.tolist() + ["new_league", "weekly_points"]].to_csv(args.out_csv, index=False)
    print(f"[INFO] wrote {args.out_csv}")

    # JSON per-league for frontend
    out_json = {}
    for l in LEAGUE_ORDER:
        out_json[l] = []
        list_members = sorted(new_members[l], key=lambda r: (-float(r.get("weekly_points",0.0)), r.get("pseudo","")))
        for r in list_members:
            out_json[l].append({
                "id": r.get("id"),
                "user_id": r.get("user_id"),
                "pseudo": r.get("pseudo"),
                "weekly_points": float(r.get("weekly_points",0.0)),
                "old_league": r.get("league", "")
            })
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump({"week": week_date_str, "leagues": out_json}, fh, ensure_ascii=False, indent=2)
    print(f"[INFO] wrote {args.out_json}")

    # SQL file with updates for rows that changed
    if args.sql_file:
        with open(args.sql_file, "w", encoding="utf-8") as fh:
            fh.write("-- SQL statements generated by update_leagues.py\n")
            fh.write("-- Run in your DB environment or use your existing migration pipeline\n")
            for _, row in changes.iterrows():
                uid = row["id"]
                newl = row["new_league"]
                # safety: escape single quote
                newl_esc = str(newl).replace("'", "''")
                fh.write(f"UPDATE users SET league = '{newl_esc}' WHERE id = '{uid}';\n")
        print(f"[INFO] wrote SQL to {args.sql_file}")

if __name__ == "__main__":
    main()