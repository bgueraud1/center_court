#!/usr/bin/env python3
"""
Weekly league updater with league instances (league_id).

Summary:
 - Each user belongs to a level (e.g. "Future F15") stored in `league`.
 - Each user also belongs to a specific instance within that level: `league_id` (int).
 - Instances have a capacity (LEAGUE_CAPACITY[level], default 20).
 - We compute weekly points, rank users inside instances, compute promote/relegate counts
   (based on PROMOTE_PCT/RELEGATE_PCT applied per-instance), then move users between
   instances (creating new instances if needed) while ensuring capacity.
 - Outputs:
    * updated users CSV with new_league and new_league_id columns,
    * JSON grouping users by level and instance id,
    * SQL update file for changed rows (if requested)
"""
import argparse
import datetime
import json
import math
import os
import sys
from collections import defaultdict, OrderedDict

import pandas as pd

# --- Configuration ----------------------------------------------------------
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

# default capacity per instance
DEFAULT_CAPACITY = 20
LEAGUE_CAPACITY = {l: DEFAULT_CAPACITY for l in LEAGUE_ORDER}

# promotion / relegation fraction per level (apply to instance size)
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
RELEGATE_PCT = PROMOTE_PCT.copy()
# ----------------------------------------------------------------------------

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

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0

def ensure_int(v, default=0):
    try:
        if pd.isna(v): return default
    except Exception:
        pass
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default

def build_instances_from_users(users_df):
    """
    Return dict: level -> dict( league_id -> [rows_as_dict] )
    If a user lacks league_id, we will assign later.
    """
    instances = {lvl: OrderedDict() for lvl in LEAGUE_ORDER}
    # collect existing league_id values per level
    for _, row in users_df.iterrows():
        lvl = row.get("league") if row.get("league") in LEAGUE_ORDER else LEAGUE_ORDER[0]
        lid = row.get("league_id", None)
        if lid is None or lid == "" or str(lid).lower() == "nan":
            # postpone assignment
            continue
        lid_i = ensure_int(lid)
        if lid_i not in instances[lvl]:
            instances[lvl][lid_i] = []
        instances[lvl][lid_i].append(row.to_dict())
    # for each level, assign sequential missing ids starting from 0.. ensuring contiguity
    # determine max existing id per level
    for lvl in LEAGUE_ORDER:
        existing_ids = list(instances[lvl].keys())
        next_id = 0
        if existing_ids:
            next_id = max(existing_ids) + 1
        # now attach users without league_id into instances next (we do that later once we know capacity)
    return instances

def pack_users_into_instances(users_df, instances):
    """
    Ensure every user is assigned to some instance within their level.
    Strategy: for each level, gather users without league_id and fill existing instances up to capacity,
    then create new instances as needed (id increments).
    """
    # result mapping level -> OrderedDict(league_id -> [user_rows_dict])
    out = {lvl: OrderedDict() for lvl in LEAGUE_ORDER}
    # start by copying existing instances
    for lvl in LEAGUE_ORDER:
        for lid, members in instances[lvl].items():
            out[lvl][lid] = list(members)[:]  # shallow copy

    # collect unassigned users per level
    unassigned = {lvl: [] for lvl in LEAGUE_ORDER}
    for _, row in users_df.iterrows():
        lvl = row.get("league") if row.get("league") in LEAGUE_ORDER else LEAGUE_ORDER[0]
        lid = row.get("league_id", None)
        if lid is None or lid == "" or str(lid).lower() == "nan":
            unassigned[lvl].append(row.to_dict())
        else:
            # already handled above
            pass

    # fill per-level
    for lvl in LEAGUE_ORDER:
        cap = LEAGUE_CAPACITY.get(lvl, DEFAULT_CAPACITY)
        # find next id
        existing_ids = list(out[lvl].keys())
        next_id = 0
        if existing_ids:
            next_id = max(existing_ids) + 1
        # fill existing instances first (by ascending id)
        for lid in sorted(list(out[lvl].keys())):
            space = cap - len(out[lvl][lid])
            if space <= 0: continue
            to_take = unassigned[lvl][:space]
            out[lvl][lid].extend(to_take)
            unassigned[lvl] = unassigned[lvl][space:]
        # create new instances as needed
        while len(unassigned[lvl]) > 0:
            chunk = unassigned[lvl][:cap]
            out[lvl][next_id] = chunk
            unassigned[lvl] = unassigned[lvl][cap:]
            next_id += 1
    return out

def aggregate_weekly_points(scores_df, week_date_str):
    """
    Return dicts:
      by_userid: user_id -> points
      by_pseudo: pseudo_lower -> points
    """
    if scores_df is None or scores_df.empty:
        return {}, {}
    # clean
    df = scores_df.copy()
    if "points" not in df.columns:
        df["points"] = 0.0
    else:
        df["points"] = df["points"].apply(safe_float)

    # ensure created_day exists
    if "created_day" not in df.columns and "created_at" in df.columns:
        df["created_day"] = df["created_at"].apply(lambda s: str(s).split(" ")[0] if isinstance(s, str) and s else "")

    df["user_id"] = df.get("user_id", "").fillna("").astype(str)
    df["pseudo"] = df.get("pseudo", "").fillna("").astype(str)

    # week-specific
    week_df = df[df["created_day"] == week_date_str]

    by_userid = {}
    by_pseudo = {}

    if not week_df.empty:
        # by user_id
        u = week_df[week_df["user_id"].astype(bool)].groupby("user_id")["points"].sum().to_dict()
        by_userid = {str(k): float(v) for k, v in u.items()}
        # by pseudo where no user_id
        up = week_df[~week_df["user_id"].astype(bool) & week_df["pseudo"].astype(bool)].groupby("pseudo")["points"].sum().to_dict()
        by_pseudo = {str(k).lower(): float(v) for k, v in up.items()}
    return by_userid, by_pseudo

def compute_instance_weekly_points(instance_members, by_userid, by_pseudo):
    """
    instance_members: list of user dicts (from users csv)
    returns list of (member_dict updated with weekly_points)
    """
    out = []
    for r in instance_members:
        uid = r.get("user_id") or ""
        pseudo = (r.get("pseudo") or "").strip()
        pts = 0.0
        if uid and uid in by_userid:
            pts = float(by_userid[uid])
        elif pseudo and pseudo.lower() in by_pseudo:
            pts = float(by_pseudo[pseudo.lower()])
        else:
            pts = 0.0
        r2 = dict(r)
        r2["weekly_points"] = float(pts)
        out.append(r2)
    return out

def compute_promote_relegate_counts(inst_members):
    """
    Given sorted member list (desc weekly_points) for one instance,
    compute number to promote/relegate based on PROMOTE_PCT/RELEGATE_PCT.
    Returns (promote_count, relegate_count)
    """
    size = len(inst_members)
    if size <= 1:
        return 0, 0
    lvl = inst_members[0].get("league") if inst_members and inst_members[0].get("league") in LEAGUE_ORDER else None
    if not lvl:
        # conservative zero
        return 0, 0
    up_pct = PROMOTE_PCT.get(lvl, 0.0)
    down_pct = RELEGATE_PCT.get(lvl, 0.0)
    promote_count = 0
    relegate_count = 0
    if up_pct > 0:
        promote_count = int(math.floor(up_pct * size))
        if promote_count == 0 and size >= 3:
            promote_count = 1  # ensure movement for non-trivial groups
    if down_pct > 0:
        relegate_count = int(math.floor(down_pct * size))
        if relegate_count == 0 and size >= 3:
            relegate_count = 1
    # cap counts to size-1 to avoid moving entire instance
    promote_count = min(promote_count, max(0, size-1))
    relegate_count = min(relegate_count, max(0, size-1-promote_count))
    return promote_count, relegate_count

def main():
    args = parse_args()

    # week date
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
    if "id" not in users.columns:
        print("[ERROR] users CSV must contain 'id' column", file=sys.stderr)
        sys.exit(2)
    # ensure league column
    if "league" not in users.columns:
        users["league"] = ""
    # ensure league_id column
    if "league_id" not in users.columns:
        users["league_id"] = ""

    # normalize league levels
    users["league"] = users["league"].apply(lambda x: x if x in LEAGUE_ORDER else (LEAGUE_ORDER[0] if not x else x))

    # load scores
    scores = pd.DataFrame()
    if os.path.exists(args.scores_csv):
        scores = pd.read_csv(args.scores_csv, dtype=str).fillna("")
        # ensure points numeric column
        if "points" in scores.columns:
            scores["points"] = scores["points"].apply(safe_float)
        else:
            scores["points"] = 0.0
        if "created_day" not in scores.columns and "created_at" in scores.columns:
            scores["created_day"] = scores["created_at"].apply(lambda s: str(s).split(" ")[0] if isinstance(s, str) and s else "")
    else:
        print(f"[WARN] scores CSV '{args.scores_csv}' not found. Weekly points assumed 0", file=sys.stderr)

    # aggregate weekly points maps
    by_userid, by_pseudo = aggregate_weekly_points(scores, week_date_str)
    print(f"[INFO] weekly points: {len(by_userid)} user_id entries, {len(by_pseudo)} pseudo entries")

    # build initial instances from existing league_id where present
    instances = build_instances_from_users(users)

    # pack all users into instances (assign missing league_id here; maintain existing ones)
    instances_packed = pack_users_into_instances(users, instances)

    # compute weekly_points for everyone in each instance
    # structure: per level -> per instance id -> member list (dict with weekly_points)
    inst_with_points = {lvl: OrderedDict() for lvl in LEAGUE_ORDER}
    for lvl in LEAGUE_ORDER:
        for lid, members in instances_packed[lvl].items():
            enriched = compute_instance_weekly_points(members, by_userid, by_pseudo)
            # ensure each member has league & league_id set for further processing
            for m in enriched:
                m["league"] = lvl
                m["league_id"] = int(lid)
            # sort descending by weekly_points, then by pseudo
            enriched_sorted = sorted(enriched, key=lambda r: (-float(r.get("weekly_points", 0.0)), r.get("pseudo",""), r.get("id","")))
            inst_with_points[lvl][lid] = enriched_sorted

    # collect moves
    promote_list = []   # tuples (member_dict, from_lvl, from_lid, to_lvl)
    relegate_list = []

    for idx, lvl in enumerate(LEAGUE_ORDER):
        members_by_inst = inst_with_points[lvl]
        next_level = LEAGUE_ORDER[idx+1] if idx+1 < len(LEAGUE_ORDER) else None
        prev_level = LEAGUE_ORDER[idx-1] if idx-1 >= 0 else None
        for lid, members in list(members_by_inst.items()):
            promote_count, relegate_count = compute_promote_relegate_counts(members)
            if promote_count > 0 and next_level:
                # top promote_count move up
                for m in members[:promote_count]:
                    promote_list.append((m, lvl, lid, next_level))
            if relegate_count > 0 and prev_level:
                # bottom relegate_count move down
                for m in members[-relegate_count:]:
                    relegate_list.append((m, lvl, lid, prev_level))

    print(f"[INFO] promote candidates: {len(promote_list)}, relegate candidates: {len(relegate_list)}")

    # apply moves into a new membership structure (start from current inst_with_points)
    new_members = {lvl: OrderedDict() for lvl in LEAGUE_ORDER}
    # copy current members (we will remove moved ones)
    for lvl in LEAGUE_ORDER:
        for lid, members in inst_with_points[lvl].items():
            new_members[lvl][lid] = [dict(m) for m in members]

    # helper to remove member by id from list
    def remove_member_from_list(lst, member_id):
        for i, it in enumerate(lst):
            if str(it.get("id")) == str(member_id):
                return lst.pop(i)
        return None

    # First, remove promote candidates from their current instances
    for m, from_lvl, from_lid, to_lvl in promote_list:
        uid = m.get("id")
        lst = new_members[from_lvl].get(from_lid, [])
        removed = remove_member_from_list(lst, uid)
        if removed is None:
            # maybe matched by pseudo only -> try match by pseudo
            for i, it in enumerate(new_members[from_lvl].get(from_lid, [])):
                if (it.get("pseudo","") or "").lower() == (m.get("pseudo","") or "").lower():
                    removed = new_members[from_lvl][from_lid].pop(i)
                    break
        # ensure removal reflected
        new_members[from_lvl][from_lid] = new_members[from_lvl].get(from_lid, [])
    # Then remove relegate candidates as well (they might overlap; promotions already removed prioritized)
    for m, from_lvl, from_lid, to_lvl in relegate_list:
        uid = m.get("id")
        lst = new_members[from_lvl].get(from_lid, [])
        removed = remove_member_from_list(lst, uid)
        if removed is None:
            for i, it in enumerate(new_members[from_lvl].get(from_lid, [])):
                if (it.get("pseudo","") or "").lower() == (m.get("pseudo","") or "").lower():
                    removed = new_members[from_lvl][from_lid].pop(i)
                    break
        new_members[from_lvl][from_lid] = new_members[from_lvl].get(from_lid, [])

    # Now insert promoted users into target higher-level instances (fill existing first, create new if needed)
    for m, from_lvl, from_lid, to_lvl in promote_list:
        cap = LEAGUE_CAPACITY.get(to_lvl, DEFAULT_CAPACITY)
        # find existing instance with space (lowest id first)
        target_found = False
        for lid in sorted(new_members[to_lvl].keys()):
            if len(new_members[to_lvl][lid]) < cap:
                # insert at end
                nm = dict(m); nm["league"] = to_lvl; nm["league_id"] = int(lid)
                new_members[to_lvl][lid].append(nm)
                target_found = True
                break
        if not target_found:
            # create new instance id
            existing_ids = list(new_members[to_lvl].keys())
            next_id = 0
            if existing_ids:
                next_id = max(existing_ids) + 1
            nm = dict(m); nm["league"] = to_lvl; nm["league_id"] = int(next_id)
            new_members[to_lvl][next_id] = [nm]

    # Insert relegated users into lower level instances (same logic)
    for m, from_lvl, from_lid, to_lvl in relegate_list:
        cap = LEAGUE_CAPACITY.get(to_lvl, DEFAULT_CAPACITY)
        target_found = False
        for lid in sorted(new_members[to_lvl].keys()):
            if len(new_members[to_lvl][lid]) < cap:
                nm = dict(m); nm["league"] = to_lvl; nm["league_id"] = int(lid)
                new_members[to_lvl][lid].append(nm)
                target_found = True
                break
        if not target_found:
            existing_ids = list(new_members[to_lvl].keys())
            next_id = 0
            if existing_ids:
                next_id = max(existing_ids) + 1
            nm = dict(m); nm["league"] = to_lvl; nm["league_id"] = int(next_id)
            new_members[to_lvl][next_id] = [nm]

    # After all moves, ensure capacities by demoting surplus bottom players iteratively
    for _iter in range(6):
        changed = False
        # process top->bottom (so surpluses get demoted)
        for i in range(len(LEAGUE_ORDER)-1, -1, -1):
            lvl = LEAGUE_ORDER[i]
            cap = LEAGUE_CAPACITY.get(lvl, DEFAULT_CAPACITY)
            lids = sorted(new_members[lvl].keys())
            for lid in lids:
                members = new_members[lvl][lid]
                if len(members) <= cap:
                    continue
                surplus = members[cap:]
                new_members[lvl][lid] = members[:cap]
                # demote surplus to next lower level (if exists), append to instance(s)
                lower_idx = i-1
                if lower_idx < 0:
                    # drop if cannot demote further
                    print(f"[WARN] dropping {len(surplus)} users from '{lvl}' instance {lid} (no lower level)")
                    continue
                lower_lvl = LEAGUE_ORDER[lower_idx]
                # append surplus to lower level; distribute into existing instances with space or create new
                for s in surplus:
                    placed = False
                    for lower_lid in sorted(new_members[lower_lvl].keys()):
                        if len(new_members[lower_lvl][lower_lid]) < LEAGUE_CAPACITY.get(lower_lvl, DEFAULT_CAPACITY):
                            s2 = dict(s); s2["league"] = lower_lvl; s2["league_id"] = int(lower_lid)
                            new_members[lower_lvl][lower_lid].append(s2)
                            placed = True
                            break
                    if not placed:
                        # create new instance
                        existing_ids = list(new_members[lower_lvl].keys())
                        nxt = 0
                        if existing_ids:
                            nxt = max(existing_ids) + 1
                        s2 = dict(s); s2["league"] = lower_lvl; s2["league_id"] = int(nxt)
                        new_members[lower_lvl][nxt] = [s2]
                    changed = True
        if not changed:
            break

    # finalize: sort members inside each instance by weekly_points desc
    for lvl in LEAGUE_ORDER:
        for lid in list(new_members[lvl].keys()):
            arr = new_members[lvl][lid]
            arr_sorted = sorted(arr, key=lambda r: (-float(r.get("weekly_points", 0.0)), r.get("pseudo",""), r.get("id","")))
            new_members[lvl][lid] = arr_sorted

    # produce final mapping user_id -> (new_league, new_league_id)
    final_map = {}
    for lvl in LEAGUE_ORDER:
        for lid, members in new_members[lvl].items():
            for m in members:
                final_map[str(m.get("id"))] = (lvl, int(lid))

    # prepare outputs
    users_out = users.copy()
    # ensure proper dtype
    users_out["weekly_points"] = users_out.get("weekly_points", "").astype(str).apply(lambda s: float(s) if s not in (None, "", "nan") else 0.0)

    def lookup_new_league(row):
        uid = str(row["id"])
        if uid in final_map:
            return final_map[uid][0]
        return row.get("league", LEAGUE_ORDER[0])

    def lookup_new_league_id(row):
        uid = str(row["id"])
        if uid in final_map:
            return final_map[uid][1]
        # fallback: existing or assign 0
        return ensure_int(row.get("league_id", 0), 0)

    users_out["new_league"] = users_out.apply(lookup_new_league, axis=1)
    users_out["new_league_id"] = users_out.apply(lookup_new_league_id, axis=1)
    # ensure weekly_points column updated from our computed values (if present)
    # recompute weekly_points from final_map members if possible
    # Build lookup for computed weekly_points in new_members
    weekly_lookup = {}
    for lvl in LEAGUE_ORDER:
        for lid, members in new_members[lvl].items():
            for m in members:
                weekly_lookup[str(m.get("id"))] = float(m.get("weekly_points", 0.0))
    # fill users_out weekly_points from lookup where available, else keep existing
    users_out["weekly_points"] = users_out.apply(lambda r: float(weekly_lookup.get(str(r["id"]), safe_float(r.get("weekly_points", 0.0)))), axis=1)

    # report changes
    changes_df = users_out[users_out["league"].astype(str) != users_out["new_league"].astype(str)][["id", "user_id", "pseudo", "league", "new_league", "league_id", "new_league_id", "weekly_points"]]
    print(f"[INFO] total moves: {len(changes_df)}")
    if len(changes_df) > 0:
        print(changes_df.to_string(index=False))

    if args.dry_run:
        print("[DRY RUN] not writing outputs (--dry-run)")
        return

    # write outputs
    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    out_cols = list(users.columns) + ["weekly_points", "new_league", "new_league_id"]
    users_out.loc[:, out_cols].to_csv(args.out_csv, index=False)
    print(f"[INFO] wrote updated users CSV to {args.out_csv}")

    # write JSON grouped by level and instance id
    out_json = {"week": week_date_str, "leagues": {}}
    for lvl in LEAGUE_ORDER:
        out_json["leagues"][lvl] = {}
        for lid, members in new_members[lvl].items():
            out_json["leagues"][lvl][str(lid)] = []
            for m in members:
                out_json["leagues"][lvl][str(lid)].append({
                    "id": m.get("id"),
                    "user_id": m.get("user_id"),
                    "pseudo": m.get("pseudo"),
                    "weekly_points": float(m.get("weekly_points", 0.0)),
                    "old_league": m.get("league") if m.get("old_league") else ""
                })
    os.makedirs(os.path.dirname(args.out_json) or ".", exist_ok=True)
    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(out_json, fh, ensure_ascii=False, indent=2)
    print(f"[INFO] wrote JSON to {args.out_json}")

    # write SQL updates for rows that changed
    if args.sql_file:
        with open(args.sql_file, "w", encoding="utf-8") as fh:
            fh.write("-- SQL statements generated by update_leagues.py\n")
            fh.write("-- Update league level and league_id for changed users\n")
            for _, row in changes_df.iterrows():
                uid = row["id"]
                newl = row["new_league"]
                newlid = int(row["new_league_id"]) if not pd.isna(row["new_league_id"]) else 0
                newl_esc = str(newl).replace("'", "''")
                fh.write(f"UPDATE users SET league = '{newl_esc}', league_id = {newlid} WHERE id = '{uid}';\n")
        print(f"[INFO] wrote SQL to {args.sql_file}")

if __name__ == "__main__":
    main()