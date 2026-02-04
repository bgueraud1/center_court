import requests
import pandas as pd
import re

def fetch_tournament_data(year, tournament_id, verbose=False):
    """
    Fetch matches and return DataFrame.
    Corrected mapping: explicitly determine playerA/playerB first, then count set wins
    and attribute winner/loser to those exact names (no home/away vs A/B confusion).
    """
    if verbose:
        print(f"Fetching data for year {year}, tournament ID {tournament_id}...")
    url = f"https://api.wtatennis.com/tennis/tournaments/{tournament_id}/{year}/matches?type=S&sort=desc"

    try:
        resp = requests.get(url, timeout=30)
    except Exception as e:
        if verbose:
            print(f"Request error: {e}")
        return pd.DataFrame()

    if resp.status_code != 200:
        if verbose:
            print(f"Error: Received status code {resp.status_code}")
        return pd.DataFrame()

    try:
        data = resp.json()
    except ValueError:
        if verbose:
            print("Error: Response is not a valid JSON")
        return pd.DataFrame()

    # tournament metadata (canonical indoor_outdoor)
    tournament_info = {}
    if isinstance(data, dict):
        tinfo = data.get("tournament") or {}
        tgroup = tinfo.get("tournamentGroup") or {}
        indoor_val = tinfo.get("inOutdoor") or tinfo.get("in_outdoor") or tgroup.get("inOutdoor") or tinfo.get("Indoor/Outdoor") or None
        tournament_info = {
            "Tournament Name": tgroup.get("name") or None,
            "Tournament Title": tinfo.get("title") or None,
            "Level": tgroup.get("level") or tinfo.get("level") or None,
            "Year": tinfo.get("year") or None,
            "Start Date": tinfo.get("startDate") or None,
            "End Date": tinfo.get("endDate") or None,
            "Surface": tinfo.get("surface") or None,
            "indoor_outdoor": indoor_val,
            "City": tinfo.get("city") or None,
            "Country": tinfo.get("country") or None,
            "Singles Draw Size": tinfo.get("singlesDrawSize") or None,
            "Doubles Draw Size": tinfo.get("doublesDrawSize") or None,
            "Prize Money": tinfo.get("prizeMoney") or None,
            "Prize Money Currency": tinfo.get("prizeMoneyCurrency") or None,
        }

    # collect candidate matches
    candidates = []
    if isinstance(data, dict) and "matches" in data:
        matches = data.get("matches")
        if isinstance(matches, list) and matches and isinstance(matches[0], list):
            for sub in matches:
                if isinstance(sub, list):
                    candidates.extend(sub)
        elif isinstance(matches, list):
            candidates = matches
    else:
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            for k,v in data.items():
                if isinstance(v, list) and v:
                    first = v[0]
                    if isinstance(first, dict):
                        candidates = v
                        break

    if verbose:
        print(f"Candidate matches found: {len(candidates)}")
    if not candidates:
        if verbose:
            print("No match candidates found in JSON.")
        return pd.DataFrame()

    # ---------- helpers ----------
    def clean_str(x):
        if x is None:
            return ""
        if isinstance(x, (int,float)):
            return str(int(x))
        return str(x).strip()

    def parse_pair(s):
        if not s or not isinstance(s, str):
            return (None, None)
        m = re.match(r"\s*(\d+)\s*-\s*(\d+)", s)
        if m:
            return int(m.group(1)), int(m.group(2))
        digits = re.findall(r"\d+", s)
        if len(digits) >= 2:
            return int(digits[0]), int(digits[1])
        return (None, None)

    def count_wins(pairs):
        a = b = 0
        for x,y in pairs:
            if x is None or y is None:
                continue
            if x > y:
                a += 1
            elif y > x:
                b += 1
        return a,b

    def normalize_set(a_raw,b_raw,tb_raw=None):
        a = clean_str(a_raw); b = clean_str(b_raw)
        if a=="" or b=="":
            return None
        tb = clean_str(tb_raw) if tb_raw is not None else ""
        if tb and re.search(r"\d", tb):
            m = re.search(r"(\d+)", tb)
            return f"{a}-{b}({m.group(1)})" if m else f"{a}-{b}"
        return f"{a}-{b}"

    def parse_score_string(score_string):
        if not score_string:
            return []
        s = str(score_string)
        parts = [p.strip() for p in s.split(",") if p.strip()]
        out=[]
        for p in parts[:5]:
            if re.match(r"^\d+\s*-\s*\d+(\s*\(\s*\d+\s*\))?$", p):
                out.append(re.sub(r"\s+","",p))
            else:
                digits = re.findall(r"\d+", p)
                if len(digits)>=2:
                    if len(digits)>=3:
                        out.append(f"{digits[0]}-{digits[1]}({digits[2]})")
                    else:
                        out.append(f"{digits[0]}-{digits[1]}")
        return out

    def build_sets_and_pairs(item):
        sets=[]; pairs=[]
        for n in range(1,6):
            a = item.get(f"ScoreSet{n}A","") or item.get(f"scorea_set{n-1}","") or item.get(f"ScoreASet{n}","")
            b = item.get(f"ScoreSet{n}B","") or item.get(f"scoreb_set{n-1}","") or item.get(f"ScoreBSet{n}","")
            tb = item.get(f"ScoreTbSet{n}","") or item.get(f"scoretb_set{n-1}","")
            if (a is None or a=="") and (b is None or b==""):
                continue
            if a is None or a=="" or b is None or b=="":
                continue
            s = normalize_set(a,b,tb)
            sets.append(s); pairs.append(parse_pair(s))
        if sets:
            return sets,pairs
        ss = item.get("ScoreString") or item.get("ResultString") or item.get("Score") or item.get("score_string")
        if ss:
            parts=[p.strip() for p in str(ss).split(",") if p.strip()]
            for p in parts[:5]:
                sets.append(p); pairs.append(parse_pair(p))
            return sets,pairs
        for n in range(0,5):
            k=f"set{n}_score"
            if k in item and item.get(k):
                v=str(item.get(k)).strip().replace("/","-")
                sets.append(v); pairs.append(parse_pair(v))
        return sets,pairs

    def winner_from_resultstring(result_string, nameA, nameB):
        if not result_string or not isinstance(result_string,str):
            return None
        delim = re.search(r"\s+d\s+|\s+def\s+|def\.|def\s+", result_string, flags=re.IGNORECASE)
        if delim:
            left = result_string[:delim.start()].strip().lower()
        else:
            left = re.split(r"\d{1,2}-\d{1,2}", result_string)[0].strip().lower()
        for full,label in ((nameA,'A'),(nameB,'B')):
            if not full:
                continue
            tokens=[t.lower() for t in str(full).split() if t]
            if tokens and tokens[-1] in left:
                return label
            if tokens and tokens[0] in left:
                return label
        return None

    rows=[]
    for idx,item in enumerate(candidates):
        if not isinstance(item, dict):
            continue

        # --- determine canonical playerA / playerB first (names, ids, seeds, countries) ---
        # Try legacy keys first (PlayerNameFirstA/B)
        pA_name = None; pB_name = None
        pA_id = None; pB_id = None
        pA_seed = None; pB_seed = None
        pA_country = None; pB_country = None

        # legacy keys
        if item.get("PlayerNameFirstA") or item.get("PlayerNameLastA") or item.get("PlayerIDA"):
            # build names from first/last if possible
            firstA = item.get("PlayerNameFirstA") or ""
            lastA = item.get("PlayerNameLastA") or ""
            pA_name = (" ".join([str(firstA).strip(), str(lastA).strip()]).strip()) or item.get("PlayerNameA") or item.get("PlayerA")
            pA_id = str(item.get("PlayerIDA")) if item.get("PlayerIDA") is not None else None
            pA_seed = item.get("SeedA") or item.get("Seed")
            pA_country = item.get("PlayerCountryA") or item.get("PlayerCountryA2") or None

        if item.get("PlayerNameFirstB") or item.get("PlayerNameLastB") or item.get("PlayerIDB"):
            firstB = item.get("PlayerNameFirstB") or ""
            lastB = item.get("PlayerNameLastB") or ""
            pB_name = (" ".join([str(firstB).strip(), str(lastB).strip()]).strip()) or item.get("PlayerNameB") or item.get("PlayerB")
            pB_id = str(item.get("PlayerIDB")) if item.get("PlayerIDB") is not None else None
            pB_seed = item.get("SeedB")
            pB_country = item.get("PlayerCountryB") or item.get("PlayerCountryB2") or None

        # If structured competitors exist and we don't have names from legacy, take them in order as A/B
        event = item.get("sport_event") or {}
        competitors = []
        if event:
            competitors = event.get("competitors", []) or []
        if competitors and (not pA_name or not pB_name):
            # take competitor[0] => A, competitor[1] => B (consistent with many feeds)
            if len(competitors) >= 1 and not pA_name:
                comp0 = competitors[0]
                pA_name = pA_name or comp0.get("name") or comp0.get("abbreviation")
                pA_id = pA_id or (str(comp0.get("id")) if comp0.get("id") is not None else None)
                pA_seed = pA_seed or comp0.get("seed")
                pA_country = pA_country or comp0.get("country")
            if len(competitors) >= 2 and not pB_name:
                comp1 = competitors[1]
                pB_name = pB_name or comp1.get("name") or comp1.get("abbreviation")
                pB_id = pB_id or (str(comp1.get("id")) if comp1.get("id") is not None else None)
                pB_seed = pB_seed or comp1.get("seed")
                pB_country = pB_country or comp1.get("country")

        # If still missing names, fallback to item fields Home Player / Away Player
        if not pA_name:
            pA_name = item.get("Home Player") or item.get("PlayerNameA") or item.get("PlayerNameFirstA") or None
        if not pB_name:
            pB_name = item.get("Away Player") or item.get("PlayerNameB") or item.get("PlayerNameFirstB") or None

        # --- now process structured branch and legacy branch, but always use pA_name/pB_name for assignment ---
        # Structured format branch
        if ("sport_event" in item) or ("sport_event_status" in item) or ("sport_event" in item.keys()):
            status = item.get("sport_event_status") or {}
            match_id = (event.get("id") or item.get("id") or item.get("MatchID"))
            date = event.get("start_time") or item.get("start_time") or item.get("startDate") or item.get("MatchTimeStamp")
            round_name = (event.get("sport_event_context") or {}).get("round", {}).get("name") or item.get("round") or item.get("RoundID")

            # gather sets & pairs
            set_scores = []; pairs=[]
            period_scores = status.get("period_scores",[]) or []
            if isinstance(period_scores, list) and period_scores:
                for s in period_scores:
                    home = s.get("home_score",""); away = s.get("away_score","")
                    if home=="" and away=="":
                        continue
                    tb = s.get("tiebreak") if isinstance(s, dict) else None
                    setstr = normalize_set(home, away, tb)
                    if setstr:
                        set_scores.append(setstr); pairs.append(parse_pair(setstr))
            if not set_scores:
                ss = status.get("score_string") or status.get("ScoreString")
                if ss:
                    set_scores = parse_score_string(ss); pairs=[parse_pair(x) for x in set_scores]

            set1 = set_scores[0] if len(set_scores)>0 else None
            set2 = set_scores[1] if len(set_scores)>1 else None
            set3 = set_scores[2] if len(set_scores)>2 else None

            # Determine winner by comparing wins of A vs B (pairs correspond to A-B ordering used above)
            a_wins,b_wins = count_wins(pairs)
            winner_label = None
            if a_wins > b_wins:
                winner_label = 'A'
                method = 'sets'
            elif b_wins > a_wins:
                winner_label = 'B'
                method = 'sets'
            else:
                # fallback: Winner field (could be '1'/'2' or id)
                w_raw = item.get("Winner")
                if w_raw is not None:
                    wr = str(w_raw).strip()
                    if wr in ("1","A","a"):
                        winner_label='A'; method='winner_field'
                    elif wr in ("2","B","b"):
                        winner_label='B'; method='winner_field'
                    elif pA_id and wr == str(pA_id):
                        winner_label='A'; method='winner_field_id'
                    elif pB_id and wr == str(pB_id):
                        winner_label='B'; method='winner_field_id'
                    else:
                        # try ResultString parse
                        res = item.get("ResultString") or item.get("ScoreString")
                        parsed = winner_from_resultstring(res, pA_name, pB_name)
                        if parsed == 'A':
                            winner_label='A'; method='resultstring'
                        elif parsed == 'B':
                            winner_label='B'; method='resultstring'
                        else:
                            winner_label=None; method='unknown'

            if winner_label == 'A':
                winner_name = pA_name; loser_name = pB_name
                winner_seed = pA_seed; loser_seed = pB_seed
                winner_country = pA_country; loser_country = pB_country
            elif winner_label == 'B':
                winner_name = pB_name; loser_name = pA_name
                winner_seed = pB_seed; loser_seed = pA_seed
                winner_country = pB_country; loser_country = pA_country
            else:
                winner_name = None; loser_name = None
                winner_seed = None; loser_seed = None
                winner_country = None; loser_country = None

            row = {
                **tournament_info,
                "Tournament ID": tournament_id,
                "Match ID": match_id,
                "Date": date,
                "Round Name": round_name,
                "Set 1 Score": set1,
                "Set 2 Score": set2,
                "Set 3 Score": set3,
                "Home Player": pA_name,
                "Home Country": pA_country,
                "Home Seed": pA_seed,
                "Away Player": pB_name,
                "Away Country": pB_country,
                "Away Seed": pB_seed,
                "winner_flag": winner_label,
                "winner_player_name": winner_name,
                "loser_player_name": loser_name,
                "winner_seed": winner_seed,
                "loser_seed": loser_seed,
                "winner_country": winner_country,
                "loser_country": loser_country,
            }

            # attach stats if any (competitors may have ids etc.)
            all_stats = item.get("statistics",{}).get("totals",{}).get("competitors",[])
            if isinstance(all_stats, list):
                for p in all_stats:
                    pid = p.get("id")
                    stats = p.get("statistics",{}) or {}
                    if pid and competitors and any(pid == c.get("id") for c in competitors):
                        if pid == competitors[0].get("id"):
                            for k,v in stats.items():
                                row[f"Home {k}"] = v
                        elif pid == competitors[1].get("id"):
                            for k,v in stats.items():
                                row[f"Away {k}"] = v
                    else:
                        for k,v in stats.items():
                            row[f"Stat {k}"] = v

            rows.append(row)
            continue

        # Legacy / flat format
                # Legacy / flat format
        legacy_keys = ("MatchID","PlayerIDA","PlayerIDB","ScoreSet1A","ScoreString","PlayerNameFirstA")
        if any(k in item for k in legacy_keys):
            match_id = item.get("MatchID") or item.get("MatchId")
            date = item.get("MatchTimeStamp") or item.get("MatchTime") or item.get("MatchDate")
            round_name = item.get("RoundID") or item.get("Round")

            # --- NEW: prefer explicit ScoreSet{n}A/B when available (avoids off-by-one / shifting)
            sets = []
            pairs = []
            # if explicit ScoreSet fields exist, build sets from them in order
            if any(f"ScoreSet{n}A" in item or f"ScoreSet{n}B" in item for n in range(1,6)):
                for n in range(1,6):
                    a = item.get(f"ScoreSet{n}A", "")
                    b = item.get(f"ScoreSet{n}B", "")
                    tb = item.get(f"ScoreTbSet{n}", "") or ""
                    # skip absent sets
                    if (a is None or a == "") and (b is None or b == ""):
                        continue
                    if a is None or a == "" or b is None or b == "":
                        # partial set info -> skip (defensive)
                        continue
                    s = normalize_set(a, b, tb)
                    if s:
                        sets.append(s)
                        pairs.append(parse_pair(s))
            else:
                # fallback to existing builder (ScoreString, set0_score, etc.)
                sets, pairs = build_sets_and_pairs(item)

            set1 = sets[0] if len(sets) > 0 else None
            set2 = sets[1] if len(sets) > 1 else None
            set3 = sets[2] if len(sets) > 2 else None

            # ensure pA_id/pB_id from item if present
            if not pA_id:
                pA_id = str(item.get("PlayerIDA")) if item.get("PlayerIDA") is not None else None
            if not pB_id:
                pB_id = str(item.get("PlayerIDB")) if item.get("PlayerIDB") is not None else None

            # --- winner detection: prefer set comparison, but fall back to Winner or ResultString
            a_wins, b_wins = count_wins(pairs)
            winner_label = None
            method = None
            if a_wins > b_wins:
                winner_label = 'A'; method = 'sets'
            elif b_wins > a_wins:
                winner_label = 'B'; method = 'sets'
            else:
                # tie or no set info -> read Winner field (could be "1"/"2" or id) or parse ResultString
                w_raw = item.get("Winner")
                if w_raw is not None:
                    wr = str(w_raw).strip()
                    # numeric/letter markers first
                    if wr in ("1", "A", "a"):
                        winner_label = 'A'; method = 'winner_field'
                    elif wr in ("2", "B", "b"):
                        winner_label = 'B'; method = 'winner_field'
                    # winner might contain player id
                    elif pA_id and wr == str(pA_id):
                        winner_label = 'A'; method = 'winner_field_id'
                    elif pB_id and wr == str(pB_id):
                        winner_label = 'B'; method = 'winner_field_id'
                # if still unknown, try to parse ResultString like "K. Madison d S. Aryna 3-6,6-2,5-7"
                if winner_label is None:
                    res = item.get("ResultString") or item.get("ScoreString") or item.get("Score") or item.get("score_string")
                    parsed = winner_from_resultstring(res, pA_name, pB_name)
                    if parsed == 'A':
                        winner_label = 'A'; method = 'resultstring'
                    elif parsed == 'B':
                        winner_label = 'B'; method = 'resultstring'
                    else:
                        # last-resort heuristic: if Winner exists but is neither 1/2 nor id,
                        # maybe winner is given as competitor index (3 etc.) -> check if it equals PlayerIDA/PlayerIDB
                        if w_raw is not None:
                            wr = str(w_raw).strip()
                            if pA_id and wr == pA_id:
                                winner_label = 'A'; method = 'winner_field_id_heuristic'
                            elif pB_id and wr == pB_id:
                                winner_label = 'B'; method = 'winner_field_id_heuristic'

            # assign canonical winner/loser names (A/B are the canonical playerA/playerB determined earlier)
            if winner_label == 'A':
                winner_name = pA_name; loser_name = pB_name
                winner_seed = pA_seed; loser_seed = pB_seed
                winner_country = pA_country; loser_country = pB_country
            elif winner_label == 'B':
                winner_name = pB_name; loser_name = pA_name
                winner_seed = pB_seed; loser_seed = pA_seed
                winner_country = pB_country; loser_country = pA_country
            else:
                winner_name = None; loser_name = None
                winner_seed = None; loser_seed = None
                winner_country = None; loser_country = None

            row = {
                **tournament_info,
                "Tournament ID": tournament_id,
                "Match ID": match_id,
                "Date": date,
                "Round Name": round_name,
                "Set 1 Score": set1,
                "Set 2 Score": set2,
                "Set 3 Score": set3,
                "Home Player": pA_name,
                "Home Country": pA_country,
                "Home Seed": pA_seed,
                "Away Player": pB_name,
                "Away Country": pB_country,
                "Away Seed": pB_seed,
                "ScoreString": item.get("ScoreString") or item.get("ResultString"),
                "WinnerFlag": item.get("Winner"),
                "winner_flag": winner_label,
                "winner_player_name": winner_name,
                "loser_player_name": loser_name,
                "winner_seed": winner_seed,
                "loser_seed": loser_seed,
                "winner_country": winner_country,
                "loser_country": loser_country,
            }

            # copy per-set stats and IDs (existing logic)
            for k, v in item.items():
                kl = str(k).lower()
                if ("set" in kl and ("score" in kl or "ace" in kl or "tb" in kl or "pt" in kl or "serv" in kl or "dbl" in kl or "break" in kl or "servgames" in kl or "pts" in kl)):
                    row[k] = v
                if k in ("PlayerIDA","PlayerIDB","PlayerCountryA","PlayerCountryB","SeedA","SeedB"):
                    row[k] = v

            rows.append(row)
            continue


        # fallback: unknown
        fallback_row = dict(tournament_info)
        fallback_row["Tournament ID"] = tournament_id
        fallback_row["raw_item"] = item
        rows.append(fallback_row)

    df = pd.DataFrame(rows)
    if 'indoor_outdoor' not in df.columns:
        df['indoor_outdoor'] = None
    if verbose:
        print(f"Processed {len(df)} matches into dataframe.")
    return df
