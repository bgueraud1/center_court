import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests

from scrape_wiki_data import enrich_csv, make_retry_session
from add_ioc_to_player_atp import enrich_country_codes_atp
from add_ioc_to_player import enrich_country_codes as enrich_country_codes_wta


SCHEMAS = {
    "ATP": {
        "base_csv": "player_data_atp.csv",
        "output_csv": "player_data_atp_enriched.csv",
        "ranking_json_candidates": ["latest_atp_ranking.json"],
        "ranking_temp_csv": "data_latest_atp.csv",
        "column_order": [
            "full_name", "player_id", "represented_country", "height_inches", "height_cm",
            "plays", "backhand", "birth_date", "birthplace", "first_appearance",
            "last_appearance", "highest_ranking", "prize_money", "reviewed_player",
            "date_review", "biography", "turned_pro", "retired",
        ],
        "ranking_col": "highest_ranking",
        "birth_date_mode": "ATP",
    },
    "WTA": {
        "base_csv": "player_data_wta.csv",
        "output_csv": "player_data_wta_enriched.csv",
        "ranking_json_candidates": [
            "latest_wta_rankin.json",
            "latest_wta_ranking.json",
        ],
        "ranking_temp_csv": "data_latest_wta.csv",
        "column_order": [
            "height_inches", "height_cm", "plays", "birth_date", "birthplace",
            "player_id", "full_name", "best_rank", "first_appearance", "last_appearance",
            "represented_country", "reviewed_player", "date_review", "biography", "backhand",
        ],
        "ranking_col": "best_rank",
        "birth_date_mode": "WTA",
    },
}


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def pick_existing_path(candidates: Sequence[Path]) -> Optional[Path]:
    for path in candidates:
        if path.exists():
            return path
    return None


def load_latest_rankings(json_path: Path) -> pd.DataFrame:
    with json_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, dict):
        if "data" in raw and isinstance(raw["data"], list):
            raw = raw["data"]
        elif "results" in raw and isinstance(raw["results"], list):
            raw = raw["results"]
        else:
            raise ValueError(f"Unexpected JSON structure in {json_path}")

    if not isinstance(raw, list):
        raise ValueError(f"Unexpected JSON structure in {json_path}")

    df = pd.DataFrame(raw)
    if df.empty:
        raise ValueError(f"No ranking rows found in {json_path}")

    if "player_id" not in df.columns:
        raise ValueError(f"{json_path} must contain a player_id field")
    if "date" not in df.columns:
        raise ValueError(f"{json_path} must contain a date field")

    df["player_id"] = df["player_id"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["player_id"] != ""].copy()
    df = df[pd.notna(df["date"])].copy()

    if df.empty:
        raise ValueError(f"No valid player_id/date rows found in {json_path}")

    return df


def normalize_birth_date(value: object, mode: str) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return clean_text(value)

    if mode.upper() == "ATP":
        return dt.strftime("%Y-%m-%d")

    return dt.strftime("%b %d %Y").replace(" 0", " ")


def make_empty_row(schema: Dict[str, object]) -> Dict[str, str]:
    return {c: "" for c in schema["column_order"]}


def build_new_player_row(row: pd.Series, schema: Dict[str, object], latest_date: str) -> Dict[str, str]:
    mode = schema["birth_date_mode"]
    ranking_col = schema["ranking_col"]

    new_row = make_empty_row(schema)

    full_name = clean_text(row.get("full_name", ""))
    player_id = clean_text(row.get("player_id", ""))
    country = clean_text(row.get("country_name", "")) or clean_text(row.get("country_code", ""))
    ranking = clean_text(row.get("ranking", ""))

    if mode == "ATP":
        new_row.update({
            "full_name": full_name,
            "player_id": player_id,
            "represented_country": country,
            "height_inches": "",
            "height_cm": "",
            "plays": "",
            "backhand": "",
            "birth_date": normalize_birth_date(row.get("birth_date", ""), mode),
            "birthplace": "",
            "first_appearance": latest_date,
            "last_appearance": latest_date,
            "highest_ranking": ranking,
            "prize_money": "",
            "reviewed_player": "False",
            "date_review": "",
            "biography": "",
            "turned_pro": "",
            "retired": "",
        })
    else:
        new_row.update({
            "height_inches": "",
            "height_cm": "",
            "plays": "",
            "birth_date": normalize_birth_date(row.get("birth_date", ""), mode),
            "birthplace": "",
            "player_id": player_id,
            "full_name": full_name,
            "best_rank": ranking,
            "first_appearance": latest_date,
            "last_appearance": latest_date,
            "represented_country": country,
            "reviewed_player": "False",
            "date_review": "",
            "biography": "",
            "backhand": "",
        })

    new_row[ranking_col] = ranking
    return new_row


def ensure_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df


def normalize_player_ids(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def update_last_appearances(master: pd.DataFrame, latest_ids: Iterable[str], latest_date: str) -> pd.DataFrame:
    if "player_id" not in master.columns:
        raise ValueError("Base CSV must contain a player_id column")
    if "last_appearance" not in master.columns:
        master["last_appearance"] = ""

    latest_ids = {str(pid).strip() for pid in latest_ids if str(pid).strip()}
    master["player_id"] = normalize_player_ids(master["player_id"])
    mask = master["player_id"].isin(latest_ids)
    master.loc[mask, "last_appearance"] = latest_date
    return master


def append_missing_players(
    master: pd.DataFrame,
    latest: pd.DataFrame,
    schema: Dict[str, object],
    latest_date: str,
) -> Tuple[pd.DataFrame, List[str]]:
    master = master.copy()
    master["player_id"] = normalize_player_ids(master["player_id"])
    existing_ids = set(master["player_id"].tolist())

    new_rows = []
    new_ids = []

    for _, row in latest.iterrows():
        pid = clean_text(row.get("player_id", ""))
        if not pid or pid in existing_ids:
            continue
        new_rows.append(build_new_player_row(row, schema, latest_date))
        new_ids.append(pid)
        existing_ids.add(pid)

    if new_rows:
        master = pd.concat([master, pd.DataFrame(new_rows)], ignore_index=True)

    return master, new_ids


def merge_non_blank_fields(
    master: pd.DataFrame,
    source: pd.DataFrame,
    key: str = "player_id",
    only_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    if key not in master.columns or key not in source.columns:
        return master

    master = master.copy()
    master[key] = normalize_player_ids(master[key])
    source = source.copy()
    source[key] = normalize_player_ids(source[key])

    if only_columns is None:
        only_columns = [c for c in source.columns if c != key]

    master_index = {pid: idx for idx, pid in zip(master.index, master[key]) if pid}

    for _, row in source.iterrows():
        pid = clean_text(row.get(key, ""))
        if not pid or pid not in master_index:
            continue

        idx = master_index[pid]
        for col in only_columns:
            if col == key or col not in master.columns or col not in source.columns:
                continue

            new_val = clean_text(row.get(col, ""))
            old_val = clean_text(master.at[idx, col])

            if new_val == "":
                continue
            if old_val == "" or old_val != new_val:
                master.at[idx, col] = new_val

    return master


def write_output(df: pd.DataFrame, path: Path, schema: Dict[str, object]) -> None:
    out = df.copy()
    ordered = list(schema["column_order"])
    for col in ordered:
        if col not in out.columns:
            out[col] = ""
    remaining = [c for c in out.columns if c not in ordered]
    out = out[ordered + remaining]
    out.to_csv(path, index=False)


def create_temp_rankings_csv(latest_rankings: pd.DataFrame, temp_dir: Path, filename: str) -> Path:
    out = temp_dir / filename
    tmp = latest_rankings[["date", "player_id"]].copy()
    tmp["date"] = tmp["date"].dt.strftime("%Y-%m-%d")
    tmp.to_csv(out, index=False)
    return out


def run(mode: str) -> None:
    mode = mode.upper().strip()
    if mode not in SCHEMAS:
        raise ValueError("mode must be 'ATP' or 'WTA'")

    schema = SCHEMAS[mode]
    root = repo_root()
    tools_dir = root / "docs" / "tools"

    base_csv = tools_dir / schema["base_csv"]
    output_csv = tools_dir / schema["output_csv"]
    ranking_json = pick_existing_path([tools_dir / p for p in schema["ranking_json_candidates"]])

    if ranking_json is None:
        candidates = ", ".join(schema["ranking_json_candidates"])
        raise FileNotFoundError(f"Unable to find latest ranking JSON. Tried: {candidates}")

    if not base_csv.exists():
        raise FileNotFoundError(f"Base CSV not found: {base_csv}")

    latest = load_latest_rankings(ranking_json)
    latest_date = latest["date"].max().strftime("%Y-%m-%d")
    latest_ids = latest["player_id"].astype(str).str.strip().tolist()

    master = pd.read_csv(base_csv, dtype=str, keep_default_na=False).fillna("")
    master = ensure_columns(master, schema["column_order"])
    master["player_id"] = normalize_player_ids(master["player_id"])

    master = update_last_appearances(master, latest_ids, latest_date)
    master, new_ids = append_missing_players(master, latest, schema, latest_date)

    wiki_session = make_retry_session(
        total_retries=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
    )

    if new_ids:
        print(f"Detected {len(new_ids)} new {mode} players.")
        new_subset = master[master["player_id"].isin(new_ids)].copy()

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            create_temp_rankings_csv(
                latest_rankings=latest,
                temp_dir=tmpdir_path,
                filename=schema["ranking_temp_csv"],
            )

            temp_input = tmpdir_path / f"new_players_{mode.lower()}_input.csv"
            temp_wiki_output = tmpdir_path / f"new_players_{mode.lower()}_wiki.csv"
            temp_ioc_output = tmpdir_path / f"new_players_{mode.lower()}_ioc.csv"
            summary_csv = tmpdir_path / f"overwrite_changes_{mode.lower()}.csv"

            new_subset.to_csv(temp_input, index=False)

            enrich_csv(
                session=wiki_session,
                input_csv=str(temp_input),
                output_csv=str(temp_wiki_output),
                summary_csv=str(summary_csv),
                rankings_dir=str(tmpdir_path),
                mode=mode,
                start_index=0,
                end_index=None,
                overwrite=False,
                min_first_date=None,
            )

            if temp_wiki_output.exists():
                wiki_df = pd.read_csv(temp_wiki_output, dtype=str, keep_default_na=False).fillna("")
                master = merge_non_blank_fields(master, wiki_df)
            else:
                print(f"Warning: wiki output not produced: {temp_wiki_output}")

            ioc_input_df = master[master["player_id"].isin(new_ids)].copy()
            ioc_input_df.to_csv(temp_input, index=False)

            with requests.Session() as sess:
                if mode == "ATP":
                    enrich_country_codes_atp(
                        session=sess,
                        input_csv=str(temp_input),
                        output_csv=str(temp_ioc_output),
                        start_index=0,
                        end_index=None,
                        overwrite=False,
                    )
                else:
                    enrich_country_codes_wta(
                        session=sess,
                        input_csv=str(temp_input),
                        output_csv=str(temp_ioc_output),
                        start_index=0,
                        end_index=None,
                        overwrite=False,
                    )

            if temp_ioc_output.exists():
                ioc_df = pd.read_csv(temp_ioc_output, dtype=str, keep_default_na=False).fillna("")
                master = merge_non_blank_fields(master, ioc_df, only_columns=["represented_country"])
            else:
                print(f"Warning: IOC output not produced: {temp_ioc_output}")

    else:
        print(f"No new {mode} players detected.")

    master = ensure_columns(master, schema["column_order"])
    write_output(master, base_csv, schema)
    write_output(master, output_csv, schema)

    print(f"Base CSV updated   -> {base_csv}")
    print(f"Output CSV written -> {output_csv}")
    print("All done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Update ATP/WTA player base from the latest ranking JSON.")
    parser.add_argument(
        "--mode",
        choices=["ATP", "WTA"],
        default=os.getenv("MODE", "WTA").upper(),
        help="Choose ATP or WTA",
    )
    args = parser.parse_args()
    run(args.mode)


if __name__ == "__main__":
    main()