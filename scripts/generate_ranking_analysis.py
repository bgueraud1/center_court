from __future__ import annotations

import argparse
import json
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


FILE_RE = re.compile(r"data_(\d{4})_(\d{2})_(\d{2})\.csv$")
SELECTED_THRESHOLDS = [1, 2, 3, 5, 10, 16, 20, 32, 50, 100, 250, 1000]
SEVERITY_BANDS = [
    {"label": "very high", "min_z": 2.5},
    {"label": "rather high", "min_z": 1.5},
    {"label": "slightly high", "min_z": 0.5},
    {"label": "normal", "min_z": -0.5, "max_z": 0.5},
    {"label": "slightly low", "max_z": -0.5},
    {"label": "rather low", "max_z": -1.5},
    {"label": "very low", "max_z": -2.5},
]


def progress_iter(iterable, total=None, desc=""):
    try:
        from tqdm.auto import tqdm

        yield from tqdm(iterable, total=total, desc=desc, unit="it")
    except Exception:
        start = time.perf_counter()
        if total is None and hasattr(iterable, "__len__"):
            total = len(iterable)

        for i, item in enumerate(iterable, 1):
            yield item
            if total:
                elapsed = time.perf_counter() - start
                pct = 100.0 * i / total
                eta = elapsed * (total - i) / max(i, 1)
                print(
                    f"\r{desc} {pct:6.2f}% | elapsed {elapsed:8.1f}s | ETA {eta:8.1f}s",
                    end="",
                    flush=True,
                )
        if total:
            print()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def json_default(obj: Any):
    if isinstance(obj, (pd.Timestamp, datetime, date)):
        return obj.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, (np.ndarray,)):
        return obj.tolist()
    if pd.isna(obj):
        return None
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def round_nested(obj: Any, decimals: int = 3):
    if isinstance(obj, dict):
        return {k: round_nested(v, decimals=decimals) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_nested(v, decimals=decimals) for v in obj]
    if isinstance(obj, tuple):
        return [round_nested(v, decimals=decimals) for v in obj]
    if isinstance(obj, np.ndarray):
        return round_nested(obj.tolist(), decimals=decimals)
    if isinstance(obj, (float, np.floating)):
        if not np.isfinite(obj):
            return None
        return round(float(obj), decimals)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    return obj


def normalize_name(name: str) -> str:
    return " ".join(str(name).strip().lower().split())


def to_str_or_none(v):
    if pd.isna(v) or v == "":
        return None
    return str(v).strip()


def build_compact_rank_grid(max_rank: int) -> np.ndarray:
    """
    Compact rank grid that keeps the JSON reasonably small while preserving detail
    where it matters most.

    Default density:
      1..150   step 1
      151..300 step 2
      301..500 step 5
      501..1000 step 10
    """
    parts = [np.arange(1, min(150, max_rank) + 1, 1, dtype=int)]

    if max_rank > 150:
        parts.append(np.arange(151, min(300, max_rank) + 1, 2, dtype=int))

    if max_rank > 300:
        parts.append(np.arange(301, min(500, max_rank) + 1, 5, dtype=int))

    if max_rank > 500:
        parts.append(np.arange(501, max_rank + 1, 10, dtype=int))

    grid = np.unique(np.concatenate(parts))
    for t in SELECTED_THRESHOLDS:
        if t <= max_rank:
            grid = np.unique(np.append(grid, t))
    return grid.astype(int)


def load_tour_rankings(folder: str | Path, tour: str) -> pd.DataFrame:
    folder = Path(folder)
    files = sorted(folder.glob("data_*.csv"))
    if not files:
        raise ValueError(f"No data_YYYY_MM_DD.csv files found in {folder}")

    frames = []
    for path in progress_iter(files, total=len(files), desc=f"Reading {tour.upper()} files"):
        m = FILE_RE.match(path.name)
        if not m:
            continue

        df = pd.read_csv(path)
        required = {"full_name", "ranking", "points", "date"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"{path.name}: missing columns {missing}")

        df = df.copy()
        df["tour"] = tour
        df["source_file"] = path.name
        df["file_date"] = pd.Timestamp(f"{m.group(1)}-{m.group(2)}-{m.group(3)}").date()

        if "player_id" not in df.columns:
            df["player_id"] = None
        if "movement" not in df.columns:
            df["movement"] = None

        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["ranking"] = pd.to_numeric(df["ranking"], errors="coerce")
        df["points"] = pd.to_numeric(df["points"], errors="coerce")
        df["movement"] = pd.to_numeric(df["movement"], errors="coerce")

        df["player_id"] = df["player_id"].apply(to_str_or_none)
        df["full_name"] = df["full_name"].astype(str)
        df["player_key"] = df.apply(
            lambda r: r["player_id"] if r["player_id"] else normalize_name(r["full_name"]),
            axis=1,
        )

        frames.append(df)

    if not frames:
        raise ValueError(f"No readable CSV files found in {folder}")

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["date", "ranking", "points"]).copy()
    out["ranking"] = out["ranking"].astype(int)
    return out


def robust_mad_scale(x: np.ndarray) -> float:
    x = np.asarray(x, dtype=float)
    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    scale = 1.4826 * mad
    if not np.isfinite(scale) or scale <= 1e-12:
        scale = np.nanstd(x, ddof=0)
    if not np.isfinite(scale) or scale <= 1e-12:
        scale = 1.0
    return float(scale)


def robust_z(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    med = np.nanmedian(x)
    scale = robust_mad_scale(x)
    return (x - med) / scale


def filter_absurd_outliers(
    df: pd.DataFrame,
    local_window: int = 5,
    local_z_threshold: float = 8.0,
    global_z_threshold: float = 8.0,
) -> pd.DataFrame:
    """
    Removes only clearly absurd outliers, not normal extremes.
    The intent is to remove data glitches, not meaningful real-world extremes.
    """
    df = df.copy().reset_index(drop=True)
    df["log_points"] = np.log1p(df["points"].astype(float))
    keep_mask = np.ones(len(df), dtype=bool)

    grouped = df.groupby(["tour", "date"], sort=True)
    for (_, _), wk in grouped:
        s = wk.sort_values("ranking").copy()
        pos = s.index.to_numpy()
        lp = s["log_points"].to_numpy(dtype=float)

        global_z_vals = robust_z(lp)

        local_med = (
            pd.Series(lp)
            .rolling(window=local_window, center=True, min_periods=1)
            .median()
            .to_numpy(dtype=float)
        )
        residual = lp - local_med
        local_z_vals = residual / robust_mad_scale(residual)

        absurd = (np.abs(global_z_vals) >= global_z_threshold) & (
            np.abs(local_z_vals) >= local_z_threshold
        )

        keep_mask[pos] = ~absurd

    filtered = df.loc[keep_mask].copy()
    filtered = filtered.drop(columns=["log_points"], errors="ignore")
    return filtered


def add_relative_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a per-date relative level based on robust z-scores of log1p(points).
    """
    df = df.copy()
    df["log_points"] = np.log1p(df["points"].astype(float))
    df["level"] = df.groupby(["tour", "date"])["log_points"].transform(
        lambda s: pd.Series(robust_z(s.to_numpy()), index=s.index)
    )
    return df


def interpolate_week_curve(week_df: pd.DataFrame, rank_grid: np.ndarray) -> np.ndarray:
    wk = week_df.sort_values("ranking")
    x = wk["ranking"].to_numpy(dtype=float)
    y = wk["level"].to_numpy(dtype=float)

    x_unique, idx = np.unique(x, return_index=True)
    y_unique = y[idx]
    rank_grid = np.asarray(rank_grid, dtype=float)

    if len(x_unique) == 1:
        return np.full_like(rank_grid, y_unique[0], dtype=float)

    return np.interp(rank_grid, x_unique, y_unique, left=np.nan, right=np.nan)


def build_weekly_matrix(df: pd.DataFrame, rank_grid: np.ndarray) -> tuple[pd.DatetimeIndex, np.ndarray]:
    dates = []
    curves = []

    grouped = list(df.groupby("date"))
    for d, wk in progress_iter(grouped, total=len(grouped), desc="Building weekly curves"):
        dates.append(pd.Timestamp(d))
        curves.append(interpolate_week_curve(wk, rank_grid))

    if not curves:
        return pd.DatetimeIndex([]), np.empty((0, len(rank_grid)), dtype=float)

    return pd.to_datetime(dates), np.vstack(curves)


def _nanquantile(mat: np.ndarray, q: float, axis: int = 0) -> np.ndarray:
    if mat.size == 0:
        return np.array([])
    return np.nanquantile(mat, q, axis=axis)


def summarize_matrix(mat: np.ndarray, rank_grid: np.ndarray) -> dict[str, Any]:
    if mat.size == 0:
        return {
            "ranks": rank_grid.tolist(),
            "mean": [],
            "median": [],
            "std": [],
            "n": [],
            "ci95_low": [],
            "ci95_high": [],
            "min": [],
            "max": [],
            "q10": [],
            "q25": [],
            "q75": [],
            "q90": [],
        }

    mean = np.nanmean(mat, axis=0)
    median = np.nanmedian(mat, axis=0)
    std = np.nanstd(mat, axis=0, ddof=1)
    n = np.sum(np.isfinite(mat), axis=0)

    se = np.divide(std, np.sqrt(n), out=np.full_like(std, np.nan), where=n > 1)
    ci_low = mean - 1.96 * se
    ci_high = mean + 1.96 * se
    mn = np.nanmin(mat, axis=0)
    mx = np.nanmax(mat, axis=0)

    return {
        "ranks": rank_grid.tolist(),
        "mean": mean.tolist(),
        "median": median.tolist(),
        "std": std.tolist(),
        "n": n.astype(int).tolist(),
        "ci95_low": ci_low.tolist(),
        "ci95_high": ci_high.tolist(),
        "min": mn.tolist(),
        "max": mx.tolist(),
        "q10": _nanquantile(mat, 0.10).tolist(),
        "q25": _nanquantile(mat, 0.25).tolist(),
        "q75": _nanquantile(mat, 0.75).tolist(),
        "q90": _nanquantile(mat, 0.90).tolist(),
    }


def selected_threshold_series(
    rank_grid: np.ndarray,
    dates: pd.DatetimeIndex,
    mat: np.ndarray,
    thresholds: list[int],
) -> dict[str, Any]:
    rank_to_pos = {int(r): i for i, r in enumerate(rank_grid)}
    series = {}

    for r in thresholds:
        if r in rank_to_pos:
            vals = mat[:, rank_to_pos[r]]
            series[str(r)] = [None if not np.isfinite(v) else round(float(v), 3) for v in vals]

    return {
        "dates": [d.date().isoformat() for d in dates],
        "series": series,
    }


def build_search_index(df: pd.DataFrame) -> dict[str, Any]:
    rows = []
    seen = set()

    for _, r in df[["player_key", "full_name", "player_id", "tour"]].drop_duplicates().iterrows():
        key = str(r["player_key"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "player_key": key,
                "full_name": str(r["full_name"]),
                "player_id": None if pd.isna(r["player_id"]) else str(r["player_id"]),
                "tour": str(r["tour"]),
                "normalized_name": normalize_name(str(r["full_name"])),
            }
        )

    return {"players": rows}


def in_reference_window(d: pd.Timestamp, reference_start: str | None, reference_end: str | None) -> bool:
    if reference_start is not None and d < pd.Timestamp(reference_start):
        return False
    if reference_end is not None and d > pd.Timestamp(reference_end):
        return False
    return True


def nearest_key_thresholds(rank_grid: np.ndarray, key_thresholds: list[int]) -> dict[int, tuple[int | None, int | None]]:
    """
    For each rank in the grid, returns the nearest key threshold strictly above
    and strictly below it, if any.

    Note:
      - "above" means a better rank number (smaller ranking)
      - "below" means a worse rank number (larger ranking)
    """
    keys = np.array(sorted(set(key_thresholds)), dtype=int)
    mapping = {}

    for r in rank_grid.astype(int):
        above = keys[keys < r]
        below = keys[keys > r]

        above_val = int(above.max()) if len(above) else None
        below_val = int(below.min()) if len(below) else None
        mapping[int(r)] = (above_val, below_val)

    return mapping


def compute_rank_profile(rank_grid: np.ndarray, mat: np.ndarray) -> dict[str, Any]:
    """
    Builds rank-level features for the front-end:
      - central tendency / dispersion
      - local slope / curvature
      - plateau score
      - gaps to nearby key thresholds
    """
    summary = summarize_matrix(mat, rank_grid)
    mean = np.array(summary["mean"], dtype=float)
    median = np.array(summary["median"], dtype=float)
    std = np.array(summary["std"], dtype=float)
    mn = np.array(summary["min"], dtype=float)
    mx = np.array(summary["max"], dtype=float)

    x = rank_grid.astype(float)

    # Derivatives on an irregular grid
    slope = np.gradient(mean, x)
    curvature = np.gradient(slope, x)

    # A simple plateau indicator: low slope + low curvature => plateau-like region
    slope_scale = robust_mad_scale(slope[np.isfinite(slope)])
    curv_scale = robust_mad_scale(curvature[np.isfinite(curvature)])

    norm_slope = slope / slope_scale if slope_scale > 0 else slope
    norm_curv = curvature / curv_scale if curv_scale > 0 else curvature

    plateau_score = 1.0 / (1.0 + np.abs(norm_slope) + 0.5 * np.abs(norm_curv))

    # Local volatility around each rank
    local_band_std = []
    half_window = 2
    for i in range(len(mean)):
        lo = max(0, i - half_window)
        hi = min(len(mean), i + half_window + 1)
        local_band_std.append(float(np.nanstd(mean[lo:hi], ddof=0)))
    local_band_std = np.array(local_band_std, dtype=float)

    key_map = nearest_key_thresholds(rank_grid, SELECTED_THRESHOLDS)

    nearest_above = []
    nearest_below = []
    gap_to_above_key = []
    gap_to_below_key = []
    rank_distance_above = []
    rank_distance_below = []

    for i, r in enumerate(rank_grid.astype(int)):
        above_key, below_key = key_map[r]

        nearest_above.append(above_key)
        nearest_below.append(below_key)

        if above_key is None:
            gap_to_above_key.append(None)
            rank_distance_above.append(None)
        else:
            above_val = float(np.interp(above_key, x, mean))
            gap_to_above_key.append(round(float(mean[i] - above_val), 6))
            rank_distance_above.append(int(r - above_key))

        if below_key is None:
            gap_to_below_key.append(None)
            rank_distance_below.append(None)
        else:
            below_val = float(np.interp(below_key, x, mean))
            gap_to_below_key.append(round(float(below_val - mean[i]), 6))
            rank_distance_below.append(int(below_key - r))

    return {
        "ranks": rank_grid.tolist(),
        "mean": mean.tolist(),
        "median": median.tolist(),
        "std": std.tolist(),
        "min": mn.tolist(),
        "max": mx.tolist(),
        "slope": slope.tolist(),
        "curvature": curvature.tolist(),
        "plateau_score": plateau_score.tolist(),
        "local_band_std": local_band_std.tolist(),
        "nearest_key_above": nearest_above,
        "nearest_key_below": nearest_below,
        "rank_distance_to_above_key": rank_distance_above,
        "rank_distance_to_below_key": rank_distance_below,
        "gap_to_above_key_mean": gap_to_above_key,
        "gap_to_below_key_mean": gap_to_below_key,
    }


def build_player_histories(df: pd.DataFrame) -> dict[str, Any]:
    """
    Optional heavy export. Keep it disabled for Git-friendly output.
    """
    histories = {}
    grouped = list(df.groupby("player_key"))

    for player_key, g in progress_iter(grouped, total=len(grouped), desc="Building player histories"):
        g = g.sort_values("date")
        row0 = g.iloc[0]

        histories[str(player_key)] = {
            "player_key": str(player_key),
            "full_name": str(row0["full_name"]),
            "player_id": None if pd.isna(row0.get("player_id", None)) else str(row0["player_id"]),
            "tour": str(row0["tour"]),
            "dates": [pd.Timestamp(d).date().isoformat() for d in g["date"]],
            "rankings": g["ranking"].astype(int).tolist(),
            "points": g["points"].astype(float).tolist(),
            "levels": [round(float(x), 3) if np.isfinite(x) else None for x in g["level"].to_numpy()],
            "movement": [
                None if pd.isna(x) else float(x)
                for x in g.get("movement", pd.Series([None] * len(g))).tolist()
            ],
        }

    return histories


def export_tour_json(
    tour: str,
    input_dir: str | Path,
    output_dir: str | Path,
    max_rank: int = 1000,
    reference_start: str | None = None,
    reference_end: str | None = None,
    local_window: int = 5,
    local_z_threshold: float = 8.0,
    global_z_threshold: float = 8.0,
    include_level_matrix: bool = True,
    include_player_histories: bool = False,
    decimals: int = 3,
) -> Path:
    input_dir = Path(input_dir)
    output_dir = ensure_dir(Path(output_dir))
    tour_output_dir = ensure_dir(output_dir / tour)

    df = load_tour_rankings(input_dir, tour=tour)
    df = filter_absurd_outliers(
        df,
        local_window=local_window,
        local_z_threshold=local_z_threshold,
        global_z_threshold=global_z_threshold,
    )
    df = add_relative_level(df)

    if reference_start or reference_end:
        ref_df = df[
            df["date"].apply(lambda d: in_reference_window(pd.Timestamp(d), reference_start, reference_end))
        ].copy()
    else:
        ref_df = df.copy()

    if ref_df.empty:
        raise ValueError(f"No rows left in the reference window for {tour.upper()}")

    rank_cap = int(min(max_rank, ref_df["ranking"].max(), df["ranking"].max()))
    rank_grid = build_compact_rank_grid(rank_cap)

    dates, mat = build_weekly_matrix(df, rank_grid)
    ref_dates, ref_mat = build_weekly_matrix(ref_df, rank_grid)

    # Useful default date for the UI
    latest_date = None if len(dates) == 0 else dates.max().date().isoformat()

    payload = {
    "meta": {
        "tour": tour,
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "input_dir": str(input_dir),
        "latest_analysis_date": latest_date,
        "max_rank": rank_cap,
        "selected_thresholds": SELECTED_THRESHOLDS,
        "severity_bands": SEVERITY_BANDS,
        "reference_window": {
            "start": reference_start,
            "end": reference_end,
        },
        "outlier_filter": {
            "type": "absurd-outlier-suppression",
            "local_window": local_window,
            "local_z_threshold": local_z_threshold,
            "global_z_threshold": global_z_threshold,
        },
        "compact_grid": {
            "enabled": True,
            "definition": [
                {"start": 1, "end": 150, "step": 1},
                {"start": 151, "end": 300, "step": 2},
                {"start": 301, "end": 500, "step": 5},
                {"start": 501, "end": 1000, "step": 10},
            ],
        },
        "latest_ranking_json": f"tools/latest_{tour}_ranking.json",
    },
    "weekly": {
        "dates": [d.date().isoformat() for d in dates],
        "rank_grid": rank_grid.tolist(),
        "level_matrix": round_nested(mat.tolist(), decimals=decimals),
        "reference_level_matrix": round_nested(ref_mat.tolist(), decimals=decimals),
        "selected_thresholds": selected_threshold_series(rank_grid, dates, mat, SELECTED_THRESHOLDS),
        "summary_all_weeks": summarize_matrix(mat, rank_grid),
        "summary_reference_window": summarize_matrix(ref_mat, rank_grid),
    },
    "rank_profile": compute_rank_profile(rank_grid, mat),
    "search_index": build_search_index(df),
}
    # Keep this on for the HTML if you need direct curve reconstruction.
    if include_level_matrix:
        payload["weekly"]["level_matrix"] = round_nested(mat.tolist(), decimals=decimals)
        payload["weekly"]["reference_level_matrix"] = round_nested(ref_mat.tolist(), decimals=decimals)

    # Disable for Git by default; turn on only for debugging or offline analysis.
    if include_player_histories:
        payload["players"] = build_player_histories(df)

    output_path = tour_output_dir / "analysis.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(
            round_nested(payload, decimals=decimals),
            f,
            ensure_ascii=False,
            separators=(",", ":"),
            default=json_default,
        )

    return output_path


def build_all(
    atp_dir: str | Path = "atp_rankings",
    wta_dir: str | Path = "wta_rankings",
    output_dir: str | Path = "ranking_analysis",
    max_rank: int = 1000,
    reference_start: str | None = None,
    reference_end: str | None = None,
    include_level_matrix: bool = True,
    include_player_histories: bool = False,
) -> list[Path]:
    outputs = []
    outputs.append(
        export_tour_json(
            tour="atp",
            input_dir=atp_dir,
            output_dir=output_dir,
            max_rank=max_rank,
            reference_start=reference_start,
            reference_end=reference_end,
            include_level_matrix=include_level_matrix,
            include_player_histories=include_player_histories,
        )
    )
    outputs.append(
        export_tour_json(
            tour="wta",
            input_dir=wta_dir,
            output_dir=output_dir,
            max_rank=max_rank,
            reference_start=reference_start,
            reference_end=reference_end,
            include_level_matrix=include_level_matrix,
            include_player_histories=include_player_histories,
        )
    )
    return outputs


def parse_args():
    p = argparse.ArgumentParser(description="Build ATP/WTA ranking analysis JSON files.")
    p.add_argument("--atp-dir", default="atp_rankings", help="Directory containing ATP CSV files.")
    p.add_argument("--wta-dir", default="wta_rankings", help="Directory containing WTA CSV files.")
    p.add_argument("--output-dir", default="ranking_analysis", help="Output directory.")
    p.add_argument("--max-rank", type=int, default=1000, help="Maximum rank included in curves.")
    p.add_argument("--reference-start", default=None, help="Reference window start date (YYYY-MM-DD).")
    p.add_argument("--reference-end", default=None, help="Reference window end date (YYYY-MM-DD).")
    p.add_argument(
        "--include-level-matrix",
        action="store_true",
        help="Export the dense weekly matrix too. Keep this off for smaller Git commits.",
    )
    p.add_argument(
        "--include-player-histories",
        action="store_true",
        help="Export per-player histories too. Keep this off for smaller Git commits.",
    )
    return p.parse_args()


def main():
    args = parse_args()
    outputs = build_all(
        atp_dir=args.atp_dir,
        wta_dir=args.wta_dir,
        output_dir=args.output_dir,
        max_rank=args.max_rank,
        reference_start=args.reference_start,
        reference_end=args.reference_end,
        include_level_matrix=args.include_level_matrix,
        include_player_histories=args.include_player_histories,
    )
    for path in outputs:
        print(path)


if __name__ == "__main__":
    main()