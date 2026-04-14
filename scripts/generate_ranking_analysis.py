from pathlib import Path
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


FILE_RE = re.compile(r"data_(\d{4})_(\d{2})_(\d{2})\.csv$")


def load_rankings(folder: str | Path) -> pd.DataFrame:
    """
    Charge tous les CSV data_YYYY_MM_DD.csv d'un dossier.

    Attendu dans chaque CSV :
        full_name,ranking,points,date

    Retourne un DataFrame concaténé.
    """
    folder = Path(folder)
    frames = []

    for path in sorted(folder.glob("data_*.csv")):
        m = FILE_RE.match(path.name)
        if not m:
            continue

        file_date = pd.Timestamp(f"{m.group(1)}-{m.group(2)}-{m.group(3)}").date()

        df = pd.read_csv(path)
        required = {"full_name", "ranking", "points", "date"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"{path.name} : colonnes manquantes {missing}")

        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ranking"] = pd.to_numeric(df["ranking"], errors="coerce")
        df["points"] = pd.to_numeric(df["points"], errors="coerce")
        df["source_file"] = path.name
        df["file_date"] = file_date

        frames.append(df)

    if not frames:
        raise ValueError("Aucun fichier data_YYYY_MM_DD.csv trouvé.")

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["date", "ranking", "points"])
    out["ranking"] = out["ranking"].astype(int)

    return out


def robust_z_from_array(x: np.ndarray) -> np.ndarray:
    """
    Score robuste:
        z = (log1p(points) - médiane) / (1.4826 * MAD)

    Ce score est calculé semaine par semaine.
    """
    x = np.asarray(x, dtype=float)
    x = np.log1p(x)

    med = np.nanmedian(x)
    mad = np.nanmedian(np.abs(x - med))
    scale = 1.4826 * mad

    if not np.isfinite(scale) or scale <= 1e-12:
        scale = np.nanstd(x, ddof=0)

    if not np.isfinite(scale) or scale <= 1e-12:
        scale = 1.0

    return (x - med) / scale


def add_relative_level(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajoute:
      - log_points
      - niveau : score normalisé par date
    """
    df = df.copy()
    df["log_points"] = np.log1p(df["points"].astype(float))

    df["niveau"] = df.groupby("date")["log_points"].transform(
        lambda s: robust_z_from_array(s.to_numpy())
    )

    return df


def evaluate_week_curve(week_df: pd.DataFrame, rank_grid: np.ndarray) -> np.ndarray:
    """
    Interpole linéairement le niveau en fonction du rang pour une semaine donnée.
    Renvoie NaN hors de la plage observée.
    """
    wk = week_df.sort_values("ranking")
    x = wk["ranking"].to_numpy(dtype=float)
    y = wk["niveau"].to_numpy(dtype=float)

    # Si jamais il y a des doublons de rang, on garde la première occurrence
    x_unique, idx = np.unique(x, return_index=True)
    y_unique = y[idx]

    rank_grid = np.asarray(rank_grid, dtype=float)

    if len(x_unique) == 1:
        return np.full_like(rank_grid, y_unique[0], dtype=float)

    return np.interp(rank_grid, x_unique, y_unique, left=np.nan, right=np.nan)


def build_weekly_matrix(df: pd.DataFrame, rank_grid: np.ndarray):
    """
    Construit une matrice:
        lignes = semaines
        colonnes = rangs
    """
    dates = []
    curves = []

    for d, wk in sorted(df.groupby("date"), key=lambda t: t[0]):
        dates.append(pd.Timestamp(d))
        curves.append(evaluate_week_curve(wk, rank_grid))

    return pd.to_datetime(dates), np.vstack(curves)


def plot_selected_ranks_evolution(
    df: pd.DataFrame,
    selected_ranks=(1, 2, 3, 5, 10, 20, 50, 100),
    outpath: str | Path = "evolution_selected_ranks.png",
):
    """
    Graphique 1:
    évolution du niveau nécessaire pour occuper certains rangs.
    """
    dates = sorted(df["date"].unique())
    xdates = pd.to_datetime(dates)

    fig, ax = plt.subplots(figsize=(13, 6))

    for r in selected_ranks:
        vals = []
        for d in dates:
            wk = df[df["date"] == d]
            v = evaluate_week_curve(wk, np.array([r], dtype=float))[0]
            vals.append(v)

        ax.plot(xdates, vals, marker="o", linewidth=1.5, label=f"Rang {r}")

    ax.set_title("Évolution du niveau nécessaire par rang")
    ax.set_xlabel("Date")
    ax.set_ylabel("Niveau relatif (z robuste sur log(points))")
    ax.grid(True, alpha=0.25)
    ax.legend(ncol=2, frameon=False)
    fig.tight_layout()
    fig.savefig(outpath, dpi=160)
    plt.close(fig)


def plot_curve_for_date(
    df: pd.DataFrame,
    target_date,
    max_rank: int | None = None,
    outpath: str | Path = "curve_for_date.png",
):
    """
    Graphique 2:
    courbe continue niveau vs rang pour une date donnée.
    """
    target_date = pd.to_datetime(target_date).date()
    wk = df[df["date"] == target_date].copy()

    if wk.empty:
        raise ValueError(f"Aucune donnée trouvée pour la date {target_date}.")

    if max_rank is None:
        max_rank = int(wk["ranking"].max())

    rank_grid = np.arange(1, max_rank + 1)
    curve = evaluate_week_curve(wk, rank_grid)

    fig, ax = plt.subplots(figsize=(13, 6))
    ax.plot(rank_grid, curve, linewidth=2, label="Courbe interpolée")
    ax.scatter(wk["ranking"], wk["niveau"], s=14, alpha=0.45, label="Points observés")

    ax.set_title(f"Niveau nécessaire par rang — {target_date}")
    ax.set_xlabel("Rang")
    ax.set_ylabel("Niveau relatif (z robuste sur log(points))")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(outpath, dpi=160)
    plt.close(fig)


def bootstrap_mean_ci(values: np.ndarray, n_boot: int = 1000, alpha: float = 0.05, seed: int = 0):
    """
    Retourne:
      mean, ci_low, ci_high, min, max

    IC 95 % par bootstrap sur la moyenne.
    """
    values = np.asarray(values, dtype=float)
    values = values[np.isfinite(values)]

    if len(values) == 0:
        return np.nan, np.nan, np.nan, np.nan, np.nan

    mean = values.mean()
    mn = values.min()
    mx = values.max()

    rng = np.random.default_rng(seed)
    boot_means = np.empty(n_boot, dtype=float)

    n = len(values)
    for i in range(n_boot):
        sample = rng.choice(values, size=n, replace=True)
        boot_means[i] = sample.mean()

    ci_low = np.quantile(boot_means, alpha / 2)
    ci_high = np.quantile(boot_means, 1 - alpha / 2)

    return mean, ci_low, ci_high, mn, mx


def summarize_by_rank(
    df: pd.DataFrame,
    max_rank: int | None = None,
    n_boot: int = 1000,
):
    """
    Produit, pour chaque rang:
      - moyenne
      - IC 95 % bootstrap
      - min / max
    sur l'ensemble des semaines.
    """
    if max_rank is None:
        max_rank = int(df["ranking"].max())

    rank_grid = np.arange(1, max_rank + 1, dtype=float)
    _, mat = build_weekly_matrix(df, rank_grid)

    rows = []
    for i, r in enumerate(rank_grid.astype(int)):
        vals = mat[:, i]
        mean, ci_low, ci_high, mn, mx = bootstrap_mean_ci(vals, n_boot=n_boot)
        rows.append(
            {
                "ranking": r,
                "mean": mean,
                "ci_low": ci_low,
                "ci_high": ci_high,
                "min": mn,
                "max": mx,
            }
        )

    return pd.DataFrame(rows)


def plot_rank_summary(
    summary_df: pd.DataFrame,
    outpath: str | Path = "summary_by_rank.png",
):
    """
    Graphique 3:
    moyenne, IC 95 %, min/max par rang.
    """
    fig, ax = plt.subplots(figsize=(13, 6))

    x = summary_df["ranking"].to_numpy()
    mean = summary_df["mean"].to_numpy()
    lo = summary_df["ci_low"].to_numpy()
    hi = summary_df["ci_high"].to_numpy()
    mn = summary_df["min"].to_numpy()
    mx = summary_df["max"].to_numpy()

    ax.fill_between(x, mn, mx, alpha=0.12, label="Extrêmes (min / max)")
    ax.fill_between(x, lo, hi, alpha=0.25, label="IC 95 %")
    ax.plot(x, mean, linewidth=2, label="Moyenne")

    ax.set_title("Niveau nécessaire par rang — résumé sur toutes les semaines")
    ax.set_xlabel("Rang")
    ax.set_ylabel("Niveau relatif (z robuste sur log(points))")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(outpath, dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    # Exemple d'utilisation
    folder = "atp_rankings"

    df = load_rankings(folder)
    df = add_relative_level(df)

    # 1) évolution de quelques rangs cibles
    plot_selected_ranks_evolution(
        df,
        selected_ranks=(1, 2, 3, 5, 10, 20, 50, 100),
        outpath="evolution_selected_ranks.png",
    )

    # 2) courbe continue pour une date précise
    plot_curve_for_date(
        df,
        target_date="2026-04-06",
        max_rank=100,  # adapte si tu veux plus de rangs
        outpath="curve_for_2026-04-06.png",
    )

    # 3) moyenne / IC / extrêmes par rang
    summary = summarize_by_rank(df, max_rank=100, n_boot=1000)
    plot_rank_summary(summary, outpath="summary_by_rank.png")

    # Si tu veux récupérer le tableau résumé:
    summary.to_csv("summary_by_rank.csv", index=False)