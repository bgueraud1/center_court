from __future__ import annotations

from pathlib import Path
from datetime import datetime, date
import pandas as pd

STATE_PATH = Path("docs/tools/last_scrape_date.csv")
DEFAULT_ROW = {"atp": "1900-01-01", "wta": "1900-01-01"}

def today_paris() -> date:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Paris")).date()
    except Exception:
        return datetime.now().date()

def load_scrape_state(path: Path = STATE_PATH) -> dict[str, str]:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([DEFAULT_ROW]).to_csv(path, index=False)

    df = pd.read_csv(path, dtype=str).fillna("")
    if df.empty:
        return DEFAULT_ROW.copy()

    row = df.iloc[0].to_dict()
    return {
        "atp": row.get("atp") or DEFAULT_ROW["atp"],
        "wta": row.get("wta") or DEFAULT_ROW["wta"],
    }

def save_scrape_state(atp: str | None = None, wta: str | None = None, path: Path = STATE_PATH) -> None:
    state = load_scrape_state(path)
    if atp is not None:
        state["atp"] = atp
    if wta is not None:
        state["wta"] = wta

    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([state]).to_csv(path, index=False)