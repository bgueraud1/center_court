from datetime import date, datetime
import os
from zoneinfo import ZoneInfo


def get_reference_date() -> date:
    """
    Date métier utilisée par le pipeline.

    - En CI avec TARGET_DATE : utilise cette date.
    - Sinon : utilise la date réelle en Europe/Paris.
    """
    target_date = os.environ.get("TARGET_DATE", "").strip()

    if target_date:
        try:
            return datetime.strptime(
                target_date,
                "%Y-%m-%d"
            ).date()
        except ValueError as exc:
            raise ValueError(
                f"Invalid TARGET_DATE={target_date!r}; expected YYYY-MM-DD"
            ) from exc

    return datetime.now(ZoneInfo("Europe/Paris")).date()