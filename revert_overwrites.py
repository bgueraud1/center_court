#!/usr/bin/env python
import pandas as pd
import sys

def revert_overwrites(
    summary_csv: str,
    enriched_csv: str,
    output_csv: str = None
):
    """
    Reads summary_csv (which must have columns
      row_index, column, old_value, new_value, reject
    and enriched_csv (the file you want to fix).
    Wherever reject == '1', this will set
      enriched_df.at[row_index, column] = old_value.
    Writes the fixed table to output_csv (or overwrites enriched_csv if no output_csv).
    """
    summary = pd.read_csv(summary_csv, dtype={'reject': str})
    # get only the flagged rows
    to_revert = summary[summary['reject'] == '1']
    if to_revert.empty:
        print("No flagged overwrites to revert.")
        return

    df = pd.read_csv(enriched_csv, dtype=str)  # load everything as str to preserve formatting

    # Apply each revert
    for _, row in to_revert.iterrows():
        idx = int(row['row_index'])
        col = row['column']
        old = row['old_value']
        print(f"Reverting row {idx} col “{col}”: {df.at[idx, col]!r} → {old!r}")
        df.at[idx, col] = old

    out = output_csv or enriched_csv
    df.to_csv(out, index=False)
    print(f"Reverted {len(to_revert)} cells. Written corrected file → {out}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python revert_overwrites.py <summary.csv> <enriched.csv> [<fixed_output.csv>]")
        sys.exit(1)
    _, summary_csv, enriched_csv, *rest = sys.argv
    output_csv = rest[0] if rest else enriched_csv
    revert_overwrites(summary_csv, enriched_csv, output_csv)
