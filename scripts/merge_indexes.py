#!/usr/bin/env python3
# scripts/merge_indexes.py
"""
Merge remote and local created_files.txt and matches_index.json.

Usage (example from the workflow):
  python3 scripts/merge_indexes.py \
    --remote-created /tmp/created_files_remote.txt \
    --local-created created_files.txt \
    --remote-index /tmp/matches_index_remote.json \
    --local-index docs/data/tournaments/matches_index.json \
    --out-created created_files.txt \
    --out-index docs/data/tournaments/matches_index.json

Behavior:
 - merges created_files: remote entries first, then local new ones; preserves order and removes duplicates.
 - merges matches_index.json: union of arrays, dedupe by 'file' or 'filename' key.
 - writes merged outputs to out-created and out-index.
"""
import argparse
import json
import sys
from pathlib import Path

def read_lines(path: Path):
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as fh:
            return [ln.rstrip("\n\r") for ln in fh if ln.strip()]
    except Exception:
        return []

def load_json_list(path: Path):
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, list) else []
    except Exception:
        return []

def write_lines(path: Path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for ln in lines:
            fh.write(ln + "\n")

def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)

def merge_created(remote_list, local_list):
    out = []
    seen = set()
    for ln in (remote_list + local_list):
        if ln not in seen and ln:
            seen.add(ln)
            out.append(ln)
    return out

def key_for_obj(o):
    if not isinstance(o, dict):
        return json.dumps(o, sort_keys=True)
    if "file" in o:
        return o.get("file")
    if "filename" in o:
        return o.get("filename")
    # fallback stable serialization
    return json.dumps(o, sort_keys=True)

def merge_index(remote_idx, local_idx):
    out = []
    seen = set()
    for o in (remote_idx + local_idx):
        k = key_for_obj(o)
        if k not in seen:
            seen.add(k)
            out.append(o)
    return out

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--remote-created", required=True, help="remote created_files (extracted via git show)")
    p.add_argument("--local-created", required=True, help="local created_files.txt path")
    p.add_argument("--remote-index", required=True, help="remote matches_index.json (extracted via git show)")
    p.add_argument("--local-index", required=True, help="local matches_index.json path")
    p.add_argument("--out-created", required=True, help="output created_files path (will overwrite)")
    p.add_argument("--out-index", required=True, help="output matches_index path (will overwrite)")
    args = p.parse_args()

    remote_created = Path(args.remote_created)
    local_created = Path(args.local_created)
    remote_index = Path(args.remote_index)
    local_index = Path(args.local_index)
    out_created = Path(args.out_created)
    out_index = Path(args.out_index)

    r_created = read_lines(remote_created)
    l_created = read_lines(local_created)
    merged_created = merge_created(r_created, l_created)
    write_lines(out_created, merged_created)

    r_idx = load_json_list(remote_index)
    l_idx = load_json_list(local_index)
    merged_idx = merge_index(r_idx, l_idx)
    write_json(out_index, merged_idx)

    print(f"Merged created_files -> {out_created} ({len(merged_created)} lines)")
    print(f"Merged index -> {out_index} ({len(merged_idx)} entries)")
    sys.exit(0)

if __name__ == "__main__":
    main()