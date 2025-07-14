#!/usr/bin/env python3
"""
rename_to_codes.py
Renames state folders and district CSVs based on state-district-code.csv mapping.
Handles spelling variations, spacing, casing, and unmatched folders/files.
"""

import argparse
import pandas as pd
from pathlib import Path
import re
import sys
from difflib import SequenceMatcher

# ---- alias layer for exact matches ----------------------------------------
ALIASES = {
    "andamanandnicobar": "35",
    "dadraandnagarhavelianddamananddiu": "26",
    "delhi": "07",
    "nationalcapitalterritoryofdelhi": "07",
    "lakshadweep": "31",
    "ladakh": "37",
    "andhrapradesh": "28",
    "andrapradesh": "28",
    "gujarat": "24",
    "jammuandkashmir": "01",
    "uttarakhand": "05",
    "uttrakhand": "05",
    "puducherry": "34",
}

# ---- helper functions ------------------------------------------------------

def normalise(name: str) -> str:
    return re.sub(r'[^a-z0-9]', '', name.lower())

def best_fuzzy_match(key, candidates, threshold=0.8):
    best = None
    best_score = 0
    for cand in candidates:
        score = SequenceMatcher(None, key, cand).ratio()
        if score > best_score:
            best = cand
            best_score = score
    return best if best_score >= threshold else None

def load_codes(csv_path: Path):
    df = pd.read_csv(csv_path).fillna('')
    df['state_code']    = df['state_code'].astype(int).apply(lambda x: f"{x:02}")
    df['district_code'] = df['district_code'].astype(int).apply(lambda x: f"{x:02}")
    state_code_map = {}
    district_code_map = {}
    for _, row in df.iterrows():
        s_norm = normalise(row['state_name'])
        d_norm = normalise(row['district_name'])
        s_code = row['state_code']
        d_code = row['district_code']
        state_code_map.setdefault(s_norm, s_code)
        district_code_map[(s_code, d_norm)] = d_code
    return state_code_map, district_code_map

# ---- main renaming logic ---------------------------------------------------

def rename_everything(root: Path, code_csv: Path, dry_run: bool = False):
    state_map, district_map = load_codes(code_csv)
    unmatched_states = []
    unmatched_districts = {}

    for state_dir in [p for p in root.iterdir() if p.is_dir()]:
        state_key = normalise(state_dir.name)
        state_code = ALIASES.get(state_key)

        if not state_code:
            match_key = best_fuzzy_match(state_key, state_map.keys())
            state_code = state_map.get(match_key) if match_key else None

        if not state_code:
            unmatched_states.append(state_dir.name)
            continue

        target_state_dir = state_dir.with_name(state_code)
        if target_state_dir.exists() and target_state_dir != state_dir:
            print(f"[WARNING] Destination folder {target_state_dir} already exists – skipping.")
            continue

        if target_state_dir != state_dir:
            if dry_run:
                print(f"[STATE] Would rename: '{state_dir.name}' → '{state_code}'")
            else:
                state_dir.rename(target_state_dir)
                print(f"[STATE] Renamed: '{state_dir.name}' → '{state_code}'")
        else:
            print(f"[STATE] Already numeric: '{state_dir.name}'")

        # Rename district files
        missing_in_this_state = []
        for file in target_state_dir.glob("*.csv"):
            dist_key = normalise(file.stem)
            dist_code = district_map.get((state_code, dist_key))
            if not dist_code:
                missing_in_this_state.append(file.name)
                continue
            new_file = file.with_name(f"{dist_code}.csv")
            if new_file.exists() and new_file != file:
                print(f"    [WARNING] Destination file {new_file.name} exists – skipping.")
                continue
            if new_file != file:
                if dry_run:
                    print(f"    [DIST] Would rename: '{file.name}' → '{dist_code}.csv'")
                else:
                    file.rename(new_file)
                    print(f"    [DIST] Renamed: '{file.name}' → '{dist_code}.csv'")
            else:
                print(f"    [DIST] Already numeric: '{file.name}'")

        if missing_in_this_state:
            unmatched_districts[state_code] = missing_in_this_state

    # ---- summary
    print("\n=== SUMMARY =============================================")
    if unmatched_states:
        print("States not found in codebook:")
        for s in unmatched_states:
            print(f"  • {s}")
    else:
        print("✓ All state folders matched")

    if unmatched_districts:
        print("\nDistricts without a match:")
        for s_code, files in unmatched_districts.items():
            for fn in files:
                print(f"  • {s_code}/{fn}")
    else:
        print("✓ Every district CSV matched")

# ---- CLI Entrypoint --------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Rename state folders and district CSVs to their numeric codes.")
    parser.add_argument("--root", default="data/marriage_muhurats", help="Root directory containing state folders")
    parser.add_argument("--codes", default="data/state-district-code.csv", help="CSV file with the codes mapping")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without renaming anything")
    args = parser.parse_args()

    root_path = Path(args.root).expanduser().resolve()
    codes_path = Path(args.codes).expanduser().resolve()

    if not root_path.exists():
        sys.exit(f"❌ Root directory '{root_path}' does not exist")
    if not codes_path.exists():
        sys.exit(f"❌ Codes CSV '{codes_path}' does not exist")

    rename_everything(root_path, codes_path, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
