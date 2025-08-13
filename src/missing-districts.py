#!/usr/bin/env python3
"""
fix_missing_districts.py  – v4
──────────────────────────────
• Renames any textual district CSV to its numeric code
  using:   1) explicit SKIP  → ignore
           2) explicit ALIAS → forced code
           3) fuzzy match    → ≥ threshold (default 0.80)
• When two source files map to the **same** numeric file, rows are merged.

Run
    python fix_missing_districts.py           # dry-run
    python fix_missing_districts.py --apply   # do renames / merges
"""

import argparse, re, sys, csv
from difflib import SequenceMatcher
from pathlib import Path
import pandas as pd

# ── CONFIG ──────────────────────────────────────────────────────────────
DEFAULT_THRESHOLD = 0.80

# 1) files never to touch
SKIP_LIST = {
    ("03", "sahibzadaajitsinghnagar"),   # Punjab, keep as its own file
    ("29", "bangaloreurban"),            # Karnataka, keep separate
    ("22", "korea"),                     # Chhattisgarh, keep separate
}



# 2) explicit alias  (state_code, norm_filename) -> district_code
ALIAS_TO_CODE = {
    # — Jaintia Hills merge (state 17)
    ("17", "eastjaintiahills"): "07",
    ("17", "westjaintiahills"): "07",

    # — Karnataka (state 29)
    ("29", "mysore"):  "20",
    ("29", "mysuru"):  "20",

    # — Maharashtra (state 27) already fixed earlier
    ("27", "raigad"):  "24",

    # — West Bengal (state 19)
    ("19", "coochbehar"):     "03",
    ("19", "howrah"):         "16",
    ("19", "purbabardhaman"): "09",

    # — Puducherry UT (state 34)
    ("34", "karaikal"): "02",
    ("34", "mahe"):     "03",
}

# ── helpers ─────────────────────────────────────────────────────────────
def norm(txt: str) -> str:
    return re.sub(r'[^a-z0-9]', '', txt.lower())

def best_match(name_norm, pool_norm, threshold):
    best, best_score = None, 0.0
    for cand in pool_norm:
        sc = SequenceMatcher(None, name_norm, cand).ratio()
        if sc > best_score:
            best, best_score = cand, sc
    return (best, best_score) if best_score >= threshold else (None, best_score)

def append_csv(src: Path, dest: Path):
    """Append rows from src into dest, skipping header and duplicates."""
    dest_rows = set()
    if dest.exists():
        with dest.open() as f:
            dest_rows.update(line.rstrip('\n') for line in f.readlines()[1:])  # skip header
    with src.open() as f_in, dest.open('a') as f_out:
        reader = csv.reader(f_in)
        header = next(reader)
        if not dest.exists() or dest.stat().st_size == 0:
            f_out.write(','.join(header) + '\n')
        for row in reader:
            line = ','.join(row)
            if line not in dest_rows:
                f_out.write(line + '\n')

# ── load code-book ──────────────────────────────────────────────────────
def load_codes(csv_path: Path):
    df = pd.read_csv(csv_path, dtype=str).fillna('')
    mask = df['state_code'].str.strip().str.isdigit() & df['district_code'].str.strip().str.isdigit()
    df  = df[mask].copy()
    df['state_code']    = df['state_code'].astype(int).apply(lambda x: f"{x:02}")
    df['district_code'] = df['district_code'].astype(int).apply(lambda x: f"{x:02}")

    by_state = {}
    for _, r in df.iterrows():
        by_state.setdefault(r.state_code, []).append(
            (norm(r.district_name), r.district_code, r.district_name)
        )
    return by_state

# ── core renamer / merger ───────────────────────────────────────────────
def fix(root: Path, codes: Path, apply: bool, threshold: float):
    by_state = load_codes(codes)
    actions  = []   # (old_path, new_path, score, label, merge?)

    for state_dir in [p for p in root.iterdir() if p.is_dir()]:
        s_code = state_dir.name
        if s_code not in by_state:                   # human-named folder
            continue

        pool_norm = [t[0] for t in by_state[s_code]]
        pool_map  = {t[0]: t[1:] for t in by_state[s_code]}  # norm → (code,name)

        for csv_file in state_dir.glob("*.csv"):
            if re.fullmatch(r"\d{2}\.csv", csv_file.name):
                continue

            n = norm(csv_file.stem)

            # 1) manual skip
            if (s_code, n) in SKIP_LIST:
                continue

            # 2) manual alias
            if (s_code, n) in ALIAS_TO_CODE:
                d_code      = ALIAS_TO_CODE[(s_code, n)]
                census_name = "alias"
                score       = 1.00
            else:
                # 3) fuzzy
                best, score = best_match(n, pool_norm, threshold)
                if not best:
                    continue
                d_code, census_name = pool_map[best]

            new_path = csv_file.with_name(f"{d_code}.csv")
            merge_flag = new_path.exists()
            actions.append((csv_file, new_path, score, census_name, merge_flag))

    if not actions:
        print("✓ No eligible textual district files left to match.")
        return

    print(f"\nPlanned operations (threshold ≥ {threshold:.2f}):")
    for old, new, sc, name, m in actions:
        op = "merge" if m else "rename"
        print(f"  • {old.parent.name}/{old.name:<25} → {name:<20} "
              f"(score {sc:.2f})  ⇒  {new.name}  ({op})")

    if not apply:
        print("\nDry-run only.  Re-run with --apply to perform these changes.")
        return

    # perform operations
    for old, new, _, _, merge in actions:
        if merge:
            append_csv(old, new)
            old.unlink()
        else:
            old.rename(new)
    print(f"\n✓ Completed {len(actions)} operation(s).")

# ── CLI ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Rename or merge textual district CSVs.")
    ap.add_argument("--root",  default="data/marriage_muhurats")
    ap.add_argument("--codes", default="data/state-district-codes.csv")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    root  = Path(args.root).expanduser().resolve()
    codes = Path(args.codes).expanduser().resolve()
    if not root.exists():  sys.exit(f"❌ root {root} not found")
    if not codes.exists(): sys.exit(f"❌ code-book {codes} not found")

    fix(root, codes, apply=args.apply, threshold=args.threshold)

if __name__ == "__main__":
    main()
