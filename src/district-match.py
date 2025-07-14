"""
district_checker.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Validate that every district in data/state-district-code.csv has a matching
CSV inside data/marriage_muhurats/<state>/.

• Handles state-name variants via ALIAS_MAP and fuzzy matching (≥ 0.80).
• Flags district-level close matches (≥ 0.70).
• Writes two CSVs:
      1) district_check_report.csv  – all issues (⚠️ + ❌)
      2) mismatch-districts.csv     – only ❌ rows plus their closest guess
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Run:
    python src/district_checker.py          # from the repo root
"""

from pathlib import Path
from difflib import SequenceMatcher
import pandas as pd
import sys

# ── paths ──────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent
DATA_DIR   = REPO_ROOT / "data"

ROOT   = DATA_DIR / "marriage_muhurats"           # state folders
MASTER = DATA_DIR / "state-district-code.csv"     # reference list

# ── thresholds & aliases ───────────────────────────────────────────────────
STATE_FUZZY_THRESHOLD    = 0.80      # ≥ 80 % similarity to accept state name
DISTRICT_FUZZY_THRESHOLD = 0.70      # ≥ 70 % similarity for close district

# known variants → canonical folder name
ALIAS_MAP = {
    "A&N Island": "Andaman and Nicobar",
    "Andra Pradesh": "Andhra Pradesh",
    "Uttrakhand": "Uttarakhand",
    # add more as needed
}

# ── helpers ────────────────────────────────────────────────────────────────
def canonical(text: str) -> str:
    """Lower-case, replace ‘&’→‘and’, collapse whitespace."""
    return " ".join(str(text).lower().replace("&", "and").split())

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, canonical(a), canonical(b)).ratio()

def pick_column(df: pd.DataFrame, want: str) -> str:
    cols = [c for c in df.columns if want in c.lower() and "name" in c.lower()]
    if not cols:
        sys.exit(f"❌ Column containing '{want} name' not found in {MASTER}. "
                 f"Available: {', '.join(df.columns)}")
    return cols[0]

# ── load master list ───────────────────────────────────────────────────────
ref_df = pd.read_csv(MASTER, dtype=str).fillna("")

col_state    = pick_column(ref_df, "state")
col_district = pick_column(ref_df, "district")

# ── index the filesystem ───────────────────────────────────────────────────
present = {d.name: [p.stem for p in d.glob("*.csv")]
           for d in ROOT.glob("*") if d.is_dir()}

# ── comparison ─────────────────────────────────────────────────────────────
records          = []   # all issues
mismatch_records = []   # only ❌ missing district rows

for _, row in ref_df.iterrows():
    st_raw  = row[col_state].strip()
    dist_raw = row[col_district].strip()
    if not st_raw or not dist_raw:
        continue  # skip blanks

    # ── resolve state folder ───────────────────────────────────────────────
    st_folder = (
        ALIAS_MAP.get(st_raw) or                     # alias
        (st_raw if st_raw in present else None)      # exact
    )

    if not st_folder:
        # fuzzy find best state
        best_state = max(present.keys(),
                         key=lambda s: similarity(st_raw, s),
                         default=None)
        if best_state and similarity(st_raw, best_state) >= STATE_FUZZY_THRESHOLD:
            st_folder = best_state

    if not st_folder:
        records.append({
            "state": st_raw,
            "district_expected": dist_raw,
            "status": "❌ missing state folder",
            "matched_file": "",
            "similarity": ""
        })
        continue  # can’t check districts without folder

    # ── check district within state ────────────────────────────────────────
    available = present[st_folder]

    if dist_raw in available:       # exact
        continue

    # always compute best candidate (even if poor)
    if available:
        best_dist  = max(available, key=lambda d: similarity(dist_raw, d))
        best_ratio = similarity(dist_raw, best_dist)
    else:                           # empty folder edge-case
        best_dist, best_ratio = "", 0.0

    if best_ratio >= DISTRICT_FUZZY_THRESHOLD:
        # close enough → ⚠️
        rec = {
            "state": st_raw,
            "district_expected": dist_raw,
            "status": "⚠️ close match",
            "matched_file": f"{best_dist}.csv",
            "similarity": f"{best_ratio:.2f}"
        }
        records.append(rec)
    else:
        # still a miss → ❌ & add to mismatch list
        rec = {
            "state": st_raw,
            "district_expected": dist_raw,
            "status": "❌ missing district",
            "matched_file": f"{best_dist}.csv" if best_dist else "",
            "similarity": f"{best_ratio:.2f}" if best_dist else ""
        }
        records.append(rec)
        mismatch_records.append(rec)

# ── write outputs ──────────────────────────────────────────────────────────
pd.DataFrame(records).sort_values(
    ["state", "district_expected"]
).to_csv(REPO_ROOT / "district_check_report.csv", index=False)

pd.DataFrame(mismatch_records).sort_values(
    ["state", "district_expected"]
).to_csv(REPO_ROOT / "mismatch-districts.csv", index=False)

print("✓ district_check_report.csv refreshed")
print("✓ mismatch-districts.csv created – review rows with blank matched_file")
