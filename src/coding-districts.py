#!/usr/bin/env python3
"""
coding-districts.py – v3.3 (Py3.8+ compatible)
──────────────────────────────────────────────
Labels Auspicious_date as:
  • 1         if the ISO date appears in the district file
  • 0         if the district file exists but the date is absent
  • "missing" if the district file itself is absent

Includes:
  • --diagnose deep checks (hit-rates, year coverage, sorted spot-checks)
  • a minimal sanity check (optionally strict)
  • optional codebook validation to flag unknown (state,district) pairs
"""

import argparse, re, sys, random
from collections import Counter
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

# ─── helpers ────────────────────────────────────────────────────────────
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June",
     "July","August","September","October","November","December"], 1)}

def month_to_int(m):
    if pd.isna(m): return None
    s=str(m).strip()
    return int(s) if s.isdigit() else _MONTHS.get(s.lower())

def iso_date(y,m,d):
    try:
        y=int(float(y)); m=month_to_int(m); d=int(float(d))
        if 1<=m<=12 and 1<=d<=31: return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        pass
    return None

def pad(code):
    try:
        return f"{int(code):02}"
    except Exception:
        return None

def _find(df,pattern):
    return next((c for c in df.columns if re.search(pattern,c,re.I)), None)

def to_iso_series(df):
    """Return a Series of YYYY-MM-DD strings (or 'NaT'/None when not parseable)."""
    y=_find(df,r'year'); m=_find(df,r'month'); d=_find(df,r'(^day$|date_)')
    if y and m and d:
        return df.apply(lambda r: iso_date(r[y],r[m],r[d]), axis=1)
    col=_find(df,r'date') or df.columns[0]
    return pd.to_datetime(df[col], errors='coerce').dt.date.astype(str)

# ─── cached district loader ─────────────────────────────────────────────
@lru_cache(maxsize=None)
def load_district_dates(path: Path):
    if not path.exists():
        return None
    df=pd.read_csv(path, dtype=str, low_memory=False)
    return {d for d in to_iso_series(df) if d and d!='NaT'}

# ─── diagnostics helpers ────────────────────────────────────────────────
def warn_unknown_pairs(df, codes_csv: Path):
    """Optional: warn if any (state_code, district_code) pairs are not in the codebook."""
    try:
        cb = pd.read_csv(codes_csv, dtype=str, low_memory=False)
        if not {'state_code','district_code'}.issubset(set(cb.columns)):
            return
        cb = cb[['state_code','district_code']].dropna()
        cb['state_code'] = cb['state_code'].astype(str).str.zfill(2)
        cb['district_code'] = cb['district_code'].astype(str).str.zfill(2)
        valid = set(map(tuple, cb[['state_code','district_code']].values))
        pairs = set(map(tuple, df[['state_code','district_code']].astype(str).values))
        unknown = sorted(pairs - valid)
        if unknown:
            print("\n[5] Unknown (state,district) code pairs found:")
            for st, dt in unknown[:20]:
                print(f"  ? {st}/{dt}")
            if len(unknown) > 20:
                print(f"  ... and {len(unknown)-20} more")
        else:
            print("\n[5] All (state,district) code pairs found in codebook.")
    except Exception:
        # Silent if codebook not readable or columns missing
        pass

# ─── main process & diagnostics ─────────────────────────────────────────
def process(dates_csv: Path, root: Path, diagnose=False, codebook: Optional[Path] = None):
    master=pd.read_csv(dates_csv, dtype=str, low_memory=False)
    col_state=_find(master,r'state'); col_dist=_find(master,r'district')
    if not col_state or not col_dist:
        sys.exit("❌ State / District columns missing in dates file")

    master["state_code"]=master[col_state].apply(pad)
    master["district_code"]=master[col_dist].apply(pad)
    master["iso_date"]=to_iso_series(master)

    auspi=[]; missing=Counter()
    hits=Counter(); total=Counter()

    for st,dt,iso in zip(master.state_code, master.district_code, master.iso_date):
        path=root/st/f"{dt}.csv"
        dateset=load_district_dates(path)
        if dateset is None:
            missing[path]+=1
            auspi.append("missing")                 # ← label when file absent
            continue
        flag = 1 if iso in dateset else 0
        auspi.append(flag)
        total[(st,dt)] += 1
        hits[(st,dt)]  += flag

    master["Auspicious_date"]=auspi

    if diagnose:
        diagnostics(master, hits, total, missing, root, codebook)

    return master, missing

def diagnostics(df, hits, total, missing_paths, root, codebook: Optional[Path]):
    print("\n=== DIAGNOSTICS =========================================")

    # [1] Unparsable dates
    bad = df[df.iso_date.isna() | df.iso_date.eq('NaT')]
    if not bad.empty:
        print(f"\n[1] Rows with unparsable date : {len(bad)}")
        print(bad.head(5).to_string(index=False))
    else:
        print("\n[1] All rows parsed into ISO dates.")

    # [2] Hit-rates across districts with files
    ratios = [(st,dt,hits[(st,dt)], total[(st,dt)],
               hits[(st,dt)]/total[(st,dt)] if total[(st,dt)] else 0.0)
              for (st,dt) in total]
    ratios.sort(key=lambda x:x[4])

    print("\n[2] Lowest 10 hit-rates (districts with a file):")
    for st,dt,h,t,r in ratios[:10]:
        print(f"  {st}/{dt}  {h}/{t}  ({r:.1%})")

    print("\n    Highest 10 hit-rates:")
    for st,dt,h,t,r in ratios[-10:]:
        print(f"  {st}/{dt}  {h}/{t}  ({r:.1%})")

    # [2b] Warnings for near-zero hit-rate despite file present
    print("\n[2b] Warnings: files present but hit-rate < 2% (n>=100):")
    any_warn = False
    for st, dt, h, t, r in ratios:
        if t >= 100 and r < 0.02:
            print(f"  ! {st}/{dt}  {h}/{t}  ({r:.1%})  — check code/date parsing")
            any_warn = True
    if not any_warn:
        print("  (none)")

    # [3] Year coverage
    df['year']=pd.to_datetime(df.iso_date, errors='coerce').dt.year
    is_one = df['Auspicious_date'].astype(str).eq('1')
    is_missing = df['Auspicious_date'].astype(str).eq('missing')
    yr = df.groupby('year').agg(
        size=('Auspicious_date','size'),
        ones=('Auspicious_date', lambda s: int(is_one[s.index].sum())),
        missing=('Auspicious_date', lambda s: int(is_missing[s.index].sum()))
    ).reset_index()

    print("\n[3] Year coverage (#rows / #Auspicious=1 / #missing):")
    for _,row in yr.iterrows():
        y = int(row['year']) if pd.notna(row['year']) else -1
        print(f"  {y} : {int(row.size)} rows , {int(row.ones)} ones , {int(row.missing)} missing")

    # [4] Sorted spot-check with year-span and stable preview
    sample = df[df.Auspicious_date.astype(str).eq('1')]
    sample = sample.sample(min(10, len(sample)), random_state=42)
    print("\n[4] Spot-check 10 rows with Auspicious=1 (min/max year & first 5 dates):")
    for _, r in sample.iterrows():
        path = root / r.state_code / f"{r.district_code}.csv"
        dates = load_district_dates(path)
        if not dates:
            print(f"  {r.state_code}/{r.district_code}  {r.iso_date}  ⇢ file missing")
            continue
        sd = sorted(dates)
        yrs = [int(d[:4]) for d in sd if len(d) >= 4 and d[:4].isdigit()]
        yr_min = min(yrs) if yrs else "?"
        yr_max = max(yrs) if yrs else "?"
        print(f"  {r.state_code}/{r.district_code}  {r.iso_date}  "
              f"⇢ years {yr_min}–{yr_max}; sample: {', '.join(sd[:5])}")

    # [5] Optional: codebook validation
    if codebook is not None:
        warn_unknown_pairs(df, codebook)

# ─── minimal sanity check ───────────────────────────────────────────────
def sanity_check(df: pd.DataFrame, missing_counter: Counter, strict: bool) -> bool:
    ok = True
    print("\n=== SANITY CHECK ========================================")

    allowed = {'0','1','missing'}
    labels = set(df['Auspicious_date'].astype(str).unique())
    bad_labels = labels - allowed
    if bad_labels:
        ok = False
        print(f"[x] Unexpected labels present: {sorted(bad_labels)}")
    else:
        print("[✓] Labels restricted to {'0','1','missing'}")

    nan_count = int(df['Auspicious_date'].isna().sum())
    if nan_count:
        ok = False
        print(f"[x] Nulls in Auspicious_date: {nan_count}")
    else:
        print("[✓] No nulls in Auspicious_date")

    miss_rows = int((df['Auspicious_date'].astype(str)=='missing').sum())
    miss_paths_sum = sum(missing_counter.values())
    if miss_rows != miss_paths_sum:
        print(f"[!] Missing rows: {miss_rows:,} ; paths missing sum: {miss_paths_sum:,} (informational)")
    else:
        print(f"[✓] Missing rows match counted absent-file rows: {miss_rows:,}")

    total=len(df)
    pos = int((df.Auspicious_date.astype(str) == '1').sum())
    miss = miss_rows
    neg = total - pos - miss
    if neg < 0:
        ok = False
        print("[x] Negative count derived for non-auspicious (0).")
    else:
        print(f"[✓] Partition OK → total={total:,} ; 1={pos:,} ; 0={neg:,} ; missing={miss:,}")

    if strict and not ok:
        print("\nSanity check failed (strict mode).")
        sys.exit(1)

    return ok

# ─── CLI ────────────────────────────────────────────────────────────────
def main():
    p=argparse.ArgumentParser(description="Create Auspicious_date with 1/0/'missing' (with diagnostics).")
    p.add_argument("--dates", default="data/dates.csv")
    p.add_argument("--root",  default="data/marriage_muhurats")
    p.add_argument("--out",   default="data/dates-coded.csv")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--diagnose", action="store_true",
                   help="Run deep checks (hit-rates, year histogram, etc.)")
    p.add_argument("--strict", action="store_true",
                   help="Exit non-zero if sanity check fails.")
    p.add_argument("--codebook", type=str, default=None,
                   help="Optional path to state-district-codes.csv for unknown-pair warnings.")
    args = p.parse_args()

    dates_csv=Path(args.dates).expanduser().resolve()
    root     =Path(args.root).expanduser().resolve()
    out_csv  =Path(args.out).expanduser().resolve()
    codebook = Path(args.codebook).expanduser().resolve() if args.codebook else None

    if not dates_csv.exists(): sys.exit(f"❌ Dates file {dates_csv} not found")
    if not root.exists():      sys.exit(f"❌ Data root {root} not found")
    if codebook is not None and not codebook.exists():
        print(f"⚠ Codebook {codebook} not found; skipping unknown-pair warnings.")
        codebook = None

    df, missing = process(dates_csv, root, diagnose=args.diagnose, codebook=codebook)

    total=len(df)
    pos=int((df.Auspicious_date.astype(str) == '1').sum())
    miss=int((df.Auspicious_date.astype(str) == 'missing').sum())
    neg= total - pos - miss
    print("\n=== SUMMARY =============================================")
    print(f"Total rows          : {total:,}")
    print(f"Auspicious (1)      : {pos:,}")
    print(f"Not auspicious (0)  : {neg:,}")
    print(f"Missing             : {miss:,}")
    uniq=len(df[['state_code','district_code']].drop_duplicates())
    print(f"Unique district files referenced : {uniq}")
    print(f"District files missing          : {len(missing)}")
    if missing:
        for path,cnt in missing.most_common():
            st=path.parents[0].name; dt=path.stem
            print(f"  • {st}/{dt}  ({cnt} rows)  – district .csv absent")

    sanity_check(df, missing, strict=args.strict)

    if args.dry_run:
        print("\nDry-run → no file written.")
        return

    out_csv.parent.mkdir(parents=True, exist_ok=True)

    helper_cols = [c for c in ['state_code','district_code','iso_date','year']
                   if c in df.columns]
    df.drop(columns=helper_cols).to_csv(out_csv, index=False)
    print(f"\n✓ Output written to {out_csv}")

if __name__=="__main__":
    main()
