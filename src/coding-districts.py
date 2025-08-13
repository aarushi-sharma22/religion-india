#!/usr/bin/env python3
"""
coding-districts.py – v3.1 (diagnostic edition)
───────────────────────────────────────────────
Adds four deep checks when you pass --diagnose and
writes dates-coded.csv when --dry-run is omitted.
"""

import argparse, re, sys, random
from collections import Counter
from functools import lru_cache
from pathlib import Path

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
    except Exception: pass
    return None

def pad(code):
    try: return f"{int(code):02}"
    except Exception: return None

def _find(df,pattern):
    return next((c for c in df.columns if re.search(pattern,c,re.I)), None)

def to_iso_series(df):
    y=_find(df,r'year'); m=_find(df,r'month'); d=_find(df,r'(^day$|date_)')
    if y and m and d:
        return df.apply(lambda r: iso_date(r[y],r[m],r[d]), axis=1)
    col=_find(df,r'date') or df.columns[0]
    return pd.to_datetime(df[col], errors='coerce').dt.date.astype(str)

# ─── cached district loader ─────────────────────────────────────────────
@lru_cache(maxsize=None)
def load_district_dates(path: Path):
    if not path.exists(): return None
    df=pd.read_csv(path, dtype=str, low_memory=False)
    return {d for d in to_iso_series(df) if d and d!='NaT'}

# ─── main process & diagnostics ─────────────────────────────────────────
def process(dates_csv:Path, root:Path, diagnose=False):
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
            auspi.append(0)
            continue
        flag = 1 if iso in dateset else 0
        auspi.append(flag)
        total[(st,dt)] += 1
        hits[(st,dt)]  += flag

    master["Auspicious_date"]=auspi

    if diagnose:
        diagnostics(master, hits, total, missing, root)

    return master, missing

# ─── diagnostics block ──────────────────────────────────────────────────
def diagnostics(df, hits, total, missing_paths, root):
    print("\n=== DIAGNOSTICS =========================================")

    bad = df[df.iso_date.isna() | df.iso_date.eq('NaT')]
    if not bad.empty:
        print(f"\n[1] Rows with unparsable date : {len(bad)}")
        print(bad.head(5).to_string(index=False))
    else:
        print("\n[1] All rows parsed into ISO dates.")

    ratios = [(st,dt,hits[(st,dt)], total[(st,dt)],
               hits[(st,dt)]/total[(st,dt)] if total[(st,dt)] else 0)
              for (st,dt) in total]
    ratios.sort(key=lambda x:x[4])

    print("\n[2] Lowest 10 hit-rates (districts with a file):")
    for st,dt,h,t,r in ratios[:10]:
        print(f"  {st}/{dt}  {h}/{t}  ({r:.1%})")

    print("\n    Highest 10 hit-rates:")
    for st,dt,h,t,r in ratios[-10:]:
        print(f"  {st}/{dt}  {h}/{t}  ({r:.1%})")

    df['year']=pd.to_datetime(df.iso_date, errors='coerce').dt.year
    yr = df.groupby('year')['Auspicious_date'].agg(['size','sum']).reset_index()
    print("\n[3] Year coverage (#rows / #Auspicious=1):")
    for _,row in yr.iterrows():
        print(f"  {int(row.year)} : {int(row.size)} rows , {int(row['sum'])} ones")

    sample = df[df.Auspicious_date.eq(1)].sample(
        min(10,len(df[df.Auspicious_date.eq(1)])), random_state=42)
    print("\n[4] Spot-check 10 rows with Auspicious=1:")
    for _,r in sample.iterrows():
        path=root/r.state_code/f"{r.district_code}.csv"
        first_dates=list(load_district_dates(path))[:5]
        print(f"  {r.state_code}/{r.district_code}  {r.iso_date}  "
              f"⇢ first dates in file: {', '.join(first_dates)}")

# ─── CLI ────────────────────────────────────────────────────────────────
def main():
    p=argparse.ArgumentParser(description="Create Auspicious_date (with diagnostics).")
    p.add_argument("--dates", default="data/dates.csv")
    p.add_argument("--root",  default="data/marriage_muhurats")
    p.add_argument("--out",   default="data/dates-coded.csv")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--diagnose", action="store_true",
                   help="Run deep checks (hit-rates, year histogram, etc.)")
    args = p.parse_args()

    dates_csv=Path(args.dates).expanduser().resolve()
    root     =Path(args.root).expanduser().resolve()
    out_csv  =Path(args.out).expanduser().resolve()
    if not dates_csv.exists(): sys.exit(f"❌ Dates file {dates_csv} not found")
    if not root.exists():      sys.exit(f"❌ Data root {root} not found")

    df, missing = process(dates_csv, root, diagnose=args.diagnose)

    # summary
    total=len(df); pos=int(df.Auspicious_date.sum())
    print("\n=== SUMMARY =============================================")
    print(f"Total rows          : {total:,}")
    print(f"Auspicious (1)      : {pos:,}")
    print(f"Not auspicious (0)  : {total-pos:,}")
    uniq=len(df[['state_code','district_code']].drop_duplicates())
    print(f"Unique district files referenced : {uniq}")
    print(f"District files missing          : {len(missing)}")
    if missing:
        for path,cnt in missing.most_common():
            st=path.parents[0].name; dt=path.stem
            print(f"  • {st}/{dt}  ({cnt} rows)  – district .csv absent")

    # write
    if args.dry_run:
        print("\nDry-run → no file written.")
        return

    out_csv.parent.mkdir(parents=True, exist_ok=True)

    # ── NEW: drop only the helper columns that actually exist
    helper_cols = [c for c in ['state_code','district_code','iso_date','year']
                   if c in df.columns]
    df.drop(columns=helper_cols).to_csv(out_csv, index=False)
    print(f"\n✓ Output written to {out_csv}")

if __name__=="__main__":
    main()
