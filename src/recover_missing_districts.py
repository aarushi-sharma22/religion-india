#!/usr/bin/env python3
import pandas as pd
from rapidfuzz import process, fuzz

# 1) Load and normalize your “missing” list
missing = pd.read_csv('data/missing_districts.csv')
cols = missing.columns.tolist()
if len(cols) >= 2:
    # assume: first column = state, second = district
    missing = missing.rename(columns={
        cols[0]: 'state_name',
        cols[1]: 'district_name'
    })
else:
    raise RuntimeError("missing_districts.csv must have at least two columns: state and district")

# drop exact duplicates if any
missing = missing.drop_duplicates(['state_name','district_name'])

# 2) Load full GeoNames dump
gn_cols = [
    'geonameid','name','asciiname','alternatenames','latitude','longitude',
    'feature_class','feature_code','country_code','cc2','admin1','admin2',
    'admin3','admin4','population','elevation','dem','timezone','modification'
]
all_gn = pd.read_csv(
    'data/IN.txt', sep='\t',
    names=gn_cols, dtype=str,
    na_filter=False
)

# 3) Build a state-level lookup: ADM1 → code
states = all_gn[all_gn.feature_code=='ADM1'][['admin1','name','alternatenames']]
state_lookup = {}
for _, r in states.iterrows():
    code, nm = r['admin1'], r['name']
    state_lookup[nm] = code
    for alt in r['alternatenames'].split(','):
        if alt: state_lookup[alt] = code
state_names = list(state_lookup.keys())

# 4) Pre-slice the ADM2 (district) set once
all_districts = all_gn[all_gn.feature_code=='ADM2'][[
    'geonameid','name','alternatenames','admin1'
]]

records = []
for _, row in missing.iterrows():
    s_q, d_q = row['state_name'], row['district_name']

    # 4a) Fuzzy-match state → admin1 code
    st_match, st_score, _ = process.extractOne(
        s_q, state_names,
        scorer=fuzz.WRatio,
        score_cutoff= 80
    ) or (None, 0, None)
    st_code = state_lookup.get(st_match)
    
    # 4b) Restrict to that state’s districts (if we found one)
    pool = all_districts
    if st_code:
        pool = pool[pool.admin1 == st_code]
    
    # build district lookup for that pool
    dist_lookup = {}
    for _, dr in pool.iterrows():
        gid, nm = dr['geonameid'], dr['name']
        dist_lookup[nm] = gid
        for alt in dr['alternatenames'].split(','):
            if alt: dist_lookup[alt] = gid

    # 4c) Fuzzy-match district within that pool
    names = list(dist_lookup.keys())
    d_match, d_score, _ = process.extractOne(
        d_q, names,
        scorer=fuzz.WRatio,
        score_cutoff= 80
    ) or (None, 0, None)
    gid = dist_lookup.get(d_match)

    records.append({
        'state_name':      s_q,
        'matched_state':   st_match,
        'state_score':     st_score,
        'district_name':   d_q,
        'matched_district':d_match,
        'district_score':  d_score,
        'geoname_id':      gid
    })

# 5) Write exactly one row per input
out = pd.DataFrame(records)
out.to_csv('data/missing-districts-recovered.csv', index=False)
print(f"Wrote {len(out)} recovered districts to data/missing-districts-recovered.csv")
