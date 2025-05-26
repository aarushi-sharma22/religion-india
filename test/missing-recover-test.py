#!/usr/bin/env python3
import pandas as pd
from rapidfuzz import process, fuzz

# 1) Load inputs
orig_missing = pd.read_csv('data/missing_districts.csv', header=None, dtype=str)
orig_missing.columns = ['state_name','district_name']

recovered = pd.read_csv('data/missing-districts-recovered.csv', dtype=str)

# 2) Load GeoNames dump
gn_cols = [
    'geonameid','name','asciiname','alternatenames','latitude','longitude',
    'feature_class','feature_code','country_code','cc2','admin1','admin2',
    'admin3','admin4','population','elevation','dem','timezone','modification'
]
geo = pd.read_csv('data/IN.txt', sep='\t', names=gn_cols, dtype=str, na_filter=False)

# Build maps: geonameid → admin1 code; and admin1 code → official state name
adm2 = geo.loc[geo.feature_code=='ADM2', ['geonameid','admin1']].astype(str)
adm1 = geo.loc[geo.feature_code=='ADM1', ['admin1','name']].rename(columns={'name':'official_state'}).astype(str)

# 3) Load Wikipedia master list
wiki = pd.read_csv('data/districts.csv', dtype=str)
wiki.columns = ['state_name','district_name']

# Dedupe
orig_missing = orig_missing.drop_duplicates()
recovered    = recovered.drop_duplicates(['state_name','district_name'])
wiki         = wiki.drop_duplicates()

# 4) Existence in GeoNames?
recovered['exists_in_geonames'] = recovered['geoname_id'].isin(geo['geonameid'])

# 5) Correct state–district coupling?
check = (
    recovered
    .merge(adm2, left_on='geoname_id', right_on='geonameid', how='left')
    .merge(adm1, on='admin1', how='left')
)

# normalize the “State of ” / “Union Territory of ” prefix
check['official_state_norm'] = (
    check['official_state']
    .str.replace(r'^(State of |Union Territory of )', '', regex=True)
)

check['state_match'] = check['official_state_norm'] == check['state_name']

# 6) Originally missing?
check['was_missing'] = check.set_index(['state_name','district_name']).index.isin(
    orig_missing.set_index(['state_name','district_name']).index
)

# 7) Present in Wiki list (fuzzy ≥80)?
wiki_map = wiki.groupby('state_name')['district_name'].apply(list).to_dict()
def wiki_check(row):
    pool = wiki_map.get(row['state_name'], [])
    m, score, _ = process.extractOne(
        row['matched_district'], pool, scorer=fuzz.WRatio
    ) or (None, 0, None)
    return (score >= 80), score

wc = check.apply(wiki_check, axis=1, result_type='expand')
check['in_wiki'], check['wiki_score'] = wc[0], wc[1]

# 8) Summarize
total = len(check)
print(f"\n=== OVERALL ({total} recovered) ===")
print(f"Exists in GeoNames DB:     {check['exists_in_geonames'].mean()*100:.1f}%")
print(f"Correct state↔district:    {check['state_match'].mean()*100:.1f}%")
print(f"Originally missing:        {check['was_missing'].mean()*100:.1f}%")
print(f"In Wikipedia list (≥80%):   {check['in_wiki'].mean()*100:.1f}%\n")

# 9) List failures
fail = check[
    (~check['exists_in_geonames']) |
    (~check['state_match'])       |
    (~check['was_missing'])       |
    (~check['in_wiki'])
]
if fail.empty:
    print("✅ All checks passed!")
else:
    print("❌ Issues found in these rows:")
    print(
        fail[
            ['state_name','district_name','matched_district',
             'geoname_id','exists_in_geonames','official_state_norm',
             'state_match','was_missing','in_wiki','wiki_score']
        ]
        .to_string(index=False)
    )
