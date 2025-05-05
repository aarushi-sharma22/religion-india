#!/usr/bin/env python3

import csv
import re
import requests
from urllib.parse import quote

# Input and output paths
INPUT_CSV  = 'data/districts.csv'
OUTPUT_CSV = 'data/districts_geonames.csv'

def canonical(text: str) -> str:
    """
    Strip leading/trailing commas & whitespace, collapse internal whitespace,
    and lower-case everything. E.g. " Malda , " → "malda"
    """
    cleaned = text.strip(" ,")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.lower()

def lookup_geoname(district, state):
    """
    Query DrikPanchang's GeoNames proxy for a given district & state,
    and return only an exact city+state match (or None).
    """
    want_dist  = canonical(district)
    want_state = canonical(state)

    search_term = quote(f"{district}, India")
    url = (
        "https://www.drikpanchang.com/placeholder/ajax/geo/dp-city-search.php"
        f"?search={search_term}"
    )
    print(f"\n🔍 LOOKUP {district}, {state}")
    print(f"   URL: {url}")

    # Fetch and parse JSON response
    resp = requests.get(url, timeout=10)
    try:
        resp_json = resp.json()
    except ValueError:
        print("   ❌ JSON parse error")
        return None

    # Filter for India candidates
    india_candidates = [
        c for c in resp_json.get('geonames', [])
        if canonical(c.get('country', '')) == 'india'
    ]

    # Look for exact match on both city (district) and state
    for cand in india_candidates:
        if (canonical(cand.get('city', ''))  == want_dist and
            canonical(cand.get('state', '')) == want_state):
            print(f"   ✅ SELECTED: {cand['id']} — {cand['city']}, {cand['state']} (exact)")
            return cand['id']

    # no exact match found
    print("   ❌ No exact district+state match")
    return None

def main():
    # Read input districts
    with open(INPUT_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    # Prepare output file (only district, state, geoname_id)
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as fout:
        writer = csv.DictWriter(fout, fieldnames=['district', 'state', 'geoname_id'])
        writer.writeheader()

        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            district = row['district']
            state    = row['state']
            print(f"[{idx}/{total}] {district}, {state}")

            geo_id = lookup_geoname(district, state)
            if geo_id:
                writer.writerow({
                    'district':    district,
                    'state':       state,
                    'geoname_id':  geo_id,
                })
            # otherwise: skip entirely

    print(f"\n✔ Saved exact matches to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()
