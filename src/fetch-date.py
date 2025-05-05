#!/usr/bin/env python3
import time
import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ─── CONFIG ────────────────────────────────────────────────────────────────────

INPUT_CSV  = "data/districts_geonames.csv"
OUTPUT_CSV = "data/marriage_muhurat_1900_2024.csv"
BASE_URL   = "https://www.drikpanchang.com/shubh-dates/shubh-marriage-dates-with-muhurat.html"
HEADERS    = {"User-Agent": "muhurat-scraper/1.0"}

YEARS = range(1900, 2025)
DELAY = 0.2   # seconds between requests

# ─── HELPERS ───────────────────────────────────────────────────────────────────

def fetch_year(geoname_id: str, year: int):
    """
    Fetch the Muhurat page for this geoname_id + year,
    parse all '.dpDayPanchangWrapper' cards, and return
    a list of dicts with the fields we care about.
    """
    params = {"year": year, "geoname-id": geoname_id}
    resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    out = []

    for card in soup.select(".dpDayPanchangWrapper"):
        link = card.select_one("a.dpDayCardTitle")
        if not link:
            continue

        # e.g. "February 2, 2025, Sunday"
        full_text = link.get_text(strip=True)
        date_part, weekday = full_text.rsplit(",", 1)
        month, day, *_ = date_part.split()
        day = day.strip(",")

        # details are pipe-separated in the .dpDayPanchangDetails element
        details_el = card.select_one(".dpDayPanchangDetails")
        parts = details_el.get_text("│", strip=True).split("│") if details_el else []

        out.append({
            "geoname_id": geoname_id,
            "year":       year,
            "month":      month,
            "day":        int(day),
            "weekday":    weekday.strip(),
            "status":     parts[0] if len(parts) > 0 else "",
            "muhurat":    parts[1].replace("Muhurat:", "").strip() if len(parts) > 1 else "",
            "nakshatra":  parts[2].replace("Nakshatra:", "").strip() if len(parts) > 2 else "",
            "tithi":      parts[3].replace("Tithi:", "").strip() if len(parts) > 3 else "",
        })

    return out

# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    # load all districts + geoname_ids
    df = pd.read_csv(INPUT_CSV, dtype=str)

    all_records = []

    for _, row in df.iterrows():
        gid = row.get("geoname_id", "").strip()
        if not gid:
            continue

        print(f"→ Fetching {gid} ({row.get('district','?')}), years {YEARS.start}–{YEARS.stop-1}")
        for year in YEARS:
            try:
                recs = fetch_year(gid, year)
                for r in recs:
                    # bring along your district + state columns if you want
                    r["district"] = row.get("district", "")
                    r["state"]    = row.get("state", "")
                all_records.extend(recs)

                # be polite
                time.sleep(DELAY)
            except Exception as e:
                print(f"   ! error {gid}@{year}: {e}")
                time.sleep(1)

    # dump to CSV
    print(f"\n ← Collected {len(all_records)} records; writing to {OUTPUT_CSV}")
    keys = [
        "district","state","geoname_id","year","month","day","weekday",
        "status","muhurat","nakshatra","tithi"
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_records)

    print("✔ Done.")

if __name__ == "__main__":
    main()
