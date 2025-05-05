#!/usr/bin/env python3
import os
import csv
import time
import requests
from bs4 import BeautifulSoup

# ─── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV   = "data/districts_geonames.csv"
OUTPUT_CSV  = "data/all_marriage_muhurats.csv"
BASE_URL    = (
    "https://www.drikpanchang.com/shubh-dates/"
    "shubh-marriage-dates-with-muhurat.html"
)
START_YEAR  = 1900
END_YEAR    = 2024
DELAY       = 0.2   # seconds between requests

# ─── SESSION SETUP ──────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) "
                       "Gecko/20100101 Firefox/118.0",
    "Accept":          "text/html,application/xhtml+xml,application/xml;"
                       "q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         BASE_URL,
})
# one “warm-up” request to get cookies / tokens
session.get(BASE_URL, timeout=10).raise_for_status()


# ─── FETCH ONE YEAR ────────────────────────────────────────────────────────────
def fetch_year(geoname_id: str, year: int):
    """
    Fetch & parse all Shubh Marriage dates for geoname_id + year.
    """
    params = {"year": year, "geoname-id": geoname_id}
    resp = session.get(BASE_URL, params=params, timeout=10)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    out = []

    for card in soup.select(".dpDayPanchangWrapper"):
        link = card.select_one("a.dpDayCardTitle")
        if not link:
            continue

        # e.g. "February 2, 2025, Sunday"
        full_text = link.get_text(strip=True)
        date_part = full_text.split(",", 1)[0]  # "February 2, 2025"
        month, day, yr = date_part.split()
        day = day.rstrip(",")

        # details, pipe-separated
        details = card.select_one(".dpDayPanchangDetails")
        parts = details.get_text("│", strip=True).split("│") if details else []

        out.append({
            "geoname_id": geoname_id,
            "year":       int(yr),
            "month":      month,
            "day":        int(day),
            "status":     parts[0].strip() if len(parts) > 0 else "",
            "muhurat":    parts[1].replace("Muhurat:", "").strip() if len(parts) > 1 else "",
            "nakshatra":  parts[2].replace("Nakshatra:", "").strip() if len(parts) > 2 else "",
            "tithi":      parts[3].replace("Tithi:", "").strip() if len(parts) > 3 else "",
        })
    return out


# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    # load geoname_ids
    with open(INPUT_CSV, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        districts = [r for r in reader if r.get("geoname_id")]

    # prepare for write vs append
    file_exists = os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0
    mode = "a" if file_exists else "w"

    fieldnames = [
        "district","state","geoname_id",
        "year","month","day",
        "status","muhurat","nakshatra","tithi",
    ]
    with open(OUTPUT_CSV, mode, newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
            fout.flush()
            os.fsync(fout.fileno())

        total = len(districts) * (END_YEAR - START_YEAR + 1)
        counter = 0

        for row in districts:
            geo_id   = row["geoname_id"].strip()
            district = row["district"]
            state    = row["state"]

            for year in range(START_YEAR, END_YEAR + 1):
                counter += 1
                print(f"[{counter}/{total}] {district} ({geo_id}) → {year}", end="\r")

                try:
                    records = fetch_year(geo_id, year)
                except Exception as e:
                    print(f"\n⚠️  Failed {district} {year}: {e}")
                    time.sleep(1)
                    continue

                for rec in records:
                    out = {
                        "district":   district,
                        "state":      state,
                        **rec
                    }
                    writer.writerow(out)
                    fout.flush()
                    os.fsync(fout.fileno())

                time.sleep(DELAY)

    print(f"\n✅ Appended all records to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
