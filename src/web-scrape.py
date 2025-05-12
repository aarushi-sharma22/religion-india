#!/usr/bin/env python3
"""
DrikPanchang marriage-muhurat scraper  –  RESUMABLE EDITION
===========================================================

• Reads  data/districts_geonames.csv                (state,district,geoname_id)
• Appends ONLY rows whose status is exactly
  “Auspicious Marriage Muhurat is Available” to
  data/all_marriage_muhurats.csv

If all rows for a district/year are already present in the output file,
that district-year is silently skipped.
"""

from __future__ import annotations
import csv, os, re, sys, time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── CONFIG ─────────────────────────────────────────────────────────────
INPUT_CSV   = Path("data/districts_geonames.csv")
OUTPUT_CSV  = Path("data/all_marriage_muhurats.csv")
BASE_URL    = ("https://www.drikpanchang.com/shubh-dates/"
               "shubh-marriage-dates-with-muhurat.html")

START_YEAR, END_YEAR = 1892, 2024
SLEEP                = 0.25          # polite delay

# ─── session with sane headers ──────────────────────────────────────────
sess = requests.Session()
sess.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) "
                   "Gecko/20100101 Firefox/118.0"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;"
               "q=0.9,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE_URL,
})
try:
    sess.get(BASE_URL, timeout=15)    # prime cookies / token
except Exception:
    pass

# ─── helpers ────────────────────────────────────────────────────────────
_date_pat = re.compile(r"(\w+)\s+(\d{1,2}),\s*(\d{4})")   # Month DD, YYYY

def parse_card(card, gid: str) -> dict | None:
    """Return a dict only for ‘Auspicious Marriage Muhurat is Available’ cards."""
    title = card.select_one("a.dpMuhurtaTitleLink")
    if not title:
        return None
    m = _date_pat.match(title.get_text(strip=True))
    if not m:
        return None

    status = card.select_one(".dpMuhurtaMessage").get_text(" ", strip=True)
    if status.lower() != "auspicious marriage muhurat is available":
        return None

    month, day, year = m.group(1), int(m.group(2)), int(m.group(3))
    muhurat = nakshatra = tithi = ""
    detail  = card.select_one(".dpCardMuhurtaDetail")
    if detail:
        for p in map(str.strip, detail.get_text("│", strip=True).split("│")):
            if   p.startswith("Muhurat:"):   muhurat   = p.split(":",1)[1].strip()
            elif p.startswith("Nakshatra:"): nakshatra = p.split(":",1)[1].strip()
            elif p.startswith("Tithi:"):     tithi     = p.split(":",1)[1].strip()

    return {
        "geoname_id": gid,
        "year":       year,
        "month":      month,
        "day":        day,
        "status":     status,
        "muhurat":    muhurat,
        "nakshatra":  nakshatra,
        "tithi":      tithi,
    }

def fetch_year(gid: str, year: int) -> list[dict]:
    r = sess.get(BASE_URL, params={"year": year, "geoname-id": gid}, timeout=20)
    r.raise_for_status()
    soup  = BeautifulSoup(r.text, "html.parser")
    cards = soup.select(".dpMuhurtaBlock > .dpSingleBlock")
    return [rec for c in cards if (rec := parse_card(c, gid))]

# ─── build “done” lookup from existing output ───────────────────────────
done: set[tuple[str,int]] = set()
if OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size:
    with OUTPUT_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                done.add((row["geoname_id"].strip(), int(row["year"])))
            except Exception:
                continue
    print(f"▶ resume-mode: {len(done):,} district-year rows already present")

# ─── main ───────────────────────────────────────────────────────────────
def main() -> None:
    if not INPUT_CSV.exists() or INPUT_CSV.stat().st_size == 0:
        sys.exit("✘  districts_geonames.csv missing or empty")

    districts = [r for r in csv.DictReader(INPUT_CSV.open(encoding="utf-8"))
                 if r.get("geoname_id")]
    if not districts:
        sys.exit("✘  no rows in districts_geonames.csv")

    first_run = not OUTPUT_CSV.exists() or OUTPUT_CSV.stat().st_size == 0
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=["district","state","geoname_id",
                        "year","month","day",
                        "status","muhurat","nakshatra","tithi"]
        )
        if first_run:
            writer.writeheader(); fout.flush()

        todo_total = len(districts) * (END_YEAR - START_YEAR + 1)
        processed  = 0

        for row in districts:
            gid   = row["geoname_id"].strip()
            dist  = row["district"]
            state = row["state"]

            for yr in range(START_YEAR, END_YEAR + 1):
                processed += 1

                # --- resume logic ---------------------------------------
                if (gid, yr) in done:
                    continue    # already scraped → skip
                # --------------------------------------------------------

                print(f"[{processed}/{todo_total}] {dist} ({gid}) – {yr}", end="\r")
                try:
                    records = fetch_year(gid, yr)
                except Exception as e:
                    print(f"\n⚠️  {dist} {yr}: {e}")
                    time.sleep(2)
                    continue

                for rec in records:
                    writer.writerow({"district": dist, "state": state, **rec})

                fout.flush(); os.fsync(fout.fileno())
                done.add((gid, yr))          # mark as completed
                time.sleep(SLEEP)

    print("\n✅  finished – data appended to", OUTPUT_CSV)

if __name__ == "__main__":
    main()
