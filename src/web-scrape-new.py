#!/usr/bin/env python3
"""
DrikPanchang – 4-district marriage-muhurat scraper  (1894-2024)  – resumable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Output: data/marriage_muhurats/<state>/<district>.csv
* Guarantees no duplicate rows if you rerun it.
"""

from __future__ import annotations
import csv, time, re, sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ── CONFIG ───────────────────────────────────────────────────────────────────
TARGETS = [
    {"state": "Maharashtra",         "district": "Mumbai",    "gid": "1275339"},
    {"state": "Punjab",  "district": "Mohali",  "gid": "6992326"},

]

START_YEAR, END_YEAR = 1894, 2024
BASE_URL = ("https://www.drikpanchang.com/shubh-dates/"
            "shubh-marriage-dates-with-muhurat.html")
OUT_ROOT = Path("data/marriage_muhurats")
SLEEP    = 0.3            # polite pause between requests (s)
RETRIES  = 3              # soft retry on network hiccups

USER_AGENT = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# ── helpers ──────────────────────────────────────────────────────────────────
MONTHS = ["january","february","march","april","may","june","july","august",
          "september","october","november","december"]
_date_pat = re.compile(r"(\w+)\s+(\d{1,2}),\s*(\d{4})", re.I)

def canon(text: str) -> str:
    return " ".join(text.split())

def parse_year_page(html: str) -> List[Dict]:
    soup  = BeautifulSoup(html, "html.parser")
    cards = soup.select(".dpMuhurtaBlock > .dpSingleBlock")
    recs  = []

    for card in cards:
        title = card.select_one("a.dpMuhurtaTitleLink")
        if not title:
            continue
        m = _date_pat.match(title.get_text(strip=True))
        if not m:
            continue

        status_elem = card.select_one(
            ".dpMuhurtaMessage,.dpMuhurtaAvail,.dpBlockMsg")
        status_txt  = canon(status_elem.get_text(" ", strip=True)) if status_elem else ""
        lo = status_txt.lower()
        if not (("marriage" in lo or "vivah" in lo or "wedding" in lo)
                and ("muhurat" in lo or "muhurta" in lo)):
            continue

        month, day, year = m.groups()
        recs.append({"year": int(year),
                     "month": month.title(),
                     "day":   int(day)})
    return recs

def fetch_year(sess: requests.Session, gid: str, year: int) -> str:
    for attempt in range(1, RETRIES + 1):
        try:
            r = sess.get(BASE_URL, params={"geoname-id": gid, "year": year},
                         timeout=15)
            r.raise_for_status()
            if len(r.text) < 2000:  # crude block/check
                raise ValueError("Suspiciously small page")
            return r.text
        except Exception as e:
            if attempt == RETRIES:
                raise
            print(f"  Warning: {e} – retry {attempt}/{RETRIES}")
            time.sleep(2)

def ensure_path(state: str, district: str) -> Path:
    folder = OUT_ROOT / state
    folder.mkdir(parents=True, exist_ok=True)
    safe_name = district.replace(" ", "_")
    return folder / f"{safe_name}.csv"

def load_existing(path: Path) -> Tuple[Set[Tuple[int,str,int]], Set[int]]:
    """
    Return (set_of_rows, set_of_years) already present in an existing CSV.
    Each row is keyed as (year, month, day).
    """
    rows, years = set(), set()
    if not path.exists():
        return rows, years
    with path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            y, m, d = int(r["year"]), r["month"], int(r["day"])
            rows.add((y, m, d))
            years.add(y)
    return rows, years

# ── main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    for t in TARGETS:
        state, dist, gid = t["state"], t["district"], t["gid"]
        path = ensure_path(state, dist)
        existing_rows, done_years = load_existing(path)

        print(f"\n{dist}, {state}  (gid={gid})")
        if done_years:
            yrs = f"{min(done_years)}–{max(done_years)}"
            print(f"  Resuming – {len(existing_rows)} rows across years {yrs}")

        with path.open("a", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=["year","month","day"])
            if path.stat().st_size == 0:
                writer.writeheader()

            for yr in range(START_YEAR, END_YEAR + 1):
                if yr in done_years:
                    print(f"   ↳ {yr} – already complete, skipped")
                    continue

                print(f"   ↳ {yr} … ", end="", flush=True)
                try:
                    html = fetch_year(sess, gid, yr)
                    records = [r for r in parse_year_page(html)
                               if (r["year"], r["month"], r["day"]) not in existing_rows]
                    if records:
                        writer.writerows(records)
                        existing_rows.update((r["year"], r["month"], r["day"])
                                             for r in records)
                        print(f"{len(records):2d} new rows")
                    else:
                        print("none")
                except Exception as e:
                    print(f"failed ({e})")
                time.sleep(SLEEP)

    print("\nDone. Data stored in", OUT_ROOT.resolve())

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
