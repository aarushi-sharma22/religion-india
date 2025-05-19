#!/usr/bin/env python3
"""
DrikPanchang marriage‑muhurat scraper  – NETWORK‑FRIENDLY, LINE‑BUFFERED
===========================================================================

* Reads `data/districts_geonames.csv` (state, district, geoname_id)
* Appends rows whose status mentions an **auspicious marriage muhurat** to
  `data/all_marriage_muhurats.csv`
* **Minimises HTTP traffic**
    • skips every (district, year) fully present in the CSV
    • fetches only the last unfinished year of each district and writes the
      remaining dates
* **Line‑buffered CSV** (`buffering=1`) — every `writer.writerow()` reaches
  the OS instantly; no manual flush/fsync needed
* **Layout‑robust** — works for pages that use `.dpMuhurtaMessage`,
  `.dpMuhurtaAvail`, `.dpBlockMsg`, or no status element at all.
"""
from __future__ import annotations

import csv
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

# ────────────────────────── CONFIG ───────────────────────────────────────
INPUT_CSV  = Path("data/districts_geonames.csv")
OUTPUT_CSV = Path("data/all_marriage_muhurats.csv")
BASE_URL   = (
    "https://www.drikpanchang.com/shubh-dates/"
    "shubh-marriage-dates-with-muhurat.html"
)

START_YEAR, END_YEAR = 1892, 2024
SLEEP = 0.25               # polite delay between HTTP requests (seconds)

MONTHS       = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]
MONTH_INDEX  = {m: i for i, m in enumerate(MONTHS, start=1)}

# ────────────────────────── HTTP session ────────────────────────────────
sess = requests.Session()
sess.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) "
        "Gecko/20100101 Firefox/118.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE_URL,
})
try:
    sess.get(BASE_URL, timeout=15)          # warm‑up for cookies, etc.
except Exception:
    pass

# ────────────────────────── helpers ─────────────────────────────────────
_date_pat = re.compile(r"(\w+)\s+(\d{1,2}),\s*(\d{4})")   # Month DD, YYYY

def normalise_ws(text: str) -> str:
    """Collapse runs of whitespace (incl. NBSP & tabs) to single spaces."""
    return " ".join(text.split())

def parse_card(card, gid: str) -> Dict | None:
    """Return a dict for cards that mention an auspicious marriage muhurat."""

    title = card.select_one("a.dpMuhurtaTitleLink")
    if not title:
        return None
    m = _date_pat.match(title.get_text(strip=True))
    if not m:
        return None

    # Robust status extraction (covers 1892‑2024 layouts)
    status_elem = card.select_one(
        ".dpMuhurtaMessage, .dpMuhurtaAvail, .dpBlockMsg"
    )
    status = normalise_ws(status_elem.get_text(" ", strip=True) if status_elem else
                          card.get_text(" ", strip=True))

    sl = status.lower()
    if not (
        ("auspicious" in sl or "shubh" in sl) and
        ("marriage"   in sl or "wedding" in sl or "vivah" in sl) and
        ("muhurat"    in sl or "muhurta" in sl)
    ):
        return None

    month, day_s, year_s = m.groups()

    muhurat = nakshatra = tithi = ""
    detail = card.select_one(".dpCardMuhurtaDetail")
    if detail:
        for part in map(str.strip, detail.get_text("│", strip=True).split("│")):
            if   part.startswith("Muhurat:"):
                muhurat = part.split(":", 1)[1].strip()
            elif part.startswith("Nakshatra:"):
                nakshatra = part.split(":", 1)[1].strip()
            elif part.startswith("Tithi:"):
                tithi = part.split(":", 1)[1].strip()

    return {
        "geoname_id": gid,
        "year"     : int(year_s),
        "month"    : month,
        "day"      : int(day_s),
        "status"   : status,
        "muhurat"  : muhurat,
        "nakshatra": nakshatra,
        "tithi"    : tithi,
    }

def fetch_year(gid: str, year: int) -> List[Dict]:
    """Download & parse one district‑year page."""
    r = sess.get(BASE_URL, params={"year": year, "geoname-id": gid}, timeout=20)
    r.raise_for_status()
    soup  = BeautifulSoup(r.text, "html.parser")
    cards = soup.select(".dpMuhurtaBlock > .dpSingleBlock")
    return [rec for card in cards if (rec := parse_card(card, gid))]

# ────────────────── scan CSV once (resume map) ──────────────────────────

done_years: set[Tuple[str, int]] = set()                 # (gid, year)
last_date : dict[str, Tuple[int, int, int]] = {}         # gid -> (y, m, d)

if OUTPUT_CSV.exists() and OUTPUT_CSV.stat().st_size:
    with OUTPUT_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            gid = row["geoname_id"].strip()
            yr  = int(row["year"])
            mi  = MONTH_INDEX[row["month"].lower()]
            d   = int(row["day"])
            done_years.add((gid, yr))
            if (gid not in last_date) or (yr, mi, d) > last_date[gid]:
                last_date[gid] = (yr, mi, d)
    print(f"▶ resume: {len(done_years):,} district‑years, {len(last_date):,} tails")

# ────────────────────────── main ────────────────────────────────────────

def main() -> None:
    if not INPUT_CSV.exists() or INPUT_CSV.stat().st_size == 0:
        sys.exit("✘  districts_geonames.csv missing or empty")

    with INPUT_CSV.open(encoding="utf-8") as f:
        districts = [r for r in csv.DictReader(f) if r.get("geoname_id")]

    first_run = not OUTPUT_CSV.exists() or OUTPUT_CSV.stat().st_size == 0

    with OUTPUT_CSV.open("a", newline="", encoding="utf-8", buffering=1) as fout:
        writer = csv.DictWriter(
            fout,
            fieldnames=[
                "district", "state", "geoname_id",
                "year", "month", "day",
                "status", "muhurat", "nakshatra", "tithi",
            ],
        )
        if first_run:
            writer.writeheader()

        for row in districts:
            gid   = row["geoname_id"].strip()
            dist  = row["district"]
            state = row["state"]
            tail  = last_date.get(gid)                 # None or (y, m, d)
            start_year = tail[0] if tail else START_YEAR

            for yr in range(start_year, END_YEAR + 1):
                # skip years already fully present (except possibly the tail year)
                if (gid, yr) in done_years and yr != start_year:
                    continue

                print(f"FETCH {dist:25s} {yr}  …", end="")
                try:
                    records = fetch_year(gid, yr)
                except Exception as e:
                    print(f" ERROR {e}")
                    time.sleep(2)
                    continue

                # keep only records *after* the last saved date in the tail year
                if tail and yr == start_year:
                    m_tail, d_tail = tail[1], tail[2]
                    records = [
                        r for r in records
                        if (MONTH_INDEX[r["month"].lower()], r["day"]) > (m_tail, d_tail)
                    ]
                if not records:
                    print(" nothing new")
                    continue

                for rec in records:
                    writer.writerow({"district": dist, "state": state, **rec})

                print(f" wrote {len(records):3d}; file {fout.tell()/1024/1024:6.2f} MB")
                time.sleep(SLEEP)

    print("\n✅ finished – data appended to", OUTPUT_CSV)


if __name__ == "__main__":
    main()
