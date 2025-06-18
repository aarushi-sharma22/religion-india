#!/usr/bin/env python3
"""
Enhanced DrikPanchang scraper with per-district files and VPN blocking detection
===============================================================================
- Stores data in data/marriage_muhurats/{state}/{district}_{geoname_id}.csv
- Detects blocking and exits with code 2 for VPN rotation
- Includes debug logging for empty responses
"""
from __future__ import annotations

import csv
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
import json

import requests
from bs4 import BeautifulSoup

# ────────────────────────── CONFIG ───────────────────────────────────────
INPUT_CSV     = Path("data/districts_geonames.csv")
OUTPUT_DIR    = Path("data/marriage_muhurats")
SUMMARY_FILE  = Path("data/marriage_muhurats_summary.json")

BASE_URL = (
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

# Blocking detection
CONSECUTIVE_EMPTY = 0
MAX_CONSECUTIVE_EMPTY = 10  # If we get 10 empty responses in a row, likely blocked
DEBUG_MODE = True  # Enable debug logging

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

# ────────────────────────── helpers ─────────────────────────────────────
_date_pat = re.compile(r"(\w+)\s+(\d{1,2}),\s*(\d{4})")   # Month DD, YYYY

def normalise_ws(text: str) -> str:
    """Collapse runs of whitespace (incl. NBSP & tabs) to single spaces."""
    return " ".join(text.split())

def sanitize_filename(name: str) -> str:
    """Make a name safe for filesystem"""
    return re.sub(r'[<>:"/\\|?*]', '_', name)

def get_district_file(state: str, district: str, geoname_id: str) -> Path:
    """Get the path for a district's CSV file"""
    state_dir = OUTPUT_DIR / sanitize_filename(state)
    state_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{sanitize_filename(district)}_{geoname_id}.csv"
    return state_dir / filename

def parse_card(card, gid: str) -> Dict | None:
    """Return a dict for cards that mention an auspicious marriage muhurat."""
    title = card.select_one("a.dpMuhurtaTitleLink")
    if not title:
        return None
    m = _date_pat.match(title.get_text(strip=True))
    if not m:
        return None

    # Robust status extraction
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
        "year"     : int(year_s),
        "month"    : month,
        "day"      : int(day_s),
        "status"   : status,
        "muhurat"  : muhurat,
        "nakshatra": nakshatra,
        "tithi"    : tithi,
    }

def fetch_year(gid: str, year: int, district: str = "", state: str = "") -> List[Dict]:
    """Download & parse one district‑year page with debug logging."""
    global CONSECUTIVE_EMPTY
    
    try:
        r = sess.get(BASE_URL, params={"year": year, "geoname-id": gid}, timeout=20)
        r.raise_for_status()
        
        # Debug logging
        if DEBUG_MODE and len(r.content) < 5000:
            print(f"\n    DEBUG: Small response ({len(r.content)} bytes)")
            if "Access Denied" in r.text:
                print("    DEBUG: Access Denied detected")
            elif "Rate Limit" in r.text:
                print("    DEBUG: Rate limit detected")
        
        # Check if we got a valid response
        if len(r.content) < 1000:  # Suspiciously small response
            CONSECUTIVE_EMPTY += 1
            print(f"\n    🚫 Response too small ({len(r.content)} bytes) - possible blocking")
            if CONSECUTIVE_EMPTY >= MAX_CONSECUTIVE_EMPTY:
                print(f"\n🚫 Likely blocked after {CONSECUTIVE_EMPTY} suspicious responses")
                sys.exit(2)
            return []
        
        soup  = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".dpMuhurtaBlock > .dpSingleBlock")
        
        # Debug: Check what we found
        if DEBUG_MODE and not cards:
            # Look for any signs of blocking
            if "cloudflare" in r.text.lower():
                print("\n    DEBUG: Cloudflare challenge detected")
                CONSECUTIVE_EMPTY += 1
            elif "please verify" in r.text.lower():
                print("\n    DEBUG: Captcha/verification required")
                CONSECUTIVE_EMPTY += 1
            else:
                # Try to find what content we got instead
                page_title = soup.find('title')
                if page_title:
                    print(f"\n    DEBUG: Page title: {page_title.text[:60]}...")
                # Check if we at least got the main container
                main_container = soup.select_one(".dpMuhurtaBlock")
                if not main_container:
                    print("    DEBUG: No .dpMuhurtaBlock container found")
                    CONSECUTIVE_EMPTY += 1
        
        if cards:
            # Reset counter only when we get actual data
            CONSECUTIVE_EMPTY = 0
            
        return [rec for card in cards if (rec := parse_card(card, gid))]
        
    except requests.exceptions.Timeout:
        print(f"\n    ⏱️  Timeout for {district}, {state} - year {year}")
        CONSECUTIVE_EMPTY += 1
        if CONSECUTIVE_EMPTY >= MAX_CONSECUTIVE_EMPTY:
            print(f"\n🚫 Too many timeouts ({CONSECUTIVE_EMPTY}) - likely blocked")
            sys.exit(2)
        return []
        
    except (requests.RequestException, Exception) as e:
        error_str = str(e).lower()
        
        # More specific error detection
        if any(indicator in error_str for indicator in [
            'timeout', 'connection', 'access denied', 'forbidden',
            '403', '429', 'too many requests', 'blocked', 'reset'
        ]):
            CONSECUTIVE_EMPTY += 1
            print(f"\n    ❌ Network error: {e}")
            if CONSECUTIVE_EMPTY >= MAX_CONSECUTIVE_EMPTY:
                print(f"\n🚫 Too many errors ({CONSECUTIVE_EMPTY}) - likely blocked")
                sys.exit(2)
        else:
            print(f"\n    ❌ Unexpected error: {e}")
        
        return []

def get_district_resume_info(state: str, district: str, geoname_id: str) -> Tuple[set, Tuple]:
    """Get resume info for a specific district"""
    district_file = get_district_file(state, district, geoname_id)
    
    done_years = set()
    last_date = None
    
    if district_file.exists():
        with district_file.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yr = int(row["year"])
                mi = MONTH_INDEX[row["month"].lower()]
                d = int(row["day"])
                done_years.add(yr)
                if not last_date or (yr, mi, d) > last_date:
                    last_date = (yr, mi, d)
    
    return done_years, last_date

def update_summary(summaries: Dict):
    """Update the summary file"""
    with SUMMARY_FILE.open("w", encoding="utf-8") as f:
        json.dump(summaries, f, indent=2)

# ────────────────────────── main ────────────────────────────────────────

def main() -> None:
    global CONSECUTIVE_EMPTY
    
    if not INPUT_CSV.exists() or INPUT_CSV.stat().st_size == 0:
        sys.exit("✘  districts_geonames.csv missing or empty")

    print("🕷️  Enhanced DrikPanchang Scraper")
    print("📁 Data will be saved to:", OUTPUT_DIR)
    print("🔍 Debug mode:", "ON" if DEBUG_MODE else "OFF")
    print("=" * 60)

    # Warm up session
    print("🌐 Testing connection...")
    try:
        test_response = sess.get(BASE_URL, timeout=15)
        print(f"✅ Connection OK (received {len(test_response.content)} bytes)")
    except Exception as e:
        print(f"⚠️  Connection test failed: {e}")

    with INPUT_CSV.open(encoding="utf-8") as f:
        districts = [r for r in csv.DictReader(f) if r.get("geoname_id")]

    # Load or create summary
    summaries = {}
    if SUMMARY_FILE.exists():
        with SUMMARY_FILE.open(encoding="utf-8") as f:
            summaries = json.load(f)

    total_districts = len(districts)
    total_dates_scraped = 0
    
    for dist_idx, row in enumerate(districts, 1):
        gid   = row["geoname_id"].strip()
        dist  = row["district"]
        state = row["state"]
        
        # Get resume info for this district
        done_years, last_date = get_district_resume_info(state, dist, gid)
        start_year = last_date[0] if last_date else START_YEAR
        
        district_key = f"{state}/{dist}/{gid}"
        district_dates = summaries.get(district_key, 0)
        
        print(f"\n[{dist_idx}/{total_districts}] Processing {dist}, {state}")
        if last_date:
            print(f"  Resuming from: {MONTHS[last_date[1]-1]} {last_date[2]}, {last_date[0]}")
        
        district_file = get_district_file(state, dist, gid)
        first_write = not district_file.exists()
        
        with district_file.open("a", newline="", encoding="utf-8", buffering=1) as fout:
            writer = csv.DictWriter(
                fout,
                fieldnames=[
                    "year", "month", "day",
                    "status", "muhurat", "nakshatra", "tithi",
                ],
            )
            if first_write:
                writer.writeheader()
            
            empty_count_this_district = 0
            
            for yr in range(start_year, END_YEAR + 1):
                # Skip completed years
                if yr in done_years and yr != start_year:
                    continue

                print(f"  FETCH {yr}  …", end="", flush=True)
                records = fetch_year(gid, yr, dist, state)
                
                # Filter records after last date
                if last_date and yr == start_year:
                    m_tail, d_tail = last_date[1], last_date[2]
                    records = [
                        r for r in records
                        if (MONTH_INDEX[r["month"].lower()], r["day"]) > (m_tail, d_tail)
                    ]
                
                if not records:
                    print(" nothing new", end="")
                    empty_count_this_district += 1
                    
                    # Additional debug for persistent empty responses
                    if DEBUG_MODE and empty_count_this_district > 5:
                        print(f" (empty #{empty_count_this_district})", end="")
                    
                    # Check global consecutive empty
                    if CONSECUTIVE_EMPTY >= MAX_CONSECUTIVE_EMPTY:
                        print(f"\n🚫 Got {CONSECUTIVE_EMPTY} consecutive empty/error responses")
                        print("💡 Likely blocked - need VPN rotation")
                        sys.exit(2)
                    continue
                
                # Reset counters on successful data
                empty_count_this_district = 0
                CONSECUTIVE_EMPTY = 0
                
                for rec in records:
                    writer.writerow(rec)
                    district_dates += 1
                    total_dates_scraped += 1

                print(f" wrote {len(records):3d}")
                time.sleep(SLEEP)
        
        # Update summary
        summaries[district_key] = district_dates
        update_summary(summaries)
        
        # Progress update every 10 districts
        if dist_idx % 10 == 0:
            print(f"\n📊 Progress: {dist_idx}/{total_districts} districts, "
                  f"{total_dates_scraped} dates scraped this session")

    print(f"\n✅ Finished!")
    print(f"📊 Total dates scraped this run: {total_dates_scraped}")
    print(f"📁 Data stored in: {OUTPUT_DIR}/")
    print(f"📋 Summary file: {SUMMARY_FILE}")


if __name__ == "__main__":
    main()