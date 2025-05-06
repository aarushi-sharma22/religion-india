#!/usr/bin/env python3
"""
Stream-scrape DrikPanchang for “Auspicious Marriage Muhurat is Available”
cards (1900-2024) for every district in data/districts_geonames.csv.

► Starts with *cloudscraper*; if that fails N times in a row it
  switches to *Playwright* automatically and continues.

Raw HTML of every final failure is dumped to debug_html/<gid>_<year>_<status>.html
so you can inspect what happened.
"""

from __future__ import annotations
import argparse, csv, json, os, random, re, sys, time
from pathlib import Path
from typing import List, Tuple

import requests
from bs4 import BeautifulSoup

# ─── CLI ──────────────────────────────────────────────────────────────────
ap = argparse.ArgumentParser()
ap.add_argument("--methods", default="cloudscraper,playwright",
                help="comma-sep priority list (default: cloudscraper,playwright)")
ap.add_argument("--max-fail", type=int, default=8,
                help="consecutive failures before switching method")
ap.add_argument("--delay",     type=float, default=5.0,
                help="base delay between successful requests (s)")
ap.add_argument("--jitter",    type=float, default=3.0,
                help="± random jitter added to delay (s)")
ap.add_argument("--retries",   type=int, default=3,
                help="retries per request before counting as failure")
ap.add_argument("--max-sleep", type=int, default=300,
                help="max back-off sleep when retrying (s)")
args = ap.parse_args()

METHODS = [m.strip().lower() for m in args.methods.split(",") if m.strip()]
if not METHODS:
    sys.exit("✘  --methods list is empty!")

# ─── PATHS & CONSTANTS ──────────────────────────────────────────────────
INPUT_CSV   = Path("data/districts_geonames.csv")
OUTPUT_CSV  = Path("data/all_marriage_muhurats.csv")
DEBUG_DIR   = Path("debug_html"); DEBUG_DIR.mkdir(exist_ok=True)

BASE_URL = ("https://www.drikpanchang.com/shubh-dates/"
            "shubh-marriage-dates-with-muhurat.html")
START, END = 1900, 2024
WANTED     = "auspicious marriage muhurat is available"

_date_pat = re.compile(r"(\w+)\s+(\d{1,2}),\s*(\d{4})")   # Month DD, YYYY

# ─── SESSION FACTORY ────────────────────────────────────────────────────
def build_session(kind: str) -> requests.Session:
    kind = kind.lower()
    if kind == "cloudscraper":
        try:
            import cloudscraper                       # type: ignore
        except ImportError:
            sys.exit("❌  cloudscraper missing – add it to requirements.txt")
        sess = cloudscraper.create_scraper(
            browser={"custom": "firefox", "platform": "windows"},
            delay=10,                                # its own throttle
        )
    elif kind == "playwright":
        try:
            from playwright.sync_api import sync_playwright   # type: ignore
        except ImportError:
            sys.exit("❌  playwright missing – add it to requirements.txt")
        COOKIE_FILE = Path("cf_cookies.json")

        def solve_once() -> list[dict]:
            with sync_playwright() as p:
                browser = p.firefox.launch(headless=True)
                page = browser.new_page()
                page.goto(BASE_URL, timeout=60_000)
                cookies = page.context.cookies()
                COOKIE_FILE.write_text(json.dumps(cookies))
                browser.close()
            return cookies

        cookies = json.loads(COOKIE_FILE.read_text()) if COOKIE_FILE.exists() else solve_once()

        sess = requests.Session()
        for c in cookies:
            sess.cookies.set(c["name"], c["value"], domain=c["domain"], path=c["path"])
    else:
        raise ValueError(f"Unknown method {kind}")

    # browser-like headers
    sess.headers.update({
        "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) "
            "Gecko/20100101 Firefox/118.0",
        "Accept":
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Referer": BASE_URL,
    })
    # warm-up
    sess.get(BASE_URL, timeout=30)
    return sess

current_idx          = 0
sess: requests.Session = build_session(METHODS[current_idx])
consecutive_failures = 0

# ─── HELPERS ─────────────────────────────────────────────────────────────
def save_debug(html: str, gid: str, yr: int, status: int) -> None:
    fn = DEBUG_DIR / f"{gid}_{yr}_{status}.html"
    fn.write_text(html, encoding="utf-8")
    print(f"\n💾  saved debug HTML → {fn}")

def next_method() -> None:
    global current_idx, sess, consecutive_failures
    if current_idx + 1 >= len(METHODS):
        return
    current_idx += 1
    print(f"\n🔀  Switching to {METHODS[current_idx]} …")
    sess = build_session(METHODS[current_idx])
    consecutive_failures = 0

def jitter_sleep() -> None:
    time.sleep(args.delay + random.uniform(-args.jitter, args.jitter))

# ─── SCRAPING CORE ───────────────────────────────────────────────────────
def parse_card(card, gid: str):
    title = card.select_one("a.dpMuhurtaTitleLink")
    if not title: return None
    m = _date_pat.match(title.get_text(strip=True))
    if not m: return None
    month, day, year = m.group(1), int(m.group(2)), int(m.group(3))

    status = card.select_one(".dpMuhurtaMessage").get_text(" ", strip=True)
    if status.lower() != WANTED:                      # filter
        return None

    muhurat = nakshatra = tithi = ""
    detail = card.select_one(".dpCardMuhurtaDetail")
    if detail:
        for p in (d.strip() for d in detail.get_text("│", strip=True).split("│")):
            if   p.startswith("Muhurat:"):   muhurat   = p.split(":",1)[1].strip()
            elif p.startswith("Nakshatra:"): nakshatra = p.split(":",1)[1].strip()
            elif p.startswith("Tithi:"):     tithi     = p.split(":",1)[1].strip()

    return dict(geoname_id=gid, year=year, month=month, day=day,
                status=status, muhurat=muhurat, nakshatra=nakshatra, tithi=tithi)

def fetch_year(gid: str, yr: int) -> Tuple[bool, List[dict]]:
    """Return (success_flag, records).  success=False means all retries failed."""
    params  = {"year": yr, "geoname-id": gid}
    backoff = args.delay
    for attempt in range(1, args.retries + 1):
        try:
            r = sess.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            soup  = BeautifulSoup(r.text, "html.parser")
            cards = soup.select(".dpMuhurtaBlock > .dpSingleBlock")
            records = [rec for c in cards if (rec := parse_card(c, gid))]
            return True, records
        except requests.HTTPError as he:
            if he.response is not None:
                save_debug(he.response.text, gid, yr, he.response.status_code)
        except Exception as e:
            print(f"\n❗ {gid}@{yr} error: {e}")

        if attempt < args.retries:
            backoff = min(backoff * 2, args.max_sleep)
            print(f"🔄  retry {attempt}/{args.retries-1} after {backoff:.1f}s …")
            time.sleep(backoff)
    return False, []      # all retries exhausted

# ─── MAIN LOOP ───────────────────────────────────────────────────────────
def main() -> None:
    rows = list(csv.DictReader(INPUT_CSV.open(encoding="utf-8")))
    if not rows:
        sys.exit("✘  districts_geonames.csv is empty!")

    first_run = not OUTPUT_CSV.exists() or OUTPUT_CSV.stat().st_size == 0
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=[
            "district","state","geoname_id",
            "year","month","day",
            "status","muhurat","nakshatra","tithi"
        ])
        if first_run:
            writer.writeheader(); fout.flush()

        total = len(rows)*(END-START+1); done = 0
        global consecutive_failures
        for row in rows:
            gid, dist, state = row["geoname_id"].strip(), row["district"], row["state"]
            for yr in range(START, END+1):
                done += 1
                print(f"[{done}/{total}] {dist} ({gid}) – {yr}", end="\r")

                success, records = fetch_year(gid, yr)
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= args.max_fail:
                        next_method()

                for rec in records:
                    writer.writerow(dict(district=dist, state=state, **rec))

                fout.flush(); os.fsync(fout.fileno())
                jitter_sleep()

    print(f"\n✅  Finished – data appended to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
