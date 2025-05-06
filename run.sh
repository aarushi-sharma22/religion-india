#!/usr/bin/env bash
set -e

# ── 1) venv & deps ────────────────────────────────────────────────────────────
if [ ! -d "venv" ]; then
  echo "Creating virtual environment…"
  python3 -m venv venv
fi
source venv/bin/activate

echo "Installing dependencies…"
pip install --upgrade pip
pip install -r requirements.txt

# ── 1a) ensure a headless browser for Playwright ──────────────────────────────
if ! playwright install --with-deps firefox >/dev/null 2>&1; then
  # fall back to a quiet install if playwright is not on PATH yet
  python -m playwright install firefox
fi

# ── 2) sanity-check data/districts_geonames.csv ───────────────────────────────
GEONAMES_CSV="data/districts_geonames.csv"
if [ ! -s "$GEONAMES_CSV" ]; then
  echo "✗ $GEONAMES_CSV missing or empty. Run src/fetch-geo-ids.py first." >&2
  exit 1
fi
echo "✓ $GEONAMES_CSV found"

# ── 3) scrape marriage dates ──────────────────────────────────────────────────
echo "Running web-scraper (src/web-scrape.py)…"
python src/web-scrape.py          # add flags here only if you want to override defaults

echo "All done."
