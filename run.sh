#!/usr/bin/env bash
set -e

# ─── 1) Bootstrap venv + install deps ────────────────────────────────────────
if [ ! -d "venv" ]; then
  echo "Creating virtual environment…"
  python3 -m venv venv
fi
source venv/bin/activate

echo "Installing dependencies…"
pip install --upgrade pip
pip install -r requirements.txt

# ─── 2) Make sure we have geonames IDs ──────────────────────────────────────
GEONAMES_CSV="data/districts_geonames.csv"
if [ -s "$GEONAMES_CSV" ]; then
  echo "✓ $GEONAMES_CSV found — skipping geo-ID lookup"
else
  echo "✗ Error: $GEONAMES_CSV not found!"
  echo "  Please run src/fetch-geo-ids.py first."
  exit 1
fi

# ─── 3) Scrape marriage dates ────────────────────────────────────────────────
echo "Running date-scraper (src/fetch-date.py)…"
python src/fetch-date.py

echo "All done."

