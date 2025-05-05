#!/bin/bash
set -e

# 1) Bootstrap venv / install deps
if [ ! -d "venv" ]; then
  echo "Creating virtual environment…"
  python3 -m venv venv
fi
source venv/bin/activate

echo "Installing dependencies…"
pip install --upgrade pip
pip install -r requirements.txt

# 2) If districts.csv is missing, run the wiki scraper; otherwise skip
if [ -s data/districts.csv ]; then
  echo "✓ data/districts.csv found — skipping wiki-district-scrape.py"
else
  echo "data/districts.csv not found — running wiki-district-scrape.py"
  python src/wiki-district-scrape.py
fi

# 3) Now run the GeoNames lookup against the existing districts.csv
echo "Running fetch-geo-ids.py (reads data/districts.csv)…"
python src/fetch-geo-ids.py

echo "All done."
