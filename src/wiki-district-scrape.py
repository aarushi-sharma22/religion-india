import requests
from bs4 import BeautifulSoup
import pandas as pd

API_URL = "https://en.wikipedia.org/w/api.php"
PAGE    = "List_of_districts_in_India"
HEADERS = {
    "User-Agent": "district-fetcher/1.0 (your_email@example.com)"
}

SKIP_SECTIONS = {
    "References",
    "See also",
    "External links",
    "Notes",
    "Districts by state and union territory",  # overview table
}

# Expected district‐counts from your overview:
EXPECTED_COUNTS = {
    # States
    "Andhra Pradesh": 26,
    "Arunachal Pradesh": 27,
    "Assam": 35,
    "Bihar": 38,
    "Chhattisgarh": 33,
    "Goa": 2,
    "Gujarat": 34,
    "Haryana": 22,
    "Himachal Pradesh": 12,
    "Jharkhand": 24,
    "Karnataka": 31,
    "Kerala": 14,
    "Madhya Pradesh": 55,
    "Maharashtra": 36,
    "Manipur": 16,
    "Meghalaya": 12,
    "Mizoram": 11,
    "Nagaland": 17,
    "Odisha": 30,
    "Punjab": 23,
    "Rajasthan": 41,
    "Sikkim": 6,
    "Tamil Nadu": 38,
    "Telangana": 33,
    "Tripura": 8,
    "Uttar Pradesh": 75,
    "Uttarakhand": 13,
    "West Bengal": 23,
    # Union Territories
    "Andaman and Nicobar Islands": 3,
    "Chandigarh": 1,
    "Dadra and Nagar Haveli and Daman and Diu": 3,
    "Jammu and Kashmir": 20,
    "Ladakh": 7,
    "Lakshadweep": 1,
    "National Capital Territory of Delhi": 11,
    "Puducherry": 4,
}


def get_sections():
    resp = requests.get(API_URL, params={
        "action": "parse",
        "page": PAGE,
        "prop": "sections",
        "format": "json",
    }, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["parse"]["sections"]


def fetch_section_html(idx):
    resp = requests.get(API_URL, params={
        "action": "parse",
        "page": PAGE,
        "prop": "text",
        "section": idx,
        "format": "json",
    }, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()["parse"]["text"]["*"]


def fetch_state_districts():
    """
    Returns:
      - state_to_districts: dict mapping full heading title -> list of district names
    """
    state_to_districts = {}

    for sec in get_sections():
        title = sec["line"]
        level = int(sec["level"])
        idx   = sec["index"]

        # only consider ==== State (CODE) ==== headings
        if level != 3 or title in SKIP_SECTIONS:
            continue

        html = fetch_section_html(idx)
        soup = BeautifulSoup(html, "html.parser")
        tbl  = soup.find("table", class_="wikitable")
        if not tbl:
            continue

        # find which column header contains “District”
        headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
        try:
            dist_col = next(i for i,h in enumerate(headers) if "District" in h)
        except StopIteration:
            continue

        # extract district names
        districts = []
        for tr in tbl.find_all("tr")[1:]:
            cols = tr.find_all(["td","th"])
            if len(cols) <= dist_col:
                continue
            name = cols[dist_col].get_text(strip=True)
            districts.append(name)

        state_to_districts[title] = districts

    return state_to_districts


def main():
    state_to_districts = fetch_state_districts()

    # 1) Print overall total
    total_found = sum(len(v) for v in state_to_districts.values())
    print(f"Total districts extracted: {total_found}\n")

    # 2) For each expected state, compare counts & list mismatches
    print("Comparison against expected counts:\n")
    for expected_state, expected_n in EXPECTED_COUNTS.items():
        # match heading like "Andhra Pradesh (AP)" → key startswith expected_state
        heading = next((h for h in state_to_districts if h.startswith(expected_state)), None)
        if not heading:
            print(f"  ✗ {expected_state}: no section found!")
            continue

        found_list = state_to_districts[heading]
        found_n    = len(found_list)
        if found_n != expected_n:
            print(f"  ✗ {expected_state:30} expected {expected_n:3d} but extracted {found_n:3d}")
            print("     → Extracted districts:")
            for d in found_list:
                print(f"       - {d}")
            print()
        else:
            print(f"  ✓ {expected_state:30} {found_n:3d} districts (as expected)")

    # 3) Save raw CSV anyway
    rows = []
    for heading, dlist in state_to_districts.items():
        state = heading.split(" (", 1)[0]
        for d in dlist:
            rows.append({"state": state, "district": d})
    df = pd.DataFrame(rows)
    df.to_csv("data/districts.csv", index=False)
    print("\nSaved data/districts.csv")


if __name__ == "__main__":
    main()
