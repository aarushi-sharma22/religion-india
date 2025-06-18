#!/usr/bin/env python3
"""
Merge districts from missing_districts_recovered.csv into districts_geonames.csv
Extracts matched_state, matched_district, and geoname_id columns
"""
import csv
from pathlib import Path

def merge_districts():
    # File paths
    main_file = Path("data/districts_geonames.csv")
    recovery_file = Path("data/missing-districts-recovered.csv")
    
    # Read existing districts to avoid duplicates
    existing_districts = set()
    existing_rows = []
    
    print("📖 Reading existing districts...")
    with open(main_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_rows.append(row)
            # Create a key for duplicate checking
            key = (row['state'], row['district'], row['geoname_id'])
            existing_districts.add(key)
    
    print(f"   Found {len(existing_rows)} existing districts")
    
    # Read recovery file and extract needed columns
    added_count = 0
    skipped_empty = 0
    skipped_duplicate = 0
    
    print("\n📖 Reading recovery file...")
    with open(recovery_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Extract the columns we need
            # Use matched_state and matched_district which are the corrected names
            state = row.get('matched_state', '').strip()
            district = row.get('matched_district', '').strip()
            geoname_id = row.get('geoname_id', '').strip()
            
            # Skip if geoname_id is empty or '0.0'
            if not geoname_id or geoname_id == '0.0' or geoname_id == '0':
                print(f"   ⏭️  Skipping {row.get('district_name', 'unknown')} - no valid geoname_id")
                skipped_empty += 1
                continue
            
            # Check for duplicates
            key = (state, district, geoname_id)
            if key in existing_districts:
                skipped_duplicate += 1
                continue
            
            # Add to our list
            new_row = {
                'state': state,
                'district': district,
                'geoname_id': geoname_id
            }
            existing_rows.append(new_row)
            existing_districts.add(key)
            added_count += 1
            
            if added_count % 50 == 0:
                print(f"   ✅ Added {added_count} districts...")
    
    # Write the merged file
    print(f"\n💾 Writing merged file...")
    with open(main_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['state', 'district', 'geoname_id']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_rows)
    
    print(f"\n📊 Summary:")
    print(f"   Districts added: {added_count}")
    print(f"   Skipped (no geoname_id): {skipped_empty}")
    print(f"   Skipped (duplicate): {skipped_duplicate}")
    print(f"   Total districts now: {len(existing_rows)}")
    
    # Show some of the added districts
    if added_count > 0:
        print(f"\n📋 Sample of added districts:")
        # Show last 5 added
        for row in existing_rows[-min(5, added_count):]:
            print(f"   - {row['district']}, {row['state']} ({row['geoname_id']})")

if __name__ == "__main__":
    print("🔄 Merging districts from recovery file...")
    
    # Check if files exist
    if not Path("data/districts_geonames.csv").exists():
        print("❌ Error: data/districts_geonames.csv not found!")
        exit(1)
    
    if not Path("data/missing-districts-recovered.csv").exists():
        print("❌ Error: data/missing-districts-recovered.csv not found!")
        exit(1)
    
    merge_districts()
    print("\n✅ Done!")