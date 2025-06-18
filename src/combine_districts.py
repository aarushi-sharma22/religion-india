#!/usr/bin/env python3
"""
Utility to combine district CSV files into a single file or by state
"""
import csv
from pathlib import Path
import argparse

def combine_all_districts(output_file="data/combined_marriage_muhurats.csv"):
    """Combine all district files into one CSV"""
    input_dir = Path("data/marriage_muhurats")
    districts_file = Path("data/districts_geonames.csv")
    
    # Load district metadata
    district_info = {}
    with districts_file.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            district_info[row['geoname_id']] = {
                'state': row['state'],
                'district': row['district']
            }
    
    total_rows = 0
    with open(output_file, "w", newline="", encoding="utf-8") as fout:
        writer = None
        
        # Process each state directory
        for state_dir in sorted(input_dir.iterdir()):
            if not state_dir.is_dir():
                continue
                
            print(f"Processing state: {state_dir.name}")
            
            # Process each district file
            for district_file in sorted(state_dir.glob("*.csv")):
                # Extract geoname_id from filename
                geoname_id = district_file.stem.split('_')[-1]
                
                if geoname_id not in district_info:
                    print(f"  Warning: Unknown geoname_id {geoname_id}")
                    continue
                
                info = district_info[geoname_id]
                
                with district_file.open(encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    
                    for row in reader:
                        # Add district info to row
                        row['state'] = info['state']
                        row['district'] = info['district']
                        row['geoname_id'] = geoname_id
                        
                        if writer is None:
                            # Initialize writer with fieldnames from first row
                            fieldnames = ['district', 'state', 'geoname_id'] + \
                                       [k for k in row.keys() if k not in ['district', 'state', 'geoname_id']]
                            writer = csv.DictWriter(fout, fieldnames=fieldnames)
                            writer.writeheader()
                        
                        writer.writerow(row)
                        total_rows += 1
                
                print(f"  ✓ {info['district']}: {district_file.stat().st_size / 1024:.1f} KB")
    
    print(f"\n✅ Combined {total_rows} rows into {output_file}")
    print(f"📊 File size: {Path(output_file).stat().st_size / 1024 / 1024:.1f} MB")

def combine_by_state(state_name, output_file=None):
    """Combine all districts from a specific state"""
    input_dir = Path("data/marriage_muhurats") / state_name
    districts_file = Path("data/districts_geonames.csv")
    
    if not input_dir.exists():
        print(f"❌ State directory not found: {input_dir}")
        return
    
    if output_file is None:
        output_file = f"data/{state_name}_marriage_muhurats.csv"
    
    # Load district metadata for this state
    district_info = {}
    with districts_file.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row['state'] == state_name:
                district_info[row['geoname_id']] = {
                    'state': row['state'],
                    'district': row['district']
                }
    
    print(f"Combining districts from {state_name}...")
    
    total_rows = 0
    with open(output_file, "w", newline="", encoding="utf-8") as fout:
        writer = None
        
        # Process each district file in this state
        for district_file in sorted(input_dir.glob("*.csv")):
            # Extract geoname_id from filename
            geoname_id = district_file.stem.split('_')[-1]
            
            if geoname_id not in district_info:
                print(f"  Warning: Unknown geoname_id {geoname_id}")
                continue
            
            info = district_info[geoname_id]
            
            with district_file.open(encoding="utf-8") as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Add district info to row
                    row['state'] = info['state']
                    row['district'] = info['district']
                    row['geoname_id'] = geoname_id
                    
                    if writer is None:
                        # Initialize writer with fieldnames from first row
                        fieldnames = ['district', 'state', 'geoname_id'] + \
                                   [k for k in row.keys() if k not in ['district', 'state', 'geoname_id']]
                        writer = csv.DictWriter(fout, fieldnames=fieldnames)
                        writer.writeheader()
                    
                    writer.writerow(row)
                    total_rows += 1
            
            print(f"  ✓ {info['district']}: {district_file.stat().st_size / 1024:.1f} KB")
    
    print(f"\n✅ Combined {total_rows} rows from {state_name} into {output_file}")
    print(f"📊 File size: {Path(output_file).stat().st_size / 1024 / 1024:.1f} MB")

def list_districts():
    """List all districts and their file sizes"""
    input_dir = Path("data/marriage_muhurats")
    districts_file = Path("data/districts_geonames.csv")
    
    if not input_dir.exists():
        print(f"❌ No data directory found at {input_dir}")
        return
    
    # Load district metadata
    district_info = {}
    with districts_file.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            district_info[row['geoname_id']] = row['district']
    
    total_size = 0
    total_files = 0
    
    for state_dir in sorted(input_dir.iterdir()):
        if not state_dir.is_dir():
            continue
            
        print(f"\n{state_dir.name}:")
        state_size = 0
        state_files = 0
        
        for district_file in sorted(state_dir.glob("*.csv")):
            size = district_file.stat().st_size
            state_size += size
            state_files += 1
            
            # Get district name from geoname_id
            geoname_id = district_file.stem.split('_')[-1]
            district_name = district_info.get(geoname_id, district_file.stem)
            
            # Count rows in file
            with district_file.open(encoding="utf-8") as f:
                row_count = sum(1 for line in f) - 1  # Subtract header
            
            print(f"  {district_name}: {size / 1024:.1f} KB ({row_count} dates)")
        
        print(f"  State total: {state_files} districts, {state_size / 1024:.1f} KB")
        total_size += state_size
        total_files += state_files
    
    print(f"\n📊 Overall total: {total_files} district files, {total_size / 1024 / 1024:.1f} MB")

def get_stats():
    """Get statistics about the scraped data"""
    input_dir = Path("data/marriage_muhurats")
    summary_file = Path("data/marriage_muhurats_summary.json")
    
    if summary_file.exists():
        import json
        with summary_file.open(encoding="utf-8") as f:
            summaries = json.load(f)
        
        print("📊 Marriage Muhurats Statistics:")
        print("=" * 50)
        
        # Group by state
        by_state = {}
        for key, count in summaries.items():
            state = key.split('/')[0]
            if state not in by_state:
                by_state[state] = {'districts': 0, 'dates': 0}
            by_state[state]['districts'] += 1
            by_state[state]['dates'] += count
        
        # Print state summaries
        total_dates = 0
        for state, stats in sorted(by_state.items()):
            print(f"{state:30s} {stats['districts']:3d} districts, {stats['dates']:6,d} dates")
            total_dates += stats['dates']
        
        print("=" * 50)
        print(f"{'Total':30s} {len(summaries):3d} districts, {total_dates:6,d} dates")
    else:
        print("No summary file found. Run the scraper first.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine district CSV files")
    parser.add_argument("action", choices=["combine", "state", "list", "stats"],
                      help="Action to perform: combine all, combine by state, list files, or show stats")
    parser.add_argument("--state", help="State name for 'state' action")
    parser.add_argument("--output", help="Output file name")
    
    args = parser.parse_args()
    
    if args.action == "combine":
        combine_all_districts(args.output or "data/combined_marriage_muhurats.csv")
    elif args.action == "state":
        if not args.state:
            print("❌ Please specify --state for state action")
            print("\nExample: python3 combine_districts.py state --state 'Uttar Pradesh'")
        else:
            combine_by_state(args.state, args.output)
    elif args.action == "list":
        list_districts()
    elif args.action == "stats":
        get_stats()