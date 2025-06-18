#!/usr/bin/env bash
set -e

# ── 0) Check if we're in Docker and set up NordVPN ──────────────────────────
if [ -f /.dockerenv ]; then
    echo "🐳 Running in Docker container"
    
    # Start NordVPN daemon if not running
    if ! pgrep -x "nordvpnd" > /dev/null; then
        echo "Starting NordVPN daemon..."
        /etc/init.d/nordvpn start
        sleep 2
    fi
    
    # Check if logged in
    if ! nordvpn account 2>&1 | grep -q "Email Address:"; then
        echo "❌ NordVPN not logged in. Please run:"
        echo "   nordvpn login --token YOUR_TOKEN_HERE"
        exit 1
    fi
    
    # Connect to VPN if not connected
    if ! nordvpn status | grep -q "Status: Connected"; then
        echo "🌐 Connecting to NordVPN..."
        nordvpn connect
        sleep 5
    fi
    
    echo "✅ NordVPN Status:"
    nordvpn status | grep -E "(Status:|Current server:|Country:)"
    echo ""
fi

# ── 1) Python setup & dependencies ────────────────────────────────────────────
# In Docker, we don't need venv - use system Python
if [ -f /.dockerenv ]; then
    echo "📦 Installing system dependencies..."
    echo "   Running: apt-get update"
    apt-get update || { echo "❌ apt-get update failed"; exit 1; }
    
    echo "   Installing: python3 python3-pip curl jq"
    apt-get install -y python3 python3-pip curl jq || { echo "❌ apt-get install failed"; exit 1; }
    
    echo "📦 Installing Python dependencies..."
    echo "   Checking pip3 version:"
    pip3 --version
    
    echo "   Installing from requirements.txt:"
    pip3 install --break-system-packages -r requirements.txt || { echo "❌ pip install requirements failed"; exit 1; }
    
    echo "   Installing rapidfuzz:"
    pip3 install --break-system-packages rapidfuzz || { echo "❌ pip install rapidfuzz failed"; exit 1; }
    
    echo "✅ Dependencies installed successfully"
    
    # Set Python alias for convenience
    alias python=python3
    alias pip=pip3
else
    # Non-Docker environment - use venv
    if [ ! -d "venv" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
    fi
    source venv/bin/activate
    
    echo "Installing dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
    pip install rapidfuzz
fi

# Note: Playwright is not needed since the scraper uses requests/BeautifulSoup

# ── 2) Ensure data directory exists ───────────────────────────────────────────
mkdir -p data

# ── 3) Check for required input files ─────────────────────────────────────────
GEONAMES_CSV="data/districts_geonames.csv"

# Check if we have the districts with geonames file
if [ ! -s "$GEONAMES_CSV" ]; then
    echo "❌ $GEONAMES_CSV missing or empty."
    echo "   Please ensure you have a CSV with 'state', 'district', and 'geoname_id' columns."
    exit 1
fi

echo "✅ $GEONAMES_CSV found with $(tail -n +2 "$GEONAMES_CSV" | wc -l) districts"

# ── 4) Check if missing districts recovery is needed ──────────────────────────
MISSING_CSV="data/missing_districts.csv"
if [ -s "$MISSING_CSV" ]; then
    echo "🔍 Found missing districts file. Attempting recovery..."
    
    # Check if we have the GeoNames data file
    GEONAMES_DATA="data/IN.txt"
    if [ ! -s "$GEONAMES_DATA" ]; then
        echo "⚠️  GeoNames data file (IN.txt) not found."
        echo "   Download from: https://download.geonames.org/export/dump/IN.zip"
        echo "   Extract IN.txt to data/ directory"
        echo "   Skipping recovery step..."
    else
        python3 src/recover_missing_districts.py
    fi
else
    echo "ℹ️  No missing districts file found. Skipping recovery step."
fi

# ── 5) VPN Location Management ────────────────────────────────────────────────
# Get all available countries and cities from NordVPN
echo "🌍 Fetching available VPN locations..."
VPN_LOCATIONS_FILE="/tmp/nordvpn_locations.txt"
BLOCKED_SERVERS_FILE="/tmp/blocked_servers.txt"

# Create blocked servers file if it doesn't exist
touch "$BLOCKED_SERVERS_FILE"

# Function to get available VPN locations
get_vpn_locations() {
    # Get list of countries - they're in 2 columns separated by spaces
    # We need to split them properly
    nordvpn countries | grep -v "A new version" | grep -v "Virtual location" | awk '{if($1) print $1; if($2) print $2}' | grep -v '^$' | sort -u > "$VPN_LOCATIONS_FILE"
    
    # Count available locations
    LOCATION_COUNT=$(wc -l < "$VPN_LOCATIONS_FILE")
    echo "✅ Found $LOCATION_COUNT available countries"
    
    # Debug: Show first few countries
    echo "📍 Sample countries: $(head -5 "$VPN_LOCATIONS_FILE" | tr '\n' ', ' | sed 's/,$//')"
}

# Function to get current server hostname
get_current_server() {
    nordvpn status | grep "Hostname:" | cut -d' ' -f2 || echo ""
}

# Function to rotate VPN on error
rotate_vpn() {
    if [ -f /.dockerenv ]; then
        echo "🔄 Rotating VPN connection..."
        
        # Get current server before disconnecting
        CURRENT_SERVER=$(get_current_server)
        if [ ! -z "$CURRENT_SERVER" ]; then
            echo "📍 Marking server as blocked: $CURRENT_SERVER"
            echo "$CURRENT_SERVER" >> "$BLOCKED_SERVERS_FILE"
        fi
        
        nordvpn disconnect
        sleep 2
        
        # Get fresh list of locations if we don't have it
        if [ ! -s "$VPN_LOCATIONS_FILE" ]; then
            get_vpn_locations
        fi
        
        # Try connecting to different locations
        CONNECTED=false
        ATTEMPTS=0
        MAX_ATTEMPTS=20
        
        while [ "$CONNECTED" = false ] && [ $ATTEMPTS -lt $MAX_ATTEMPTS ]; do
            # Pick a random country from the list
            RANDOM_COUNTRY=$(shuf -n 1 "$VPN_LOCATIONS_FILE")
            
            echo "🌍 Attempting to connect to $RANDOM_COUNTRY..."
            
            # Try to connect and capture the output
            CONNECT_OUTPUT=$(nordvpn connect "$RANDOM_COUNTRY" 2>&1)
            
            if echo "$CONNECT_OUTPUT" | grep -q "You are connected"; then
                # Extract the server from the output (e.g., de744.nordvpn.com)
                NEW_SERVER=$(echo "$CONNECT_OUTPUT" | grep -oE '[a-z]+[0-9]+\.nordvpn\.com' | head -1)
                
                if [ -z "$NEW_SERVER" ]; then
                    # Try alternative extraction method
                    NEW_SERVER=$(nordvpn status | grep "Hostname:" | awk '{print $2}')
                fi
                
                # Check if this server is blocked
                if [ ! -z "$NEW_SERVER" ] && grep -q "^$NEW_SERVER$" "$BLOCKED_SERVERS_FILE"; then
                    echo "⚠️  Connected to blocked server $NEW_SERVER, trying again..."
                    nordvpn disconnect
                    sleep 1
                else
                    CONNECTED=true
                    echo "✅ Connected successfully!"
                    sleep 3
                    nordvpn status | grep -E "(Status:|Current server:|Country:|City:)"
                fi
            else
                echo "❌ Failed to connect to $RANDOM_COUNTRY"
                # Debug: show the actual error
                echo "   Error: $(echo "$CONNECT_OUTPUT" | grep -v "A new version" | head -1)"
            fi
            
            ATTEMPTS=$((ATTEMPTS + 1))
        done
        
        if [ "$CONNECTED" = false ]; then
            echo "❌ Could not connect to any unblocked server after $MAX_ATTEMPTS attempts!"
            echo "🔄 Clearing blocked servers list and trying again..."
            > "$BLOCKED_SERVERS_FILE"
            
            # Try one more time with a clean slate
            if nordvpn connect; then
                echo "✅ Connected with clean slate"
            else
                echo "❌ Fatal: Cannot establish VPN connection"
                exit 1
            fi
        fi
    fi
}

# Get initial VPN locations
if [ -f /.dockerenv ]; then
    get_vpn_locations
fi

# ── 6) Run the web scraper with VPN rotation support ──────────────────────────
echo ""
echo "🕷️  Starting web scraper..."
echo "=================================================="

# Run scraper with error handling
MAX_RETRIES=10
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    echo "🚀 Scraping attempt $((RETRY_COUNT + 1))/$MAX_RETRIES"
    
    # Run the scraper and capture exit code
    if python3 src/web-scrape.py; then
        echo "✅ Scraping completed successfully!"
        break
    else
        EXIT_CODE=$?
        echo "❌ Scraper failed with exit code: $EXIT_CODE"
        
        if [ $RETRY_COUNT -lt $((MAX_RETRIES - 1)) ]; then
            # Rotate VPN immediately if blocked (exit code 2)
            if [ $EXIT_CODE -eq 2 ]; then
                rotate_vpn
                echo "⏳ Waiting 5 seconds before retry..."
                sleep 5
            else
                # For other errors, wait a bit longer
                echo "⏳ Waiting 10 seconds before retry..."
                sleep 10
            fi
        fi
        
        RETRY_COUNT=$((RETRY_COUNT + 1))
    fi
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ Scraping failed after $MAX_RETRIES attempts"
    exit 1
fi

# ── 7) Show results ───────────────────────────────────────────────────────────
OUTPUT_DIR="data/marriage_muhurats"
SUMMARY_FILE="data/marriage_muhurats_summary.json"

if [ -d "$OUTPUT_DIR" ]; then
    echo ""
    echo "📊 Results:"
    
    # Count total files and calculate size
    TOTAL_FILES=$(find "$OUTPUT_DIR" -name "*.csv" | wc -l)
    TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" 2>/dev/null | cut -f1)
    
    echo "   Total district files: $TOTAL_FILES"
    echo "   Total size: $TOTAL_SIZE"
    echo "   Output directory: $OUTPUT_DIR"
    
    if [ -f "$SUMMARY_FILE" ]; then
        echo ""
        echo "📋 Summary statistics available in: $SUMMARY_FILE"
    fi
fi

echo ""
echo "✨ All done!"

# Cleanup
rm -f "$VPN_LOCATIONS_FILE"