#!/bin/bash

# Colors for better output
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Directory setup
DOWNLOAD_DIR="${DOWNLOAD_DIR:-$FLOX_ENV_PROJECT}"
FILE_ID="1QnIiS70uiRtZYniw12Uh645zR7EBz2j0"
FILENAME="iowa_liquor_sales.csv"
FULL_PATH="${DOWNLOAD_DIR}/${FILENAME}"

# Create download directory
mkdir -p "$DOWNLOAD_DIR"

# Check if file exists
if [[ -f "$FULL_PATH" ]]; then
    SIZE=$(du -h "$FULL_PATH" | cut -f1)
    echo -e "${YELLOW}File already exists: $SIZE${NC}"
    
    # Ask to resume or restart download
    echo -n "Restart download from beginning? [y/N] "
    read -r answer
    if [[ "$answer" =~ ^[Yy] ]]; then
        echo "Removing existing file..."
        rm -f "$FULL_PATH"
    else
        echo "Keeping existing file. Exiting."
        exit 0
    fi
fi

# Start download in background
echo -e "${CYAN}Starting download...${NC}"
curl -s -L -o "$FULL_PATH" \
    "https://drive.usercontent.google.com/download?id=${FILE_ID}&export=download&confirm=t" >/dev/null 2>&1 &
curl_pid=$!

# Spinner animation with file size
spinner=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
i=0
last_size="0"

echo "Press Ctrl+C to cancel download"

# Trap Ctrl+C to clean up
trap 'echo -e "\n${RED}Download cancelled${NC}"; kill $curl_pid 2>/dev/null; exit 1' INT

while kill -0 $curl_pid 2>/dev/null; do
    i=$(( (i+1) % ${#spinner[@]} ))
    
    if [[ -f "$FULL_PATH" ]]; then
        current_size=$(du -h "$FULL_PATH" 2>/dev/null | cut -f1 || echo "calculating...")
        
        # Only update if size changed
        if [[ "$current_size" != "$last_size" ]]; then
            last_size="$current_size"
        fi
        
        echo -ne "\r${spinner[$i]} Downloaded: $last_size     "
    else
        echo -ne "\r${spinner[$i]} Initializing download...     "
    fi
    
    sleep 0.2
done

echo -e "\n"

# Check result
if [[ -s "$FULL_PATH" ]]; then
    final_size=$(du -h "$FULL_PATH" | cut -f1)
    echo -e "${GREEN}✓ Downloaded sample database file ($final_size)${NC}"
    echo -e "File location: ${CYAN}$FULL_PATH${NC}"
else
    echo -e "${RED}✗ Download failed${NC}"
    rm -f "$FULL_PATH"
    exit 1
fi
