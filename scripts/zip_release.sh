#!/bin/bash
set -e

# Configuration
EXTRACT_DIR="/home/mike/Downloads/SRP/srp_for_oracle/Extract"
RELEASES_DIR="/home/mike/Downloads/SRP/srp_for_oracle/Releases"
TEMP_DIR="/tmp/srp_release_build_$$"
PASSWORD="SrpT3st2025!"
RELEASE_DATE=$(date +%Y%m%d_%H%M%S)
RELEASE_NAME="SRP_BATCH_REL_${RELEASE_DATE}.srp"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SRP Release Builder${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Extract dir: ${EXTRACT_DIR}"
echo -e "Releases dir: ${RELEASES_DIR}"
echo -e "Password: ${YELLOW}${PASSWORD}${NC}"
echo -e "Release file: ${GREEN}${RELEASE_NAME}${NC}"
echo ""

# Ensure directories exist
mkdir -p "${RELEASES_DIR}"
mkdir -p "${TEMP_DIR}"

# Change to extract directory
cd "${EXTRACT_DIR}"

# Count folders
FOLDER_COUNT=$(find . -maxdepth 1 -type d ! -path . | wc -l)
echo -e "${BLUE}Found ${FOLDER_COUNT} folders to zip${NC}"
echo ""

# Counter for progress
COUNT=0

# Zip each folder individually with password
for folder in */; do
    # Remove trailing slash
    folder_name="${folder%/}"

    # Skip if not a directory
    [ ! -d "$folder_name" ] && continue

    COUNT=$((COUNT + 1))

    # Create SRP file name (folder name + .srp extension)
    srp_file="${TEMP_DIR}/${folder_name}.srp"

    echo -e "${YELLOW}[${COUNT}/${FOLDER_COUNT}]${NC} Zipping ${GREEN}${folder_name}${NC}..."

    # Zip with password using AES encryption if available, otherwise standard
    if zip -h 2>&1 | grep -q "encryption method"; then
        # Use AES-256 encryption if available
        zip -r -q -P "${PASSWORD}" -e "${srp_file}" "${folder_name}"
    else
        # Fallback to standard encryption
        zip -r -q -P "${PASSWORD}" "${srp_file}" "${folder_name}"
    fi

    echo -e "   ✓ Created ${srp_file##*/}"
done

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Creating Release Package${NC}"
echo -e "${BLUE}========================================${NC}"

# Count SRP files created
SRP_COUNT=$(ls -1 "${TEMP_DIR}"/*.srp 2>/dev/null | wc -l)
echo -e "Packaging ${GREEN}${SRP_COUNT}${NC} .srp files into release..."

# Create the release zip containing all individual SRP files
cd "${TEMP_DIR}"
zip -r -q -P "${PASSWORD}" "${RELEASES_DIR}/${RELEASE_NAME}" *.srp

# Get file size
RELEASE_SIZE=$(du -h "${RELEASES_DIR}/${RELEASE_NAME}" | cut -f1)

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Release Created Successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "Location: ${RELEASES_DIR}/${RELEASE_NAME}"
echo -e "Size: ${RELEASE_SIZE}"
echo -e "Contains: ${SRP_COUNT} individual .srp files"
echo -e "Password: ${YELLOW}${PASSWORD}${NC}"
echo ""

# Cleanup temp directory
rm -rf "${TEMP_DIR}"

echo -e "${BLUE}Cleaned up temporary files${NC}"
echo -e "${GREEN}Done!${NC}"
