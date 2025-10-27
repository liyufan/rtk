#!/usr/bin/env bash
# Parse intersections from Excel and save to args.txt
# Usage: bash 1_parase_excel.sh [excel_path]

set -euo pipefail

EXCEL_PATH=${1:-"1.xls"}
# Expand ~ if present
EXCEL_PATH=${EXCEL_PATH/#~/$HOME}

# Generate args lines: index|name|"lat lon"|radius
ARGS_FILE="args.txt"
python3 src/excel.py "$EXCEL_PATH" > "$ARGS_FILE"
