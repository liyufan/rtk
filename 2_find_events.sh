#!/usr/bin/env bash
# Run find.py for each row in args.txt
# Usage: ./2_find_events.sh [bags_dir] [args_file]

set -euo pipefail

BAGS_DIR=${1:-"/path/to/bags"}
# Expand ~ if present
BAGS_DIR=${BAGS_DIR/#~/$HOME}

ARGS_FILE=${2:-"args.txt"}

# Create output directory
OUT_DIR="events"
mkdir -p "$OUT_DIR"

while IFS='|' read -r IDX NAME COORDS RADIUS; do
  # Sanitize name for filename (replace spaces and slashes)
  SAFE_NAME=$(echo "$NAME" | sed 's/[\\/:\*?"<>|]/_/g; s/ /_/g')
  OUT_FILE="$OUT_DIR/${IDX}_${SAFE_NAME}.json"
  echo "Processing $NAME (index=$IDX, radius=$RADIUS) -> $OUT_FILE" >&2
  python3 src/find.py \
    "$BAGS_DIR" "$COORDS" "$RADIUS" \
    --cache --cache-dir gps_cache --cache-format parquet \
    --output "$OUT_FILE"
done < "$ARGS_FILE"

echo -e "\nDone. Outputs in $OUT_DIR" >&2
