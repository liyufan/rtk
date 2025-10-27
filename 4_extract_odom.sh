#!/usr/bin/env bash
# Extract odometry from rosbags to CSV
# Usage: bash 4_extract_odom.sh [bag_dir] [output_dir] [--topic TOPIC]

set -euo pipefail

BAG_DIR=${1:-"processed_bags"}
OUTPUT_DIR=${2:-"csv_output"}

# Additional arguments (e.g., --topic, --pattern) can be passed after positional args
shift 2 2>/dev/null || true
EXTRA_ARGS=("$@")

python3 src/odom.py "$BAG_DIR" "$OUTPUT_DIR" "${EXTRA_ARGS[@]}"
