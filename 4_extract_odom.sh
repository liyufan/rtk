#!/usr/bin/env bash
# Extract odometry from rosbags to CSV
# Usage: ./4_extract_odom.sh [bags_dir] [output_dir] [topic]

set -euo pipefail

BAGS_DIR=${1:-"processed_bags"}
OUTPUT_DIR=${2:-"csv_output"}
TOPIC=${3:-"/Odometry"}

python3 src/odom.py "$BAGS_DIR" "$OUTPUT_DIR" --topic $TOPIC
