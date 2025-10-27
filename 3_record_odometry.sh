#!/usr/bin/env bash
# Record odometry from rosbag and save to csv
# Usage: bash 3_record_odometry.sh [events_directory] [save_directory]

set -euo pipefail

EVENTS_DIR=${1:-"events"}
SAVE_DIR=${2:-"processed_bags"}

python3 src/process.py $EVENTS_DIR $SAVE_DIR
