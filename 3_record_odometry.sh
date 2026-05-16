#!/usr/bin/env bash
# Record odometry from rosbag and save to csv
# Usage: ./3_record_odometry.sh [events_or_bags_directory]

set -euo pipefail

EVENTS_OR_BAGS_DIR=${1:-"events"}
# Expand ~ if present
EVENTS_OR_BAGS_DIR=${EVENTS_OR_BAGS_DIR/#~/$HOME}

SAVE_DIR=${2:-"processed_bags"}

# For events directory generated in step 2
python3 src/process.py $SAVE_DIR --events-dir $EVENTS_OR_BAGS_DIR

# For bags directory containing already filtered bags
# python3 src/process.py $SAVE_DIR --bags-dir $EVENTS_OR_BAGS_DIR
