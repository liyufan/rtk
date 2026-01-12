#!/usr/bin/env bash
# Record odometry from rosbag and save to csv
# Usage: ./3_record_odometry.sh [project_name] [bags_or_events_directory] [save_directory]

set -euo pipefail

PROJECT_NAME=${1:-"CEDD"}

EVENTS_OR_BAGS_DIR=${2:-"events"}
# Expand ~ if present
EVENTS_OR_BAGS_DIR=${EVENTS_OR_BAGS_DIR/#~/$HOME}

SAVE_DIR=${3:-"processed_bags"}

# For events directory generated in step 2
python3 src/process.py $SAVE_DIR --project-name $PROJECT_NAME --events-dir $EVENTS_OR_BAGS_DIR

# For bags directory containing already filtered bags
# python3 src/process.py $SAVE_DIR --project-name $PROJECT_NAME --bags-dir $EVENTS_OR_BAGS_DIR
