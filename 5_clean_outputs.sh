#!/usr/bin/env bash
# Clean outputs
# Usage: yes | ./5_clean_outputs.sh

set -euo pipefail

OUTPUTS=(csv_output events gps_cache processed_bags traj* args.txt)

read -p "Clean all outputs? (y/n) " clean_all
if [ "$clean_all" = "y" ]; then
    for output in "${OUTPUTS[@]}"; do
        rm -rf $output
    done
    echo "All outputs cleaned"
    exit 0
else
    for output in "${OUTPUTS[@]}"; do
        read -p "Clean $output? (y/n) " clean_output
        if [ "$clean_output" = "y" ]; then
            rm -rf $output
        fi
    done
fi
