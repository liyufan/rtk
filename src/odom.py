#!/usr/bin/env python3
"""
Extract odometry data from ROS bag files to CSV format
Usage: python odom.py [bag_dir] [output_dir] [--topic TOPIC]
"""

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Tuple

import numpy as np
import rosbag


def quaternion_to_euler(
    x: float, y: float, z: float, w: float
) -> Tuple[float, float, float]:
    """
    Convert quaternion to Euler angles (roll, pitch, yaw) in radians.

    Args:
        x, y, z, w: Quaternion components

    Returns:
        tuple: (roll, pitch, yaw) in radians
    """
    # Roll (x-axis rotation)
    sinr_cosp = 2 * (w * x + y * z)
    cosr_cosp = 1 - 2 * (x * x + y * y)
    roll = np.arctan2(sinr_cosp, cosr_cosp)

    # Pitch (y-axis rotation)
    sinp = 2 * (w * y - z * x)
    if abs(sinp) >= 1:
        pitch = np.sign(sinp) * np.pi / 2  # Use 90 degrees if out of range
    else:
        pitch = np.arcsin(sinp)

    # Yaw (z-axis rotation)
    siny_cosp = 2 * (w * z + x * y)
    cosy_cosp = 1 - 2 * (y * y + z * z)
    yaw = np.arctan2(siny_cosp, cosy_cosp)

    return roll, pitch, yaw


def extract_odometry_to_csv(bag_file: str, output_csv: str, topic: str = '/Odometry'):
    """
    Extract Odometry data from a ROS bag file and save to CSV.

    Args:
        bag_file: Path to the ROS bag file
        output_csv: Path to the output CSV file
        topic: ROS topic to read (default: '/Odometry')
    """
    try:
        # Open the bag file
        bag = rosbag.Bag(bag_file, 'r')
    except Exception as e:
        print(f"Error: Could not open {bag_file}: {str(e)}")
        return

    # Open CSV file for writing
    try:
        with open(output_csv, 'w', newline='') as csvfile:
            # Define CSV headers
            # Format: Row, x, y, Yaw, T
            headers = ['Row', 'x', 'y', 'Yaw', 'T']

            writer = csv.writer(csvfile)
            writer.writerow(headers)  # Write the header row

            row = 0

            # Iterate through the bag file
            for topic_name, msg, t in bag.read_messages(topics=[topic]):
                # Extract timestamp (in seconds)
                timestamp = msg.header.stamp.to_sec()

                # Extract position
                x = msg.pose.pose.position.x
                y = msg.pose.pose.position.y
                z = msg.pose.pose.position.z

                # Extract orientation (quaternion)
                qx = msg.pose.pose.orientation.x
                qy = msg.pose.pose.orientation.y
                qz = msg.pose.pose.orientation.z
                qw = msg.pose.pose.orientation.w

                # Convert quaternion to Euler angles
                roll, pitch, yaw = quaternion_to_euler(qx, qy, qz, qw)

                # Write data to CSV
                writer.writerow([row, x, y, yaw, timestamp])
                row += 1

    except Exception as e:
        print(f"Error: Could not write to {output_csv}: {str(e)}")
        bag.close()
        return

    # Close the bag file
    bag.close()
    print(f"Data successfully saved to {output_csv}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Extract odometry data from ROS bag files to CSV format',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'bag_dir',
        nargs='?',
        default='processed_bags',
        help='Directory of processed bags containing odometry data',
    )
    parser.add_argument(
        'output_dir',
        nargs='?',
        default='csv_output',
        help='Directory to save output CSV files',
    )
    parser.add_argument(
        '--topic',
        default='/Odometry',
        help='ROS topic to extract from (default: /Odometry)',
    )
    parser.add_argument(
        '--pattern',
        default='*.bag',
        help='File pattern to match (default: *.bag)',
    )

    args = parser.parse_args()

    # Check if bag directory exists
    if not os.path.isdir(args.bag_dir):
        print(f"Error: Bag directory '{args.bag_dir}' does not exist")
        sys.exit(1)

    # Create output directory if it doesn't exist
    if not os.path.isdir(args.output_dir):
        print(f"Creating output directory: {args.output_dir}")
        os.makedirs(args.output_dir, exist_ok=True)

    print(f"Input directory: {args.bag_dir}")
    print(f"Output directory: {args.output_dir}")
    print(f"Topic: {args.topic}")
    print(f"Pattern: {args.pattern}")
    print("=" * 50)

    # Get all bag files
    bag_files = list(Path(args.bag_dir).glob(args.pattern))
    bag_files.sort()

    if not bag_files:
        print(f"No bag files found in {args.bag_dir} matching pattern {args.pattern}")
        return

    print(f"Found {len(bag_files)} bag file(s) to process\n")

    # Process each bag file
    for i, bag_file in enumerate(bag_files, 1):
        print(f"Processing ({i}/{len(bag_files)}): {bag_file.name}")

        # Generate output CSV filename
        output_csv = os.path.join(args.output_dir, bag_file.stem + '.csv')

        # Extract odometry to CSV
        extract_odometry_to_csv(str(bag_file), output_csv, args.topic)
        print()

    print(f"All files processed. Outputs saved to {args.output_dir}")


if __name__ == '__main__':
    main()
