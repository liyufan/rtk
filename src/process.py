#!/usr/bin/env python3
"""
Script for processing enter/exit events in JSON files and executing corresponding recording commands
Usage: python process.py [events_directory] [save_directory] [--dry-run]
"""

import argparse
import glob
import json
import os
import signal
import subprocess
import sys
import time
from typing import Optional


def process_events(
    json_file: str,
    project_name: str,
    intersection_num: str,
    save_dir: str,
    dry_run: bool = False,
):
    """Process events in a single JSON file"""
    try:
        with open(json_file, 'r') as f:
            events = json.load(f)

        # Group events by bag_path
        bag_events = {}
        for event in events:
            bag_path = event['bag_path']
            if bag_path not in bag_events:
                bag_events[bag_path] = {'enter': [], 'exit': []}
            bag_events[bag_path][event['event']].append(event)

        # Collect all bag file information that needs to be processed
        bag_tasks = []
        for bag_path, events_by_type in bag_events.items():
            enter_events = events_by_type['enter']
            exit_events = events_by_type['exit']

            # Get original bag file name
            original_bag_name = os.path.basename(bag_path)

            # Determine recording file name, save to save directory
            output_bag_name = f'{project_name}_J{intersection_num}_{original_bag_name}'
            output_bag_path = os.path.join(save_dir, output_bag_name)

            print(f'Preparing to process bag file: {original_bag_name}')
            print(
                f'Number of enter events: {len(enter_events)}, Number of exit events: {len(exit_events)}'
            )

            # Handle different scenarios
            if len(enter_events) > 0 and len(exit_events) > 0:
                # Both enter and exit: find first enter and last exit
                first_enter = min(enter_events, key=lambda x: x['bag_rel_time'])
                last_exit = max(exit_events, key=lambda x: x['bag_rel_time'])

                start_time = int(first_enter['bag_rel_time'])
                end_time = int(last_exit['bag_rel_time'])
                duration = end_time - start_time

                print(
                    f'Start time: {start_time}, End time: {end_time}, Duration: {duration}'
                )

                bag_tasks.append((bag_path, start_time, duration, output_bag_path))

            elif len(enter_events) > 0 and len(exit_events) == 0:
                # Only enter, no exit: play from first enter time to end of bag
                first_enter = min(enter_events, key=lambda x: x['bag_rel_time'])
                start_time = int(first_enter['bag_rel_time'])

                print(
                    f'Only enter events, playing from time {start_time} to end of bag'
                )

                bag_tasks.append((bag_path, start_time, None, output_bag_path))

            elif len(enter_events) == 0 and len(exit_events) > 0:
                # Only exit, no enter: play from beginning to last exit time
                last_exit = max(exit_events, key=lambda x: x['bag_rel_time'])
                end_time = int(last_exit['bag_rel_time'])
                duration = end_time

                print(f'Only exit events, playing from beginning to time {end_time}')

                bag_tasks.append((bag_path, 0, duration, output_bag_path))

            else:
                print(
                    f'Warning: Bag file {original_bag_name} has no valid enter/exit events'
                )

            print('---')

        # Process all bag files serially
        for i, (bag_path, start_time, duration, output_bag_path) in enumerate(
            bag_tasks, 1
        ):
            # Convert output_bag_path to output_prefix (remove .bag extension)
            output_prefix = os.path.splitext(output_bag_path)[0]
            print(f'\n=== Starting to process bag file {i}/{len(bag_tasks)} ===')
            execute_recording_command(
                bag_path, start_time, duration, output_prefix, dry_run
            )
            print(f'=== Bag file {i}/{len(bag_tasks)} processing completed ===\n')

            # Automatically continue processing the next bag file

    except Exception as e:
        print(f'Error processing file {json_file}: {e}')


def rename_pcd_file(output_prefix: str, pcd_dir: str = None):
    """
    Rename scans.pcd file to prevent overwriting.
    Uses the same naming format as output_prefix.

    Args:
        output_prefix: Prefix for output files (without extension, used to generate matching name)
        pcd_dir: Directory where PCD files are stored (default: FAST_LIO/PCD/)
    """
    if pcd_dir is None:
        # Use rospack to find fast_lio package path
        try:
            result = subprocess.run(
                ['rospack', 'find', 'fast_lio'],
                capture_output=True,
                text=True,
                check=True,
            )
            fast_lio_path = result.stdout.strip()
            pcd_dir = os.path.join(fast_lio_path, 'PCD')
        except subprocess.CalledProcessError:
            print(
                'Warning: Could not find fast_lio package using rospack, skipping PCD rename'
            )
            return
        except FileNotFoundError:
            print('Warning: rospack command not found, skipping PCD rename')
            return

        if not os.path.isdir(pcd_dir):
            print(f'Warning: PCD directory not found at {pcd_dir}, skipping PCD rename')
            return

    scans_pcd_path = os.path.join(pcd_dir, 'scans.pcd')

    if not os.path.exists(scans_pcd_path):
        print(f'Warning: PCD file not found at {scans_pcd_path}, skipping rename')
        return

    # Generate filename matching output_prefix format
    output_prefix_name = os.path.basename(output_prefix)
    new_pcd_name = f'{output_prefix_name}.pcd'
    new_pcd_path = os.path.join(pcd_dir, new_pcd_name)

    try:
        os.rename(scans_pcd_path, new_pcd_path)
        print(f'PCD file renamed: {scans_pcd_path} -> {new_pcd_path}')
    except Exception as e:
        print(f'Error renaming PCD file: {e}')


def execute_recording_command(
    bag_path: str,
    start_time: int,
    duration: Optional[int],
    output_prefix: str,
    dry_run: bool = False,
):
    """Execute recording command"""
    # Generate full output bag path from prefix
    output_bag_path = f'{output_prefix}.bag'

    duration_str = str(duration) if duration is not None else 'to end of bag'
    bag_name = os.path.basename(bag_path)
    output_name = os.path.basename(output_bag_path)

    print(f'==========================================')
    print(f'Starting to process bag file: {bag_name}')
    print(f'Output file: {output_name}')
    print(f'Save path: {output_bag_path}')
    print(f'Start time: {start_time}, Duration: {duration_str}')

    if dry_run:
        print('*** Dry run mode: commands not actually executed ***')
        print(f'Commands that would be executed:')
        print(f'1. roslaunch fast_lio mapping_avia.launch')
        print(f'2. rosbag record -O {output_bag_path} /Odometry')
        duration_part = f'--duration {duration}' if duration is not None else ''
        print(f'3. rosbag play {bag_path} --start {start_time} {duration_part}')
        output_prefix_name = os.path.basename(output_prefix)
        new_pcd_name = f'{output_prefix_name}.pcd'
        print(f'4. Rename PCD file: scans.pcd -> {new_pcd_name}')
        return

    # Store process objects
    processes = []

    try:
        # 1. Start fast_lio
        print('Starting fast_lio...')
        fast_lio_process = subprocess.Popen(
            ['roslaunch', 'fast_lio', 'mapping_avia.launch'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        )
        processes.append(('fast_lio', fast_lio_process))

        # Wait for fast_lio to start
        print('Waiting for fast_lio to start...')
        time.sleep(8)

        # 2. Start recording
        print('Starting recording...')
        record_process = subprocess.Popen(
            ['rosbag', 'record', '-O', output_bag_path, '/Odometry'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        )
        processes.append(('rosbag_record', record_process))

        # Wait for recording to start
        print('Waiting for recording to start...')
        time.sleep(5)

        # 3. Play bag file
        print('Starting to play bag file...')
        play_cmd = ['rosbag', 'play', bag_path, '--start', str(start_time)]
        if duration is not None:
            play_cmd.extend(['--duration', str(duration)])

        print(f'Play command: {" ".join(play_cmd)}')
        play_process = subprocess.Popen(
            play_cmd,
            stdout=None,  # Show output
            stderr=None,  # Show errors
            preexec_fn=os.setsid,
        )
        processes.append(('rosbag_play', play_process))

        # Wait for playback to complete
        print('Waiting for playback to complete...')
        play_process.wait()

        print(f'Bag file {bag_name} processing completed')

        # Stop recording first (no longer needed)
        print('Stopping recording...')
        if record_process.poll() is None:
            try:
                os.killpg(os.getpgid(record_process.pid), signal.SIGTERM)
                record_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(record_process.pid), signal.SIGKILL)
            except Exception as e:
                print(f'Error stopping recording: {e}')

        # Close fast_lio gracefully to trigger PCD save
        # fast_lio only saves PCD when it receives SIGINT (Ctrl+C)
        print('Closing fast_lio to save PCD file...')
        if fast_lio_process.poll() is None:
            try:
                # Send SIGINT to fast_lio (same as Ctrl+C), which will trigger PCD save
                os.killpg(os.getpgid(fast_lio_process.pid), signal.SIGINT)
                # Wait for fast_lio to exit (it will save PCD during shutdown)
                print('Waiting for fast_lio to save PCD and exit...')
                fast_lio_process.wait(timeout=10)
                print('fast_lio exited successfully')
            except subprocess.TimeoutExpired:
                print('Warning: fast_lio did not exit in time, forcing kill')
                os.killpg(os.getpgid(fast_lio_process.pid), signal.SIGKILL)
            except Exception as e:
                print(f'Error closing fast_lio: {e}')

        # Wait a moment to ensure PCD file is written to disk
        time.sleep(1)

        # Rename PCD file after fast_lio has saved it
        print('Renaming PCD file...')
        rename_pcd_file(output_prefix)

    except Exception as e:
        print(f'Error during processing: {e}')

    finally:
        # Clean up any remaining processes
        print('Cleaning up remaining processes...')
        for name, process in processes:
            try:
                if process.poll() is None:  # Process is still running
                    print(f'Terminating remaining process: {name}')
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    time.sleep(2)
                    if process.poll() is None:  # If still not ended, force kill
                        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except Exception as e:
                print(f'Error cleaning up process {name}: {e}')

        print('==========================================')


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Process enter/exit events in JSON files and execute recording commands',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'events_dir',
        nargs=argparse.OPTIONAL,
        default='events',
        help='JSON files directory',
    )
    parser.add_argument(
        'save_dir',
        nargs=argparse.OPTIONAL,
        default='processed_bags',
        help='Directory to save recorded files',
    )
    parser.add_argument(
        '--project-name',
        default='CEDD',
        help='Project name',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode, only show commands without actual execution',
    )

    args = parser.parse_args()

    # Check if outputs directory exists
    if not os.path.isdir(args.events_dir):
        print(f"Error: json directory '{args.events_dir}' does not exist")
        sys.exit(1)

    # Check if save directory exists, create if not
    if not os.path.isdir(args.save_dir):
        print(f"Creating save directory: {args.save_dir}")
        os.makedirs(args.save_dir, exist_ok=True)
    print(f"Starting to process json directory: {args.events_dir}")
    print(f"Save directory: {args.save_dir}")
    print(f"Project name: {args.project_name}")
    if args.dry_run:
        print("*** Dry run mode - only show commands, no actual execution ***")
    print("==========================================")

    # Get all JSON files
    json_files = glob.glob(os.path.join(args.events_dir, '*.json'))

    if not json_files:
        print(f"No JSON files found in {args.events_dir}")
        return

    # Process each JSON file
    for json_file in sorted(json_files):
        filename = os.path.basename(json_file)
        filename_without_ext = os.path.splitext(filename)[0]

        # Extract intersection number from filename (assuming format "number_intersection_name")
        intersection_num = filename_without_ext.split('_')[0]

        print(f'Processing file: {filename} (intersection number: {intersection_num})')
        process_events(
            json_file, args.project_name, intersection_num, args.save_dir, args.dry_run
        )
        print('==========================================')

    print('All files processing completed')


if __name__ == '__main__':
    main()
