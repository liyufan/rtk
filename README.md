# rtk

## 0. Setup
```bash
conda create -n rtk python=3
conda activate rtk
pip install -r requirements.txt
```

## 1. Parse excel to args.txt
This command will generate `args.txt` in the current directory. Check `args.txt` to make sure the excel is parsed correctly. Remove the rows you don't want to process.
```bash
./1_parse_excel.sh [excel_path]
```

## 2. Find events
This command will generate `events` directory containing JSON files of enter/exit events.
```bash
./2_find_events.sh [bags_dir]
```

## 3. Record odometry
This command will generate `processed_bags` directory containing rosbags of odometry data.

**Please modify the script to use either events directory or bags directory.**
```bash
./3_record_odometry.sh [project_name] [bags_or_events_directory]
```

## 4. Extract odometry to CSV
This command will generate `csv_output` directory containing CSV files extracted from processed bags.
```bash
./4_extract_odom.sh
```
