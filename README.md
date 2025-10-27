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
bash 1_parase_excel.sh [excel_path]
```

## 2. Find events
This command will generate `events` directory containing JSON files of enter/exit events.
```bash
bash 2_find_events.sh [bag_dir]
```

## 3. Record odometry
This command will generate `processed_bags` directory containing rosbags of odometry data.
```bash
bash 3_record_odometry.sh
```

## 4. Extract odometry to CSV
This command will generate `csv_output` directory containing CSV files extracted from processed bags.
```bash
bash 4_extract_odom.sh
```

Example with custom topic:
```bash
bash 4_extract_odom.sh --topic /Odometry
```
