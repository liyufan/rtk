#!/usr/bin/env python3
"""
Parse intersections from an Excel file and emit arguments for find.py.

Input Excel is expected to contain columns for index, name, coordinates, and radius.
Column headers are matched fuzzily to support Chinese/English variants:
  - index: 序号, 索引, id, index, 序
  - name: 街道, 路口, 名称, name, 标题
  - coordinates: 坐标, 位置, 经纬度, latlon, coords, latitude/longitude pair or separate lat/lon columns
  - radius: 半径, 范围, 半径m, radius

Output format (default): one line per row, pipe-delimited fields suitable for bash:
  序号|街道名|"lat lon"|radius

Example usage:
  python3 excel.py "庙街项目关键街道坐标.xls" > args.txt
  # Each line can be read with: IFS='|' read -r idx name coords radius
"""

import argparse
import math
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


def _normalize_column(name: str) -> str:
    return re.sub(r"\s+", "", str(name or "").strip().lower())


def _match_column(cols: List[str], candidates: List[str]) -> Optional[int]:
    for i, c in enumerate(cols):
        for cand in candidates:
            if cand in c:
                return i
    return None


def _parse_coordinate_str(s: str) -> Optional[Tuple[float, float]]:
    if not s:
        return None
    s = str(s).strip()
    # Normalize common Chinese punctuation and wrappers
    s = s.replace("，", ",").replace("、", ",").replace("；", ",")
    s = s.replace("（", "(").replace("）", ")")
    s = s.replace("：", ":")
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1].strip()

    # Try to extract two numbers anywhere in the string
    nums = re.findall(r"[-+]?\d*\.?\d+", s)
    if len(nums) >= 2:
        try:
            a = float(nums[0])
            b = float(nums[1])
            # Auto-detect lon/lat order: if first looks like lon (>90), swap
            if abs(a) > 90 and abs(b) <= 90:
                lat, lon = b, a
            else:
                lat, lon = a, b
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return lat, lon
        except Exception:
            pass

    # Fallback: comma-separated strict
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) == 2:
            try:
                a = float(parts[0])
                b = float(parts[1])
                if abs(a) > 90 and abs(b) <= 90:
                    return b, a
                return a, b
            except Exception:
                pass
    # Fallback: space-separated strict
    parts = s.split()
    if len(parts) == 2:
        try:
            a = float(parts[0])
            b = float(parts[1])
            if abs(a) > 90 and abs(b) <= 90:
                return b, a
            return a, b
        except Exception:
            pass
    return None


def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    # Extract first numeric token to support values like "50米", "约 30 m"
    try:
        s = str(x)
        m = re.search(r"[-+]?\d*\.?\d+", s)
        if m:
            xf = float(m.group(0))
            if math.isfinite(xf):
                return xf
    except Exception:
        pass
    try:
        xf = float(x)  # final attempt
        if math.isfinite(xf):
            return xf
    except Exception:
        return None
    return None


def load_rows_from_excel(
    xls_path: str, default_radius: float, sheet: Optional[str] = None
) -> List[Dict[str, Any]]:
    try:
        import pandas as pd
    except Exception as e:
        print("ERROR: pandas is required to parse Excel files", file=sys.stderr)
        raise e

    try:
        # pick engine explicitly to avoid pandas guessing errors
        ext = os.path.splitext(xls_path)[1].lower()
        engine = None
        if ext in (".xls",):
            engine = "xlrd"
        elif ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            engine = "openpyxl"
        read_kwargs: Dict[str, Any] = {}
        if sheet is not None:
            read_kwargs["sheet_name"] = sheet
        if engine is not None:
            read_kwargs["engine"] = engine
        df = pd.read_excel(xls_path, **read_kwargs)
    except Exception as e:
        print(f"ERROR: failed to read Excel '{xls_path}': {e}", file=sys.stderr)
        raise e

    if df is None or df.empty:
        return []

    norm_cols = [_normalize_column(c) for c in df.columns]

    idx_col = _match_column(
        norm_cols,
        [
            "序号",
            "索引",
            "index",
            "编号",
            "id",
            "序",
        ],
    )  # type: ignore[arg-type]
    name_col = _match_column(
        norm_cols,
        [
            "街道",
            "街道名",
            "路口",
            "路口名",
            "名称",
            "name",
            "地点",
            "标题",
            "位置名",
        ],
    )  # type: ignore[arg-type]
    coord_col = _match_column(
        norm_cols,
        [
            "坐标",
            "经纬度",
            "坐標",
            "latlon",
            "coords",
            "coordinate",
            "坐标(纬度经度)",
        ],
    )  # type: ignore[arg-type]
    radius_col = _match_column(
        norm_cols,
        [
            "半径",
            "半径m",
            "半径（m）",
            "半徑",
            "范围",
            "半径(米)",
            "radius",
            "range",
        ],
    )  # type: ignore[arg-type]

    # Also support separate lat/lon columns
    lat_col = _match_column(
        norm_cols,
        [
            "纬度",
            "緯度",
            "lat",
            "latitude",
            "纬度(度)",
            "纬度°",
            "y",
            "y坐标",
        ],
    )  # type: ignore[arg-type]
    lon_col = _match_column(
        norm_cols,
        [
            "经度",
            "經度",
            "lon",
            "lng",
            "longitude",
            "经度(度)",
            "经度°",
            "x",
            "x坐标",
        ],
    )  # type: ignore[arg-type]

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        idx_val: Any = row.iloc[idx_col] if idx_col is not None else None
        name_val: Any = row.iloc[name_col] if name_col is not None else None

        lat: Optional[float] = None
        lon: Optional[float] = None
        if coord_col is not None:
            latlon = _parse_coordinate_str(str(row.iloc[coord_col]))
            if latlon is not None:
                lat, lon = latlon
        if lat is None and lat_col is not None:
            lat = _coerce_float(row.iloc[lat_col])
        if lon is None and lon_col is not None:
            lon = _coerce_float(row.iloc[lon_col])

        radius: Optional[float] = None
        if radius_col is not None:
            radius = _coerce_float(row.iloc[radius_col])
        if radius is None:
            radius = default_radius

        # Skip rows without name or coordinates
        name_str = (
            str(name_val).strip()
            if name_val is not None and str(name_val).strip() != "nan"
            else ""
        )
        if not name_str:
            continue
        if lat is None or lon is None:
            continue

        # Prefer integer-like index; fallback to 1-based running index
        try:
            index_num = (
                int(float(idx_val))
                if idx_val is not None and str(idx_val).strip() != "nan"
                else None
            )
        except Exception:
            index_num = None

        rows.append(
            {
                "index": index_num,
                "name": name_str,
                "lat": float(lat),
                "lon": float(lon),
                "radius": float(radius),
            }
        )

    # Fill missing indices with 1-based enumeration
    running = 1
    for r in rows:
        if r["index"] is None:
            r["index"] = running
        running += 1

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Parse Excel and emit args for find.py"
    )
    parser.add_argument("excel", help="Path to the Excel file (.xls/.xlsx)")
    parser.add_argument(
        "--default-radius",
        type=float,
        default=50.0,
        help="Default radius (meters) when missing",
    )
    parser.add_argument(
        "--format", choices=["args", "jsonl"], default="args", help="Output format"
    )
    parser.add_argument("--sheet", default=None, help="Excel sheet name (optional)")
    args = parser.parse_args()

    xls_path = args.excel
    if not os.path.exists(xls_path):
        print(f"ERROR: Excel file not found: {xls_path}", file=sys.stderr)
        sys.exit(1)

    rows = load_rows_from_excel(xls_path, args.default_radius, args.sheet)
    if not rows:
        # Emit detected columns to help user adjust headers
        try:
            import pandas as pd

            ext = os.path.splitext(xls_path)[1].lower()
            engine = "xlrd" if ext == ".xls" else "openpyxl"
            df_preview = pd.read_excel(xls_path, engine=engine, nrows=5)
            cols_preview = ", ".join([str(c) for c in df_preview.columns])
            print(
                f"No valid rows parsed. Detected columns: {cols_preview}. "
                f"Expect headers like 序号/名称(街道/路口)/坐标 或 经度/纬度 以及 半径.",
                file=sys.stderr,
            )
        except Exception:
            print(
                "No valid rows parsed and failed to preview columns.", file=sys.stderr
            )
        sys.exit(2)

    if args.format == "jsonl":
        import json

        for r in rows:
            print(json.dumps(r, ensure_ascii=False))
        return

    # Default: args lines -> index|name|"lat lon"|radius
    for r in rows:
        idx = r["index"]
        name = r["name"]
        coords = f"{r['lat']} {r['lon']}"
        radius = r["radius"]
        # Keep name as-is; consumer should quote/sanitize appropriately
        print(f"{idx}|{name}|{coords}|{radius}")


if __name__ == "__main__":
    main()
