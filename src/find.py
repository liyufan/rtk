#!/usr/bin/env python3
"""
CLI tool to scan ROS1 bag files, extract GPS (sensor_msgs/NavSatFix),
and find enter/exit events for a given geodesic circle.

Outputs absolute ROS times (message header.stamp where available) and
relative times (to the bag start time).

Supports optional preprocessing cache (Parquet preferred, falls back to CSV)
to avoid re-reading bags on repeated runs.
"""

import argparse
import hashlib
import json
import math
import os
import sys
import textwrap
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from genpy import Message

# Progress bar support
try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

    # Fallback progress bar using print
    def tqdm(iterable, desc="", total=None, unit="", **kwargs):
        if total is None:
            try:
                total = len(iterable)
            except TypeError:
                total = None

        count = 0
        for item in iterable:
            yield item
            count += 1
            if total and count % max(1, total // 20) == 0:  # Update every 5%
                percent = (count / total) * 100
                print(f"\r{desc}: {percent:.1f}% ({count}/{total})", end="", flush=True)

        if total:
            print(f"\r{desc}: 100.0% ({total}/{total})")


def _import_distance_impl():
    """Best-effort geodesic distance in meters between two lat/lon points.

    Priority: geographiclib -> pyproj -> haversine (custom fallback).
    Returns a callable: distance_m(lat1, lon1, lat2, lon2) -> float
    """
    try:
        from geographiclib.geodesic import Geodesic

        geod = Geodesic.WGS84

        def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            r = geod.Inverse(lat1, lon1, lat2, lon2)
            return float(r["s12"])  # meters

        return distance_m
    except Exception:
        pass

    try:
        from pyproj import Geod

        geod = Geod(ellps="WGS84")

        def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            _, _, dist = geod.inv(lon1, lat1, lon2, lat2)
            return float(dist)

        return distance_m
    except Exception:
        pass

    # Simple haversine fallback (spherical earth)
    def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        rad = math.radians
        R = 6371008.8  # mean Earth radius in meters
        dlat = rad(lat2 - lat1)
        dlon = rad(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(rad(lat1)) * math.cos(rad(lat2)) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    return distance_m


distance_m = _import_distance_impl()


def _is_navsatfix(msg: Message) -> bool:
    """Check by duck-typing to avoid strict import dependency for types."""
    try:
        # sensor_msgs/NavSatFix has fields: latitude, longitude, altitude, status, position_covariance, etc.
        return (
            hasattr(msg, "latitude")
            and hasattr(msg, "longitude")
            and hasattr(msg, "header")
            and hasattr(msg.header, "stamp")
        )
    except Exception:
        return False


def _get_ros_time_from_header(msg: Message) -> Optional[float]:
    try:
        return float(msg.header.stamp.to_sec())  # type: ignore[attr-defined]
    except Exception:
        return None


def _list_bag_files(input_dir: str) -> List[str]:
    results: List[str] = []
    for root, _dirs, files in os.walk(input_dir):
        for name in files:
            if name.endswith(".bag"):
                results.append(os.path.join(root, name))
    results.sort()
    return results


def _stable_cache_key(file_path: str) -> str:
    try:
        st = os.stat(file_path)
        identity = f"{os.path.abspath(file_path)}|{st.st_size}|{int(st.st_mtime)}"
    except FileNotFoundError:
        identity = os.path.abspath(file_path)
    return hashlib.md5(identity.encode("utf-8")).hexdigest()


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_cache(df_like, out_path: str) -> str:
    ext = os.path.splitext(out_path)[1].lower()
    if ext in (".parquet", ".pq"):
        try:
            import pandas as pd

            df: "pd.DataFrame" = df_like
            df.to_parquet(out_path, index=False)
            return out_path
        except Exception:
            # fall back to csv
            out_path = out_path.rsplit(".", 1)[0] + ".csv"
    try:
        import pandas as pd

        df: "pd.DataFrame" = df_like
        df.to_csv(out_path, index=False)
        return out_path
    except Exception:
        # Minimal CSV writer without pandas
        import csv

        rows = df_like  # expect list of dicts
        if not rows:
            with open(out_path, "w", newline="") as f:
                pass
            return out_path
        fieldnames = list(rows[0].keys())
        with open(out_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return out_path


def _read_cache(in_path: str):
    try:
        import pandas as pd

        if in_path.endswith((".parquet", ".pq")):
            return pd.read_parquet(in_path)
        if in_path.endswith(".csv"):
            return pd.read_csv(in_path)
    except Exception:
        pass
    # lightweight csv loader
    import csv

    with open(in_path, "r") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _extract_gps_from_bag(
    bag_path: str,
    topic_filters: Optional[List[str]] = None,
    show_progress: bool = True,
) -> List[Dict[str, float]]:
    """Return list of dict rows: {ros_time, bag_rel_time, lat, lon, topic}.

    Uses header.stamp when available; relative time is header.stamp - bag_start.
    """
    try:
        import rosbag
    except Exception as e:
        print(
            "ERROR: rosbag Python API not available. Source this workspace and use Python for ROS1.",
            file=sys.stderr,
        )
        raise e

    rows: List[Dict[str, float]] = []
    with rosbag.Bag(bag_path, "r") as bag:
        bag_start_wall = float(bag.get_start_time())
        # If user provided topic filters, restrict; else, scan connections to find NavSatFix topics first
        topics_to_read: Optional[List[str]] = None
        if topic_filters:
            topics_to_read = topic_filters
        else:
            topics_to_read = []
            for c in bag._get_connections():  # private but effective
                if c.datatype.endswith("/NavSatFix"):
                    topics_to_read.append(c.topic)
            if not topics_to_read:
                # As fallback, read all topics and filter by duck typing
                topics_to_read = None

        # Get total message count for progress bar
        total_msgs = (
            bag.get_message_count(topic_filters=topics_to_read)
            if show_progress
            else None
        )
        bag_name = os.path.basename(bag_path)

        # Create progress bar for bag reading
        if show_progress and total_msgs:
            pbar = tqdm(
                total=total_msgs,
                desc=f"Reading {bag_name}",
                unit="msgs",
                disable=not HAS_TQDM or not show_progress,
            )
        else:
            pbar = None

        try:
            for topic, msg, t in bag.read_messages(topics=topics_to_read):
                if pbar:
                    pbar.update(1)

                try:
                    if not _is_navsatfix(msg):
                        continue
                    lat = float(msg.latitude)
                    lon = float(msg.longitude)
                except Exception:
                    continue

                header_time = _get_ros_time_from_header(msg)
                if header_time is None:
                    # fall back to connection time 't' from bag
                    header_time = float(t.to_sec())
                rel_time = float(header_time - bag_start_wall)

                rows.append(
                    {
                        "ros_time": header_time,
                        "bag_rel_time": rel_time,
                        "lat": lat,
                        "lon": lon,
                        "topic": topic,
                    }
                )
        finally:
            if pbar:
                pbar.close()
    return rows


@dataclass
class CircleEvent:
    event: str  # "enter" or "exit"
    ros_time: float
    bag_rel_time: float
    lat: float
    lon: float
    bag_path: str
    topic: str


def _detect_circle_events(
    rows: List[Dict[str, float]],
    center_lat: float,
    center_lon: float,
    radius_m: float,
) -> List[CircleEvent]:
    # Sort rows by time to ensure monotonic processing
    rows_sorted = sorted(rows, key=lambda r: (r["ros_time"], r["bag_rel_time"]))

    events: List[CircleEvent] = []
    # Track inside/outside state per topic to support multiple GPS sources
    inside_state_by_topic: Dict[str, bool] = {}

    for r in rows_sorted:
        topic = str(r.get("topic", ""))
        lat = float(r["lat"]) if r.get("lat") is not None else float("nan")
        lon = float(r["lon"]) if r.get("lon") is not None else float("nan")
        if not (math.isfinite(lat) and math.isfinite(lon)):
            continue

        d = distance_m(lat, lon, center_lat, center_lon)
        is_inside = d <= radius_m

        was_inside = inside_state_by_topic.get(topic, False)
        if is_inside != was_inside:
            event_type = "enter" if is_inside else "exit"
            events.append(
                CircleEvent(
                    event=event_type,
                    ros_time=float(r["ros_time"]),
                    bag_rel_time=float(r["bag_rel_time"]),
                    lat=lat,
                    lon=lon,
                    bag_path=str(r.get("bag_path", "")),
                    topic=topic,
                )
            )
            inside_state_by_topic[topic] = is_inside
        else:
            # maintain state even if unchanged, to initialize absent keys
            inside_state_by_topic.setdefault(topic, is_inside)

    return events


def _rows_to_dataframe(rows: List[Dict[str, float]]):
    try:
        import pandas as pd

        return pd.DataFrame(rows)
    except Exception:
        return rows


def _events_to_rows(events: List[CircleEvent]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for e in events:
        out.append(
            {
                "event": e.event,
                "ros_time": e.ros_time,
                "bag_rel_time": e.bag_rel_time,
                "lat": e.lat,
                "lon": e.lon,
                "bag_path": e.bag_path,
                "topic": e.topic,
            }
        )
    return out


def _create_kml_document(name: str) -> ET.Element:
    """Create a KML Document element with proper namespaces."""
    kml = ET.Element("kml")
    kml.set("xmlns", "http://www.opengis.net/kml/2.2")

    document = ET.SubElement(kml, "Document")
    ET.SubElement(document, "name").text = name
    ET.SubElement(document, "description").text = f"GPS trajectory from {name}"

    return kml


def _create_kml_style(style_id: str, color: str, width: int = 3) -> ET.Element:
    """Create a KML Style element for line styling."""
    style = ET.Element("Style")
    style.set("id", style_id)

    line_style = ET.SubElement(style, "LineStyle")
    ET.SubElement(line_style, "color").text = color
    ET.SubElement(line_style, "width").text = str(width)

    poly_style = ET.SubElement(style, "PolyStyle")
    ET.SubElement(poly_style, "color").text = color

    return style


def _create_kml_placemark(
    name: str,
    description: str,
    coordinates: List[Tuple[float, float]],
    style_url: str = None,
) -> ET.Element:
    """Create a KML Placemark element with LineString."""
    placemark = ET.Element("Placemark")
    ET.SubElement(placemark, "name").text = name
    ET.SubElement(placemark, "description").text = description

    if style_url:
        ET.SubElement(placemark, "styleUrl").text = style_url

    line_string = ET.SubElement(placemark, "LineString")
    ET.SubElement(line_string, "tessellate").text = "1"

    # Format coordinates as "lon,lat,alt" (KML format)
    coord_text = " ".join([f"{lon},{lat},0" for lat, lon in coordinates])
    ET.SubElement(line_string, "coordinates").text = coord_text

    return placemark


def _export_kml_trajectories(
    all_rows: List[Dict[str, float]], output_dir: str
) -> List[str]:
    """Export KML files for each bag's trajectory. Returns list of created KML file paths."""
    created_files: List[str] = []

    # Group rows by bag_path
    bags_data: Dict[str, List[Dict[str, float]]] = {}
    for row in all_rows:
        bag_path = row.get("bag_path", "")
        if bag_path:
            if bag_path not in bags_data:
                bags_data[bag_path] = []
            bags_data[bag_path].append(row)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    for bag_path, rows in bags_data.items():
        if not rows:
            continue

        # Sort rows by time
        rows_sorted = sorted(rows, key=lambda r: (r["ros_time"], r["bag_rel_time"]))

        # Create KML document
        bag_name = os.path.splitext(os.path.basename(bag_path))[0]
        kml = _create_kml_document(f"Trajectory_{bag_name}")
        document = kml.find("Document")

        # Add styles
        document.append(
            _create_kml_style("trajectory_style", "ff0000ff", 3)
        )  # Red line
        document.append(
            _create_kml_style("start_style", "ff00ff00", 5)
        )  # Green line for start
        document.append(
            _create_kml_style("end_style", "ffff0000", 5)
        )  # Blue line for end

        # Group by topic if multiple GPS sources
        topics = sorted(set(r.get("topic", "") for r in rows_sorted))

        for topic in topics:
            topic_rows = [r for r in rows_sorted if r.get("topic", "") == topic]
            if not topic_rows:
                continue

            # Extract valid coordinates
            coordinates = []
            for row in topic_rows:
                lat = row.get("lat")
                lon = row.get("lon")
                if lat is not None and lon is not None:
                    try:
                        lat_f = float(lat)
                        lon_f = float(lon)
                        if math.isfinite(lat_f) and math.isfinite(lon_f):
                            coordinates.append((lat_f, lon_f))
                    except (ValueError, TypeError):
                        continue

            if len(coordinates) < 2:
                continue

            # Create placemark for this topic
            topic_display = topic.replace("/", "_") if topic else "default"
            placemark_name = f"{bag_name}_{topic_display}"
            description = f"GPS trajectory from {bag_name}\nTopic: {topic}\nPoints: {len(coordinates)}"

            placemark = _create_kml_placemark(
                placemark_name, description, coordinates, "#trajectory_style"
            )
            document.append(placemark)

            # Add start and end markers if there are enough points
            if len(coordinates) >= 2:
                # Start marker
                start_coords = [
                    coordinates[0],
                    coordinates[0],
                ]  # Duplicate for LineString
                start_placemark = _create_kml_placemark(
                    f"{placemark_name}_start",
                    f"Start point: {coordinates[0][0]:.6f}, {coordinates[0][1]:.6f}",
                    start_coords,
                    "#start_style",
                )
                document.append(start_placemark)

                # End marker
                end_coords = [
                    coordinates[-1],
                    coordinates[-1],
                ]  # Duplicate for LineString
                end_placemark = _create_kml_placemark(
                    f"{placemark_name}_end",
                    f"End point: {coordinates[-1][0]:.6f}, {coordinates[-1][1]:.6f}",
                    end_coords,
                    "#end_style",
                )
                document.append(end_placemark)

        # Write KML file
        kml_filename = f"{bag_name}.kml"
        kml_path = os.path.join(output_dir, kml_filename)

        try:
            # Pretty print XML
            ET.indent(kml, space="  ", level=0)
            tree = ET.ElementTree(kml)
            tree.write(kml_path, encoding="utf-8", xml_declaration=True)
            created_files.append(kml_path)
            print(f"Created KML: {kml_path}", file=sys.stderr)
        except Exception as e:
            print(f"Failed to write KML {kml_path}: {e}", file=sys.stderr)

    return created_files


def _parse_coordinates(coord_str: str) -> Tuple[float, float]:
    """Parse coordinates from either space-separated or comma-separated format.

    Examples:
        "22.284684 114.134877" -> (22.284684, 114.134877)
        "22.284684137862257, 114.1348770469222" -> (22.284684137862257, 114.1348770469222)
        "22.284684,114.134877" -> (22.284684, 114.134877)
    """
    # Try comma-separated first (Google Maps format)
    if ',' in coord_str:
        parts = coord_str.split(',')
        if len(parts) == 2:
            try:
                lat = float(parts[0].strip())
                lon = float(parts[1].strip())
                return lat, lon
            except ValueError:
                pass

    # Try space-separated (traditional format)
    parts = coord_str.split()
    if len(parts) == 2:
        try:
            lat = float(parts[0])
            lon = float(parts[1])
            return lat, lon
        except ValueError:
            pass

    # If neither format works, raise an error
    raise ValueError(
        f"Invalid coordinate format: '{coord_str}'. Expected 'lat,lon' or 'lat lon'"
    )


def _export_circle_markers_kml(
    events: List[CircleEvent],
    center_lat: float,
    center_lon: float,
    radius_m: float,
    output_dir: str,
) -> Optional[str]:
    """Export a KML file showing the search circle and enter/exit events."""
    if not events:
        return None

    kml = _create_kml_document("Circle_Crossing_Events")
    document = kml.find("Document")

    # Add styles
    document.append(_create_kml_style("circle_style", "ff0000ff", 2))  # Red circle
    document.append(
        _create_kml_style("enter_style", "ff00ff00", 4)
    )  # Green enter events
    document.append(_create_kml_style("exit_style", "ffff0000", 4))  # Blue exit events

    # Add circle center marker
    center_placemark = ET.Element("Placemark")
    ET.SubElement(center_placemark, "name").text = "Circle Center"
    ET.SubElement(center_placemark, "description").text = (
        f"Center: {center_lat:.6f}, {center_lon:.6f}\nRadius: {radius_m}m"
    )

    point = ET.SubElement(center_placemark, "Point")
    ET.SubElement(point, "coordinates").text = f"{center_lon},{center_lat},0"

    document.append(center_placemark)

    # Add circle boundary (approximated as polygon with many points)
    circle_placemark = ET.Element("Placemark")
    ET.SubElement(circle_placemark, "name").text = f"Search Circle (R={radius_m}m)"
    ET.SubElement(circle_placemark, "styleUrl").text = "#circle_style"

    # Create circle polygon (36 points for smooth circle)
    circle_coords = []
    for i in range(37):  # 37 points to close the circle
        angle = 2 * math.pi * i / 36
        # Simple approximation: 1 degree ≈ 111km, so radius_m/111000 degrees
        lat_offset = (radius_m / 111000) * math.cos(angle)
        lon_offset = (
            (radius_m / 111000) * math.sin(angle) / math.cos(math.radians(center_lat))
        )

        circle_lat = center_lat + lat_offset
        circle_lon = center_lon + lon_offset
        circle_coords.append((circle_lat, circle_lon))

    polygon = ET.SubElement(circle_placemark, "Polygon")
    ET.SubElement(polygon, "tessellate").text = "1"

    outer_boundary = ET.SubElement(polygon, "outerBoundaryIs")
    linear_ring = ET.SubElement(outer_boundary, "LinearRing")
    coord_text = " ".join([f"{lon},{lat},0" for lat, lon in circle_coords])
    ET.SubElement(linear_ring, "coordinates").text = coord_text

    document.append(circle_placemark)

    # Add enter/exit event markers
    for i, event in enumerate(events):
        event_placemark = ET.Element("Placemark")
        event_type = event.event
        style = "#enter_style" if event_type == "enter" else "#exit_style"

        ET.SubElement(event_placemark, "name").text = f"{event_type.title()} #{i+1}"
        ET.SubElement(event_placemark, "description").text = (
            f"Event: {event_type}\n"
            f"Time: {event.ros_time:.3f}\n"
            f"Relative: {event.bag_rel_time:.3f}s\n"
            f"Location: {event.lat:.6f}, {event.lon:.6f}\n"
            f"Bag: {os.path.basename(event.bag_path)}\n"
            f"Topic: {event.topic}"
        )
        ET.SubElement(event_placemark, "styleUrl").text = style

        point = ET.SubElement(event_placemark, "Point")
        ET.SubElement(point, "coordinates").text = f"{event.lon},{event.lat},0"

        document.append(event_placemark)

    # Write KML file
    kml_path = os.path.join(output_dir, "circle_crossing_events.kml")

    try:
        ET.indent(kml, space="  ", level=0)
        tree = ET.ElementTree(kml)
        tree.write(kml_path, encoding="utf-8", xml_declaration=True)
        return kml_path
    except Exception as e:
        print(f"Failed to write circle events KML: {e}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Extract GPS trajectories from rosbag files and optionally find circle crossings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Examples:
            # Export KML trajectories only (for route visualization in Google Maps)
            %(prog)s /path/to/bags --kml-dir ./trajectories

            # Find circle crossings with space-separated coordinates
            %(prog)s /path/to/bags 31.2304 121.4737 50 --output events.json

            # Find circle crossings with comma-separated coordinates (Google Maps format)
            %(prog)s /path/to/bags "31.2304,121.4737" 50 --output events.json

            # Export trajectories and find crossings with KML events
            %(prog)s /path/to/bags "31.2304,121.4737" 50 --kml-dir ./trajectories --kml-events

            # Use cache for faster repeated runs
            %(prog)s /path/to/bags --kml-dir ./trajectories --cache

            # Disable progress bars (useful for scripts)
            %(prog)s /path/to/bags "31.2304,121.4737" 50 --no-progress
        """,
        ),
    )
    parser.add_argument(
        "bag_dir", help="Directory containing .bag files (recursively scanned)"
    )
    parser.add_argument(
        "center_coords",
        nargs="?",
        help="Circle center coordinates - can be 'lat,lon' or 'lat lon' format (required for circle crossing detection or --kml-events)",
    )
    parser.add_argument(
        "radius_m",
        type=float,
        nargs="?",
        help="Circle radius in meters (e.g., 50) - required for circle crossing detection or --kml-events",
    )
    parser.add_argument(
        "--topics",
        nargs="*",
        default=None,
        help="Optional list of GPS topics to include (defaults to auto-detect)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output file path (.json or .csv). If omitted, prints JSON to stdout",
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Enable preprocessing cache of GPS streams (Parquet/CSV)",
    )
    parser.add_argument(
        "--cache-dir",
        default="gps_cache",
        help="Cache directory path (default: gps_cache)",
    )
    parser.add_argument(
        "--cache-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Preferred cache format",
    )
    parser.add_argument(
        "--merge-topics",
        action="store_true",
        help="Merge all GPS topics into one stream when detecting events",
    )
    parser.add_argument(
        "--kml-dir",
        default=None,
        help="Export KML files for trajectory visualization (creates one KML per bag)",
    )
    parser.add_argument(
        "--kml-events",
        action="store_true",
        help="Also export KML showing circle and enter/exit events",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable progress bars",
    )
    args = parser.parse_args()

    # Determine mode based on arguments
    kml_only_mode = args.center_coords is None and args.radius_m is None
    need_coords = args.kml_events or not kml_only_mode

    # Validate arguments and parse coordinates
    if need_coords:
        if args.center_coords is None or args.radius_m is None:
            if args.kml_events:
                print(
                    "ERROR: center coordinates and radius are required for --kml-events",
                    file=sys.stderr,
                )
            else:
                print(
                    "ERROR: center coordinates and radius are required for circle crossing detection",
                    file=sys.stderr,
                )
            sys.exit(1)

        # Parse coordinates from string
        try:
            center_lat, center_lon = _parse_coordinates(args.center_coords)
            print(
                f"Parsed coordinates: lat={center_lat:.6f}, lon={center_lon:.6f}",
                file=sys.stderr,
            )
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        center_lat = center_lon = None
        if args.kml_dir:
            print(
                "KML-only mode: exporting trajectories without circle detection",
                file=sys.stderr,
            )

    bag_files = _list_bag_files(args.bag_dir)
    if not bag_files:
        print("No .bag files found.", file=sys.stderr)
        sys.exit(1)

    all_rows: List[Dict[str, float]] = []
    preferred_ext = ".parquet" if args.cache_format == "parquet" else ".csv"

    # Create progress bar for processing bags
    show_progress = HAS_TQDM and not args.no_progress
    if not HAS_TQDM and not args.no_progress:
        print(
            "Tip: Install 'tqdm' for progress bars: pip install tqdm", file=sys.stderr
        )

    bag_pbar = tqdm(
        bag_files, desc="Processing bags", unit="bag", disable=not show_progress
    )

    for bag_path in bag_pbar:
        cache_rows: Optional[List[Dict[str, float]]] = None
        if args.cache:
            _ensure_dir(args.cache_dir)
            cache_key = _stable_cache_key(bag_path)
            cache_base = os.path.join(args.cache_dir, f"gps_{cache_key}")
            cache_path = cache_base + preferred_ext
            alt_csv_path = cache_base + ".csv"
            # Load cache if exists
            cache_file_to_use = None
            if os.path.exists(cache_path):
                cache_file_to_use = cache_path
            elif os.path.exists(alt_csv_path):
                cache_file_to_use = alt_csv_path
            if cache_file_to_use:
                if show_progress:
                    bag_pbar.set_description(
                        f"Loading cache for {os.path.basename(bag_path)}"
                    )
                cached = _read_cache(cache_file_to_use)
                try:
                    import pandas as pd

                    if isinstance(cached, pd.DataFrame):
                        df = cached
                        df["bag_path"] = bag_path
                        cache_rows = df.to_dict(orient="records")
                    else:
                        # list of dicts
                        cache_rows = []
                        for r in cached:
                            r["bag_path"] = bag_path
                            cache_rows.append(
                                {
                                    "ros_time": float(r["ros_time"]),
                                    "bag_rel_time": float(r["bag_rel_time"]),
                                    "lat": float(r["lat"]),
                                    "lon": float(r["lon"]),
                                    "topic": str(r.get("topic", "")),
                                    "bag_path": bag_path,
                                }
                            )
                except Exception:
                    # best-effort without pandas
                    if isinstance(cached, list):
                        cache_rows = []
                        for r in cached:
                            try:
                                cache_rows.append(
                                    {
                                        "ros_time": float(r["ros_time"]),
                                        "bag_rel_time": float(r["bag_rel_time"]),
                                        "lat": float(r["lat"]),
                                        "lon": float(r["lon"]),
                                        "topic": str(r.get("topic", "")),
                                        "bag_path": bag_path,
                                    }
                                )
                            except Exception:
                                continue

        if cache_rows is None:
            # Extract from bag
            if show_progress:
                bag_pbar.set_description(f"Reading {os.path.basename(bag_path)}")
            rows = _extract_gps_from_bag(
                bag_path, topic_filters=args.topics, show_progress=show_progress
            )
            # Attach bag_path for downstream
            for r in rows:
                r["bag_path"] = bag_path
            # Optionally write cache
            if args.cache:
                if show_progress:
                    bag_pbar.set_description(f"Caching {os.path.basename(bag_path)}")
                cache_key = _stable_cache_key(bag_path)
                cache_base = os.path.join(args.cache_dir, f"gps_{cache_key}")
                out_path = cache_base + (
                    ".parquet" if args.cache_format == "parquet" else ".csv"
                )
                df_like = _rows_to_dataframe(
                    [
                        {
                            k: v
                            for k, v in r.items()
                            if k in ("ros_time", "bag_rel_time", "lat", "lon", "topic")
                        }
                        for r in rows
                    ]
                )
                try:
                    _write_cache(df_like, out_path)
                except Exception:
                    pass
            cache_rows = rows

        all_rows.extend(cache_rows)

    if not all_rows:
        print("No GPS data found in provided bag files.", file=sys.stderr)
        sys.exit(2)

    # Export KML files if requested
    if args.kml_dir:
        print("Exporting KML trajectory files...", file=sys.stderr)
        kml_files = _export_kml_trajectories(all_rows, args.kml_dir)
        print(f"Created {len(kml_files)} trajectory KML files", file=sys.stderr)

    # If KML-only mode, we're done
    if kml_only_mode:
        return

    # Continue with circle crossing detection
    # Optionally merge topics by simple concatenation then sorting
    if args.merge_topics:
        rows_for_detection = all_rows
    else:
        # Detect per-topic and merge events
        rows_for_detection = []
        for topic in sorted({r.get("topic", "") for r in all_rows}):
            topic_rows = [r for r in all_rows if r.get("topic", "") == topic]
            rows_for_detection.extend(topic_rows)

    events = _detect_circle_events(
        rows=rows_for_detection,
        center_lat=center_lat,
        center_lon=center_lon,
        radius_m=args.radius_m,
    )

    # Prepare output
    out_rows = _events_to_rows(events)

    # Export events KML if requested
    if args.kml_events and events:
        print("Exporting circle crossing events KML...", file=sys.stderr)
        events_kml = _export_circle_markers_kml(
            events, center_lat, center_lon, args.radius_m, args.kml_dir
        )
        if events_kml:
            print(f"Created events KML: {events_kml}", file=sys.stderr)

    # Write output
    if args.output:
        out_ext = os.path.splitext(args.output)[1].lower()
        if out_ext == ".json":
            with open(args.output, "w") as f:
                json.dump(out_rows, f, indent=4, ensure_ascii=False)
        elif out_ext == ".csv":
            try:
                import pandas as pd

                pd.DataFrame(out_rows).to_csv(args.output, index=False)
            except Exception:
                import csv

                if out_rows:
                    with open(args.output, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
                        writer.writeheader()
                        writer.writerows(out_rows)
                else:
                    with open(args.output, "w", newline="") as f:
                        pass
        else:
            print("Unsupported output extension. Use .json or .csv", file=sys.stderr)
            sys.exit(3)
    else:
        print(json.dumps(out_rows, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    main()
