"""Entry point for the bag-tool CLI."""

from __future__ import annotations
import argparse
from pathlib import Path

from bag_tool import __version__
from bag_tool.config import get_vio_topic, set_vio_topic
from bag_tool.ros2_detect import detect_ros2_distro_verbose, detect_stores_enum
from bag_tool.processor import run as run_convert
from bag_tool.trim import run as run_trim
from bag_tool.altimeter import run as run_compare_altimeter
from bag_tool.convert_jazzy import run as run_convert_jazzy
from bag_tool.add_topics import run as run_add_topics
from bag_tool.align import run as run_align


_DEFAULT_VIO_TOPIC = "/ov_srvins/poseimu"


def _ask_vio_topic(current_default: str | None) -> str:
    prompt = "VIO topic"
    if current_default:
        prompt += f" [{current_default}]"
    prompt += ": "

    value = input(prompt).strip()
    if not value:
        if current_default:
            return current_default
        print("ERROR: VIO topic cannot be empty.")
        raise SystemExit(1)
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bag-tool",
        description="ROS2 bag utility tool.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"bag-tool {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ---- convert subcommand ----
    convert_parser = subparsers.add_parser(
        "convert",
        help="RTK → ENU pose converter and VIO alignment.",
    )
    convert_parser.add_argument("input_bag",  help="Input bag (.mcap file or directory)")
    convert_parser.add_argument("output_bag", nargs="?", default=None,
                                help="Output bag directory to create (default: <input_bag>_aligned)")
    convert_parser.add_argument(
        "-n", "--new-topic",
        action="store_true",
        help="Always prompt for the VIO topic, ignoring the stored default.",
    )
    convert_parser.add_argument(
        "-q", "--quick",
        action="store_true",
        help="Only write path topics (skip per-message pose topics).",
    )

    # ---- convert-to-jazzy subcommand ----
    jazzy_parser = subparsers.add_parser(
        "convert-to-jazzy",
        help="Re-serialise a Humble bag using Jazzy message definitions (adds missing fields with defaults).",
    )
    jazzy_parser.add_argument("input_bag",  help="Input bag (.mcap file or directory)")
    jazzy_parser.add_argument("output_bag", help="Output bag directory to create")

    # ---- compare-altimeter subcommand ----
    alt_parser = subparsers.add_parser(
        "compare-altimeter",
        help="Compare altimeter range vs RTK altitude.",
    )
    alt_parser.add_argument("input_bag",  help="Input bag (.mcap file or directory)")
    alt_parser.add_argument("output_bag", help="Output bag directory to create")

    # ---- align subcommand ----
    align_parser = subparsers.add_parser(
        "align",
        help="Compute RTK-VIO alignment and write aligned bag next to the input.",
    )
    align_parser.add_argument("input_bag", help="Bag with RTK + VIO data to align")
    align_parser.add_argument("ref_bag", nargs="?", default=None,
                              help="Optional reference bag used only for timestamp alignment "
                                   "(e.g. the original DJI recording)")
    align_parser.add_argument(
        "-n", "--new-topic",
        action="store_true",
        help="Always prompt for the VIO topic, ignoring the stored default.",
    )
    align_parser.add_argument(
        "-q", "--quick",
        action="store_true",
        help="Only write path topics (skip per-message pose topics).",
    )

    # ---- add-topics subcommand ----
    addtopics_parser = subparsers.add_parser(
        "add-topics",
        help="Append topics from a source bag into an existing dest bag (in-place).",
    )
    addtopics_parser.add_argument("source_bag", help="Source bag to read topics FROM")
    addtopics_parser.add_argument("dest_bag",   help="Existing bag to append topics INTO")
    addtopics_parser.add_argument("topics", nargs="*", help="Topic name(s) to copy (default: all topics in source bag)")

    # ---- trim subcommand ----
    trim_parser = subparsers.add_parser(
        "trim",
        help="Copy a bag, keeping only messages within a time window.",
    )
    trim_parser.add_argument("input_bag",  help="Input bag (.mcap file or directory)")
    trim_parser.add_argument("output_bag", help="Output bag directory to create")
    trim_parser.add_argument(
        "--start", type=float, default=None, metavar="SECONDS",
        help="Start offset in seconds from the beginning of the bag.",
    )
    trim_parser.add_argument(
        "--end", type=float, default=None, metavar="SECONDS",
        help="End offset in seconds from the beginning of the bag.",
    )

    args = parser.parse_args()

    print(f"bag-tool  v{__version__}")

    if args.command == "convert":
        details = detect_ros2_distro_verbose()
        stores  = detect_stores_enum()
        print(f"Detected OS : Ubuntu {details['ubuntu_version']} ({details['ubuntu_codename']})")
        print(f"ROS2 distro : {details['ros2_distro']}")
        print()

        stored = get_vio_topic()
        if args.new_topic or stored is None:
            vio_topic = _ask_vio_topic(stored or _DEFAULT_VIO_TOPIC)
            set_vio_topic(vio_topic)
        else:
            vio_topic = stored

        print(f"VIO topic   : {vio_topic}")
        print()

        input_path = Path(args.input_bag)
        output_bag = args.output_bag or str(input_path.parent / (input_path.name + '_aligned'))
        run_convert(args.input_bag, output_bag, vio_topic, stores, quick=args.quick)

    elif args.command == "convert-to-jazzy":
        print()
        run_convert_jazzy(args.input_bag, args.output_bag)

    elif args.command == "compare-altimeter":
        print()
        run_compare_altimeter(args.input_bag, args.output_bag)

    elif args.command == "align":
        details = detect_ros2_distro_verbose()
        stores  = detect_stores_enum()
        print(f"Detected OS : Ubuntu {details['ubuntu_version']} ({details['ubuntu_codename']})")
        print(f"ROS2 distro : {details['ros2_distro']}")
        print()

        stored = get_vio_topic()
        if args.new_topic or stored is None:
            vio_topic = _ask_vio_topic(stored or _DEFAULT_VIO_TOPIC)
            set_vio_topic(vio_topic)
        else:
            vio_topic = stored

        print(f"VIO topic   : {vio_topic}")
        print()

        run_align(args.input_bag, vio_topic, stores, ref_bag=args.ref_bag, quick=args.quick)

    elif args.command == "add-topics":
        print()
        run_add_topics(args.source_bag, args.dest_bag, args.topics)

    elif args.command == "trim":
        print()
        run_trim(args.input_bag, args.output_bag, args.start, args.end)
