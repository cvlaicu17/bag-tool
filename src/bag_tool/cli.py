"""Entry point for the bag-tool CLI."""

from __future__ import annotations
import argparse

from bag_tool import __version__
from bag_tool.config import get_vio_topic, set_vio_topic
from bag_tool.ros2_detect import detect_ros2_distro_verbose, detect_stores_enum
from bag_tool.processor import run as run_convert
from bag_tool.trim import run as run_trim


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
    convert_parser.add_argument("output_bag", help="Output bag directory to create")
    convert_parser.add_argument(
        "-n", "--new-topic",
        action="store_true",
        help="Always prompt for the VIO topic, ignoring the stored default.",
    )

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

        run_convert(args.input_bag, args.output_bag, vio_topic, stores)

    elif args.command == "trim":
        print()
        run_trim(args.input_bag, args.output_bag, args.start, args.end)
