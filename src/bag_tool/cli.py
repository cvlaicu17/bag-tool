"""Entry point for the bag-tool CLI."""

from __future__ import annotations
import argparse

from bag_tool import __version__
from bag_tool.config import get_vio_topic, set_vio_topic
from bag_tool.ros2_detect import detect_ros2_distro_verbose, detect_stores_enum
from bag_tool.processor import run


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
    details = detect_ros2_distro_verbose()
    stores  = detect_stores_enum()

    parser = argparse.ArgumentParser(
        description="RTK → ENU pose converter and VIO alignment tool.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input_bag",  help="Input bag (.mcap file or directory)")
    parser.add_argument("output_bag", help="Output bag directory to create")
    parser.add_argument(
        "-n", "--new-topic",
        action="store_true",
        help="Always prompt for the VIO topic, ignoring the stored default.",
    )
    args = parser.parse_args()

    print(f"bag-tool  v{__version__}")
    print(f"Detected OS : Ubuntu {details['ubuntu_version']} ({details['ubuntu_codename']})")
    print(f"ROS2 distro : {details['ros2_distro']}")
    print()

    stored = get_vio_topic()

    if args.new_topic or stored is None:
        # -n flag or no default saved yet: always ask
        vio_topic = _ask_vio_topic(stored or _DEFAULT_VIO_TOPIC)
        set_vio_topic(vio_topic)
    else:
        vio_topic = stored

    print(f"VIO topic   : {vio_topic}")
    print()

    run(args.input_bag, args.output_bag, vio_topic, stores)
