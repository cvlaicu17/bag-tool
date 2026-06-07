"""Entry point for the bag-tool CLI."""

from __future__ import annotations
import argparse
from pathlib import Path

from bag_tool import __version__
from bag_tool.config import get_vio_topic, set_vio_topic, get_vib_ref, set_vib_ref
from bag_tool.ros2_detect import detect_ros2_distro_verbose, detect_stores_enum
from bag_tool.processor import run as run_convert
from bag_tool.trim import run as run_trim
from bag_tool.altimeter import run as run_compare_altimeter
from bag_tool.convert_jazzy import run as run_convert_jazzy
from bag_tool.add_topics import run as run_add_topics
from bag_tool.add_paths import run as run_add_paths
from bag_tool.align import run as run_align
from bag_tool.eval import run as run_eval
from bag_tool.vio_check import run as run_vio_check
from bag_tool.filter_imu import run as run_filter_imu
from bag_tool.vib_check import run as run_vib_check
from bag_tool.platforms import PLATFORMS, detect_from_bag, detect_from_bags


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
        help="Only write per-message pose topics (skip paths and ATE/RTE).",
    )
    convert_parser.add_argument(
        "--eval",
        action="store_true",
        help="Write only RTK aligned poses and srvins poses (no paths, no ATE/RTE).",
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
        help="Only write per-message pose topics (skip paths and ATE/RTE).",
    )
    align_parser.add_argument(
        "--eval",
        action="store_true",
        help="Write only RTK aligned poses and srvins poses (no paths, no ATE/RTE).",
    )
    align_parser.add_argument(
        "--rte-window",
        type=float, default=1.0, metavar="SECONDS",
        help="Window size in seconds for relative trajectory error (default: 1.0).",
    )
    align_parser.add_argument(
        "--manual",
        action="store_true",
        help="Skip ArUco detection and use the RTK yaw method directly.",
    )
    align_parser.add_argument(
        "--shrink",
        action="store_true",
        help="Skip writing the nav_msgs/Path topics (keep poses, ATE, RTE, and passthrough).",
    )
    align_parser.add_argument(
        "--platform",
        choices=["auto", *PLATFORMS.keys()],
        default="auto",
        help="Ground-truth platform (default: auto-detect from input topics).",
    )
    align_parser.add_argument(
        "--yaw-rot",
        type=int, choices=[0, 1, 2, 3], default=0,
        help="Pre-rotate ground-truth pose by N×90° around Z (default: 0). "
             "Use to compensate for unknown IMU mounting orientation in the sensor casket.",
    )
    align_parser.add_argument(
        "--out-suffix", default="", metavar="STR",
        help="Suffix appended to the output directory name "
             "(default: ''; use to avoid clobbering prior runs when sweeping --yaw-rot).",
    )

    # ---- eval subcommand ----
    eval_parser = subparsers.add_parser(
        "eval",
        help="Compute RTE metrics from an already-aligned bag (output of align --eval).",
    )
    eval_parser.add_argument("aligned_bag", help="Aligned bag directory or .mcap file")
    eval_parser.add_argument(
        "--rte-window",
        type=float, default=1.0, metavar="SECONDS",
        help="Window size in seconds for relative trajectory error (default: 1.0).",
    )

    # ---- add-topics subcommand ----
    addtopics_parser = subparsers.add_parser(
        "add-topics",
        help="Append topics from a source bag into an existing dest bag (in-place).",
        description=(
            "Append topics from a source bag into an existing destination bag (modified in-place).\n"
            "\n"
            "Examples:\n"
            "  # Copy all topics from source into dest:\n"
            "  bag-tool add-topics source/ dest/\n"
            "\n"
            "  # Copy only two specific topics:\n"
            "  bag-tool add-topics source/ dest/ /camera/image_mono /imu/data_raw\n"
            "\n"
            "  # Merge an aligned bag's pose topics into the original bag:\n"
            "  bag-tool add-topics my_bag_aligned/ my_bag/ /ov_srvins/gt/aligned /ov_srvins/vio/pose\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    addtopics_parser.add_argument("source_bag", help="Source bag to read topics FROM")
    addtopics_parser.add_argument("dest_bag",   help="Existing bag to append topics INTO (modified in-place)")
    addtopics_parser.add_argument("topics", nargs="*", help="Topic name(s) to copy (default: all topics in source bag)")

    # ---- add-paths subcommand ----
    addpaths_parser = subparsers.add_parser(
        "add-paths",
        help="Generate accumulated nav_msgs/Path topics from poses in a bag.",
        description=(
            "Take a poses-only bag and produce a sibling bag containing accumulated\n"
            "nav_msgs/Path messages, one per pose, derived from every pose-like topic\n"
            "found in the input. Output is written to <input_stem>_paths/ next to the\n"
            "input bag. /tf_static is passed through so the output bag is viewable\n"
            "standalone in Foxglove."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    addpaths_parser.add_argument("input_bag", help="Bag with pose topics (.mcap file or directory)")

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

    # ---- vib-check subcommand ----
    vibcheck_parser = subparsers.add_parser(
        "vib-check",
        help="Analyse IMU vibration levels (PSD, band energy, resonance detection).",
    )
    vibcheck_parser.add_argument("input_bag", help="Bag to analyse (.mcap file or directory)")
    vibcheck_parser.add_argument(
        "--set-ref", default=None, metavar="BAG",
        help="Save BAG as the default vibration reference and use it for this run.",
    )
    vibcheck_parser.add_argument("--imu-topic", default="/imu/data_raw",
                                 help="IMU topic (default: /imu/data_raw)")
    vibcheck_parser.add_argument("--plot", action="store_true",
                                 help="Show interactive matplotlib plot")
    vibcheck_parser.add_argument("--save-plot", default=None, metavar="FILE",
                                 help="Save plot to FILE instead of displaying it")

    # ---- filter-imu subcommand ----
    filterimu_parser = subparsers.add_parser(
        "filter-imu",
        help="Apply a low-pass filter to the IMU topic to suppress vibration.",
    )
    filterimu_parser.add_argument("input_bag",  help="Input bag (.mcap file or directory)")
    filterimu_parser.add_argument("output_bag", help="Output bag directory to create")
    filterimu_parser.add_argument(
        "--imu-topic", default="/imu/data_raw",
        help="IMU topic to filter (default: /imu/data_raw)",
    )
    filterimu_parser.add_argument(
        "--cutoff", type=float, default=80.0, metavar="HZ",
        help="Low-pass cutoff frequency in Hz (default: 80.0)",
    )
    filterimu_parser.add_argument(
        "--order", type=int, default=4,
        help="Butterworth filter order (default: 4)",
    )

    # ---- vio-check subcommand ----
    viocheck_parser = subparsers.add_parser(
        "vio-check",
        help="Analyze bag for VIO readiness (frequency, jitter, IMU-camera sync, "
             "barometer-vs-GT error profile).",
    )
    viocheck_parser.add_argument("input_bag", help="Bag directory or .mcap file")
    viocheck_parser.add_argument("--camera", default="/camera/image_mono",
                                 help="Camera topic (default: /camera/image_mono)")
    viocheck_parser.add_argument("--imu",    default="/imu/data_raw",
                                 help="IMU topic (default: /imu/data_raw)")
    viocheck_parser.add_argument("--platform",
                                 choices=["auto", *PLATFORMS.keys()],
                                 default="auto",
                                 help="Ground-truth platform (default: auto-detect from input topics).")
    viocheck_parser.add_argument("--rtk", default=None,
                                 help="Ground-truth topic to check at ~5 Hz "
                                      "(default: platform.gps_topic; pass '' to skip).")
    viocheck_parser.add_argument("--cam-hz", type=float, default=None,
                                 help="Expected camera rate Hz (auto-detect if omitted)")
    viocheck_parser.add_argument("--imu-hz", type=float, default=None,
                                 help="Expected IMU rate Hz (auto-detect if omitted)")
    viocheck_parser.add_argument("--gap-mult", type=float, default=3.0,
                                 help="Gap threshold multiplier (default: 3.0×)")
    viocheck_parser.add_argument("--min-imu-per-frame", type=int, default=10,
                                 help="Min acceptable IMU samples per camera frame (default: 10)")
    viocheck_parser.add_argument("--baro", default="/altimeter/range",
                                 help="Barometer topic (sensor_msgs/Range) to profile vs GT "
                                      "altitude (default: /altimeter/range; pass '' to skip)")
    viocheck_parser.add_argument("--baro-ground-h", type=float, default=1.0,
                                 help="GPS height (m above takeoff) below which counts as on-ground; "
                                      "the on-ground baro average is used as the constant bias (default: 1.0)")
    viocheck_parser.add_argument("--no-baro", action="store_true",
                                 help="Skip the barometer-vs-GT analysis")
    viocheck_parser.add_argument("--plot", action="store_true",
                                 help="Show matplotlib plots")

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
        run_convert(args.input_bag, output_bag, vio_topic, stores, quick=args.quick,
                    eval_mode=args.eval)

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

        if args.platform == "auto":
            # Look at both input_bag and ref_bag — the GT topic that identifies the
            # platform may only live in one of them (e.g., OpenVINS output as input,
            # original recording as ref_bag).
            platform = detect_from_bags([args.input_bag, args.ref_bag])
            print(f"Platform    : {platform.name} (auto-detected)")
        else:
            platform = PLATFORMS[args.platform]
            print(f"Platform    : {platform.name}")
        print()

        run_align(args.input_bag, vio_topic, stores, ref_bag=args.ref_bag, quick=args.quick,
                  rte_window=args.rte_window, eval_mode=args.eval, manual=args.manual,
                  shrink=args.shrink, platform=platform, yaw_rot=args.yaw_rot,
                  out_suffix=args.out_suffix)

    elif args.command == "eval":
        stores = detect_stores_enum()
        print()
        run_eval(args.aligned_bag, stores, rte_window_ns=int(args.rte_window * 1e9))

    elif args.command == "add-topics":
        print()
        run_add_topics(args.source_bag, args.dest_bag, args.topics)

    elif args.command == "add-paths":
        stores = detect_stores_enum()
        print()
        run_add_paths(args.input_bag, stores)

    elif args.command == "vib-check":
        if args.set_ref:
            set_vib_ref(args.set_ref)
            print(f"Saved vibration reference: {args.set_ref}")
        args.ref = args.set_ref or get_vib_ref()
        if args.ref:
            print(f"Reference : {args.ref}")
        print()
        run_vib_check(args)

    elif args.command == "filter-imu":
        stores = detect_stores_enum()
        print()
        run_filter_imu(
            args.input_bag, args.output_bag, stores,
            imu_topic=args.imu_topic,
            cutoff=args.cutoff,
            order=args.order,
        )

    elif args.command == "trim":
        print()
        run_trim(args.input_bag, args.output_bag, args.start, args.end)

    elif args.command == "vio-check":
        if args.platform == "auto":
            platform = detect_from_bag(args.input_bag)
            print(f"Platform    : {platform.name} (auto-detected)")
        else:
            platform = PLATFORMS[args.platform]
            print(f"Platform    : {platform.name}")
        if args.rtk is None:
            args.rtk = platform.gps_topic
        args.platform_obj = platform
        print()
        run_vio_check(args)
