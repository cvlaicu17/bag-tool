"""Entry point for the bag-tool CLI."""

from __future__ import annotations
import argparse
from pathlib import Path

from bag_tool import __version__
from bag_tool.config import (get_vio_topic, set_vio_topic, get_vib_ref, set_vib_ref,
                             get_vib_targets, set_vib_targets, get_vib_state, set_vib_state,
                             get_vib_harmonic_targets, set_vib_harmonic_targets)
from bag_tool.ros2_detect import detect_ros2_distro_verbose, detect_stores_enum
from bag_tool.processor import run as run_convert
from bag_tool.trim import run as run_trim
from bag_tool.altimeter import run as run_compare_altimeter
from bag_tool.convert_jazzy import run as run_convert_jazzy
from bag_tool.add_topics import run as run_add_topics
from bag_tool.add_paths import run as run_add_paths
from bag_tool.align import run as run_align
from bag_tool.eval import run as run_eval
from bag_tool.scale_eval import run as run_scale_eval
from bag_tool.vio_check import run as run_vio_check
from bag_tool.filter_imu import run as run_filter_imu
from bag_tool.vib_check import run as run_vib_check
from bag_tool.vib_verify import (run as run_vib_verify, measure_targets as vib_measure_targets,
                                 measure_harmonic_targets as vib_measure_harmonic_targets)
from bag_tool.vib_state import run as run_vib_state
from bag_tool.vib_realism import run as run_vib_realism
from bag_tool.sim_check import run as run_sim_check
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


# Keys accepted in an `align --config` YAML file (argparse dests of align flags).
# Positionals and --config itself are deliberately excluded.
ALIGN_CONFIG_KEYS = {
    "vio_topic": str,
    "platform": str,
    "eval": bool,
    "quick": bool,
    "shrink": bool,
    "manual": bool,
    "scale": bool,
    "new_topic": bool,
    "rte_window": (int, float),
    "yaw_rot": int,
    "out_suffix": str,
}


def _load_align_config(parser: argparse.ArgumentParser, path: str) -> dict:
    """Load an align --config YAML into a dict of argparse defaults.

    set_defaults() bypasses argparse's choices/type validation, so key names,
    value types, and enumerated values are validated here instead.
    """
    try:
        import yaml
    except ImportError:
        parser.error("--config requires pyyaml (pip install pyyaml)")
    try:
        with open(path) as fh:
            cfg = yaml.safe_load(fh)
    except OSError as exc:
        parser.error(f"--config: cannot read {path}: {exc}")
    except yaml.YAMLError as exc:
        parser.error(f"--config: invalid YAML in {path}: {exc}")
    if cfg is None:
        return {}
    if not isinstance(cfg, dict):
        parser.error(f"--config: {path} must contain a YAML mapping")

    unknown = set(cfg) - set(ALIGN_CONFIG_KEYS)
    if unknown:
        parser.error(f"--config: unknown keys {sorted(unknown)} "
                     f"(accepted: {sorted(ALIGN_CONFIG_KEYS)})")
    for key, value in cfg.items():
        expected = ALIGN_CONFIG_KEYS[key]
        if not isinstance(value, expected) or isinstance(value, bool) is not (expected is bool):
            parser.error(f"--config: key '{key}' expects {expected}, got {value!r}")
    if "platform" in cfg and cfg["platform"] not in ("auto", *PLATFORMS.keys()):
        parser.error(f"--config: platform must be one of auto/{'/'.join(PLATFORMS)}, "
                     f"got {cfg['platform']!r}")
    if "yaw_rot" in cfg and cfg["yaw_rot"] not in (0, 1, 2, 3):
        parser.error(f"--config: yaw_rot must be 0-3, got {cfg['yaw_rot']!r}")
    return cfg


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
        "--vio-topic", default=None, metavar="TOPIC",
        help="Use TOPIC as the VIO topic for this run only, without prompting and "
             "without changing the stored default. Needed to score estimators other "
             "than eagle-vision (e.g. VINS-Fusion publishes /vins_estimator/odometry).",
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
        "--vio-topic", default=None, metavar="TOPIC",
        help="Use TOPIC as the VIO topic for this run only, without prompting and "
             "without changing the stored default. Needed to score estimators other "
             "than eagle-vision (e.g. VINS-Fusion publishes /vins_estimator/odometry).",
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
        "--scale",
        action="store_true",
        help=("After aligning, split the error into SCALE vs SHAPE (rigid SE3 vs "
              "scaled Sim3 alignment), globally and per window."),
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
    align_parser.add_argument(
        "--config", default=None, metavar="FILE",
        help="YAML file of align options (keys mirror the long flag names, e.g. "
             "vio_topic, platform, eval, manual, shrink, rte_window). Values act as "
             "defaults: any flag given explicitly on the command line wins.",
    )

    # ---- graft subcommand ----
    gr_parser = subparsers.add_parser(
        "graft",
        help="Generate drift-free (GT) feature-injection arms from an "
             "offline-tracked feature bag (GRAFT experiment).",
    )
    gr_parser.add_argument("feat_bag", help="run_offline_klt output bag")
    gr_parser.add_argument("src_bag", help="Source mission bag (depth channel)")
    gr_parser.add_argument("out_prefix", help="Output prefix; writes "
                                              "<prefix>_realm and <prefix>_gt")
    gr_parser.add_argument("--platform", default="alexios")
    gr_parser.add_argument("--noise-px", type=float, default=0.0,
                           help="iid pixel noise on the GT arm (default 0; "
                                "0.5 px iid is known to collapse descent)")

    # ---- feat-depth subcommand ----
    fd_parser = subparsers.add_parser(
        "feat-depth",
        help="Per-feature depth GT for the estimator's landmarks "
             "(joins feat3d_slam/feat3d_msckf against depth-image ground truth).",
    )
    fd_parser.add_argument("result_bag", help="Serial-runner output bag (with feat3d topics)")
    fd_parser.add_argument("src_bag", help="Source mission bag (depth channel; "
                                           "/features/tracks fallback for injected runs)")
    fd_parser.add_argument("--platform", default="alexios",
                           help="Camera extrinsic/intrinsic set (default: alexios)")
    fd_parser.add_argument("--dump-npz", default=None,
                           help="Optional path for the raw per-observation join")
    fd_parser.add_argument("--tracks-bag", default=None,
                           help="Bag carrying /features/tracks for injected runs "
                                "(default: src_bag)")

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
    eval_parser.add_argument(
        "--scale",
        action="store_true",
        help=("Also split the error into SCALE vs SHAPE by aligning ground truth "
              "twice (rigid SE3 and scaled Sim3), globally and per window. "
              "Reprojection-based health metrics are blind to the scale part."),
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

    # ---- vib-verify subcommand ----
    vibverify_parser = subparsers.add_parser(
        "vib-verify",
        help="Phase-resolved vibration verification of a bag against a real reference "
             "(band-stds per climb/cruise/descent, aliased line family, ground silence).",
    )
    vibverify_parser.add_argument("input_bag", help="Bag to verify (.mcap file or directory)")
    vibverify_parser.add_argument("--targets", default=None, metavar="JSON",
                                  help="Phase-resolved targets JSON (measure_vib_targets_v2 schema)")
    vibverify_parser.add_argument("--ref", default=None, metavar="BAG",
                                  help="Derive targets from this reference bag instead")
    vibverify_parser.add_argument("--save-targets", default=None, metavar="FILE",
                                  help="With --ref: also save the derived targets JSON to FILE")
    vibverify_parser.add_argument("--set-targets", default=None, metavar="JSON",
                                  help="Save JSON as the default targets and use it for this run")
    vibverify_parser.add_argument("--imu-topic", default="/imu/data_raw",
                                  help="IMU topic (default: /imu/data_raw)")
    vibverify_parser.add_argument("--gt-topic", default="/pf_geo_loc/fc_local_position",
                                  help="Ground-truth pose topic for phase classification")
    vibverify_parser.add_argument(
        "--state", default=None, metavar="JSON",
        help="vib-fit-state coefficients JSON (day207_vib_state_v3.json schema). If "
             "given (or a default is set via 'vib-fit-state --set-default'), reports "
             "what fraction of this bag's flight state falls outside the range those "
             "coefficients were fitted over -- informational, not scored.")
    vibverify_parser.add_argument(
        "--sat-k", type=float, default=3.0,
        help="Soft-saturation ceiling multiple used to report the 'deep extrapolation' "
             "fraction (default: 3.0, matching add_vibration.py's VIB_STATE_SAT_K default)")
    vibverify_parser.add_argument(
        "--harmonic-targets", default=None, metavar="JSON",
        help="Harmonic-indexed targets JSON (vib-fit-harmonic-targets schema). If given "
             "(or a default is set via 'vib-fit-harmonic-targets --set-default'), also "
             "grades each harmonic at wherever it actually sits, plus a bucket-agnostic "
             "total-energy figure -- instead of only the fixed 20-100/100-195 Hz buckets.")
    vibverify_parser.add_argument(
        "--rotor-npz", default=None, metavar="NPZ",
        help="This bag's own <bag>_rotor_states.npz sidecar (sim bags only). With "
             "--harmonic-targets, tells the harmonic-indexed check where THIS bag's own "
             "lines actually sit, instead of assuming the targets file's frequencies.")

    # ---- vib-fit-state subcommand ----
    vibstate_parser = subparsers.add_parser(
        "vib-fit-state",
        help="Fit a continuous flight-state model (climb/sink vertical speed + "
             "horizontal speed) to a real bag's per-harmonic vibration amplitude, "
             "superseding the three discrete climb/cruise/descent phases.",
    )
    vibstate_parser.add_argument("input_bag",
                                 help="Reference bag to fit from (.mcap file or directory)")
    vibstate_parser.add_argument("--imu-topic", default="/imu/data_raw",
                                 help="IMU topic (default: /imu/data_raw)")
    vibstate_parser.add_argument("--gt-topic", default="/pf_geo_loc/fc_local_position",
                                 help="Ground-truth pose topic (default: /pf_geo_loc/fc_local_position)")
    vibstate_parser.add_argument(
        "--harmonics", default=None, metavar="F1,F2,F3",
        help="Comma-separated Hz for harmonic orders 1,2,3 (default: 51.04,103.94,149.01"
             " -- the assumed real aliased family; the simulator side does not use these"
             " directly, it aliases its own rotor speed)")
    vibstate_parser.add_argument(
        "--half-width-hz", type=float, default=4.0,
        help="Half-width in Hz of each harmonic's fixed measurement band (default: 4.0)")
    vibstate_parser.add_argument("--window-s", type=float, default=2.0,
                                 help="Sliding window length in seconds (default: 2.0)")
    vibstate_parser.add_argument("--hop-s", type=float, default=0.5,
                                 help="Sliding window hop in seconds (default: 0.5)")
    vibstate_parser.add_argument("--json", default=None, metavar="FILE",
                                 help="Output coefficients JSON path "
                                      "(default: <bag>_vib_state_v3.json)")
    vibstate_parser.add_argument(
        "--set-default", action="store_true",
        help="After writing the JSON, also save it as bag-tool's default vib-fit-state "
             "coefficients")

    # ---- vib-realism subcommand ----
    vibrealism_parser = subparsers.add_parser(
        "vib-realism",
        help="Reference-free realism gate: does this bag vibrate the way real drones "
             "vibrate? Structural invariants per layer (white/colored/tones/coupling), "
             "no reference bag, no legacy flight phases.",
    )
    vibrealism_parser.add_argument("input_bag", help="Bag to check (.mcap file or directory)")
    vibrealism_parser.add_argument("--imu-topic", default="/imu/data_raw",
                                   help="IMU topic (default: /imu/data_raw)")
    vibrealism_parser.add_argument("--gt-topic", default=None,
                                   help="GT topic (default: auto-detect from known "
                                        "platform topics; metrics degrade gracefully "
                                        "with no GT at all)")
    vibrealism_parser.add_argument("--profile", default=None, metavar="JSON",
                                   help="Threshold overrides (keys of "
                                        "vib_realism.DEFAULT_PROFILE)")
    vibrealism_parser.add_argument("--json", default=None, metavar="FILE",
                                   help="Also write the full result as JSON")

    # ---- vib-fit-harmonic-targets subcommand ----
    vibharm_parser = subparsers.add_parser(
        "vib-fit-harmonic-targets",
        help="Measure phase-resolved, HARMONIC-INDEXED vibration targets from a real "
             "bag: each harmonic at its own frequency (not a fixed 20-100/100-195 Hz "
             "bucket), plus a bucket-agnostic total-energy figure.",
    )
    vibharm_parser.add_argument("input_bag",
                                help="Reference bag to measure from (.mcap file or directory)")
    vibharm_parser.add_argument("--imu-topic", default="/imu/data_raw",
                                help="IMU topic (default: /imu/data_raw)")
    vibharm_parser.add_argument("--gt-topic", default="/pf_geo_loc/fc_local_position",
                                help="Ground-truth pose topic (default: /pf_geo_loc/fc_local_position)")
    vibharm_parser.add_argument(
        "--harmonics", default=None, metavar="F1,F2,F3",
        help="Comma-separated Hz for harmonic orders 1,2,3 (default: 51.04,103.94,149.01 "
             "-- the assumed real aliased family, same default as vib-fit-state)")
    vibharm_parser.add_argument(
        "--half-width-hz", type=float, default=4.0,
        help="Half-width in Hz of each harmonic's measurement band (default: 4.0)")
    vibharm_parser.add_argument("--json", default=None, metavar="FILE",
                                help="Output targets JSON path "
                                     "(default: <bag>_vib_harmonic_targets.json)")
    vibharm_parser.add_argument(
        "--set-default", action="store_true",
        help="After writing the JSON, also save it as bag-tool's default harmonic targets")

    # ---- sim subcommand group (tracker-GT benchmark) ----
    sim_parser = subparsers.add_parser(
        "sim",
        help="Sim tracker-GT benchmark: generate GT correspondences and evaluate trackers.",
    )
    sim_sub = sim_parser.add_subparsers(dest="sim_command", required=True)
    sgen = sim_sub.add_parser(
        "generate-tracker-gt",
        help="Write tracker output (VINS_TRACK_DUMP) + per-point GT correspondences "
             "into one self-contained result bag (/tracker_eval/points + /tracker_eval/gt).",
    )
    sgen.add_argument("src_bag", help="Source sim bag (depth + GT poses)")
    sgen.add_argument("out_bag", help="Output result bag to create")
    sgen.add_argument("--tracker-dump", required=True,
                      help="VINS_TRACK_DUMP binary from the tracker run")
    sgen.add_argument("--platform", default="alexios",
                      help="Camera platform constants (default: alexios)")
    seval = sim_sub.add_parser(
        "evaluate-tracker",
        help="Score a generate-tracker-gt result bag: endpoint error per phase/yaw "
             "bucket, drift vs track age, gate quality vs truth; writes a score JSON.",
    )
    seval.add_argument("result_bag", help="Bag written by generate-tracker-gt")
    seval.add_argument("--json", default=None, help="Score JSON path "
                       "(default: <result_bag>.tracker_score.json)")

    # ---- sim-check subcommand ----
    simcheck_parser = subparsers.add_parser(
        "sim-check",
        help="Validate a Project AirSim bag's GT/camera cadence and IMU vibrations.",
    )
    simcheck_parser.add_argument("input_bag", help="PAS bag directory or .mcap file")
    simcheck_parser.add_argument("--imu-topic", default="/imu/data_raw",
                                 help="IMU topic (default: /imu/data_raw)")
    simcheck_parser.add_argument("--gt-topic", default="/pf_geo_loc/fc_local_position",
                                 help="Ground-truth pose topic (default: /pf_geo_loc/fc_local_position)")
    simcheck_parser.add_argument("--camera-topic", default="/camera/image_mono",
                                 help="Monochrome camera topic (default: /camera/image_mono)")
    simcheck_parser.add_argument("--depth-topic", default="/camera/depth",
                                 help="Depth camera topic (default: /camera/depth)")
    simcheck_parser.add_argument("--range-topic", default="/altimeter/range",
                                 help="Range topic (default: /altimeter/range)")
    simcheck_parser.add_argument("--imu-hz", type=float, default=400.0,
                                 help="Expected IMU rate (default: 400)")
    simcheck_parser.add_argument("--gt-hz", type=float, default=400.0,
                                 help="Expected GT pose rate (default: 400)")
    simcheck_parser.add_argument("--camera-hz", type=float, default=20.0,
                                 help="Expected mono/depth rate (default: 20)")
    simcheck_parser.add_argument("--range-hz", type=float, default=12.6,
                                 help="Expected range rate (default: 12.6)")
    simcheck_parser.add_argument("--imu-tolerance", type=float, default=0.02,
                                 help="Fractional IMU cadence tolerance (default: 0.02)")
    simcheck_parser.add_argument("--gt-tolerance", type=float, default=0.02,
                                 help="Fractional GT cadence tolerance (default: 0.02)")
    simcheck_parser.add_argument("--camera-tolerance", type=float, default=0.10,
                                 help="Fractional mono/depth cadence tolerance (default: 0.10)")
    simcheck_parser.add_argument("--range-tolerance", type=float, default=0.20,
                                 help="Fractional range cadence tolerance (default: 0.20)")
    simcheck_parser.add_argument("--gap-mult", type=float, default=3.0,
                                 help="Interval multiple considered a timestamp gap (default: 3)")

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
    viocheck_parser.add_argument("--no-baro-baseline", action="store_true",
                                 help="Don't compare barometer metrics to the baked-in Day20 baseline")
    viocheck_parser.add_argument("--plot", action="store_true",
                                 help="Show matplotlib plots")

    # Two-pass parse so `align --config FILE` can supply defaults while explicit
    # CLI flags keep priority (supplied args out-rank set_defaults automatically).
    pre_args, _ = parser.parse_known_args()
    if getattr(pre_args, "command", None) == "align" and getattr(pre_args, "config", None):
        align_parser.set_defaults(**_load_align_config(parser, pre_args.config))

    args = parser.parse_args()

    print(f"bag-tool  v{__version__}")

    if args.command == "convert":
        details = detect_ros2_distro_verbose()
        stores  = detect_stores_enum()
        print(f"Detected OS : Ubuntu {details['ubuntu_version']} ({details['ubuntu_codename']})")
        print(f"ROS2 distro : {details['ros2_distro']}")
        print()

        stored = get_vio_topic()
        if getattr(args, "vio_topic", None):
            # Explicit override: one-shot, never persisted, so the stored
            # eagle-vision default survives for the next run.
            vio_topic = args.vio_topic
        elif args.new_topic or stored is None:
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
        if getattr(args, "vio_topic", None):
            # Explicit override: one-shot, never persisted, so the stored
            # eagle-vision default survives for the next run.
            vio_topic = args.vio_topic
        elif args.new_topic or stored is None:
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
        if getattr(args, "scale", False):
            from pathlib import Path as _P
            out = args.out_suffix or "_aligned"
            aligned = str(_P(args.input_bag).with_suffix("")) + out
            print()
            run_scale_eval(aligned, stores)

    elif args.command == "graft":

        from bag_tool.graft import run as run_graft

        run_graft(args.feat_bag, args.src_bag, args.out_prefix,
                  platform=args.platform, noise_px=args.noise_px)

    elif args.command == "feat-depth":

        from bag_tool.feat_depth import run as run_feat_depth

        run_feat_depth(args.result_bag, args.src_bag, platform=args.platform,
                       dump_npz=args.dump_npz, tracks_bag=args.tracks_bag)

    elif args.command == "eval":
        stores = detect_stores_enum()
        print()
        run_eval(args.aligned_bag, stores, rte_window_ns=int(args.rte_window * 1e9))
        if args.scale:
            print()
            run_scale_eval(args.aligned_bag, stores)

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

    elif args.command == "vib-verify":
        import json as _json, tempfile as _tempfile
        if args.set_targets:
            set_vib_targets(args.set_targets)
            print(f"Saved default vib targets: {args.set_targets}")
            args.targets = args.set_targets
        if args.ref and not args.targets:
            print(f"Deriving targets from reference: {args.ref} ...")
            tgt = vib_measure_targets(args.ref, args.imu_topic, args.gt_topic)
            out = args.save_targets or _tempfile.mktemp(suffix="_vib_targets.json")
            with open(out, "w") as fh:
                _json.dump(tgt, fh)
            print(f"Targets written: {out}")
            args.targets = out
        if not args.targets:
            args.targets = get_vib_targets()
        if not args.targets:
            print("ERROR: no targets. Use --targets JSON, --ref BAG, or --set-targets JSON.")
            raise SystemExit(1)
        if not args.state:
            args.state = get_vib_state()   # silently absent is fine -- envelope report
                                            # is informational, not required to verify
        if not args.harmonic_targets:
            args.harmonic_targets = get_vib_harmonic_targets()  # silently absent is fine --
                                            # harmonic-indexed check is additive, not required
        print()
        run_vib_verify(args)

    elif args.command == "vib-fit-state":
        out_path = run_vib_state(args)
        if args.set_default:
            set_vib_state(out_path)
            print(f"Saved default vib-fit-state coefficients: {out_path}")

    elif args.command == "vib-realism":
        print()
        raise SystemExit(run_vib_realism(args))

    elif args.command == "vib-fit-harmonic-targets":
        harmonics = None
        if args.harmonics:
            harmonics = {i: float(tok) for i, tok in
                        enumerate(args.harmonics.split(","), start=1)}
        result = vib_measure_harmonic_targets(args.input_bag, harmonics, args.half_width_hz,
                                              args.imu_topic, args.gt_topic)
        out_path = args.json or str(Path(args.input_bag).with_suffix("")) + "_vib_harmonic_targets.json"
        import json as _json2
        with open(out_path, "w") as fh:
            _json2.dump(result, fh, indent=2)
        print(f"harmonics (Hz): " + ", ".join(f"{h}x={f:.2f}" for h, f in
              sorted(((int(k), v) for k, v in result["harmonics_hz"].items()))))
        for pname, d in result["phases"].items():
            print(f"  [{pname}] {d['seconds']:.0f}s")
        print(f"\nwrote {out_path}")
        if args.set_default:
            set_vib_harmonic_targets(out_path)
            print(f"Saved default harmonic targets: {out_path}")

    elif args.command == "sim":
        from bag_tool.sim_tracker import generate, evaluate
        if args.sim_command == "generate-tracker-gt":
            generate(args.src_bag, args.out_bag, args.tracker_dump, args.platform)
        else:
            evaluate(args.result_bag, args.json)

    elif args.command == "sim-check":
        print()
        run_sim_check(args)

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
