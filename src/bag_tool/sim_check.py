"""Quality checks for Project AirSim mission bags.

``bag-tool sim-check`` is deliberately separate from ``vio-check``: PAS emits
ground truth at IMU rate, while the physical platforms use a much slower GPS/RTK
stream.  It verifies the recorded and capture/header cadence of the simulator
topics and compares phase-resolved IMU vibration against the calibrated drone
reference embedded below.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.vib_verify import _band, _peaks, _phase_masks, _read


_TS = get_typestore(Stores.ROS2_JAZZY)

# Calibrated from day207_vib_targets_v2.json.  Keeping the compact, relevant
# values in the command makes it usable without a separately distributed JSON.
VIBRATION_TARGETS = {
    "climb": {
        "accel_x": (0.241672, 0.253317), "accel_y": (0.251615, 0.526992),
        "accel_z": (0.449738, 0.817481), "gyro_x": (0.019661, 0.000761),
        "gyro_y": (0.013348, 0.001350), "gyro_z": (0.009089, 0.000287),
    },
    "cruise": {
        "accel_x": (0.479441, 0.384754), "accel_y": (0.516122, 0.652997),
        "accel_z": (0.993423, 0.928294), "gyro_x": (0.045927, 0.001458),
        "gyro_y": (0.030656, 0.002899), "gyro_z": (0.021717, 0.000517),
    },
    "descent": {
        "accel_x": (0.584190, 0.365782), "accel_y": (0.675857, 0.612006),
        "accel_z": (1.381201, 0.844915), "gyro_x": (0.048825, 0.002003),
        "gyro_y": (0.035569, 0.002609), "gyro_z": (0.021101, 0.001112),
    },
}
CRUISE_ACCZ_PEAKS = (51.040, 60.329, 65.120, 103.938, 149.013)


@dataclass(frozen=True)
class TopicExpectation:
    topic: str
    rate_hz: float
    tolerance: float


def _header_stamp_ns(message) -> int | None:
    """Return a standard ROS Header stamp, if the message has one."""
    header = getattr(message, "header", None)
    stamp = getattr(header, "stamp", None)
    if stamp is None:
        return None
    value = int(stamp.sec) * 1_000_000_000 + int(stamp.nanosec)
    return value if value > 0 else None


def _topic_stats(timestamps: list[int], gap_mult: float = 3.0) -> dict:
    """Cadence statistics robust to a single bad/non-monotonic timestamp."""
    result = {"count": len(timestamps), "rate_hz": 0.0, "median_ms": 0.0,
              "jitter_cv": 0.0, "gaps": 0, "non_monotonic": 0}
    if len(timestamps) < 2:
        return result
    values = np.asarray(timestamps, dtype=np.int64)
    delta = np.diff(values)
    good = delta > 0
    result["non_monotonic"] = int((~good).sum())
    positive = delta[good].astype(float)
    if not len(positive):
        return result
    median_ns = float(np.median(positive))
    result["median_ms"] = median_ns / 1e6
    result["jitter_cv"] = float(np.std(positive) / np.mean(positive))
    result["gaps"] = int((positive > gap_mult * median_ns).sum())
    duration_ns = values[-1] - values[0]
    if duration_ns > 0:
        result["rate_hz"] = float((len(values) - 1) * 1e9 / duration_ns)
    return result


def _read_cadence(bag_path: str, topics: list[str]) -> dict[str, dict[str, list[int]]]:
    result = {topic: {"log": [], "header": []} for topic in topics}
    reader_path = Path(bag_path)
    if reader_path.is_file():
        reader_path = reader_path.parent
    with Reader(reader_path) as reader:
        connections = [conn for conn in reader.connections if conn.topic in result]
        for conn, timestamp, raw in reader.messages(connections=connections):
            result[conn.topic]["log"].append(timestamp)
            msg = _TS.deserialize_cdr(raw, conn.msgtype)
            stamp = _header_stamp_ns(msg)
            if stamp is not None:
                result[conn.topic]["header"].append(stamp)
    return result


def _rate_ok(rate_hz: float, expected_hz: float, tolerance: float) -> bool:
    return rate_hz > 0 and abs(rate_hz - expected_hz) / expected_hz <= tolerance


def _report_topic(expectation: TopicExpectation, captured: dict[str, list[int]],
                  gap_mult: float) -> bool:
    log = _topic_stats(captured["log"], gap_mult)
    header = _topic_stats(captured["header"], gap_mult)
    log_ok = _rate_ok(log["rate_hz"], expectation.rate_hz, expectation.tolerance)
    header_ok = (not captured["header"] or
                 _rate_ok(header["rate_hz"], expectation.rate_hz, expectation.tolerance))
    clean = (log["non_monotonic"] == 0 and log["gaps"] == 0 and
             header["non_monotonic"] == 0 and header["gaps"] == 0)
    passed = log_ok and header_ok and clean
    marker = "PASS" if passed else "FAIL"
    header_text = "n/a"
    if captured["header"]:
        header_text = f"{header['rate_hz']:.2f} Hz"
    print(f"  [{marker}] {expectation.topic}: n={log['count']}, "
          f"log={log['rate_hz']:.2f} Hz, header={header_text}, "
          f"median={log['median_ms']:.3f} ms, jitter={100 * log['jitter_cv']:.2f}%, "
          f"gaps={log['gaps']}/{header['gaps']}, nonmono={log['non_monotonic']}/{header['non_monotonic']} "
          f"(expected {expectation.rate_hz:g} Hz ±{100 * expectation.tolerance:g}%)")
    return passed


def _grade(value: float, reference: float, tiny: float) -> str:
    if (value < tiny and reference < tiny) or abs(value - reference) < tiny:
        return "ok"
    ratio = value / max(reference, 1e-12)
    if 0.8 <= ratio <= 1.25:
        return "ok"
    if 0.6 <= ratio <= 1.6:
        return "warn"
    return "fail"


def _vibration_check(bag_path: str, imu_topic: str, gt_topic: str) -> tuple[bool, int, int]:
    """Return (passes, warnings, failures) after phase/axis band checks."""
    ti, acc, gyro, tg, zg = _read(bag_path, imu_topic, gt_topic)
    if len(ti) < 8:
        print("  [FAIL] IMU has too few samples for vibration analysis")
        return False, 0, 1
    fs = 1.0 / np.median(np.diff(ti))
    masks, height = _phase_masks(ti, tg, zg)
    warnings = failures = 0
    print(f"  IMU sampling rate used for spectra: {fs:.2f} Hz")
    for phase, mask in masks.items():
        seconds = mask.sum() / fs
        if seconds < 5.0:
            warnings += 1
            print(f"  [WARN] {phase}: only {seconds:.1f}s; skipped vibration bands")
            continue
        counts = {"ok": 0, "warn": 0, "fail": 0}
        for name, samples, tiny in (("accel", acc, 0.02), ("gyro", gyro, 0.01)):
            for axis, label in enumerate("xyz"):
                measured = (_band(samples[mask, axis], 20, 100, fs),
                            _band(samples[mask, axis], 100, 195, fs))
                target = VIBRATION_TARGETS[phase][f"{name}_{label}"]
                grades = tuple(_grade(v, ref, tiny) for v, ref in zip(measured, target))
                for grade in grades:
                    counts[grade] += 1
        warnings += counts["warn"]
        failures += counts["fail"]
        status = "PASS" if counts["fail"] == 0 else "FAIL"
        print(f"  [{status}] {phase}: {counts['ok']} ok, {counts['warn']} warn, "
              f"{counts['fail']} fail band comparisons ({seconds:.0f}s)")

    cruise = masks["cruise"]
    if cruise.sum() >= fs * 5:
        peaks = _peaks(acc[cruise, 2], fs)
        missing = [ref for ref in CRUISE_ACCZ_PEAKS
                   if not any(abs(found - ref) <= 6.0 for found in peaks)]
        if missing:
            warnings += 1
            print("  [WARN] cruise accel-Z peak family incomplete; missing "
                  + ", ".join(f"{value:.1f}" for value in missing) + " Hz")
        else:
            print("  [PASS] cruise accel-Z peak family present")

    ground = (ti < ti[0] + 30.0) & (height < 1.0)
    if ground.sum() >= fs * 3:
        rest_std = float(acc[ground, 2].std())
        if rest_std > 0.25:
            warnings += 1
            print(f"  [WARN] ground-rest accel-Z std: {rest_std:.3f} m/s² (>0.250)")
        else:
            print(f"  [PASS] ground-rest accel-Z std: {rest_std:.3f} m/s²")
    return failures == 0, warnings, failures


def run(args) -> None:
    """Run the PAS ground-truth/camera cadence and vibration checks."""
    expectations = (
        TopicExpectation(args.imu_topic, args.imu_hz, args.imu_tolerance),
        TopicExpectation(args.gt_topic, args.gt_hz, args.gt_tolerance),
        TopicExpectation(args.camera_topic, args.camera_hz, args.camera_tolerance),
        TopicExpectation(args.depth_topic, args.camera_hz, args.camera_tolerance),
        TopicExpectation(args.range_topic, args.range_hz, args.range_tolerance),
    )
    print(f"sim-check: {args.input_bag}")
    print("Cadence (log/header timestamp):")
    recorded = _read_cadence(args.input_bag, [item.topic for item in expectations])
    cadence_results = [_report_topic(item, recorded[item.topic], args.gap_mult)
                       for item in expectations]
    cadence_pass = all(cadence_results)

    mono = recorded[args.camera_topic]["header"] or recorded[args.camera_topic]["log"]
    depth = recorded[args.depth_topic]["header"] or recorded[args.depth_topic]["log"]
    paired = mono == depth and len(mono) > 0
    print(f"  [{'PASS' if paired else 'FAIL'}] mono/depth timestamp pairing: "
          f"{len(mono)} / {len(depth)} messages")

    print("Vibration (20–100 Hz and 100–195 Hz bands):")
    vibration_pass, warnings, failures = _vibration_check(
        args.input_bag, args.imu_topic, args.gt_topic)
    passed = cadence_pass and paired and vibration_pass
    print(f"SIM-CHECK: {'PASS' if passed else 'FAIL'} "
          f"({warnings} warning(s), {failures} vibration failure(s))")
    if not passed:
        raise SystemExit(1)
