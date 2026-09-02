"""``bag-tool sim-check``: the ONE command that validates a Project AirSim mission bag
before it is used. Three sections, one verdict, exit 1 on any fail:

 1. CADENCE       recorded and header timestamps of the simulator topics (IMU, GT
                  pose, mono, depth, range) against their expected rates: rate,
                  jitter, gaps, monotonicity; plus every mono frame has its depth
                  frame at the same stamp. (bag_tool.vio_check machinery -- PAS emits
                  GT at IMU rate, unlike the physical platforms, hence its own command.)
 2. DEPTH HOLES   landscape-seam dropouts in the depth stream (bag_tool.depth_holes,
                  the port of native/analysis/detect_depth_holes.py). The 5.7 map
                  fixes the defect at the source, so ANY accepted dropout is a fail.
                  Sky fraction per frame is reported alongside as information.
 3. VIBRATION     bag_tool.vib_realism -- the three-tier reference-free realism gate
                  (fail / warn / report). The raw bag is auto-detected beside a
                  <raw>_vib[_TAG] output for the < 5 Hz invariant.

Deliberately gone from this command: the v2-era phase-resolved band comparison
against day20.7 and the exact cruise peak family. Both insisted on legacy
climb/cruise/descent phases and on the reference drone's line frequencies, which the
vibration model no longer targets (frequencies come from sim physics). That
comparison lives on in ``bag-tool vib-verify`` as a CALIBRATION-time tool.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass

from bag_tool.vib_check import ok, warn, fail, hdr, SEP
from bag_tool.vio_check import analyze_topic, read_timestamps
from bag_tool import depth_holes, vib_realism


@dataclass(frozen=True)
class TopicExpectation:
    topic: str
    rate_hz: float
    tolerance: float


def _report_topic(expectation: TopicExpectation, captured: dict, gap_mult: float) -> dict:
    result = analyze_topic(expectation.topic, captured["log_times"], captured["stamp_times"],
                           expectation.rate_hz, gap_mult=gap_mult, latency_outliers=False,
                           rate_tolerance=expectation.tolerance, strict_gaps=True)
    log = result.get("log_interval", {})
    header = result.get("stamp_interval", {})
    mark = "✔" if result["pass"] else "✘"
    header_text = f"{result['stamp_mean_hz']:.2f} Hz" if header else "n/a"
    print(f"  {mark} {expectation.topic:<32} n={result['msg_count']:<7} "
          f"log {result.get('mean_hz', 0.0):.2f} Hz  header {header_text}  "
          f"median {log.get('p50', log.get('mean', 0.0)) / 1e6:.3f} ms  "
          f"jitter {100 * result.get('jitter_cv', 0.0):.2f}%  "
          f"gaps {len(result.get('gaps', []))}/{len(result.get('stamp_gaps', []))}  "
          f"nonmono {result.get('non_monotonic_log', 0)}/{result.get('non_monotonic_stamp', 0)}  "
          f"(expected {expectation.rate_hz:g} Hz ±{100 * expectation.tolerance:g}%)")
    return {"topic": expectation.topic, "pass": bool(result["pass"]),
            "msgs": result["msg_count"], "log_hz": result.get("mean_hz"),
            "stamp_hz": result.get("stamp_mean_hz"), "jitter_cv": result.get("jitter_cv"),
            "gaps": len(result.get("gaps", []))}


def _check_depth_association(mono: list[int], depth: list[int]) -> tuple[bool, int, int]:
    """Every mono capture must have one depth capture at the same stamp. The recorder
    can legitimately emit an initial/final depth-only frame: reported, never a fail."""
    mono_counts = Counter(mono)
    depth_counts = Counter(depth)
    missing_mono = sum((mono_counts - depth_counts).values())
    extra_depth = sum((depth_counts - mono_counts).values())
    return bool(mono) and missing_mono == 0, missing_mono, extra_depth


def run(args) -> int:
    summary = {"bag": args.input_bag, "sections": {}}
    fails: list[str] = []
    warns: list[str] = []
    print(hdr(f"sim-check: {args.input_bag}"))

    # ---- 1. cadence ----
    print(hdr("1. cadence (log / header timestamps)"))
    expectations = (
        TopicExpectation(args.imu_topic, args.imu_hz, args.imu_tolerance),
        TopicExpectation(args.gt_topic, args.gt_hz, args.gt_tolerance),
        TopicExpectation(args.camera_topic, args.camera_hz, args.camera_tolerance),
        TopicExpectation(args.depth_topic, args.camera_hz, args.camera_tolerance),
        TopicExpectation(args.range_topic, args.range_hz, args.range_tolerance),
    )
    recorded = read_timestamps(args.input_bag, [e.topic for e in expectations])
    cadence = [_report_topic(e, recorded[e.topic], args.gap_mult) for e in expectations]
    for c in cadence:
        if not c["pass"]:
            fails.append(f"cadence:{c['topic']}")
    mono = recorded[args.camera_topic]["stamp_times"] or recorded[args.camera_topic]["log_times"]
    depth = recorded[args.depth_topic]["stamp_times"] or recorded[args.depth_topic]["log_times"]
    paired, mono_missing, depth_extra = _check_depth_association(mono, depth)
    print(f"  {'✔' if paired else '✘'} mono/depth association: {len(mono)} mono, {len(depth)} depth; "
          f"missing mono->depth {mono_missing}, extra depth {depth_extra}")
    if not paired:
        fails.append("cadence:mono-depth-association")
    summary["sections"]["cadence"] = {"topics": cadence, "mono_depth_paired": paired,
                                      "missing_mono_depth": mono_missing, "extra_depth": depth_extra}

    # ---- 2. depth holes ----
    print(hdr(f"2. depth holes (landscape-seam dropouts, every {args.depth_stride} frame(s))"))
    if args.skip_holes:
        print("  - skipped (--skip-holes)")
        summary["sections"]["depth_holes"] = {"skipped": True}
    else:
        h = depth_holes.scan_bag(args.input_bag, args.depth_topic, stride=args.depth_stride,
                                 limit=args.depth_limit)
        summary["sections"]["depth_holes"] = h
        if h.get("error"):
            print(f"  ✘ {h['error']}")
            fails.append("holes:unreadable")
        else:
            bad = h["frames_with_dropouts"] > 0
            print(f"  {'✘' if bad else '✔'} seam dropouts: {h['frames_with_dropouts']} of "
                  f"{h['frames']} scanned frames ({h['dropout_px']} px) -- expected 0 on the 5.7 map")
            for ex in h["examples"]:
                print(f"      frame {ex['frame']}: {ex['n_blobs']} blob(s), first {ex['first']}")
            print(f"  · sky in frame: {h['sky_frames']} frames with any sky, max sky fraction "
                  f"{100 * h['max_sky_frac']:.2f}%  (coverage, scored by check_frame_coverage.py)")
            if bad:
                fails.append("holes:seam-dropouts")

    # ---- 3. vibration ----
    print(hdr("3. vibration realism (bag-tool vib-realism)"))
    raw = args.raw or vib_realism.guess_raw_bag(args.input_bag)
    res = vib_realism.analyze(args.input_bag, args.imu_topic, None, None, raw)
    counts = vib_realism.print_report(res)
    summary["sections"]["vibration"] = res
    for c in res["checks"]:
        if c["verdict"] == "fail":
            fails.append(f"vib:{c['id']}")
        elif c["verdict"] == "warn":
            warns.append(f"vib:{c['id']}")

    # ---- verdict ----
    print(SEP)
    summary["fails"] = fails
    summary["warns"] = warns
    if fails:
        print(fail(f"SIM-CHECK FAIL: {', '.join(fails)}" + (f"   warns: {', '.join(warns)}" if warns else "")))
    elif warns:
        print(warn(f"SIM-CHECK PASS with warns: {', '.join(warns)}"))
    else:
        print(ok("SIM-CHECK PASS"))
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(summary, fh, indent=2)
        print(f"wrote {args.json}")
    return 1 if fails else 0
