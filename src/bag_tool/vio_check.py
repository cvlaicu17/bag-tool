"""vio-check: VIO bag quality analysis subcommand."""

from __future__ import annotations

import struct
from pathlib import Path

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


# ── Color helpers ─────────────────────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"

def ok(s):   return f"{C.GREEN}✔ {s}{C.RESET}"
def warn(s): return f"{C.YELLOW}⚠ {s}{C.RESET}"
def fail(s): return f"{C.RED}✘ {s}{C.RESET}"
def hdr(s):  return f"{C.BOLD}{C.CYAN}{s}{C.RESET}"

SEP = "─" * 70


# ── CDR header stamp decoder ──────────────────────────────────────────────────
def decode_header_stamp_ns(data: bytes) -> int | None:
    """Extract header.stamp from CDR-serialised ROS2 message (any msg with std_msgs/Header first)."""
    if len(data) < 12:
        return None
    sec, nsec = struct.unpack_from("<II", data, 4)
    return sec * 1_000_000_000 + nsec


# ── Bag reader (rosbags) ──────────────────────────────────────────────────────
def read_timestamps(bag_path: str, topics: list[str]) -> dict[str, dict]:
    """Returns {topic: {log_times: [...], stamp_times: [...]}} using rosbags."""
    from rosbags.rosbag2 import Reader

    data: dict[str, dict] = {t: {"log_times": [], "stamp_times": []} for t in topics}
    path = Path(bag_path)
    reader_path = path.parent if path.is_file() else path

    with Reader(reader_path) as reader:
        conns = [c for c in reader.connections if c.topic in set(topics)]
        for connection, timestamp, rawdata in reader.messages(connections=conns):
            t = connection.topic
            data[t]["log_times"].append(timestamp)
            stamp = decode_header_stamp_ns(rawdata)
            if stamp is not None and stamp > 0:
                data[t]["stamp_times"].append(stamp)

    return data


# ── Statistics helpers ────────────────────────────────────────────────────────
def intervals_ns(ts: list[int]) -> list[float]:
    return [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]

def stats(vals: list[float]) -> dict:
    if not vals:
        return {}
    if HAS_NUMPY:
        a = np.array(vals, dtype=float)
        return {
            "n":    len(vals),
            "mean": float(np.mean(a)),
            "std":  float(np.std(a)),
            "min":  float(np.min(a)),
            "max":  float(np.max(a)),
            "p95":  float(np.percentile(a, 95)),
            "p99":  float(np.percentile(a, 99)),
        }
    else:
        n    = len(vals)
        mean = sum(vals) / n
        var  = sum((v - mean) ** 2 for v in vals) / n
        srt  = sorted(vals)
        return {
            "n":    n,
            "mean": mean,
            "std":  var ** 0.5,
            "min":  srt[0],
            "max":  srt[-1],
            "p95":  srt[int(0.95 * n)],
            "p99":  srt[int(0.99 * n)],
        }

def hz(period_ns: float) -> float:
    return 1e9 / period_ns if period_ns > 0 else 0.0

def fmt_ns(ns: float) -> str:
    if abs(ns) >= 1e9: return f"{ns/1e9:.3f} s"
    if abs(ns) >= 1e6: return f"{ns/1e6:.3f} ms"
    if abs(ns) >= 1e3: return f"{ns/1e3:.1f} µs"
    return f"{ns:.0f} ns"


# ── Per-topic analysis ────────────────────────────────────────────────────────
def analyze_topic(name: str, log_times: list[int], stamp_times: list[int],
                  expected_hz: float | None, gap_mult: float = 3.0) -> dict:
    result = {"name": name, "issues": [], "pass": True}
    n = len(log_times)
    result["msg_count"] = n
    if n < 2:
        result["issues"].append("Too few messages to analyse")
        result["pass"] = False
        return result

    result["duration_s"] = (log_times[-1] - log_times[0]) / 1e9

    # Monotonicity
    non_mono = sum(1 for i in range(n - 1) if log_times[i + 1] <= log_times[i])
    result["non_monotonic_log"] = non_mono
    if non_mono > 0:
        result["issues"].append(f"{non_mono} non-monotonic log timestamps")
        result["pass"] = False

    if stamp_times:
        non_mono_h = sum(1 for i in range(len(stamp_times) - 1)
                         if stamp_times[i + 1] <= stamp_times[i])
        result["non_monotonic_stamp"] = non_mono_h
        if non_mono_h > 0:
            result["issues"].append(f"{non_mono_h} non-monotonic header stamps")
            result["pass"] = False

    # Interval stats
    ivals = intervals_ns(log_times)
    s = stats(ivals)
    result["log_interval"] = s
    mean_hz = hz(s["mean"])
    result["mean_hz"] = mean_hz

    if expected_hz and expected_hz > 0:
        if abs(mean_hz - expected_hz) / expected_hz > 0.15:
            result["issues"].append(
                f"Mean freq {mean_hz:.1f} Hz deviates >15% from expected {expected_hz:.1f} Hz"
            )
            result["pass"] = False
    else:
        expected_hz = mean_hz

    # Jitter
    cv = s["std"] / s["mean"] if s["mean"] > 0 else 0
    result["jitter_cv"] = cv
    if cv > 0.35:
        result["issues"].append(f"High jitter CV={cv:.3f} (>0.35) — pathological")
        result["pass"] = False
    elif cv > 0.10:
        result["issues"].append(f"Moderate jitter CV={cv:.3f} (>0.10) — normal for USB IMU")

    # Gaps
    expected_period_ns = 1e9 / expected_hz
    gap_thresh_ns = gap_mult * expected_period_ns
    gaps = [(i, v) for i, v in enumerate(ivals) if v > gap_thresh_ns]
    result["gaps"] = gaps
    total_gap_s = sum(v for _, v in gaps) / 1e9
    result["total_gap_s"] = total_gap_s
    if gaps:
        result["issues"].append(
            f"{len(gaps)} gap(s) > {gap_mult:.1f}× expected interval "
            f"(total lost: {total_gap_s:.2f} s)"
        )
        if len(gaps) > 15 or total_gap_s > 2.0:
            result["pass"] = False

    # Dropped frames
    dropped_frames = []
    if cv <= 0.05:
        drop_lo = 1.5 * expected_period_ns
        for i, v in enumerate(ivals):
            if drop_lo <= v < gap_thresh_ns:
                n_dropped = round(v / expected_period_ns) - 1
                if n_dropped >= 1:
                    dropped_frames.append((i, v, (log_times[i] - log_times[0]) / 1e9, n_dropped))
    result["dropped_frames"] = dropped_frames
    result["total_dropped_frames"] = sum(nd for _, _, _, nd in dropped_frames)
    if dropped_frames:
        result["issues"].append(
            f"{len(dropped_frames)} dropped-frame interval(s), "
            f"~{result['total_dropped_frames']} frame(s) lost"
        )

    # Header stamp latency
    if stamp_times and len(stamp_times) == n:
        latencies = [log_times[i] - stamp_times[i] for i in range(n)]
        ls = stats(latencies)
        result["stamp_latency"] = ls
        if ls["mean"] < -50_000_000:
            result["issues"].append(
                f"Header stamps ahead of log time by {fmt_ns(-ls['mean'])} on average"
            )
            result["pass"] = False
        if ls["std"] > 50_000_000:
            result["issues"].append(f"High latency jitter: std={fmt_ns(ls['std'])}")

    return result


# ── Camera-IMU sync analysis ──────────────────────────────────────────────────
def analyze_cam_imu_sync(cam_stamps: list[int], imu_stamps: list[int],
                         min_imu_per_frame: int = 10,
                         cam_times_ipf: list[int] | None = None,
                         imu_times_ipf: list[int] | None = None) -> dict:
    result = {"issues": [], "pass": True}
    if not cam_stamps or not imu_stamps:
        result["issues"].append("Missing stamps for sync analysis")
        result["pass"] = False
        return result

    _cam_ipf = cam_times_ipf if cam_times_ipf is not None else cam_stamps
    _imu_ipf = imu_times_ipf if imu_times_ipf is not None else imu_stamps

    if HAS_NUMPY:
        cam = np.array(cam_stamps, dtype=np.int64)
        imu = np.array(imu_stamps, dtype=np.int64)
        idxs  = np.searchsorted(imu, cam)
        idxs  = np.clip(idxs, 1, len(imu) - 1)
        left  = np.abs(imu[idxs - 1] - cam)
        right = np.abs(imu[idxs]     - cam)
        best  = np.where(left < right, idxs - 1, idxs)
        offsets = (cam - imu[best]).tolist()

        cam_ipf = np.array(_cam_ipf, dtype=np.int64)
        imu_ipf = np.array(_imu_ipf, dtype=np.int64)
        imu_per_frame = [
            int(np.searchsorted(imu_ipf, cam_ipf[i + 1], side='left') -
                np.searchsorted(imu_ipf, cam_ipf[i],     side='left'))
            for i in range(len(cam_ipf) - 1)
        ]
    else:
        import bisect
        offsets = []
        for c in cam_stamps:
            lo, hi = 0, len(imu_stamps) - 1
            while lo < hi:
                mid = (lo + hi) // 2
                if imu_stamps[mid] < c: lo = mid + 1
                else: hi = mid
            best = lo
            if lo > 0 and abs(imu_stamps[lo-1] - c) < abs(imu_stamps[lo] - c):
                best = lo - 1
            offsets.append(c - imu_stamps[best])
        imu_per_frame = [
            bisect.bisect_left(_imu_ipf, _cam_ipf[i+1]) - bisect.bisect_left(_imu_ipf, _cam_ipf[i])
            for i in range(len(_cam_ipf) - 1)
        ]

    os_stat = stats([float(v) for v in offsets])
    result["sync_offset"] = os_stat

    imu_period_ns = ((imu_stamps[-1] - imu_stamps[0]) / max(len(imu_stamps) - 1, 1)
                     if imu_stamps else 2_500_000)
    tol_ns = max(imu_period_ns / 2, 2_000_000)
    result["sync_tolerance_ns"] = tol_ns

    bad_sync = sum(1 for v in offsets if abs(v) > tol_ns)
    pct = 100 * bad_sync / len(offsets) if offsets else 0
    result["frames_bad_sync"] = bad_sync
    result["frames_bad_sync_pct"] = pct
    if pct > 5:
        result["issues"].append(f"{pct:.1f}% of frames have sync offset > {fmt_ns(tol_ns)}")
        result["pass"] = False
    elif pct > 1:
        result["issues"].append(f"{pct:.1f}% of frames have sync offset > {fmt_ns(tol_ns)} (minor)")

    if imu_per_frame:
        ipf_stat = stats([float(v) for v in imu_per_frame])
        result["imu_per_frame"] = ipf_stat
        result["min_imu_per_frame"] = min_imu_per_frame

        t0_ipf = _cam_ipf[0]
        n_intervals = len(imu_per_frame)
        zero_intervals, zero_startup, low_intervals = [], [], []
        for i, cnt in enumerate(imu_per_frame):
            t_start_s = (_cam_ipf[i]     - t0_ipf) / 1e9
            t_end_s   = (_cam_ipf[i + 1] - t0_ipf) / 1e9
            entry = (i, cnt, t_start_s, t_end_s)
            if cnt == 0:
                (zero_startup if i == 0 or i == n_intervals - 1 else zero_intervals).append(entry)
            elif cnt < min_imu_per_frame:
                low_intervals.append(entry)

        result["zero_imu_intervals"]         = zero_intervals
        result["zero_imu_startup_intervals"] = zero_startup
        result["low_imu_intervals"]          = low_intervals

        if zero_startup:
            result["issues"].append(
                f"{len(zero_startup)} zero-IMU interval(s) at start/end (startup artifact)"
            )
        if zero_intervals:
            result["issues"].append(
                f"{len(zero_intervals)} mid-recording frame gap(s) with ZERO IMU — VIO will lose track"
            )
            result["pass"] = False
        if low_intervals:
            result["issues"].append(
                f"{len(low_intervals)} frame gap(s) with < {min_imu_per_frame} IMU samples"
            )

        cv = ipf_stat["std"] / ipf_stat["mean"] if ipf_stat["mean"] > 0 else 0
        result["imu_per_frame_cv"] = cv

        pileup_thresh = 1.5 * ipf_stat["mean"]
        pileup_intervals = [
            (i, cnt, (_cam_ipf[i] - t0_ipf) / 1e9, (_cam_ipf[i+1] - t0_ipf) / 1e9)
            for i, cnt in enumerate(imu_per_frame) if cnt > pileup_thresh
        ]
        result["pileup_intervals"] = pileup_intervals
        if pileup_intervals:
            result["issues"].append(
                f"{len(pileup_intervals)} frame(s) with IMU pile-up "
                f"(>{pileup_thresh:.0f} samples, prior frame likely dropped)"
            )

    return result


# ── Report printers ───────────────────────────────────────────────────────────
def print_topic_report(r: dict):
    status = ok("PASS") if r["pass"] else fail("FAIL")
    s   = r.get("log_interval", {})
    cv  = r.get("jitter_cv", 0)
    gaps = r.get("gaps", [])

    print(f"\n{hdr(r['name'])}  [{status}]")
    print(f"  Messages : {r['msg_count']:,}   Duration : {r.get('duration_s', 0):.1f} s"
          f"   Mean rate : {r.get('mean_hz', 0):.2f} Hz")
    if s:
        print(f"  Interval : mean={fmt_ns(s['mean'])}  std={fmt_ns(s['std'])}  "
              f"min={fmt_ns(s['min'])}  max={fmt_ns(s['max'])}")
        print(f"             p95={fmt_ns(s['p95'])}  p99={fmt_ns(s['p99'])}")
    print(f"  Jitter CV: {cv:.4f}  "
          f"{'(OK)' if cv < 0.05 else '(WARN)' if cv < 0.15 else '(HIGH)'}")

    if "stamp_latency" in r:
        ls = r["stamp_latency"]
        print(f"  Hdr latency: mean={fmt_ns(ls['mean'])}  std={fmt_ns(ls['std'])}  "
              f"min={fmt_ns(ls['min'])}  max={fmt_ns(ls['max'])}")

    if gaps:
        print(f"  Gaps (>{3}× period): {len(gaps)} detected, total={r['total_gap_s']:.2f} s")
        for i, (idx, gns) in enumerate(gaps[:5]):
            print(f"    gap[{i}] after msg #{idx}: {fmt_ns(gns)}")
        if len(gaps) > 5:
            print(f"    ... and {len(gaps) - 5} more")

    dropped = r.get("dropped_frames", [])
    if dropped:
        print(f"  Dropped frames: {len(dropped)} interval(s), ~{r['total_dropped_frames']} lost")
        for idx, gns, t_s, nd in dropped[:10]:
            print(f"    after msg #{idx:6d}  t={t_s:7.2f} s  interval={fmt_ns(gns)}  (~{nd} dropped)")
        if len(dropped) > 10:
            print(f"    ... and {len(dropped) - 10} more")

    for issue in r["issues"]:
        print(f"  {warn(issue)}")


def print_sync_report(r: dict):
    status = ok("PASS") if r["pass"] else fail("FAIL")
    print(f"\n{hdr('Camera-IMU Sync')}  [{status}]")

    os_ = r.get("sync_offset", {})
    tol = r.get("sync_tolerance_ns", 0)
    if os_:
        print(f"  Cam→IMU offset : mean={fmt_ns(os_['mean'])}  std={fmt_ns(os_['std'])}  "
              f"max={fmt_ns(os_['max'])}")
        print(f"  Tolerance      : ±{fmt_ns(tol)}")
        print(f"  Out-of-tolerance: {r['frames_bad_sync']} frames ({r['frames_bad_sync_pct']:.1f}%)")

    if "imu_per_frame" in r:
        ipf = r["imu_per_frame"]
        cv  = r.get("imu_per_frame_cv", 0)
        mipf = r.get("min_imu_per_frame", 0)
        print(f"  IMU/frame      : mean={ipf['mean']:.1f}  std={ipf['std']:.1f}  "
              f"min={ipf['min']:.0f}  max={ipf['max']:.0f}  CV={cv:.3f}  (threshold: ≥{mipf})")

        startup = r.get("zero_imu_startup_intervals", [])
        zero    = r.get("zero_imu_intervals", [])
        low     = r.get("low_imu_intervals",  [])
        pileup  = r.get("pileup_intervals",   [])

        if startup:
            print(f"  {warn(f'{len(startup)} zero-IMU interval(s) at bag start/end (startup artifact)')}")
        if zero:
            print(f"  {fail(f'ZERO-IMU intervals mid-recording: {len(zero)} found')}")
            for idx, cnt, ts, te in zero[:10]:
                print(f"    frame #{idx:5d}  t={ts:7.2f}–{te:.2f} s  imu={cnt}")
            if len(zero) > 10:
                print(f"    ... and {len(zero) - 10} more")
        elif not startup:
            print(f"  {ok('No zero-IMU intervals')}")

        if low:
            print(f"  {warn(f'Low-IMU intervals (< {mipf} samples): {len(low)} found')}")
            for idx, cnt, ts, te in low[:10]:
                print(f"    frame #{idx:5d}  t={ts:7.2f}–{te:.2f} s  imu={cnt}")
            if len(low) > 10:
                print(f"    ... and {len(low) - 10} more")
        else:
            print(f"  {ok(f'All intervals have ≥{mipf} IMU samples')}")

        if pileup:
            exp = r["imu_per_frame"]["mean"]
            print(f"  {warn(f'IMU pile-up (>{1.5*exp:.0f} samples): {len(pileup)} found')}")
            for idx, cnt, ts, te in pileup[:10]:
                print(f"    frame #{idx:5d}  t={ts:7.2f}–{te:.2f} s  imu={cnt}  (expected ~{exp:.0f})")
            if len(pileup) > 10:
                print(f"    ... and {len(pileup) - 10} more")
        else:
            print(f"  {ok('No IMU pile-up detected')}")

    for issue in r["issues"]:
        print(f"  {warn(issue)}")


# ── Plotting ──────────────────────────────────────────────────────────────────
def plot_results(topics_data: dict, sync_result: dict):
    if not HAS_MATPLOTLIB:
        print("[INFO] matplotlib not available — skipping plots")
        return

    n_topics = len(topics_data)
    fig, axes = plt.subplots(n_topics + 1, 2, figsize=(14, 4 * (n_topics + 1)))
    if n_topics + 1 == 1:
        axes = [axes]

    for row, (topic, tdata) in enumerate(topics_data.items()):
        log_times = tdata["log_times"]
        if len(log_times) < 2:
            continue
        ivals_ms = [v / 1e6 for v in intervals_ns(log_times)]
        t_axis   = [(log_times[i] - log_times[0]) / 1e9 for i in range(1, len(log_times))]
        short    = topic.split("/")[-1]
        ax0, ax1 = axes[row]
        ax0.plot(t_axis, ivals_ms, linewidth=0.6, color="steelblue")
        ax0.set_title(f"{short} – inter-message interval (ms)")
        ax0.set_xlabel("Time (s)"); ax0.set_ylabel("Interval (ms)")
        ax1.hist(ivals_ms, bins=100, color="steelblue", edgecolor="none")
        ax1.set_title(f"{short} – interval histogram")
        ax1.set_xlabel("Interval (ms)"); ax1.set_ylabel("Count")

    row = n_topics
    ax0, ax1 = axes[row]
    os_ = sync_result.get("sync_offset", {})
    if os_:
        labels = ["mean", "std", "p95", "p99"]
        ax0.bar(labels, [abs(os_.get(k, 0)) / 1e6 for k in labels],
                color=["green", "orange", "red", "darkred"])
        ax0.set_title("Camera-IMU sync offset (ms)"); ax0.set_ylabel("ms")
    ipf = sync_result.get("imu_per_frame", {})
    if ipf:
        ax1.bar(["mean", "std", "min", "max"], [ipf.get(k, 0) for k in ["mean","std","min","max"]],
                color="steelblue")
        ax1.set_title("IMU samples per camera frame"); ax1.set_ylabel("Count")

    plt.tight_layout()
    plt.show()


# ── Entry point (called from cli.py) ─────────────────────────────────────────
def run(args) -> None:
    cam_topic = args.camera
    imu_topic = args.imu
    rtk_topic = args.rtk

    read_topics = [cam_topic, imu_topic]
    if rtk_topic:
        read_topics.append(rtk_topic)

    print(SEP)
    print(f"  VIO Bag Quality Report")
    print(f"  Bag    : {args.input_bag}")
    rtk_label = f"  |  {rtk_topic}" if rtk_topic else ""
    print(f"  Topics : {cam_topic}  |  {imu_topic}{rtk_label}")
    print(SEP)

    print("[INFO] Reading bag …")
    topics_raw = read_timestamps(args.input_bag, read_topics)

    cam_log   = topics_raw[cam_topic]["log_times"]
    cam_stamp = topics_raw[cam_topic]["stamp_times"]
    imu_log   = topics_raw[imu_topic]["log_times"]
    imu_stamp = topics_raw[imu_topic]["stamp_times"]
    rtk_log   = topics_raw[rtk_topic]["log_times"]  if rtk_topic else []
    rtk_stamp = topics_raw[rtk_topic]["stamp_times"] if rtk_topic else []

    if not cam_log:
        print(fail(f"No messages found on camera topic '{cam_topic}'"))
    if not imu_log:
        print(fail(f"No messages found on IMU topic '{imu_topic}'"))
    if rtk_topic and not rtk_log:
        print(warn(f"No messages found on RTK topic '{rtk_topic}' — skipping RTK check"))

    print(SEP)
    results = {}
    if cam_log:
        r = analyze_topic(cam_topic, cam_log, cam_stamp, args.cam_hz, args.gap_mult)
        results[cam_topic] = r
        print_topic_report(r)
    if imu_log:
        r = analyze_topic(imu_topic, imu_log, imu_stamp, args.imu_hz, args.gap_mult)
        results[imu_topic] = r
        print_topic_report(r)
    if rtk_topic and rtk_log:
        r = analyze_topic(rtk_topic, rtk_log, rtk_stamp, 5.0, args.gap_mult)
        results[rtk_topic] = r
        print_topic_report(r)

    print(SEP)
    cam_r = results.get(cam_topic, {})
    cam_stamp_std = (cam_r.get("stamp_latency") or {}).get("std", 0)
    cam_period_ns = 1e9 / cam_r["mean_hz"] if cam_r.get("mean_hz") else 50_000_000
    cam_stamp_jitter_high = cam_stamp_std > cam_period_ns / 2

    use_cam_sync = cam_stamp if (len(cam_stamp) == len(cam_log) and cam_stamp) else cam_log
    use_imu_sync = imu_stamp if (len(imu_stamp) == len(imu_log) and imu_stamp) else imu_log
    use_cam_ipf  = cam_log if cam_stamp_jitter_high else use_cam_sync
    use_imu_ipf  = imu_log if cam_stamp_jitter_high else use_imu_sync
    if cam_stamp_jitter_high:
        print(f"[INFO] Camera stamp jitter high (std={fmt_ns(cam_stamp_std)}) "
              f"— using log_times for IMU-per-frame counting")

    sync_result = analyze_cam_imu_sync(
        sorted(use_cam_sync), sorted(use_imu_sync),
        min_imu_per_frame=args.min_imu_per_frame,
        cam_times_ipf=sorted(use_cam_ipf),
        imu_times_ipf=sorted(use_imu_ipf),
    )
    print_sync_report(sync_result)

    all_pass = all(v["pass"] for v in results.values()) and sync_result["pass"]
    print(f"\n{SEP}")
    if all_pass:
        print(f"  Overall: {ok('BAG IS READY FOR VIO')}")
    else:
        print(f"  Overall: {fail('BAG HAS ISSUES — review warnings above')}")
    print(SEP + "\n")

    if args.plot:
        plot_results(topics_raw, sync_result)
