"""cam-lag: measure the camera CONTENT lag against the IMU/GT clock, per stream.

Why this exists. The PAS streaming camera delivers image k carrying request k's
timestamp but the RENDERED CONTENT of request k-1 (async render/readback). The
recorder repairs this by re-pairing each frame with the previous request's stamp --
an INDEX-based fix that is only correct if the offset really is one frame at the
rate being flown. It was verified at 19 and 20 Hz; every change of camera rate (or
of the capture code -- on 5.7 the depth capture became synchronous) must re-measure
it rather than assume it. This tool is that measurement, on the finished bag.

Method: image ROTATION rate vs GT yaw rate. A nadir camera turning with the aircraft
sees the ground rotate about the image centre. Consecutive frames are related by a
2-D similarity (rotation, scale, translation) estimated from tracked features
(goodFeaturesToTrack + pyramidal LK + RANSAC estimateAffinePartial2D), for the mono
image and for the depth image alike (terrain relief, contrast-stretched). The route's
heading changes are sharp events, so the residual between measured rotation rate and
the GT yaw rate, evaluated at candidate time shifts, has a well-defined minimum. The
fitted gain is reported and must come out near +-1: that is the proof the measurement
is physical. Two methods were tried first and rejected: centre depth vs altitude over
the pad (DEGENERATE on a fleet mission -- its climbs are near-constant-rate ramps, and
a time shift on a ramp is absorbed by the unknown ground height) and log-polar phase
correlation (forward motion translates the image at the same time, so the "rotation"
it reports is noise).

Sign: lag > 0 means the frame's content is OLDER than its stamp (stamped late).
After the recorder's repair both streams sit within a few ms of 0: measured on
fleet1/d01 at 20 fps mono -3.4 / depth -9.3 ms and on the 40 fps POC of the same
route mono -3.4 / depth -4.7 ms -- a small CONSTANT offset, not one that scales
with the frame interval, which is exactly what a one-frame error would do (-50 ms
at 20 fps, -25 ms at 40 fps). Injecting +50 / +25 ms moved the estimates by 49.7-50.0
/ 25.0-25.2 ms, so the estimator resolves well under a tick; the sharpness figure
(RMS rise at +-1 frame vs the best) is a weak statistic on the depth stream (1.07 at
40 fps) because depth relief tracks worse than mono texture, hence the low bar on it.

Self-test (--inject-ms): shift the camera stamps by a known amount before fitting;
the tool must report that amount back. Run it once per new rate.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.vib_check import ok, warn, fail, hdr, SEP

_TS = get_typestore(Stores.ROS2_JAZZY)
SKY_M = 5000.0


def _to_u8(img):
    """Contrast-stretch a float image (depth relief) to uint8 for feature tracking."""
    lo, hi = np.percentile(img, [1, 99])
    if hi - lo < 1e-6:
        return np.zeros(img.shape, np.uint8)
    return np.clip((img - lo) / (hi - lo) * 255.0, 0, 255).astype(np.uint8)


def _similarity(prev, cur):
    """(rotation rad, scale, n_inliers) of the 2-D similarity mapping prev -> cur, or None."""
    import cv2
    p0 = cv2.goodFeaturesToTrack(prev, maxCorners=400, qualityLevel=0.01, minDistance=8)
    if p0 is None or len(p0) < 30:
        return None
    p1, st, _ = cv2.calcOpticalFlowPyrLK(prev, cur, p0, None, winSize=(21, 21), maxLevel=3)
    good = st.ravel() == 1
    if good.sum() < 20:
        return None
    M, inl = cv2.estimateAffinePartial2D(p0[good], p1[good], method=cv2.RANSAC, ransacReprojThreshold=1.0)
    if M is None or inl is None or int(inl.sum()) < 15:
        return None
    return math.atan2(M[1, 0], M[0, 0]), math.hypot(M[0, 0], M[1, 0]), int(inl.sum())


def _rotation_series(bag_path, gt_topic, depth_topic, mono_topic, scale, min_agl_m):
    """One streaming pass. Returns gt dict and, per stream, arrays (t_mid, rot_deg_per_s)
    of frame-to-frame image rotation from tracked features."""
    import cv2
    gt_t, gt_z, gt_yaw = [], [], []
    ground = None
    prev = {"mono": None, "depth": None}
    out = {"mono": [], "depth": []}
    with Reader(Path(bag_path)) as reader:
        topics = {c.topic: c for c in reader.connections}
        if gt_topic not in topics:
            raise SystemExit(f"need {gt_topic} in the bag")
        conns = [topics[t] for t in (gt_topic, depth_topic, mono_topic) if t in topics]
        for conn, ts, raw in reader.messages(connections=conns):
            msg = _TS.deserialize_cdr(raw, conn.msgtype)
            if conn.topic == gt_topic:
                p = msg.pose.position; o = msg.pose.orientation
                if ground is None:
                    ground = -p.z
                w, x, y, z = o.w, o.x, o.y, o.z
                gt_t.append(ts); gt_z.append(p.z)
                gt_yaw.append(math.atan2(2 * (w * z + x * y), 1 - 2 * (y * y + z * z)))
                continue
            if ground is None or (-gt_z[-1] - ground) < min_agl_m:
                prev["mono"] = prev["depth"] = None      # on the ground: no rotation signal
                continue
            if conn.topic == depth_topic:
                key = "depth"
                if msg.encoding == "32FC1":
                    img = np.frombuffer(bytes(msg.data), np.float32).reshape(msg.height, msg.step // 4)[:, :msg.width]
                else:
                    img = np.frombuffer(bytes(msg.data), np.uint16).reshape(msg.height, msg.step // 2)[:, :msg.width].astype(np.float32)
                img = np.where(img >= SKY_M, np.nan, img)
                img = np.nan_to_num(img, nan=float(np.nanmedian(img)))
                small = _to_u8(cv2.resize(img, (img.shape[1] // scale, img.shape[0] // scale), interpolation=cv2.INTER_AREA))
            else:
                key = "mono"
                if msg.encoding not in ("mono8", "8UC1"):
                    continue
                img = np.frombuffer(bytes(msg.data), np.uint8).reshape(msg.height, msg.step)[:, :msg.width]
                small = cv2.resize(img, (img.shape[1] // scale, img.shape[0] // scale), interpolation=cv2.INTER_AREA)
            if prev[key] is not None and 0 < (ts - prev[key][0]) < 0.25e9:
                r = _similarity(prev[key][1], small)
                if r is not None:
                    dt = (ts - prev[key][0]) / 1e9
                    out[key].append(((ts + prev[key][0]) / 2.0, math.degrees(r[0]) / dt))
            prev[key] = (ts, small)
    gt = {"t": np.array(gt_t, float), "yaw": np.unwrap(np.array(gt_yaw))}
    return gt, {k: (np.array([a for a, _ in v], float), np.array([b for _, b in v])) for k, v in out.items()}


def fit_lag(gt, t_mid, rot, frame_ms, lags_ms=np.arange(-150, 150.5, 0.5)):
    """Least-squares gain (sign included: the polar angle convention need not match
    the yaw sign) between measured rotation rate and GT yaw rate at each candidate
    lag; the lag with the smallest residual wins, refined parabolically."""
    if len(t_mid) < 50:
        return None
    yaw_rate_all = np.gradient(gt["yaw"], gt["t"] / 1e9)
    # limit the fit to where the aircraft actually turns -- flat stretches carry no
    # timing information and only add noise
    turning = np.abs(np.interp(t_mid, gt["t"], yaw_rate_all)) > np.deg2rad(2.0)
    if turning.sum() < 30:
        return None
    rms, gains = [], []
    for L in lags_ms:
        pred = np.rad2deg(np.interp(t_mid + L * 1e6, gt["t"], yaw_rate_all))
        gain = float(np.dot(rot[turning], pred[turning]) / max(np.dot(pred[turning], pred[turning]), 1e-12))
        gains.append(gain)
        rms.append(float(np.sqrt(np.mean((rot[turning] - gain * pred[turning]) ** 2))))
    rms = np.array(rms)
    i = int(np.argmin(rms))
    off = 0.0
    if 0 < i < len(rms) - 1:
        y0, y1, y2 = rms[i - 1], rms[i], rms[i + 1]
        den = (y0 - 2 * y1 + y2)
        off = 0.5 * (y0 - y2) / den if den > 0 else 0.0
    step = lags_ms[1] - lags_ms[0]
    shift = float(lags_ms[i] + off * step)
    # `shift` is the time ADDED to the stamp to reach the GT that matches the content;
    # content OLDER than its stamp (stamped late) needs a negative shift, so the lag
    # in the documented sign (positive = stamped late) is -shift. Verified by the
    # self-test: injecting +50 ms moved shift by -49.7/-49.9 ms.
    best = -shift
    j = int(np.argmin(np.abs(lags_ms)))
    k = int(round(frame_ms / step))               # sharpness judged at +-ONE FRAME
    return {"lag_ms": round(best, 2), "gain": round(gains[i], 3),
            "rms": round(float(rms[i]), 3), "rms_at_zero": round(float(rms[j]), 3),
            "rms_at_pm1frame": round(float(min(rms[max(0, i - k)], rms[min(len(rms) - 1, i + k)])), 3),
            "n_pairs": int(len(t_mid)), "n_turning": int(turning.sum())}


def analyze(bag_path, gt_topic="/pf_geo_loc/fc_local_position", depth_topic="/camera/depth",
            mono_topic="/camera/image_mono", inject_ms=0.0, min_agl_m=5.0, scale=2):
    gt, series = _rotation_series(bag_path, gt_topic, depth_topic, mono_topic, scale, min_agl_m)
    res = {"bag": str(bag_path), "inject_ms": inject_ms}
    side = str(Path(bag_path.rstrip("/"))) + "_camera.json"
    rate = None
    if Path(side).exists():
        try:
            c = json.load(open(side)); rate = c.get("rate_hz"); res["tick_ms"] = c.get("clock_step_ns", 2.5e6) / 1e6
        except Exception:
            pass
    if not rate:                                  # measure it from the mono stamps
        t_mid = series["mono"][0]
        rate = 1e9 / float(np.median(np.diff(t_mid))) if len(t_mid) > 10 else 20.0
    res["rate_hz"] = rate
    res["frame_ms"] = 1000.0 / rate
    for key in ("depth", "mono"):
        t_mid, rot = series[key]
        if inject_ms:
            t_mid = t_mid + inject_ms * 1e6
        res[key] = fit_lag(gt, t_mid, rot, res["frame_ms"])
    return res


def run(args) -> int:
    print(hdr(f"cam-lag: {args.input_bag}" + (f"  (injected {args.inject_ms:+g} ms)" if args.inject_ms else "")))
    r = analyze(args.input_bag, args.gt_topic, args.depth_topic, args.mono_topic, args.inject_ms,
                args.min_agl)
    frame_ms = r["frame_ms"]
    tol = args.tolerance_ms if args.tolerance_ms > 0 else 0.4 * frame_ms
    print(f"rate {r['rate_hz']:.3g} Hz (frame {frame_ms:.2f} ms)   clock tick {r.get('tick_ms', 2.5)} ms   "
          f"min AGL {args.min_agl} m   method: tracked-feature image rotation vs GT yaw rate at the turns")
    print(f"the question is ZERO or ONE frame: a one-frame stamp error would read ±{frame_ms:.1f} ms; "
          f"pass if |lag - expected| <= {tol:.1f} ms and the fit is sharp at ±1 frame")
    print(SEP)
    rc = 0
    for stream in ("depth", "mono"):
        f = r[stream]
        if f is None:
            print(f"  - {stream:6s} not enough turning frames to fit")
            continue
        expect = args.inject_ms
        err = f["lag_ms"] - expect
        sharp = f["rms_at_pm1frame"] / max(f["rms"], 1e-9)
        good = abs(err) <= tol and sharp >= 1.05 and 0.7 <= abs(f["gain"]) <= 1.3
        mark = "✔" if good else "✘"
        print(f"  {mark} {stream:6s} content lag {f['lag_ms']:+7.2f} ms  (expected {expect:+g} ms, "
              f"error {err:+.2f} ms; gain {f['gain']:+.3f} (must be ~±1); pairs {f['n_pairs']}, turning "
              f"{f['n_turning']}; RMS deg/s at best / zero / ±1 frame = {f['rms']} / {f['rms_at_zero']} / "
              f"{f['rms_at_pm1frame']}, sharpness {sharp:.2f})")
        if not good:
            rc = 1
    print(SEP)
    print((ok if rc == 0 else fail)("camera stamps consistent with content (no one-frame error)"
                                    if rc == 0 else "camera content lag OUTSIDE tolerance (or fit not sharp)"))
    if getattr(args, "json", None):
        json.dump(r, open(args.json, "w"), indent=1); print(f"wrote {args.json}")
    return rc
