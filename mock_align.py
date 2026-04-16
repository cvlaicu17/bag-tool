#!/usr/bin/env python3
"""Mock ArUco north alignment without VIO/poseimu messages.

Assumes R_ItoG = I  (VIO global frame == IMU frame at the moment of detection).
Also reads the RTK yaw topic and computes a parallel RTK-based bearing in the
same IMU frame, so the two can be compared directly — revealing the ~7° gap
between the ArUco method and the RTK compass without needing any VIO messages.

RTK → IMU-frame bearing derivation
────────────────────────────────────
From R_cam_imu calibration:
  camera-Y (image-down) ≈ −IMU-X   →  IMU-X = drone backward
  camera-X (image-right) ≈ −IMU-Y  →  IMU-Y = drone left

For a level drone at compass heading ψ (degrees CW from N):
  north_imu_rtk = R_GtoI @ [0,1,0] = [−cos(ψ),  sin(ψ), 0]
  bearing_rtk   = atan2(north_imu_rtk[0], north_imu_rtk[1])
                = atan2(−cos(ψ), sin(ψ))

Usage:
    python mock_align.py <bag_path> \\
        [--topic /camera/image_mono] \\
        [--rtk-topic /m300/rtk/yaw] \\
        [--rtk-scale 10.0]          \\   # raw_value × scale = heading in degrees
        [--max-frames 900]
"""

import argparse
import bisect
import math
from pathlib import Path

import cv2
import numpy as np
from scipy.spatial.transform import Rotation

# ── calibration constants (same as aruco_align.py) ───────────────────────────

_K = np.array([
    [1129.02, 0.0,     620.05],
    [0.0,     1129.82, 499.31],
    [0.0,     0.0,     1.0   ],
], dtype=np.float64)

_DIST = np.array([0.0324, -0.0627, 0.00047, -0.00031], dtype=np.float64)

_R_CAM_IMU = np.array([
    [ 0.0074, -0.9999, -0.0109],
    [-0.9999, -0.0074, -0.0043],
    [ 0.0043,  0.0109, -0.9999],
], dtype=np.float64)

_R_IMU_CAM = Rotation.from_matrix(_R_CAM_IMU.T)

TARGET_ID   = 70
MARKER_SIZE = 0.235   # metres


_CLAHE = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))


def _preprocess_variants(gray):
    yield gray
    yield _CLAHE.apply(gray)
    p98 = float(np.percentile(gray, 98))
    clipped = np.clip(gray, 0, p98)
    yield cv2.normalize(clipped, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)


def _make_detector():
    aruco_dict = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_5X5_100)
    if hasattr(cv2.aruco, 'DetectorParameters'):
        p = cv2.aruco.DetectorParameters()
    else:
        p = cv2.aruco.DetectorParameters_create()
    p.adaptiveThreshWinSizeMin        = 3
    p.adaptiveThreshWinSizeMax        = 73
    p.adaptiveThreshWinSizeStep       = 4
    p.errorCorrectionRate             = 0.9
    p.maxErroneousBitsInBorderRate    = 0.5
    p.minMarkerPerimeterRate          = 0.02

    if hasattr(cv2.aruco, 'ArucoDetector'):
        detector = cv2.aruco.ArucoDetector(aruco_dict, p)
        def detect(gray):
            for img in _preprocess_variants(gray):
                corners, ids, rej = detector.detectMarkers(img)
                if ids is not None:
                    return corners, ids, rej
            return [], None, []
    else:
        def detect(gray):
            for img in _preprocess_variants(gray):
                corners, ids, rej = cv2.aruco.detectMarkers(img, aruco_dict, parameters=p)
                if ids is not None:
                    return corners, ids, rej
            return [], None, []
    return detect


def _nearest(ts_list, ts):
    """Return value from [(ts, val), ...] nearest to ts."""
    if not ts_list:
        return None
    idx = bisect.bisect_left(ts_list, (ts,))
    candidates = ts_list[max(0, idx - 1): idx + 1]
    return min(candidates, key=lambda x: abs(x[0] - ts))[1]


def _wrap180(deg):
    """Wrap angle to (−180, +180]."""
    return (deg + 180.0) % 360.0 - 180.0


def run(bag_path: Path, cam_topic: str, rtk_topic: str,
        rtk_scale: float, max_frames: int, min_detections: int = 10) -> None:
    from rosbags.rosbag2 import Reader
    from rosbags.typesys import get_typestore, Stores

    detect = _make_detector()

    for store in (Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE):
        try:
            typestore = get_typestore(store)
            break
        except Exception:
            continue

    # ── 1. Load RTK yaw messages ──────────────────────────────────────────────
    rtk_ts_vals = []   # [(bag_ts_ns, heading_deg), ...]
    if rtk_topic:
        with Reader(bag_path) as reader:
            rtk_conns = [c for c in reader.connections if c.topic == rtk_topic]
            if not rtk_conns:
                print(f'Warning: RTK topic {rtk_topic!r} not found — skipping RTK comparison')
                rtk_topic = ''
            else:
                for conn, bag_ts, rawdata in reader.messages(connections=rtk_conns):
                    try:
                        msg = typestore.deserialize_cdr(rawdata, conn.msgtype)
                        rtk_ts_vals.append((bag_ts, float(msg.data) * rtk_scale))
                    except Exception:
                        pass
                rtk_ts_vals.sort()
                print(f'Loaded {len(rtk_ts_vals)} RTK yaw messages')

    # ── 2. Detect ArUco in camera frames ─────────────────────────────────────
    aruco_sin = aruco_cos = 0.0
    rtk_sins  = []
    rtk_cos_  = []
    n_det = n_frames = 0

    with Reader(bag_path) as reader:
        conns = [c for c in reader.connections if c.topic == cam_topic]
        if not conns:
            print(f'Topic {cam_topic!r} not found in bag.')
            return

        for conn, bag_ts, rawdata in reader.messages(connections=conns):
            if n_frames >= max_frames:
                break
            n_frames += 1

            msg      = typestore.deserialize_cdr(rawdata, 'sensor_msgs/msg/Image')
            img_data = np.frombuffer(bytes(msg.data), dtype=np.uint8)
            gray     = img_data.reshape(msg.height, msg.width)

            corners, ids, _ = detect(gray)
            if ids is None:
                continue
            found = [i for i, mid in enumerate(ids.flatten()) if mid == TARGET_ID]
            if not found:
                continue

            corn = np.array([corners[found[0]]], dtype=np.float32)
            rvec, tvec, _ = cv2.aruco.estimatePoseSingleMarkers(
                corn, MARKER_SIZE, _K, _DIST)
            R_cam_marker, _ = cv2.Rodrigues(rvec[0])

            north_cam = R_cam_marker @ np.array([0.0, 1.0, 0.0])
            north_imu = _R_IMU_CAM.apply(north_cam)   # R_ItoG = I → north_G = north_imu

            aruco_bearing = math.atan2(north_imu[0], north_imu[1])

            # RTK bearing at this timestamp
            rtk_heading = _nearest(rtk_ts_vals, bag_ts) if rtk_ts_vals else None
            rtk_bearing = None
            if rtk_heading is not None:
                psi = math.radians(rtk_heading)
                # north_imu_rtk = [−cos(ψ), sin(ψ), 0]  (IMU-X=backward, IMU-Y=left)
                rtk_bearing = math.atan2(-math.cos(psi), math.sin(psi))
                rtk_sins.append(math.sin(rtk_bearing))
                rtk_cos_.append(math.cos(rtk_bearing))

            if n_det < 5:
                rtk_str = (f'  rtk_hdg={rtk_heading:.1f}°  rtk_bearing={math.degrees(rtk_bearing):+.2f}°'
                           f'  diff={_wrap180(math.degrees(aruco_bearing - rtk_bearing)):+.2f}°'
                           if rtk_bearing is not None else '')
                print(f'  det {n_det}: north_imu={north_imu.round(3)}  '
                      f'aruco_bearing={math.degrees(aruco_bearing):+.2f}°{rtk_str}')

            aruco_sin += math.sin(aruco_bearing)
            aruco_cos += math.cos(aruco_bearing)
            n_det     += 1

            if n_det >= min_detections:
                break

    print(f'\nDetected marker in {n_det} / {n_frames} frames scanned')

    if n_det < min_detections:
        print(f'Too few detections ({n_det} < {min_detections}), aborting.')
        return

    # ── 3. Summary ───────────────────────────────────────────────────────────
    aruco_mean = math.atan2(aruco_sin, aruco_cos)
    imu_x_bearing = (90.0 - math.degrees(aruco_mean)) % 360.0

    print(f'\n── ArUco (image + calibration, R_ItoG = I) ─────────────────')
    print(f'  mean bearing : {math.degrees(aruco_mean):+.2f}°')
    print(f'  IMU-X compass: {imu_x_bearing:.1f}°')

    if rtk_sins:
        rtk_mean = math.atan2(sum(rtk_sins) / len(rtk_sins),
                              sum(rtk_cos_)  / len(rtk_cos_))
        rtk_imu_x = (90.0 - math.degrees(rtk_mean)) % 360.0
        diff = _wrap180(math.degrees(aruco_mean - rtk_mean))

        print(f'\n── RTK (from /m300/rtk/yaw, IMU-X=backward convention) ─────')
        print(f'  mean bearing : {math.degrees(rtk_mean):+.2f}°')
        print(f'  IMU-X compass: {rtk_imu_x:.1f}°')

        print(f'\n── Comparison ───────────────────────────────────────────────')
        print(f'  ArUco − RTK  : {diff:+.2f}°  '
              f'(positive → ArUco places north further CW from IMU-X)')
        print(f'  Interpretation: ArUco implies drone heading '
              f'{_wrap180(rtk_imu_x - imu_x_bearing):+.1f}° '
              f'{"more" if imu_x_bearing < rtk_imu_x else "less"} than RTK reports')


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('bag',  help='Bag path (.mcap file or directory)')
    ap.add_argument('--topic',     default='/camera/image_mono')
    ap.add_argument('--rtk-topic', default='/m300/rtk/yaw',
                    help='RTK yaw topic (std_msgs/Float32 .data field)')
    ap.add_argument('--rtk-scale', type=float, default=10.0,
                    help='Multiply raw .data value by this to get degrees (default 10.0)')
    ap.add_argument('--max-frames', type=int, default=900,
                    help='Max camera frames to scan (default 900 ≈ 30 s at 30 fps)')
    args = ap.parse_args()

    bag_path = Path(args.bag)
    if not bag_path.exists():
        print(f'ERROR: {bag_path} does not exist')
        raise SystemExit(1)

    run(bag_path, args.topic, args.rtk_topic, args.rtk_scale, args.max_frames)


if __name__ == '__main__':
    main()
