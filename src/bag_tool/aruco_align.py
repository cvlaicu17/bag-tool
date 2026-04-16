"""ArUco-based north alignment for RTK↔VIO calibration.

Uses an ArUco marker (DICT_5X5_100 ID 70) displayed by the sky-lign app.
The app orients the marker so its top edge (marker +Y axis) always points north.
Detecting the marker in the camera frames gives a direct north measurement in
the camera frame, which is rotated into the VIO world frame via:
  camera → IMU (kalibr T_cam_imu) → VIO global (poseimu orientation).

Calibration (sky-lign / skyline config, kalibr_imucam_chain.yaml):
  Camera: /camera/image_mono, mono8, 1280×720
  Intrinsics: fx=1129.02, fy=1129.82, cx=620.05, cy=499.31
  Distortion (radtan): k1=0.0324, k2=-0.0627, p1=0.00047, p2=-0.00031
  T_cam_imu rotation (R_cam_imu):
    [ 0.0074, -0.9999, -0.0109]
    [-0.9999, -0.0074, -0.0043]
    [ 0.0043,  0.0109, -0.9999]
"""

from __future__ import annotations

import bisect
import math
from pathlib import Path

import numpy as np
from scipy.spatial.transform import Rotation


# ---------------------------------------------------------------------------
# Hard-coded calibration constants
# ---------------------------------------------------------------------------
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

# R_imu_cam rotates a vector from camera frame into IMU frame
_R_IMU_CAM = Rotation.from_matrix(_R_CAM_IMU.T)


def detect_aruco_north(
    reader_path: Path,
    posimus: list,
    typestore,
    cam_topic: str = '/camera/image_mono',
    min_detections: int = 10,
    max_frames: int = 600,
    marker_size_m: float = 0.235,
    cam_to_vio_ts_offset: int = 0,
) -> float | None:
    """Detect ArUco marker ID 70 (DICT_5X5_100) in camera frames and return align_yaw_rad.

    For each frame where the marker is detected, north_G is derived and accumulated
    via a circular mean. Returns the yaw offset if ≥ min_detections frames contain
    the marker, otherwise returns None with a warning.

    cam_to_vio_ts_offset: subtract from camera bag-level timestamps to get the
    equivalent VIO bag timestamp for poseimu lookup. Required when the camera
    (ref_bag, DJI boot-clock) and VIO bag use different clock domains.

    align_yaw_rad is defined the same way as in compute_alignment() so it can be
    fed directly into Rotation.from_euler('z', align_yaw_rad).
    """
    import cv2

    aruco_dict = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_5X5_100)

    # Detector parameters tuned for robustness against glare / partial occlusion
    if hasattr(cv2.aruco, 'DetectorParameters'):
        _p = cv2.aruco.DetectorParameters()
    else:
        _p = cv2.aruco.DetectorParameters_create()
    _p.adaptiveThreshWinSizeMin        = 3
    _p.adaptiveThreshWinSizeMax        = 73
    _p.adaptiveThreshWinSizeStep       = 4
    _p.errorCorrectionRate             = 0.9
    _p.maxErroneousBitsInBorderRate    = 0.5
    _p.minMarkerPerimeterRate          = 0.02

    _clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))

    def _preprocess_variants(gray):
        yield gray
        yield _clahe.apply(gray)
        p98 = float(np.percentile(gray, 98))
        clipped = np.clip(gray, 0, p98)
        yield cv2.normalize(clipped, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)

    if hasattr(cv2.aruco, 'ArucoDetector'):
        _aruco_detector = cv2.aruco.ArucoDetector(aruco_dict, _p)
        def _detect_markers(gray):
            for img in _preprocess_variants(gray):
                corners, ids, rej = _aruco_detector.detectMarkers(img)
                if ids is not None:
                    return corners, ids, rej
            return [], None, []
    else:
        def _detect_markers(gray):
            for img in _preprocess_variants(gray):
                corners, ids, rej = cv2.aruco.detectMarkers(
                    img, aruco_dict, parameters=_p)
                if ids is not None:
                    return corners, ids, rej
            return [], None, []

    TARGET_ID = 70

    # Index posimus by bag-level timestamp (not header stamp) to avoid cross-clock issues.
    posimus_bag_ts = [ts for ts, _ in posimus]

    def _nearest_poseimu(vio_bag_ts: int):
        if not posimus_bag_ts:
            return None
        idx = bisect.bisect_left(posimus_bag_ts, vio_bag_ts)
        if idx == 0:
            return posimus[0][1]
        if idx >= len(posimus_bag_ts):
            return posimus[-1][1]
        before_ts, before_msg = posimus[idx - 1]
        after_ts,  after_msg  = posimus[idx]
        return before_msg if (vio_bag_ts - before_ts) <= (after_ts - vio_bag_ts) else after_msg

    sin_sum = 0.0
    cos_sum = 0.0
    n_detected = 0
    n_frames = 0

    IMG_TYPE = 'sensor_msgs/msg/Image'

    from rosbags.rosbag2 import Reader

    with Reader(reader_path) as reader:
        cam_conns = [c for c in reader.connections if c.topic == cam_topic]
        if not cam_conns:
            print(f'ArUco precheck: camera topic {cam_topic!r} not found in bag')
            return None

        for conn, bag_ts, rawdata in reader.messages(connections=cam_conns):
            if n_frames >= max_frames:
                break
            n_frames += 1

            msg = typestore.deserialize_cdr(rawdata, IMG_TYPE)
            img_data = np.frombuffer(bytes(msg.data), dtype=np.uint8)
            img = img_data.reshape(msg.height, msg.width)

            corners, ids, _ = _detect_markers(img)
            if ids is None:
                continue

            found = [i for i, mid in enumerate(ids.flatten()) if mid == TARGET_ID]
            if not found:
                continue

            marker_corners = np.array([corners[found[0]]], dtype=np.float32)

            rvec, tvec, _ = cv2.aruco.estimatePoseSingleMarkers(
                marker_corners, marker_size_m, _K, _DIST,
            )
            R_cam_marker, _ = cv2.Rodrigues(rvec[0])

            # marker +Y axis = north direction in camera frame
            north_cam = R_cam_marker @ np.array([0.0, 1.0, 0.0])

            # camera → IMU → VIO global
            north_imu = _R_IMU_CAM.apply(north_cam)

            # Use bag-level timestamp converted to VIO clock domain for poseimu lookup.
            pm = _nearest_poseimu(bag_ts - cam_to_vio_ts_offset)
            if pm is None:
                continue

            R_ItoG = Rotation.from_quat([
                pm.pose.pose.orientation.x,
                pm.pose.pose.orientation.y,
                pm.pose.pose.orientation.z,
                pm.pose.pose.orientation.w,
            ])
            north_G = R_ItoG.apply(north_imu)

            bearing = math.atan2(north_G[0], north_G[1])
            if n_detected < 5:
                t = tvec[0][0]  # marker centre in camera frame (metres)
                angle_h = math.degrees(math.atan2(t[0], t[2]))   # + = right of camera axis
                angle_v = math.degrees(math.atan2(t[1], t[2]))   # + = down in camera image
                cen = corners[found[0]][0].mean(axis=0)           # image pixel centroid
                print(f'  det {n_detected}: img_cen=({cen[0]:.1f},{cen[1]:.1f})  '
                      f'cam_angle h={angle_h:+.1f}° v={angle_v:+.1f}°  dist={np.linalg.norm(t):.2f}m  '
                      f'north_cam={north_cam.round(3)}  north_imu={north_imu.round(3)}  '
                      f'north_G={north_G.round(3)}  bearing={math.degrees(bearing):.2f}°')
            sin_sum += math.sin(bearing)
            cos_sum += math.cos(bearing)
            n_detected += 1

            if n_detected >= min_detections:
                break

    print(f'ArUco precheck: detected marker in {n_detected} frames')

    if n_detected < min_detections:
        print(f'ArUco precheck failed ({n_detected} < {min_detections} detections) '
              f'— falling back to RTK yaw')
        return None

    mean_bearing = math.atan2(sin_sum, cos_sum)
    align_yaw_rad = mean_bearing
    print(f'ArUco mean bearing : {math.degrees(mean_bearing):.2f}°')
    print(f'ArUco north alignment: yaw={align_yaw_rad:.4f} rad ({math.degrees(align_yaw_rad):.2f}°) '
          f'from {n_detected} detections')
    return align_yaw_rad
