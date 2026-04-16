#!/usr/bin/env python3
"""Extract frames containing ArUco marker ID 70 (DICT_5X5_100) from a ROS2 bag
and save them as annotated PNG images.

Usage:
    python draw_aruco.py <bag_path> [--topic /camera/image_mono] [--out ./aruco_frames]
"""

import argparse
import math
from pathlib import Path

import cv2
import numpy as np


TARGET_ID    = 70
ARUCO_DICT   = cv2.aruco.DICT_5X5_100
MARKER_SIZE  = 0.235  # metres, same as aruco_align.py

_K = np.array([
    [1129.02, 0.0,     620.05],
    [0.0,     1129.82, 499.31],
    [0.0,     0.0,     1.0   ],
], dtype=np.float64)

_DIST = np.array([0.0324, -0.0627, 0.00047, -0.00031], dtype=np.float64)


def _make_params():
    """ArUco detector parameters tuned for robustness against glare/occlusion."""
    if hasattr(cv2.aruco, 'DetectorParameters'):
        p = cv2.aruco.DetectorParameters()
    else:
        p = cv2.aruco.DetectorParameters_create()
    # Wider adaptive-threshold window range catches markers under uneven lighting
    p.adaptiveThreshWinSizeMin  = 3
    p.adaptiveThreshWinSizeMax  = 73
    p.adaptiveThreshWinSizeStep = 4
    # More lenient error correction for partially-occluded / glared markers
    p.errorCorrectionRate          = 0.9
    p.maxErroneousBitsInBorderRate = 0.5
    p.minMarkerPerimeterRate       = 0.02   # detect slightly smaller apparent size
    return p


_CLAHE = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))


def _preprocess_variants(gray):
    """Yield (label, image) preprocessing variants, most conservative first."""
    yield 'orig', gray
    yield 'clahe', _CLAHE.apply(gray)
    yield 'eq',    cv2.equalizeHist(gray)
    # clip the brightest 2 % of pixels then rescale — removes glare hotspots
    p98 = np.percentile(gray, 98)
    clipped = np.clip(gray, 0, p98)
    yield 'clip98', cv2.normalize(clipped, None, 0, 255, cv2.NORM_MINMAX).astype(np.uint8)


def _make_detector():
    aruco_dict = cv2.aruco.getPredefinedDictionary(ARUCO_DICT)
    params = _make_params()

    if hasattr(cv2.aruco, 'ArucoDetector'):
        detector = cv2.aruco.ArucoDetector(aruco_dict, params)
        def detect(gray):
            for label, img in _preprocess_variants(gray):
                corners, ids, rej = detector.detectMarkers(img)
                if ids is not None:
                    return corners, ids, rej, label
            return [], None, [], 'none'
    else:
        def detect(gray):
            for label, img in _preprocess_variants(gray):
                corners, ids, rej = cv2.aruco.detectMarkers(
                    img, aruco_dict, parameters=params)
                if ids is not None:
                    return corners, ids, rej, label
            return [], None, [], 'none'

    return detect, aruco_dict


def process_bag(bag_path: Path, cam_topic: str, out_dir: Path) -> None:
    from rosbags.rosbag2 import Reader
    from rosbags.typesys import get_typestore, Stores

    out_dir.mkdir(parents=True, exist_ok=True)
    detect, aruco_dict = _make_detector()

    # Try Jazzy first, fall back to Humble
    for store in (Stores.ROS2_JAZZY, Stores.ROS2_HUMBLE):
        try:
            typestore = get_typestore(store)
            break
        except Exception:
            continue

    IMG_TYPE   = 'sensor_msgs/msg/Image'
    n_saved    = 0
    n_frames   = 0

    with Reader(bag_path) as reader:
        conns = [c for c in reader.connections if c.topic == cam_topic]
        if not conns:
            topics = sorted({c.topic for c in reader.connections})
            print(f'Topic {cam_topic!r} not found. Available topics:')
            for t in topics:
                print(f'  {t}')
            return

        print(f'Scanning {cam_topic} in {bag_path.name} ...')

        for conn, bag_ts, rawdata in reader.messages(connections=conns):
            n_frames += 1

            msg      = typestore.deserialize_cdr(rawdata, IMG_TYPE)
            img_data = np.frombuffer(bytes(msg.data), dtype=np.uint8)
            gray     = img_data.reshape(msg.height, msg.width)

            corners, ids, rejected, prep_label = detect(gray)
            if ids is None:
                continue

            found = [i for i, mid in enumerate(ids.flatten()) if mid == TARGET_ID]
            if not found:
                continue

            # Convert to BGR for annotation
            bgr = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

            # Draw ALL detected markers (grey) then our target (green)
            cv2.aruco.drawDetectedMarkers(bgr, corners, ids, borderColor=(128, 128, 128))

            # Highlight target marker
            idx  = found[0]
            corn = np.array([corners[idx]], dtype=np.float32)
            cv2.aruco.drawDetectedMarkers(bgr, corn,
                                          np.array([[TARGET_ID]]),
                                          borderColor=(0, 255, 0))

            # Pose estimation
            rvec, tvec, _ = cv2.aruco.estimatePoseSingleMarkers(
                corn, MARKER_SIZE, _K, _DIST,
            )
            cv2.drawFrameAxes(bgr, _K, _DIST, rvec[0], tvec[0], MARKER_SIZE * 0.5)

            # Compute horizontal angle from camera axis
            t = tvec[0][0]
            angle_h = math.degrees(math.atan2(t[0], t[2]))
            angle_v = math.degrees(math.atan2(t[1], t[2]))
            dist    = np.linalg.norm(t)

            # 2-D marker rotation in the image, from corners alone (no 3-D math).
            # ArUco corners order: [top-left, top-right, bottom-right, bottom-left]
            # The marker +Y axis (= "north" per sky-lign) points from the top edge
            # toward the bottom edge in the marker's own frame, but the top edge of
            # the marker as seen on screen is edge corner0→corner1.
            # We compute the angle of the marker's "up" direction in the image:
            # mid of top edge (c0,c1) → mid of bottom edge (c2,c3), i.e. +Y of marker.
            c = corners[idx][0]           # shape (4,2): tl, tr, br, bl
            top_mid = (c[0] + c[1]) / 2  # midpoint of top edge
            bot_mid = (c[2] + c[3]) / 2  # midpoint of bottom edge
            cen_f   = (top_mid + bot_mid) / 2

            # vector from top-mid to bot-mid = marker +Y direction in image pixels
            marker_y_img = bot_mid - top_mid   # (dx, dy) in image coords

            # Angle CW from image-up (-Y pixel axis). 0° = image top, +CW.
            # atan2(dx, -dy): dx>0 → right → positive CW from up.
            marker_angle_img = math.degrees(math.atan2(marker_y_img[0], -marker_y_img[1]))

            # Draw the marker +Y arrow (cyan) from centre
            arrow_len = int(np.linalg.norm(marker_y_img) * 0.6)
            cen_i = tuple(cen_f.astype(int))
            tip   = (int(cen_f[0] + marker_y_img[0] * 0.6),
                     int(cen_f[1] + marker_y_img[1] * 0.6))
            cv2.arrowedLine(bgr, cen_i, tip, (255, 255, 0), 2, tipLength=0.2)

            # Draw a reference "image-up" arrow (white) same length
            up_tip = (cen_i[0], cen_i[1] - arrow_len)
            cv2.arrowedLine(bgr, cen_i, up_tip, (200, 200, 200), 1, tipLength=0.2)

            # Annotate image
            cen = cen_f.astype(int)
            ts_s = bag_ts / 1e9
            label = (f'ID {TARGET_ID}  dist={dist:.2f}m  '
                     f'h={angle_h:+.1f}deg  v={angle_v:+.1f}deg  [{prep_label}]')
            cv2.putText(bgr, label, (10, 30),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2, cv2.LINE_AA)
            cv2.putText(bgr, f'marker_angle={marker_angle_img:+.1f}deg (CW from image-up)',
                        (10, 60), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 0), 2, cv2.LINE_AA)
            cv2.putText(bgr, f't={ts_s:.3f}s', (10, 90),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (200, 200, 0), 1, cv2.LINE_AA)

            fname = out_dir / f'frame_{n_saved:04d}_ts{bag_ts}.png'
            cv2.imwrite(str(fname), bgr)
            n_saved += 1
            print(f'  [{n_saved:3d}] frame {n_frames:5d}  t={ts_s:.3f}  '
                  f'dist={dist:.2f}m  h={angle_h:+.1f}°  v={angle_v:+.1f}°  '
                  f'marker_angle={marker_angle_img:+.1f}°  -> {fname.name}')

    print(f'\nDone: {n_saved} frames with marker ID {TARGET_ID} saved to {out_dir}/ '
          f'(scanned {n_frames} total frames)')


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('bag', help='Path to the ROS2 bag (.mcap file or directory)')
    ap.add_argument('--topic', default='/camera/image_mono',
                    help='Camera topic (default: /camera/image_mono)')
    ap.add_argument('--out', default='./aruco_frames',
                    help='Output directory for annotated frames (default: ./aruco_frames)')
    args = ap.parse_args()

    bag_path = Path(args.bag)
    if not bag_path.exists():
        print(f'ERROR: {bag_path} does not exist')
        raise SystemExit(1)

    process_bag(bag_path, args.topic, Path(args.out))


if __name__ == '__main__':
    main()
