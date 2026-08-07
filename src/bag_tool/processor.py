"""
Offline ground-truth → VIO alignment for multiple platforms.

Reads an input ROS2 bag containing a platform-specific ground-truth pose source plus
a VIO topic, and writes a new bag with the aligned ground-truth and VIO trajectories.

Platforms are described in bag_tool.platforms; their PlatformConfig drives:
  - which topic supplies the ground-truth pose (NavSatFix vs PoseStamped)
  - whether yaw comes from a separate topic or the pose's quaternion
  - the source coordinate frame (geodetic, NED, ENU)
  - the body-frame offset baked into the first-fix alignment

Always written:
  /ov_srvins/gt/aligned       PoseWithCovarianceStamped  (aligned to VIO frame)
  /ov_srvins/gt/aligned_path  nav_msgs/Path
  /ov_srvins/vio/pose         PoseWithCovarianceStamped
  /ov_srvins/vio/path         nav_msgs/Path

Written only when platform.emit_raw_rtk_topics (prince):
  /ov_srvins/rtk/pose         PoseWithCovarianceStamped  (raw ENU, starts at 0,0,0)
  /ov_srvins/rtk/path         nav_msgs/Path
"""

import bisect
import math
import struct
import sys
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore
from scipy.spatial.transform import Rotation

from bag_tool.add_topics import _normalize_msgdef
from bag_tool.platforms import PLATFORMS, PlatformConfig

# ---------------------------------------------------------------------------
# Fast CDR helpers for nav_msgs/Path with frame_id="global"
#
# CDR layout (confirmed against rosbags serialize_cdr):
#   Path header  : 28 bytes  (encap[4] + stamp[8] + frame_id[12] + count[4])
#   PoseStamped  : 80 bytes  (stamp[8] + frame_id+pad[16] + position[24] + orientation[32])
#
# The 5-byte padding before float64 is constant for every element because 80 % 8 == 0.
# ---------------------------------------------------------------------------
_ENCAP        = b'\x00\x01\x00\x00'
_PATH_FID     = b'\x07\x00\x00\x00global\x00\x00'            # len(4) + "global\0"(7) + pad(1) = 12
_POSE_FID_PAD = b'\x07\x00\x00\x00global\x00\x00\x00\x00\x00\x00'  # len(4) + "global\0"(7) + pad(5) = 16

def _path_header_cdr(stamp_ns: int, n_poses: int) -> bytes:
    sec, nsec = divmod(stamp_ns, 10 ** 9)
    return _ENCAP + struct.pack('<II', sec, nsec) + _PATH_FID + struct.pack('<I', n_poses)


def _pose_cdr_bytes(stamp_ns: int, pos, q) -> bytes:
    sec, nsec = divmod(stamp_ns, 10 ** 9)
    return (struct.pack('<II', sec, nsec)
            + _POSE_FID_PAD
            + struct.pack('<ddd', float(pos[0]), float(pos[1]), float(pos[2]))
            + struct.pack('<dddd', float(q[0]), float(q[1]), float(q[2]), float(q[3])))


# ---------------------------------------------------------------------------
# WGS84 constants
# ---------------------------------------------------------------------------
WGS84_A  = 6_378_137.0
WGS84_E2 = 6.69437999014e-3


def geodetic_to_ecef(lat_deg: float, lon_deg: float, alt: float):
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    N = WGS84_A / math.sqrt(1.0 - WGS84_E2 * math.sin(lat) ** 2)
    x = (N + alt) * math.cos(lat) * math.cos(lon)
    y = (N + alt) * math.cos(lat) * math.sin(lon)
    z = (N * (1.0 - WGS84_E2) + alt) * math.sin(lat)
    return x, y, z


def ecef_to_enu(dx: float, dy: float, dz: float,
                ref_lat_deg: float, ref_lon_deg: float):
    lat0 = math.radians(ref_lat_deg)
    lon0 = math.radians(ref_lon_deg)
    e = -math.sin(lon0) * dx + math.cos(lon0) * dy
    n = (-math.sin(lat0) * math.cos(lon0) * dx
         - math.sin(lat0) * math.sin(lon0) * dy
         + math.cos(lat0) * dz)
    u = (math.cos(lat0) * math.cos(lon0) * dx
         + math.cos(lat0) * math.sin(lon0) * dy
         + math.sin(lat0) * dz)
    return np.array([e, n, u])


def dji_yaw_to_enu_rad(yaw_deg: float) -> float:
    """Convert DJI heading (degrees CW from North) to ENU yaw (radians CCW from East)."""
    return math.radians(90.0 - yaw_deg)


JUMP_THRESHOLD = 1.0  # metres


def rte_values_from_ate(
    ate_records: list[tuple[int, int, float]],
    rte_window_ns: int,
) -> list[float]:
    """Compute per-frame RTE from ATE records. RTE = ATE(t) - ATE(t - window)."""
    ate_header_stamps = [r[1] for r in ate_records]
    n_ate = len(ate_records)
    rte_values: list[float] = []
    for _, vio_stamp_ns, ate in ate_records:
        past_stamp = vio_stamp_ns - rte_window_ns
        j = bisect.bisect_left(ate_header_stamps, past_stamp)
        if j >= n_ate:
            j = n_ate - 1
        if j > 0 and (ate_header_stamps[j] - past_stamp) > (past_stamp - ate_header_stamps[j - 1]):
            j -= 1
        rte_values.append(ate - ate_records[j][2])
    return rte_values


# --- Ascent/descent segment detection (phase-specific ATE/RTE) -------------
# A segment is a maximal run of GT fixes that never violates the vertical
# profile: horizontal speed < SEG_VH_MAX. Within the run, fixes are
# "vertical" when |vz| > SEG_VZ_MIN; hover fixes (low xy AND low z) do NOT
# break the run — a pause mid-climb or the hover between descent stages is
# still part of the segment. A run qualifies only if its accumulated
# VERTICAL time exceeds SEG_MIN_VERT_S (pure hover never qualifies: it is
# only ever absorbed into a run seeded by vertical motion). Direction is the
# sign of the net z displacement. Multiple segments per flight are expected
# (takeoff ascent, staged landing descent). Velocities are finite
# differences over a ~1 s stamp baseline, robust to 5-100 Hz GT rates.
SEG_VH_MAX = 1.0       # m/s horizontal — above this the vertical profile is violated
SEG_VZ_MIN = 0.8       # m/s — |vz| above this counts as vertical motion
SEG_MIN_VERT_S = 5.0   # s of accumulated vertical motion to qualify
SEG_MIN_NET_DZ = 3.0   # m net |dz| sanity floor


def detect_vertical_segments(fixes):
    """fixes: [(stamp_ns, pos ENU)] time-ordered. Returns
    [(dir, start_ns, end_ns)] with dir in {'ascent', 'descent'}."""
    n = len(fixes)
    if n < 3:
        return []
    stamps = [s for s, _ in fixes]

    def vel(i):
        t = stamps[i]
        j = i
        while j > 0 and t - stamps[j - 1] < 500_000_000:
            j -= 1
        k = i
        while k < n - 1 and stamps[k + 1] - t < 500_000_000:
            k += 1
        dt = (stamps[k] - stamps[j]) / 1e9
        if dt <= 1e-6:
            return 0.0, 0.0
        dp = fixes[k][1] - fixes[j][1]
        return float(np.hypot(dp[0], dp[1]) / dt), float(dp[2] / dt)

    segments = []
    cur_start = None
    vert_time = 0.0
    prev_stamp = None
    for i in range(n):
        vh, vz = vel(i)
        if vh < SEG_VH_MAX:
            if cur_start is None:
                if abs(vz) > SEG_VZ_MIN:
                    cur_start = stamps[i]
                    vert_time = 0.0
                    prev_stamp = stamps[i]
            else:
                if abs(vz) > SEG_VZ_MIN and prev_stamp is not None:
                    vert_time += (stamps[i] - prev_stamp) / 1e9
                prev_stamp = stamps[i]
        else:
            if cur_start is not None:
                segments.append((cur_start, stamps[i - 1] if i else cur_start, vert_time))
                cur_start = None
    if cur_start is not None:
        segments.append((cur_start, stamps[-1], vert_time))

    out = []
    z_at = {s: p[2] for s, p in fixes}
    for start, end, vt in segments:
        if vt < SEG_MIN_VERT_S:
            continue
        dz = z_at[end] - z_at[start]
        if abs(dz) < SEG_MIN_NET_DZ:
            continue
        out.append(('ascent' if dz > 0 else 'descent', start, end))
    return out


def rms_jump_penalty(rte_values: list[float]) -> tuple[float, float]:
    """Return (rms_rte, jump_penalty) from a list of per-frame RTE values."""
    rte_arr = np.array(rte_values)
    rms_rte = float(np.sqrt(np.mean(rte_arr ** 2)))
    if len(rte_arr) > 1:
        excess = np.maximum(0.0, np.abs(np.diff(rte_arr)) - JUMP_THRESHOLD)
        jump_penalty = float(np.sqrt(np.sum(excess ** 2)))
    else:
        jump_penalty = 0.0
    return rms_rte, jump_penalty


# Topics written by compute_alignment / write_alignment_topics.
# Used to exclude them from passthrough when the source bag is also the input bag.
# Superset across all platforms — safe to over-exclude.
COMPUTED_TOPICS = frozenset({
    '/ov_srvins/rtk/pose',
    '/ov_srvins/rtk/path',
    '/ov_srvins/gt/aligned',
    '/ov_srvins/gt/aligned_path',
    '/ov_srvins/vio/pose',
    '/ov_srvins/vio/path',
    '/ov_srvins/ate',
    '/ov_srvins/rte',
    '/ov_srvins/eval_rms_rte',
    '/ov_srvins/eval_jump_penalty',
    '/ov_srvins/eval_avg_slam_feats',
    '/ov_srvins/eval_ascent_ate',
    '/ov_srvins/eval_ascent_rte',
    '/ov_srvins/eval_descent_ate',
    '/ov_srvins/eval_descent_rte',
})

# NED→ENU is a proper rotation (det = +1, equivalent to 180° about the (1,1,0)/√2 axis).
# Built once at import; applied to both positions and quaternions in the altair branch.
_R_NED_TO_ENU = Rotation.from_matrix([[0.0, 1.0, 0.0],
                                       [1.0, 0.0, 0.0],
                                       [0.0, 0.0, -1.0]])


# ---------------------------------------------------------------------------
# Per-platform GPS readers — produce a unified list of
# (bag_ts, stamp_ns, enu_pos, rot) records that the alignment loop consumes.
# ---------------------------------------------------------------------------
def _navsatfix_records(fixes, yaws):
    """Prince path: NavSatFix lat/lon/alt + Float64 DJI yaw → ENU records."""
    if not yaws or not fixes:
        return []
    records = []
    ref_lat = ref_lon = None
    ref_ecef = None
    yaw_idx = 0
    last_yaw_idx = len(yaws) - 1
    prev_q_arr = None

    for fix_ts, fix_msg in fixes:
        lat = fix_msg.latitude
        lon = fix_msg.longitude
        alt = fix_msg.altitude
        if lat == 0.0 and lon == 0.0:
            continue
        if ref_lat is None:
            ref_lat, ref_lon = lat, lon
            ref_ecef = geodetic_to_ecef(lat, lon, alt)
            print(f'ENU origin set: lat={lat:.7f} lon={lon:.7f} alt={alt:.3f}')
            continue

        cx, cy, cz = geodetic_to_ecef(lat, lon, alt)
        enu = ecef_to_enu(cx - ref_ecef[0], cy - ref_ecef[1], cz - ref_ecef[2],
                          ref_lat, ref_lon)

        while yaw_idx < last_yaw_idx and yaws[yaw_idx + 1][0] <= fix_ts:
            yaw_idx += 1
        if fix_ts <= yaws[0][0]:
            yaw_deg = yaws[0][1]
        elif yaw_idx >= last_yaw_idx:
            yaw_deg = yaws[last_yaw_idx][1]
        else:
            before_ts, before_yaw = yaws[yaw_idx]
            after_ts, after_yaw = yaws[yaw_idx + 1]
            yaw_deg = before_yaw if (fix_ts - before_ts) <= (after_ts - fix_ts) else after_yaw

        q_arr = Rotation.from_euler('z', dji_yaw_to_enu_rad(yaw_deg)).as_quat()
        if prev_q_arr is not None and np.dot(q_arr, prev_q_arr) < 0:
            q_arr = -q_arr
        prev_q_arr = q_arr
        rot = Rotation.from_quat(q_arr)

        hdr_sec = fix_msg.header.stamp.sec
        hdr_nsec = fix_msg.header.stamp.nanosec
        stamp_ns = hdr_sec * 10**9 + hdr_nsec if (hdr_sec or hdr_nsec) else fix_ts
        records.append((fix_ts, stamp_ns, enu, rot))
    return records


def _decode_posimus(posimus: list) -> list:
    """Decode raw VIO PoseWithCovarianceStamped messages into a uniform
    (bag_ts, stamp_ns, pos: np.ndarray, rot: Rotation) list.

    Substitutes frames with NaN/Inf positions or zero-norm orientation quaternions
    (OpenVINS produces these at estimator-failure boundaries) with the last valid
    pose. Frames before the first valid pose are dropped. Prints a sanitization
    summary if any frames were touched.
    """
    decoded: list = []
    last_pos = None
    last_rot = None
    n_dropped = 0
    n_replaced = 0
    for vio_ts, pm in posimus:
        stamp_ns = pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec
        pos = np.array([pm.pose.pose.position.x,
                        pm.pose.pose.position.y,
                        pm.pose.pose.position.z])
        q = np.array([pm.pose.pose.orientation.x,
                      pm.pose.pose.orientation.y,
                      pm.pose.pose.orientation.z,
                      pm.pose.pose.orientation.w])
        valid = (np.all(np.isfinite(pos))
                 and np.all(np.isfinite(q))
                 and np.linalg.norm(q) > 1e-9)
        if not valid:
            if last_pos is None:
                n_dropped += 1
                continue
            pos = last_pos
            rot = last_rot
            n_replaced += 1
        else:
            rot = Rotation.from_quat(q)
            last_pos = pos
            last_rot = rot
        decoded.append((vio_ts, stamp_ns, pos, rot))
    if n_dropped or n_replaced:
        print(f'WARNING: VIO sanitization — {n_dropped} dropped (no prior valid), '
              f'{n_replaced} replaced with last valid (NaN/Inf pos or zero-norm quat)')
    return decoded


def _pointstamped_records(points, platform):
    """Altair-style path with position-only GT (no quaternion).

    Each output record uses a constant first-fix orientation (identity, then optionally
    rotated by NED→ENU). With no per-sample quaternion, all aligned-pose orientations
    will inherit `vio_init_rot` (the first VIO orientation) — useful for plotting the
    position path in Foxglove even though the orientation axes won't be meaningful.
    """
    if platform.coord_frame == 'ned':
        const_rot = _R_NED_TO_ENU
    else:
        const_rot = Rotation.identity()
    records = []
    for ts, msg in points:
        pos = np.array([msg.point.x, msg.point.y, msg.point.z])
        if platform.coord_frame == 'ned':
            pos = _R_NED_TO_ENU.apply(pos)
        stamp_ns = msg.header.stamp.sec * 10**9 + msg.header.stamp.nanosec
        if not stamp_ns:
            stamp_ns = ts
        records.append((ts, stamp_ns, pos, const_rot))
    return records


def _posestamped_records(poses, platform):
    """Altair-style path: PoseStamped in `fc_local_ned` (or ENU) → ENU records."""
    records = []
    prev_q_arr = None
    for ts, msg in poses:
        pos = np.array([msg.pose.position.x, msg.pose.position.y, msg.pose.position.z])
        q = np.array([msg.pose.orientation.x, msg.pose.orientation.y,
                      msg.pose.orientation.z, msg.pose.orientation.w])
        if platform.coord_frame == 'ned':
            pos = _R_NED_TO_ENU.apply(pos)
            rot = _R_NED_TO_ENU * Rotation.from_quat(q)
            q_arr = rot.as_quat()
        else:
            q_arr = q
            rot = Rotation.from_quat(q_arr)
        if prev_q_arr is not None and np.dot(q_arr, prev_q_arr) < 0:
            q_arr = -q_arr
            rot = Rotation.from_quat(q_arr)
        prev_q_arr = q_arr
        stamp_ns = msg.header.stamp.sec * 10**9 + msg.header.stamp.nanosec
        if not stamp_ns:
            stamp_ns = ts
        records.append((ts, stamp_ns, pos, rot))
    return records


# ---------------------------------------------------------------------------
# Phase 1 + 2: load ground-truth/VIO data and compute poses (no I/O side-effects)
# ---------------------------------------------------------------------------
def compute_alignment(
    input_bag: str,
    vio_topic: str,
    stores_enum,
    aruco_yaw_rad: float | None = None,
    platform: PlatformConfig | None = None,
    yaw_rot: int = 0,
) -> tuple:
    """Load ground-truth + VIO data and compute raw + aligned trajectories.

    Returns (out_poses, out_aligned, posimus, typestore, reader_path,
             input_start_ns, input_end_ns, diag_tracking, platform).
    out_poses     : list of (gt_ts, stamp_ns, enu_pos, rot)    — raw ENU
    out_aligned   : list of (gt_ts, stamp_ns, pos_al, rot_al)  — VIO-aligned
    posimus       : list of (timestamp_ns, msg)                — raw VIO poses
    typestore     : rosbags typestore (needed for Phase 3 serialisation)
    reader_path   : Path that was opened
    input_start_ns/input_end_ns : bag time bounds
    diag_tracking : list of (header_stamp_ns, num_feats_slam) from diag/tracking; [] if absent
    platform      : the resolved PlatformConfig used (echoed back for write_alignment_topics)

    aruco_yaw_rad : pre-computed ArUco north yaw (radians); ignored if not platform.aruco_supported.
    yaw_rot       : int in {0,1,2,3}; pre-rotates ground-truth pose by N×90° about Z (both
                    position and orientation) before first-fix alignment. Use to compensate
                    for unknown IMU mounting orientation in the sensor casket.
    """
    if platform is None:
        platform = PLATFORMS['prince']
    typestore = get_typestore(stores_enum)

    fixes         = []  # NavSatFix path (prince)
    yaws          = []  # DJI yaw (prince)
    gt_poses      = []  # PoseStamped path (altair-like, full pose)
    gt_points     = []  # PointStamped path (altair-like, position-only)
    posimus       = []
    diag_tracking = []
    gt_msgtype: str | None = None  # the actual GT msgtype seen in the bag

    input_path  = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    wanted_topics: set[str] = {vio_topic, '/ov_srvins/diag/tracking', platform.gps_topic}
    if platform.yaw_topic:
        wanted_topics.add(platform.yaw_topic)

    with Reader(reader_path) as reader:
        input_start_ns = reader.start_time
        input_end_ns = reader.start_time + reader.duration

        relevant_connections = [c for c in reader.connections if c.topic in wanted_topics]

        for connection, timestamp, rawdata in reader.messages(connections=relevant_connections):
            topic = connection.topic
            if topic == platform.gps_topic:
                if connection.msgtype not in platform.gps_msg_types:
                    raise ValueError(
                        f"platform {platform.name!r} does not accept msgtype "
                        f"{connection.msgtype!r} on {platform.gps_topic} "
                        f"(accepted: {platform.gps_msg_types})"
                    )
                if gt_msgtype is None:
                    gt_msgtype = connection.msgtype
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                if connection.msgtype == 'sensor_msgs/msg/NavSatFix':
                    fixes.append((timestamp, msg))
                elif connection.msgtype == 'geometry_msgs/msg/PoseStamped':
                    gt_poses.append((timestamp, msg))
                elif connection.msgtype == 'geometry_msgs/msg/PointStamped':
                    gt_points.append((timestamp, msg))
                else:
                    raise NotImplementedError(
                        f"no reader implemented for GT msgtype {connection.msgtype!r}"
                    )
            elif platform.yaw_topic is not None and topic == platform.yaw_topic:
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                yaws.append((timestamp, msg.data * 10.0))  # DJI publishes tenths-of-degrees
            elif topic == vio_topic:
                posimus.append((timestamp, typestore.deserialize_cdr(rawdata, connection.msgtype)))
            elif topic == '/ov_srvins/diag/tracking':
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                header_ns = msg.header.stamp.sec * 10**9 + msg.header.stamp.nanosec
                for kv in msg.status[0].values:
                    if kv.key == 'num_feats_slam':
                        diag_tracking.append((header_ns, int(float(kv.value))))
                        break

    fixes.sort(key=lambda x: x[0])
    yaws.sort(key=lambda x: x[0])
    gt_poses.sort(key=lambda x: x[0])
    gt_points.sort(key=lambda x: x[0])
    posimus.sort(key=lambda x: x[0])

    # Dispatch by which list is populated (driven by the actual bag's msgtype, not the
    # platform's default). A bag should contain only one of these — if both poses and
    # points exist, prefer poses (more information).
    if fixes:
        if not yaws:
            print(f'WARNING: no {platform.yaw_topic} messages found — skipping ground-truth')
            fixes = []
        print(f'Loaded {len(fixes)} fixes, {len(yaws)} yaw msgs, {len(posimus)} poseimu msgs')
        gps_records = _navsatfix_records(fixes, yaws)
    elif gt_poses:
        print(f'Loaded {len(gt_poses)} ground-truth poses (PoseStamped), {len(posimus)} poseimu msgs')
        gps_records = _posestamped_records(gt_poses, platform)
    elif gt_points:
        print(f'Loaded {len(gt_points)} ground-truth points (PointStamped, position-only), '
              f'{len(posimus)} poseimu msgs')
        gps_records = _pointstamped_records(gt_points, platform)
    else:
        print(f'WARNING: no {platform.gps_topic} messages found — '
              f'skipping ground-truth, emitting VIO path only')
        gps_records = []

    if not posimus:
        print(f'WARNING: no {vio_topic} messages — aligned topics will be empty')

    # yaw-rot pre-rotation: rotate ground-truth POSITION (only) by N×90° around Z before
    # first-fix alignment. Orientation is left untouched on purpose: a rotation around Z
    # applied to both position and orientation commutes with the yaw-only first-fix
    # alignment, so it would cancel and produce identical outputs for every N. Rotating
    # position alone gives the user 4 visibly distinct trajectories to compare in Foxglove.
    if yaw_rot:
        if yaw_rot not in (0, 1, 2, 3):
            raise ValueError(f"yaw_rot must be in {{0,1,2,3}}, got {yaw_rot}")
        R_yawrot = Rotation.from_euler('z', yaw_rot * math.pi / 2)
        gps_records = [(ts, sn, R_yawrot.apply(p), r) for ts, sn, p, r in gps_records]
        print(f'Applied yaw_rot={yaw_rot} (= {yaw_rot * 90}°) pre-rotation to ground-truth position')

    # Shift first record to origin so out_poses starts at (0,0,0).
    if gps_records:
        _, _, first_pos, _ = gps_records[0]
        pose_offset = first_pos.copy()
        gps_records = [(ts, sn, p - pose_offset, r) for ts, sn, p, r in gps_records]

    # VIO init from the first VALID poseimu message. Some recordings have NaN
    # positions or zero-norm orientation quaternions at the head (e.g. when the
    # rosbag2 recorder lost transport-layer messages and the estimator hadn't yet
    # produced anything publishable); scipy's Rotation.from_quat raises on those,
    # so skip ahead to the first frame that parses cleanly.
    vio_init_pos = vio_init_rot = None
    n_skipped_for_init = 0
    for _, pm in posimus:
        pos = np.array([pm.pose.pose.position.x,
                        pm.pose.pose.position.y,
                        pm.pose.pose.position.z])
        q = np.array([pm.pose.pose.orientation.x,
                      pm.pose.pose.orientation.y,
                      pm.pose.pose.orientation.z,
                      pm.pose.pose.orientation.w])
        if (np.all(np.isfinite(pos))
                and np.all(np.isfinite(q))
                and np.linalg.norm(q) > 1e-9):
            vio_init_pos = pos
            vio_init_rot = Rotation.from_quat(q)
            break
        n_skipped_for_init += 1
    if n_skipped_for_init:
        print(f'Skipped {n_skipped_for_init} invalid leading VIO frame(s) before finding init pose')

    # First-fix alignment: rotate ground-truth into VIO frame using the first matching pair.
    rtk_init_rot  = None
    align_rot     = None
    align_rot_pos = None
    align_trans   = None
    prev_q_al_arr = None

    out_poses   = []
    out_aligned = []

    use_aruco = platform.aruco_supported and aruco_yaw_rad is not None

    for gt_ts, stamp_ns, enu, rot in gps_records:
        if rtk_init_rot is None:
            rtk_init_rot = rot
            if vio_init_rot is not None:
                rtk_align_rot = vio_init_rot * rtk_init_rot.inv()
                rtk_yaw_rad   = rtk_align_rot.as_euler('zyx')[0]

                # TODO: root-cause platform.yaw_correction_rad. Per-msgtype overrides
                # exist because PointStamped GT (no orientation data) ends up rotated
                # very differently from PoseStamped under our first-fix scheme.
                # See PlatformConfig.yaw_correction_rad_by_msgtype.
                yaw_corr = platform.yaw_correction_rad_by_msgtype.get(
                    gt_msgtype, platform.yaw_correction_rad
                )
                R_corr   = Rotation.from_euler('z', yaw_corr)

                if use_aruco:
                    diff_deg = math.degrees(aruco_yaw_rad - rtk_yaw_rad)
                    print(f'RTK yaw  : {math.degrees(rtk_yaw_rad):+.2f}°')
                    print(f'ArUco yaw: {math.degrees(aruco_yaw_rad):+.2f}°  (diff {diff_deg:+.2f}°)')
                    align_rot     = R_corr * Rotation.from_euler('z', aruco_yaw_rad)
                    align_rot_pos = Rotation.from_euler('z', platform.position_frame_offset_rad + aruco_yaw_rad + yaw_corr)
                    align_trans   = vio_init_pos
                    print(f'Ground-truth ↔ VIO alignment computed (ArUco method, platform={platform.name})')
                else:
                    align_rot     = R_corr * rtk_align_rot
                    # Strip pitch/roll: use only yaw for position rotation.
                    align_rot_pos = Rotation.from_euler('z', platform.position_frame_offset_rad + rtk_yaw_rad + yaw_corr)
                    align_trans   = vio_init_pos
                    print(f'Ground-truth ↔ VIO alignment computed (first-fix method, platform={platform.name})')
                if yaw_corr:
                    print(f'Applied platform yaw_correction = {math.degrees(yaw_corr):+.3f}° (TODO: root-cause and remove)')

        out_poses.append((gt_ts, stamp_ns, enu, rot))

        if align_rot is not None:
            p_al = align_rot_pos.apply(enu) + align_trans
            q_al_arr = (align_rot * rot).as_quat()
            if prev_q_al_arr is not None and np.dot(q_al_arr, prev_q_al_arr) < 0:
                q_al_arr = -q_al_arr
            prev_q_al_arr = q_al_arr
            out_aligned.append((gt_ts, stamp_ns, p_al, Rotation.from_quat(q_al_arr)))

    print(f'Emitted {len(out_poses)} ground-truth poses, {len(out_aligned)} aligned poses')
    return (out_poses, out_aligned, posimus, typestore, reader_path,
            input_start_ns, input_end_ns, diag_tracking, platform)


# ---------------------------------------------------------------------------
# Phase 3: register computed connections on an open Writer and write messages
# ---------------------------------------------------------------------------
def write_alignment_topics(
    writer,
    typestore,
    out_poses: list,
    out_aligned: list,
    posimus: list,
    quick: bool,
    ts_offset: int = 0,
    rte_window_ns: int = 1_000_000_000,
    eval_mode: bool = False,
    diag_tracking: list | None = None,
    shrink: bool = False,
    platform: PlatformConfig | None = None,
) -> None:
    """Register computed alignment connections and write all their messages."""

    if platform is None:
        platform = PLATFORMS['prince']
    emit_raw = platform.emit_raw_rtk_topics

    FRAME_ID  = 'global'
    POSE_TYPE = 'geometry_msgs/msg/PoseWithCovarianceStamped'
    PATH_TYPE = 'nav_msgs/msg/Path'
    PoseWithCovStamped = typestore.types['geometry_msgs/msg/PoseWithCovarianceStamped']
    Path_ = typestore.types['nav_msgs/msg/Path']
    PoseStamped = typestore.types['geometry_msgs/msg/PoseStamped']
    Header = typestore.types['std_msgs/msg/Header']
    Time = typestore.types['builtin_interfaces/msg/Time']
    Pose = typestore.types['geometry_msgs/msg/Pose']
    PoseWithCov = typestore.types['geometry_msgs/msg/PoseWithCovariance']
    Point = typestore.types['geometry_msgs/msg/Point']
    Quat = typestore.types['geometry_msgs/msg/Quaternion']

    def make_pose_msg(stamp_ns, frame_id, pos, rot):
        q = rot.as_quat()
        sec  = int(stamp_ns // 10**9)
        nsec = int(stamp_ns %  10**9)
        return PoseWithCovStamped(
            header=Header(stamp=Time(sec=sec, nanosec=nsec), frame_id=frame_id),
            pose=PoseWithCov(
                pose=Pose(
                    position=Point(x=float(pos[0]), y=float(pos[1]), z=float(pos[2])),
                    orientation=Quat(x=float(q[0]), y=float(q[1]),
                                     z=float(q[2]), w=float(q[3])),
                ),
                covariance=np.zeros(36, dtype=np.float64),
            ),
        )

    def make_path_msg(stamp_ns, frame_id, poses_so_far):
        sec  = int(stamp_ns // 10**9)
        nsec = int(stamp_ns %  10**9)
        pose_list = []
        for ps_stamp_ns, pos, rot in poses_so_far:
            q = rot.as_quat()
            ps_sec  = int(ps_stamp_ns // 10**9)
            ps_nsec = int(ps_stamp_ns %  10**9)
            pose_list.append(PoseStamped(
                header=Header(stamp=Time(sec=ps_sec, nanosec=ps_nsec), frame_id=frame_id),
                pose=Pose(
                    position=Point(x=float(pos[0]), y=float(pos[1]), z=float(pos[2])),
                    orientation=Quat(x=float(q[0]), y=float(q[1]),
                                     z=float(q[2]), w=float(q[3])),
                ),
            ))
        return Path_(
            header=Header(stamp=Time(sec=sec, nanosec=nsec), frame_id=frame_id),
            poses=pose_list,
        )

    def _computed_conn(topic, msgtype):
        msgdef, rihs01 = typestore.generate_msgdef(msgtype)
        return writer.add_connection(
            topic, msgtype,
            msgdef=_normalize_msgdef(msgdef),
            rihs01=rihs01,
            serialization_format='cdr',
            offered_qos_profiles='',
        )

    if eval_mode:
        conn_pose_al       = _computed_conn('/ov_srvins/gt/aligned',          POSE_TYPE)
        conn_vio_pose      = _computed_conn('/ov_srvins/vio/pose',            POSE_TYPE)
        conn_rte           = _computed_conn('/ov_srvins/rte',                 'std_msgs/msg/Float64')
        conn_rms_rte       = _computed_conn('/ov_srvins/eval_rms_rte',        'std_msgs/msg/Float64')
        conn_jump_penalty  = _computed_conn('/ov_srvins/eval_jump_penalty',   'std_msgs/msg/Float64')
        conn_avg_slam      = _computed_conn('/ov_srvins/eval_avg_slam_feats', 'std_msgs/msg/Float64')

        # Landing detection: TOUCHDOWN = start of the trailing GT segment whose
        # ENU Z stays within LANDING_ALT of the FINAL fix, provided a descent
        # precedes it. Referencing the final Z (not absolute 0) handles landing
        # terrain offset from the takeoff datum; taking the segment START (not
        # the rightmost below-threshold fix) excludes post-touchdown ground
        # rest on bags that keep recording after landing — the old rule kept
        # that rest in the metrics, and on offset terrain could latch onto the
        # PRE-takeoff ground and silently drop the entire flight.
        LANDING_ALT = 0.5  # metres
        landing_stamp_ns = None
        if out_poses:
            z_end = out_poses[-1][2][2]
            run_start = None
            for _, stamp_ns, pos, _ in reversed(out_poses):
                if abs(pos[2] - z_end) <= LANDING_ALT:
                    run_start = stamp_ns
                else:
                    break
            if run_start is not None:
                # Descent guard: ~10 s before touchdown the vehicle must have
                # been well above the landing band, else the bag ends mid-air
                # (e.g. truncated recording) and nothing should be cut. The
                # immediately-preceding fix is useless here — a smooth final
                # descent sits just above the band by construction.
                z_before = None
                for _, stamp_ns, pos, _ in reversed(out_poses):
                    if stamp_ns <= run_start - 10_000_000_000:
                        z_before = pos[2]
                        break
                if z_before is not None and z_before > z_end + 2.0:
                    landing_stamp_ns = run_start
        if landing_stamp_ns is not None:
            n_before = len(posimus)
            posimus       = [(ts, pm) for ts, pm in posimus
                             if pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec <= landing_stamp_ns]
            out_aligned   = [(ft, sn, p, r) for ft, sn, p, r in out_aligned if sn <= landing_stamp_ns]
            if diag_tracking:
                diag_tracking = [(h, c) for h, c in diag_tracking if h <= landing_stamp_ns]
            n_dropped = n_before - len(posimus)
            print(f'Landing detected (ENU Z ≤ {LANDING_ALT}m) — dropped {n_dropped} post-landing VIO frames')
        else:
            print('WARNING: no landing point detected — using all VIO data')

        for fix_ts, stamp_ns, pos, rot in out_aligned:
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))

        vio_frames = _decode_posimus(posimus)

        for vio_ts, stamp_ns, pos, rot in vio_frames:
            writer.write(conn_vio_pose, vio_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        if out_aligned and vio_frames:
            al_stamps    = [stamp_ns for _, stamp_ns, _, _ in out_aligned]
            al_positions = [pos_al   for _, _, pos_al, _ in out_aligned]
            n_al = len(al_stamps)
            def _nearest_al_eval(vio_stamp_ns):
                idx = bisect.bisect_left(al_stamps, vio_stamp_ns)
                if idx == 0:    return 0
                if idx >= n_al: return n_al - 1
                return idx if (al_stamps[idx] - vio_stamp_ns) <= (vio_stamp_ns - al_stamps[idx - 1]) else idx - 1
            ate_records: list[tuple[int, int, float]] = []
            for vio_ts, vio_stamp_ns, vio_pos, _ in vio_frames:
                ate = float(np.linalg.norm(vio_pos - al_positions[_nearest_al_eval(vio_stamp_ns)]))
                ate_records.append((vio_ts, vio_stamp_ns, ate))

            # VIO-failure extension: if VIO stopped before landing, freeze last position.
            n_frozen = 0
            if vio_frames and landing_stamp_ns is not None:
                last_vio_stamp_ns = vio_frames[-1][1]
                if last_vio_stamp_ns < landing_stamp_ns:
                    frozen_pos = vio_frames[-1][2]
                    frozen_rot = vio_frames[-1][3]
                    for fix_ts, stamp_ns, _, _ in out_aligned:
                        if stamp_ns <= last_vio_stamp_ns:
                            continue
                        ate = float(np.linalg.norm(frozen_pos - al_positions[_nearest_al_eval(stamp_ns)]))
                        ate_records.append((fix_ts, stamp_ns, ate))
                        writer.write(conn_vio_pose, fix_ts + ts_offset, typestore.serialize_cdr(
                            make_pose_msg(stamp_ns, FRAME_ID, frozen_pos, frozen_rot), POSE_TYPE))
                        n_frozen += 1
                    if n_frozen:
                        print(f'VIO failure detected — {n_frozen} frozen frames appended to landing')

            rte_values = rte_values_from_ate(ate_records, rte_window_ns)
            for (vio_ts, _, _), rte in zip(ate_records, rte_values):
                writer.write(conn_rte, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte))
            rms_rte, jump_penalty = rms_jump_penalty(rte_values)
            first_ts = ate_records[0][0]
            writer.write(conn_rms_rte,      first_ts + ts_offset, _ENCAP + struct.pack('<d', rms_rte))
            writer.write(conn_jump_penalty, first_ts + ts_offset, _ENCAP + struct.pack('<d', jump_penalty))
            print(f'RMS RTE      : {rms_rte:.4f} m')
            print(f'Jump penalty : {jump_penalty:.4f} m  (threshold={JUMP_THRESHOLD}m)')

            # Phase-specific metrics: ATE/RTE restricted to detected vertical
            # (ascent/descent) segments. Runs on the TRUNCATED pose stream, so
            # descent inherits the pre-touchdown cutoff above. Self-activates
            # only when segments are found.
            seg_fixes = [(sn, np.asarray(p, dtype=float)) for _, sn, p, _ in out_aligned]
            segments = detect_vertical_segments(seg_fixes)
            phase_metrics: dict = {}
            for direction in ('ascent', 'descent'):
                segs = [(a, b) for d_, a, b in segments if d_ == direction]
                if not segs:
                    continue
                sel = [k for k, (_, sn, _) in enumerate(ate_records)
                       if any(a <= sn <= b for a, b in segs)]
                if not sel:
                    continue
                p_ate = float(np.sqrt(np.mean([ate_records[k][2] ** 2 for k in sel])))
                p_rte = float(np.sqrt(np.mean([rte_values[k] ** 2 for k in sel])))
                phase_metrics[f'{direction}_ate'] = p_ate
                phase_metrics[f'{direction}_rte'] = p_rte
                phase_metrics[f'{direction}_segments'] = len(segs)
                dur = sum((b - a) for a, b in segs) / 1e9
                print(f'{direction.capitalize():7s} ({len(segs)} seg, {dur:.0f}s): '
                      f'ATE {p_ate:.3f} m  RTE {p_rte:.4f} m')
                first_ts = ate_records[0][0]
                for key, val in ((f'/ov_srvins/eval_{direction}_ate', p_ate),
                                 (f'/ov_srvins/eval_{direction}_rte', p_rte)):
                    conn = _computed_conn(key, 'std_msgs/msg/Float64')
                    writer.write(conn, first_ts + ts_offset, _ENCAP + struct.pack('<d', val))
            if not segments:
                print('No ascent/descent segments detected — phase metrics skipped')
            if diag_tracking:
                avg_slam_feats = float(np.mean([c for _, c in diag_tracking]))
                writer.write(conn_avg_slam, first_ts + ts_offset, _ENCAP + struct.pack('<d', avg_slam_feats))
                print(f'Avg SLAM feats: {avg_slam_feats:.1f} features/frame')
            else:
                avg_slam_feats = None
                print('WARNING: diag/tracking not in bag — avg_slam_feats skipped')
            metrics: dict = {'rms_rte': rms_rte, 'jump_penalty': jump_penalty,
                             **phase_metrics}
            if avg_slam_feats is not None:
                metrics['avg_slam_feats'] = avg_slam_feats
            return metrics
        return {}

    no_paths = quick or shrink
    conn_pose_al  = _computed_conn('/ov_srvins/gt/aligned',      POSE_TYPE)
    conn_vio_pose = _computed_conn('/ov_srvins/vio/pose',        POSE_TYPE)
    conn_pose     = (None if quick or not emit_raw
                     else _computed_conn('/ov_srvins/rtk/pose',  POSE_TYPE))
    conn_path     = (None if no_paths or not emit_raw
                     else _computed_conn('/ov_srvins/rtk/path',  PATH_TYPE))
    conn_path_al  = None if no_paths else _computed_conn('/ov_srvins/gt/aligned_path', PATH_TYPE)
    conn_vio_path = None if no_paths else _computed_conn('/ov_srvins/vio/path',        PATH_TYPE)
    conn_ate      = None if quick    else _computed_conn('/ov_srvins/ate', 'std_msgs/msg/Float64')
    conn_rte      = None if quick    else _computed_conn('/ov_srvins/rte', 'std_msgs/msg/Float64')

    if quick:
        for fix_ts, stamp_ns, pos, rot in out_aligned:
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        for vio_ts, stamp_ns, pos, rot in _decode_posimus(posimus):
            writer.write(conn_vio_pose, vio_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        return

    else:
        if conn_pose is not None:
            rtk_buf = bytearray()
            for k, (fix_ts, stamp_ns, pos, rot) in enumerate(out_poses, 1):
                writer.write(conn_pose, fix_ts + ts_offset, typestore.serialize_cdr(
                    make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
                if conn_path is not None:
                    rtk_buf.extend(_pose_cdr_bytes(stamp_ns, pos, rot.as_quat()))
                    writer.write(conn_path, fix_ts + ts_offset,
                                 _path_header_cdr(stamp_ns, k) + bytes(rtk_buf))

        al_buf = bytearray()
        for k, (fix_ts, stamp_ns, pos, rot) in enumerate(out_aligned, 1):
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
            if conn_path_al is not None:
                al_buf.extend(_pose_cdr_bytes(stamp_ns, pos, rot.as_quat()))
                writer.write(conn_path_al, fix_ts + ts_offset,
                             _path_header_cdr(stamp_ns, k) + bytes(al_buf))

        vio_frames = _decode_posimus(posimus)

        pose_buf = bytearray()
        for k, (vio_ts, stamp_ns, pos, rot) in enumerate(vio_frames, 1):
            q = rot.as_quat()
            writer.write(conn_vio_pose, vio_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
            if conn_vio_path is not None:
                pose_buf.extend(_pose_cdr_bytes(stamp_ns, pos, q))
                writer.write(conn_vio_path, vio_ts + ts_offset,
                             _path_header_cdr(stamp_ns, k) + bytes(pose_buf))

    # ATE + RTE: one Float64 per VIO frame (skipped in quick mode)
    if conn_ate is not None and out_aligned and vio_frames:
        al_stamps    = [stamp_ns for _, stamp_ns, _, _ in out_aligned]
        al_positions = [pos_al   for _, _, pos_al, _ in out_aligned]
        n_al = len(al_stamps)

        def _nearest_al(vio_stamp_ns):
            idx = bisect.bisect_left(al_stamps, vio_stamp_ns)
            if idx == 0:
                return 0
            if idx >= n_al:
                return n_al - 1
            return idx if (al_stamps[idx] - vio_stamp_ns) <= (vio_stamp_ns - al_stamps[idx - 1]) else idx - 1

        # First pass: compute ATE for every VIO frame and record (bag_ts, header_stamp_ns, ate).
        ate_records: list[tuple[int, int, float]] = []
        for vio_ts, vio_stamp_ns, vio_pos, _ in vio_frames:
            ate = float(np.linalg.norm(vio_pos - al_positions[_nearest_al(vio_stamp_ns)]))
            ate_records.append((vio_ts, vio_stamp_ns, ate))
            writer.write(conn_ate, vio_ts + ts_offset, _ENCAP + struct.pack('<d', ate))

        # Second pass: RTE = ate(t) - ate(t - rte_window_ns), using header stamps for the window.
        for (vio_ts, _, _), rte in zip(ate_records, rte_values_from_ate(ate_records, rte_window_ns)):
            writer.write(conn_rte, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte))


# ---------------------------------------------------------------------------
# convert: write computed topics + passthrough of input bag topics
# ---------------------------------------------------------------------------
def run(input_bag: str, output_bag: str, vio_topic: str, stores_enum, quick: bool = False,
        eval_mode: bool = False) -> None:
    from bag_tool.aruco_align import detect_aruco_north
    # For convert, camera images are in the input bag itself.
    _input_path = Path(input_bag)
    _reader_path_conv = _input_path.parent if _input_path.is_file() else _input_path
    _store = get_typestore(stores_enum)
    _posimus = []
    with Reader(_reader_path_conv) as _r:
        _conns = [c for c in _r.connections if c.topic == vio_topic]
        for _c, _bag_ts, _raw in _r.messages(connections=_conns):
            _posimus.append((_bag_ts, _store.deserialize_cdr(_raw, _c.msgtype)))
    _posimus.sort(key=lambda x: x[0])
    aruco_yaw_rad = detect_aruco_north(_reader_path_conv, _posimus, _store)

    (out_poses, out_aligned, posimus, typestore, reader_path,
     _, _, diag_tracking, platform) = compute_alignment(
        input_bag, vio_topic, stores_enum, aruco_yaw_rad=aruco_yaw_rad,
    )

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        write_alignment_topics(writer, typestore, out_poses, out_aligned, posimus, quick,
                               eval_mode=eval_mode, diag_tracking=diag_tracking,
                               platform=platform)

        if eval_mode or quick:
            print(f'Output bag written to: {out_dir}')
            return

        # Phase 4: copy all remaining source topics as raw bytes
        with Reader(reader_path) as src_reader:
            src_conn_map: dict[int, object] = {}
            for c in src_reader.connections:
                if c.topic in COMPUTED_TOPICS:
                    continue
                msgdef = _normalize_msgdef(
                    c.msgdef.data if isinstance(c.msgdef.data, str) else c.msgdef.data.decode()
                )
                src_conn_map[c.id] = writer.add_connection(
                    c.topic, c.msgtype,
                    msgdef=msgdef,
                    rihs01=c.digest or 'rihs01:' + '0' * 64,
                    serialization_format=c.ext.serialization_format,
                    offered_qos_profiles=c.ext.offered_qos_profiles,
                )
            passthrough_conns = [c for c in src_reader.connections if c.id in src_conn_map]
            for c, ts, rawdata in src_reader.messages(connections=passthrough_conns):
                writer.write(src_conn_map[c.id], ts, rawdata)
        print(f'Copied {len(src_conn_map)} source topics into output')

    print(f'Output bag written to: {out_dir}')
