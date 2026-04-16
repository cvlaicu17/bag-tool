"""
Offline RTK → ENU pose converter and VIO alignment.

Reads an input ROS2 bag containing:
  /m300/rtk/fix    sensor_msgs/NavSatFix
  /m300/rtk/yaw    std_msgs/Float64  (degrees, clockwise from North)
  <vio_topic>      geometry_msgs/PoseWithCovarianceStamped

Writes a new bag with:
  /ov_srvins/rtk/pose          PoseWithCovarianceStamped  (ENU, starts at 0,0,0)
  /ov_srvins/rtk/path          nav_msgs/Path
  /ov_srvins/rtk/pose_aligned  PoseWithCovarianceStamped  (aligned to VIO frame)
  /ov_srvins/rtk/path_aligned  nav_msgs/Path
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


# Topics written by compute_alignment / write_alignment_topics.
# Used to exclude them from passthrough when the source bag is also the input bag.
COMPUTED_TOPICS = frozenset({
    '/ov_srvins/rtk/pose',
    '/ov_srvins/rtk/path',
    '/ov_srvins/rtk/pose_aligned',
    '/ov_srvins/rtk/path_aligned',
    '/ov_srvins/vio/pose',
    '/ov_srvins/vio/path',
    '/ov_srvins/ate',
    '/ov_srvins/rte',
    '/ov_srvins/eval_rms_rte',
    '/ov_srvins/eval_jump_penalty',
})


# ---------------------------------------------------------------------------
# Phase 1 + 2: load RTK/VIO data and compute poses (no I/O side-effects)
# ---------------------------------------------------------------------------
def compute_alignment(
    input_bag: str,
    vio_topic: str,
    stores_enum,
) -> tuple:
    """Load RTK/VIO data from input_bag and compute ENU + aligned poses.

    Returns (out_poses, out_aligned, posimus, typestore, reader_path,
             input_start_ns, input_end_ns).
    out_poses   : list of (fix_ts, stamp_ns, enu_pos, rot)   — raw ENU
    out_aligned : list of (fix_ts, stamp_ns, pos_al, rot_al) — VIO-aligned
    posimus     : list of (timestamp_ns, msg)                — raw VIO poses
    typestore   : rosbags typestore (needed for Phase 3 serialisation)
    reader_path : Path that was opened (for Phase 4 passthrough in convert)
    input_start_ns/input_end_ns : bag time bounds (for align timestamp-offset detection)
    """
    typestore = get_typestore(stores_enum)

    fixes   = []
    yaws    = []
    posimus = []

    input_path  = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    with Reader(reader_path) as reader:
        input_start_ns = reader.start_time
        input_end_ns = reader.start_time + reader.duration

        relevant_connections = [
            connection
            for connection in reader.connections
            if connection.topic in {'/m300/rtk/fix', '/m300/rtk/yaw', vio_topic}
        ]

        for connection, timestamp, rawdata in reader.messages(connections=relevant_connections):
            topic = connection.topic
            if topic == '/m300/rtk/fix':
                fixes.append((timestamp, typestore.deserialize_cdr(rawdata, connection.msgtype)))
            elif topic == '/m300/rtk/yaw':
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                yaws.append((timestamp, msg.data * 10.0))  # DJI publishes tenths-of-degrees
            elif topic == vio_topic:
                posimus.append((timestamp, typestore.deserialize_cdr(rawdata, connection.msgtype)))

    fixes.sort(key=lambda x: x[0])
    yaws.sort(key=lambda x: x[0])
    posimus.sort(key=lambda x: x[0])

    if not fixes:
        print('WARNING: no /m300/rtk/fix messages found — skipping RTK, emitting VIO path only')

    if not yaws and fixes:
        print('WARNING: no /m300/rtk/yaw messages found — skipping RTK')
        fixes = []

    if not posimus:
        print(f'WARNING: no {vio_topic} messages — aligned topics will be empty')

    print(f'Loaded {len(fixes)} fixes, {len(yaws)} yaw msgs, {len(posimus)} poseimu msgs')

    # VIO init from first poseimu message
    vio_init_pos = vio_init_rot = None
    if posimus:
        _, pm = posimus[0]
        vio_init_pos = np.array([pm.pose.pose.position.x,
                                  pm.pose.pose.position.y,
                                  pm.pose.pose.position.z])
        vio_init_rot = Rotation.from_quat([pm.pose.pose.orientation.x,
                                            pm.pose.pose.orientation.y,
                                            pm.pose.pose.orientation.z,
                                            pm.pose.pose.orientation.w])

    # Phase 2: process fixes → ENU poses + aligned poses
    ref_lat = ref_lon = ref_alt = None
    ref_ecef = None
    pose_offset   = None
    rtk_init_rot  = None
    align_rot     = None
    align_rot_pos = None  # position uses an additional -90° Z correction (ENU→VIO frame)
    align_trans   = None
    prev_q_arr    = None
    prev_q_al_arr = None

    out_poses   = []
    out_aligned = []
    yaw_idx = 0
    last_yaw_idx = len(yaws) - 1

    for fix_ts, fix_msg in fixes:
        lat = fix_msg.latitude
        lon = fix_msg.longitude
        alt = fix_msg.altitude

        if lat == 0.0 and lon == 0.0:
            continue

        if ref_lat is None:
            ref_lat, ref_lon, ref_alt = lat, lon, alt
            ref_ecef = geodetic_to_ecef(lat, lon, alt)
            print(f'ENU origin set: lat={lat:.7f} lon={lon:.7f} alt={alt:.3f}')
            continue

        cx, cy, cz = geodetic_to_ecef(lat, lon, alt)
        dx, dy, dz = cx - ref_ecef[0], cy - ref_ecef[1], cz - ref_ecef[2]
        enu = ecef_to_enu(dx, dy, dz, ref_lat, ref_lon)

        if pose_offset is None:
            pose_offset = enu.copy()
        enu = enu - pose_offset

        while yaw_idx < last_yaw_idx and yaws[yaw_idx + 1][0] <= fix_ts:
            yaw_idx += 1

        if fix_ts <= yaws[0][0]:
            yaw_deg = yaws[0][1]
        elif yaw_idx >= last_yaw_idx:
            yaw_deg = yaws[last_yaw_idx][1]
        else:
            before_ts, before_yaw = yaws[yaw_idx]
            after_ts, after_yaw = yaws[yaw_idx + 1]
            if (fix_ts - before_ts) <= (after_ts - fix_ts):
                yaw_deg = before_yaw
            else:
                yaw_deg = after_yaw

        yaw_rad = dji_yaw_to_enu_rad(yaw_deg)
        q_arr = Rotation.from_euler('z', yaw_rad).as_quat()
        if prev_q_arr is not None and np.dot(q_arr, prev_q_arr) < 0:
            q_arr = -q_arr
        prev_q_arr = q_arr
        rot = Rotation.from_quat(q_arr)

        if rtk_init_rot is None:
            rtk_init_rot = rot
            if vio_init_rot is not None:
                align_rot     = vio_init_rot * rtk_init_rot.inv()
                # Strip pitch/roll from the position rotation: RTK altitude is geodetic
                # (Z = up) and applying pitch/roll would tilt the whole trajectory.
                # Use only the yaw component of align_rot for position.
                align_yaw_rad = align_rot.as_euler('zyx')[0]
                align_rot_pos = Rotation.from_euler('z', -math.pi / 2 + align_yaw_rad)
                align_trans   = vio_init_pos
                print('RTK↔VIO alignment computed')

        hdr_sec  = fix_msg.header.stamp.sec
        hdr_nsec = fix_msg.header.stamp.nanosec
        stamp_ns = hdr_sec * 10**9 + hdr_nsec if (hdr_sec or hdr_nsec) else fix_ts

        out_poses.append((fix_ts, stamp_ns, enu, rot))

        if align_rot is not None:
            p_al = align_rot_pos.apply(enu) + align_trans
            q_al_arr = (align_rot * rot).as_quat()
            if prev_q_al_arr is not None and np.dot(q_al_arr, prev_q_al_arr) < 0:
                q_al_arr = -q_al_arr
            prev_q_al_arr = q_al_arr
            out_aligned.append((fix_ts, stamp_ns, p_al, Rotation.from_quat(q_al_arr)))

    print(f'Emitted {len(out_poses)} RTK poses, {len(out_aligned)} aligned poses')
    return out_poses, out_aligned, posimus, typestore, reader_path, input_start_ns, input_end_ns


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
) -> None:
    """Register computed alignment connections and write all their messages."""

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
        conn_pose_al      = _computed_conn('/ov_srvins/rtk/pose_aligned',   POSE_TYPE)
        conn_vio_pose     = _computed_conn('/ov_srvins/vio/pose',            POSE_TYPE)
        conn_rte          = _computed_conn('/ov_srvins/rte',                 'std_msgs/msg/Float64')
        conn_rms_rte      = _computed_conn('/ov_srvins/eval_rms_rte',        'std_msgs/msg/Float64')
        conn_jump_penalty = _computed_conn('/ov_srvins/eval_jump_penalty',   'std_msgs/msg/Float64')

        # Landing detection: rightmost RTK fix at or below 0.5 m ENU Z (height above takeoff).
        LANDING_ALT = 0.5  # metres
        landing_stamp_ns = None
        for _, stamp_ns, pos, _ in reversed(out_poses):
            if pos[2] <= LANDING_ALT:
                landing_stamp_ns = stamp_ns
                break
        if landing_stamp_ns is not None:
            n_before = len(posimus)
            posimus     = [(ts, pm) for ts, pm in posimus
                           if pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec <= landing_stamp_ns]
            out_aligned = [(ft, sn, p, r) for ft, sn, p, r in out_aligned if sn <= landing_stamp_ns]
            n_dropped = n_before - len(posimus)
            print(f'Landing detected (ENU Z ≤ {LANDING_ALT}m) — dropped {n_dropped} post-landing VIO frames')
        else:
            print('WARNING: no landing point detected — using all VIO data')

        for fix_ts, stamp_ns, pos, rot in out_aligned:
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        for vio_ts, pm in posimus:
            stamp_ns = pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec
            pos = np.array([pm.pose.pose.position.x, pm.pose.pose.position.y, pm.pose.pose.position.z])
            rot = Rotation.from_quat([pm.pose.pose.orientation.x, pm.pose.pose.orientation.y,
                                      pm.pose.pose.orientation.z, pm.pose.pose.orientation.w])
            writer.write(conn_vio_pose, vio_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        if out_aligned and posimus:
            al_stamps    = [stamp_ns for _, stamp_ns, _, _ in out_aligned]
            al_positions = [pos_al   for _, _, pos_al, _ in out_aligned]
            n_al = len(al_stamps)
            def _nearest_al_eval(vio_stamp_ns):
                idx = bisect.bisect_left(al_stamps, vio_stamp_ns)
                if idx == 0:    return 0
                if idx >= n_al: return n_al - 1
                return idx if (al_stamps[idx] - vio_stamp_ns) <= (vio_stamp_ns - al_stamps[idx - 1]) else idx - 1
            ate_records: list[tuple[int, int, float]] = []
            for vio_ts, pm in posimus:
                vio_pos      = np.array([pm.pose.pose.position.x,
                                         pm.pose.pose.position.y,
                                         pm.pose.pose.position.z])
                vio_stamp_ns = pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec
                ate = float(np.linalg.norm(vio_pos - al_positions[_nearest_al_eval(vio_stamp_ns)]))
                ate_records.append((vio_ts, vio_stamp_ns, ate))
            ate_header_stamps = [r[1] for r in ate_records]
            n_ate = len(ate_records)
            rte_values: list[float] = []
            for vio_ts, vio_stamp_ns, ate in ate_records:
                past_stamp = vio_stamp_ns - rte_window_ns
                j = bisect.bisect_left(ate_header_stamps, past_stamp)
                if j >= n_ate: j = n_ate - 1
                if j > 0 and (ate_header_stamps[j] - past_stamp) > (past_stamp - ate_header_stamps[j - 1]):
                    j -= 1
                rte = ate - ate_records[j][2]
                rte_values.append(rte)
                writer.write(conn_rte, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte))
            JUMP_THRESHOLD = 1.0  # metres
            rte_arr = np.array(rte_values)
            rms_rte = float(np.sqrt(np.mean(rte_arr ** 2)))
            if len(rte_arr) > 1:
                excess       = np.maximum(0.0, np.abs(np.diff(rte_arr)) - JUMP_THRESHOLD)
                jump_penalty = float(np.sqrt(np.sum(excess ** 2)))
            else:
                jump_penalty = 0.0
            first_ts = ate_records[0][0]
            writer.write(conn_rms_rte,      first_ts + ts_offset, _ENCAP + struct.pack('<d', rms_rte))
            writer.write(conn_jump_penalty, first_ts + ts_offset, _ENCAP + struct.pack('<d', jump_penalty))
            print(f'RMS RTE      : {rms_rte:.4f} m')
            print(f'Jump penalty : {jump_penalty:.4f} m  (threshold={JUMP_THRESHOLD}m)')
            return {'rms_rte': rms_rte, 'jump_penalty': jump_penalty}
        return {}

    conn_pose_al  = _computed_conn('/ov_srvins/rtk/pose_aligned', POSE_TYPE)
    conn_vio_pose = _computed_conn('/ov_srvins/vio/pose',         POSE_TYPE)
    conn_pose     = None if quick else _computed_conn('/ov_srvins/rtk/pose',         POSE_TYPE)
    conn_path     = None if quick else _computed_conn('/ov_srvins/rtk/path',         PATH_TYPE)
    conn_path_al  = None if quick else _computed_conn('/ov_srvins/rtk/path_aligned', PATH_TYPE)
    conn_vio_path = None if quick else _computed_conn('/ov_srvins/vio/path',         PATH_TYPE)
    conn_ate      = None if quick else _computed_conn('/ov_srvins/ate', 'std_msgs/msg/Float64')
    conn_rte      = None if quick else _computed_conn('/ov_srvins/rte', 'std_msgs/msg/Float64')

    if quick:
        for fix_ts, stamp_ns, pos, rot in out_aligned:
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        for vio_ts, pm in posimus:
            stamp_ns = pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec
            pos = np.array([pm.pose.pose.position.x, pm.pose.pose.position.y, pm.pose.pose.position.z])
            rot = Rotation.from_quat([pm.pose.pose.orientation.x, pm.pose.pose.orientation.y,
                                      pm.pose.pose.orientation.z, pm.pose.pose.orientation.w])
            writer.write(conn_vio_pose, vio_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
        return

    else:
        rtk_buf = bytearray()
        for k, (fix_ts, stamp_ns, pos, rot) in enumerate(out_poses, 1):
            q = rot.as_quat()
            rtk_buf.extend(_pose_cdr_bytes(stamp_ns, pos, q))
            writer.write(conn_pose, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
            writer.write(conn_path, fix_ts + ts_offset,
                         _path_header_cdr(stamp_ns, k) + bytes(rtk_buf))

        al_buf = bytearray()
        for k, (fix_ts, stamp_ns, pos, rot) in enumerate(out_aligned, 1):
            q = rot.as_quat()
            al_buf.extend(_pose_cdr_bytes(stamp_ns, pos, q))
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
            writer.write(conn_path_al, fix_ts + ts_offset,
                         _path_header_cdr(stamp_ns, k) + bytes(al_buf))

        pose_buf = bytearray()
        for k, (vio_ts, pm) in enumerate(posimus, 1):
            stamp_ns = pm.header.stamp.sec * 10 ** 9 + pm.header.stamp.nanosec
            pos = (pm.pose.pose.position.x, pm.pose.pose.position.y, pm.pose.pose.position.z)
            q   = (pm.pose.pose.orientation.x, pm.pose.pose.orientation.y,
                   pm.pose.pose.orientation.z, pm.pose.pose.orientation.w)
            pose_buf.extend(_pose_cdr_bytes(stamp_ns, pos, q))
            rot = Rotation.from_quat(q)
            writer.write(conn_vio_pose, vio_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, np.array(pos), rot), POSE_TYPE))
            writer.write(conn_vio_path, vio_ts + ts_offset,
                         _path_header_cdr(stamp_ns, k) + bytes(pose_buf))

    # ATE + RTE: one Float64 per VIO frame (skipped in quick mode)
    if conn_ate is not None and out_aligned and posimus:
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
        for vio_ts, pm in posimus:
            vio_pos      = np.array([pm.pose.pose.position.x,
                                     pm.pose.pose.position.y,
                                     pm.pose.pose.position.z])
            vio_stamp_ns = pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec
            ate = float(np.linalg.norm(vio_pos - al_positions[_nearest_al(vio_stamp_ns)]))
            ate_records.append((vio_ts, vio_stamp_ns, ate))
            writer.write(conn_ate, vio_ts + ts_offset, _ENCAP + struct.pack('<d', ate))

        # Second pass: RTE = ate(t) - ate(t - rte_window_ns), using header stamps for the window.
        ate_header_stamps = [r[1] for r in ate_records]
        n_ate = len(ate_records)
        for i, (vio_ts, vio_stamp_ns, ate) in enumerate(ate_records):
            past_stamp = vio_stamp_ns - rte_window_ns
            j = bisect.bisect_left(ate_header_stamps, past_stamp)
            if j >= n_ate:
                j = n_ate - 1
            # Pick whichever neighbour is closer to past_stamp.
            if j > 0 and (ate_header_stamps[j] - past_stamp) > (past_stamp - ate_header_stamps[j - 1]):
                j -= 1
            rte = ate - ate_records[j][2]
            writer.write(conn_rte, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte))


# ---------------------------------------------------------------------------
# convert: write computed topics + passthrough of input bag topics
# ---------------------------------------------------------------------------
def run(input_bag: str, output_bag: str, vio_topic: str, stores_enum, quick: bool = False,
        eval_mode: bool = False) -> None:
    out_poses, out_aligned, posimus, typestore, reader_path, _, _ = \
        compute_alignment(input_bag, vio_topic, stores_enum)

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        write_alignment_topics(writer, typestore, out_poses, out_aligned, posimus, quick,
                               eval_mode=eval_mode)

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
