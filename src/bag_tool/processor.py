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


def nearest_yaw(yaw_times: list, yaw_values: list, query_ns: int) -> float:
    """Return the yaw value whose timestamp is closest to query_ns."""
    idx = bisect.bisect_left(yaw_times, query_ns)
    if idx == 0:
        return yaw_values[0]
    if idx >= len(yaw_times):
        return yaw_values[-1]
    before = yaw_times[idx - 1]
    after  = yaw_times[idx]
    if (query_ns - before) <= (after - query_ns):
        return yaw_values[idx - 1]
    return yaw_values[idx]


# ---------------------------------------------------------------------------
# Main processing function
# ---------------------------------------------------------------------------
def run(input_bag: str, output_bag: str, vio_topic: str, stores_enum, quick: bool = False) -> None:
    """
    Process input_bag and write output_bag.

    Args:
        input_bag:   Path to input bag (.mcap file or directory).
        output_bag:  Path to output bag directory to create.
        vio_topic:   ROS2 topic name for PoseWithCovarianceStamped VIO messages.
        stores_enum: rosbags Stores enum value matching the ROS2 distro.
    """
    typestore = get_typestore(stores_enum)

    # ------------------------------------------------------------------
    # Phase 1: load all messages from input bag
    # ------------------------------------------------------------------
    fixes   = []   # (timestamp_ns, msg)
    yaws    = []   # (timestamp_ns, yaw_deg)
    posimus = []   # (timestamp_ns, msg)

    input_path = Path(input_bag)
    reader_path = input_path.parent if input_path.is_file() else input_path

    with Reader(reader_path) as reader:
        for connection, timestamp, rawdata in reader.messages():
            topic = connection.topic
            if topic == '/m300/rtk/fix':
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                fixes.append((timestamp, msg))
            elif topic == '/m300/rtk/yaw':
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                yaws.append((timestamp, msg.data * 10.0))  # DJI publishes tenths-of-degrees
            elif topic == vio_topic:
                msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
                posimus.append((timestamp, msg))

    fixes.sort(key=lambda x: x[0])
    yaws.sort(key=lambda x: x[0])
    posimus.sort(key=lambda x: x[0])

    yaw_times  = [t for t, _ in yaws]
    yaw_values = [v for _, v in yaws]

    if not fixes:
        print('ERROR: no /m300/rtk/fix messages found in bag', file=sys.stderr)
        sys.exit(1)

    if not posimus:
        print(f'WARNING: no {vio_topic} messages — aligned topics will be empty')

    print(f'Loaded {len(fixes)} fixes, {len(yaws)} yaw msgs, {len(posimus)} poseimu msgs')

    # VIO init from first poseimu message
    vio_init_pos = None
    vio_init_rot = None
    if posimus:
        _, pm = posimus[0]
        vio_init_pos = np.array([pm.pose.pose.position.x,
                                  pm.pose.pose.position.y,
                                  pm.pose.pose.position.z])
        vio_init_rot = Rotation.from_quat([pm.pose.pose.orientation.x,
                                            pm.pose.pose.orientation.y,
                                            pm.pose.pose.orientation.z,
                                            pm.pose.pose.orientation.w])

    # ------------------------------------------------------------------
    # Phase 2: process fixes
    # ------------------------------------------------------------------
    ref_lat = ref_lon = ref_alt = None
    ref_ecef = None
    pose_offset = None
    rtk_init_rot = None
    align_rot = None
    align_trans = None

    out_poses   = []   # (timestamp_ns, header_stamp_ns, pos, rot)  raw ENU
    out_aligned = []   # (timestamp_ns, header_stamp_ns, pos, rot)  aligned

    # Previous quaternion arrays [x,y,z,w] for sign-continuity enforcement
    prev_q_arr    = None
    prev_q_al_arr = None

    for fix_ts, fix_msg in fixes:
        lat = fix_msg.latitude
        lon = fix_msg.longitude
        alt = fix_msg.altitude

        # Drop DJI startup garbage
        if lat == 0.0 and lon == 0.0:
            continue

        # First valid fix → set ENU reference, do not emit a pose
        if ref_lat is None:
            ref_lat, ref_lon, ref_alt = lat, lon, alt
            ref_ecef = geodetic_to_ecef(lat, lon, alt)
            print(f'ENU origin set: lat={lat:.7f} lon={lon:.7f} alt={alt:.3f}')
            continue

        # Subsequent fixes → compute ENU
        cx, cy, cz = geodetic_to_ecef(lat, lon, alt)
        dx, dy, dz = cx - ref_ecef[0], cy - ref_ecef[1], cz - ref_ecef[2]
        enu = ecef_to_enu(dx, dy, dz, ref_lat, ref_lon)

        # Zero the starting position
        if pose_offset is None:
            pose_offset = enu.copy()
        enu = enu - pose_offset

        # Build orientation (roll=pitch=0, yaw from RTK).
        # Enforce sign continuity to prevent 180° flicker from quaternion double-cover.
        yaw_deg = nearest_yaw(yaw_times, yaw_values, fix_ts)
        yaw_rad = dji_yaw_to_enu_rad(yaw_deg)
        q_arr = Rotation.from_euler('z', yaw_rad).as_quat()
        if prev_q_arr is not None and np.dot(q_arr, prev_q_arr) < 0:
            q_arr = -q_arr
        prev_q_arr = q_arr
        rot = Rotation.from_quat(q_arr)

        # Capture RTK init orientation for alignment (first emitted pose is at 0,0,0)
        if rtk_init_rot is None:
            rtk_init_rot = rot
            if vio_init_rot is not None:
                align_rot   = vio_init_rot * rtk_init_rot.inv()
                align_trans = vio_init_pos
                print('RTK↔VIO alignment computed')

        # Header stamp: use the fix header if available, otherwise bag timestamp
        hdr_sec  = fix_msg.header.stamp.sec
        hdr_nsec = fix_msg.header.stamp.nanosec
        stamp_ns = hdr_sec * 10**9 + hdr_nsec if (hdr_sec or hdr_nsec) else fix_ts

        out_poses.append((fix_ts, stamp_ns, enu, rot))

        if align_rot is not None:
            p_al = align_rot.apply(enu) + align_trans
            q_al_arr = (align_rot * rot).as_quat()
            if prev_q_al_arr is not None and np.dot(q_al_arr, prev_q_al_arr) < 0:
                q_al_arr = -q_al_arr
            prev_q_al_arr = q_al_arr

            # TODO: TEMPORARY - extra 90° position rotation for axis investigation.
            # Orientation is left as computed by alignment; only the position vector
            # is rotated. Do NOT use for production — will be removed once the axis
            # confusion is understood and fixed properly.
            _rot90 = Rotation.from_euler('z', -math.pi / 2)
            p_al = _rot90.apply(p_al)

            out_aligned.append((fix_ts, stamp_ns, p_al, Rotation.from_quat(q_al_arr)))

    print(f'Emitted {len(out_poses)} RTK poses, {len(out_aligned)} aligned poses')

    # ------------------------------------------------------------------
    # Phase 3: write output bag  (all input topics + new computed ones)
    # ------------------------------------------------------------------
    def make_pose_msg(stamp_ns, frame_id, pos, rot):
        q = rot.as_quat()   # [x, y, z, w]
        PoseWithCovStamped = typestore.types['geometry_msgs/msg/PoseWithCovarianceStamped']
        Header = typestore.types['std_msgs/msg/Header']
        Time   = typestore.types['builtin_interfaces/msg/Time']
        Pose   = typestore.types['geometry_msgs/msg/Pose']
        PoseWithCov = typestore.types['geometry_msgs/msg/PoseWithCovariance']
        Point  = typestore.types['geometry_msgs/msg/Point']
        Quat   = typestore.types['geometry_msgs/msg/Quaternion']

        sec  = int(stamp_ns // 10**9)
        nsec = int(stamp_ns %  10**9)

        return PoseWithCovStamped(
            header=Header(
                stamp=Time(sec=sec, nanosec=nsec),
                frame_id=frame_id,
            ),
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
        Path_        = typestore.types['nav_msgs/msg/Path']
        PoseStamped  = typestore.types['geometry_msgs/msg/PoseStamped']
        Header       = typestore.types['std_msgs/msg/Header']
        Time         = typestore.types['builtin_interfaces/msg/Time']
        Pose         = typestore.types['geometry_msgs/msg/Pose']
        Point        = typestore.types['geometry_msgs/msg/Point']
        Quat         = typestore.types['geometry_msgs/msg/Quaternion']

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

    FRAME_ID  = 'global'
    POSE_TYPE = 'geometry_msgs/msg/PoseWithCovarianceStamped'
    PATH_TYPE = 'nav_msgs/msg/Path'

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        conn_pose    = None if quick else writer.add_connection('/ov_srvins/rtk/pose',         POSE_TYPE, typestore=typestore)
        conn_path    = writer.add_connection('/ov_srvins/rtk/path',         PATH_TYPE, typestore=typestore)
        conn_pose_al = None if quick else writer.add_connection('/ov_srvins/rtk/pose_aligned', POSE_TYPE, typestore=typestore)
        conn_path_al = writer.add_connection('/ov_srvins/rtk/path_aligned', PATH_TYPE, typestore=typestore)
        conn_vio_path = writer.add_connection('/ov_srvins/vio/path',        PATH_TYPE, typestore=typestore)

        if quick:
            # Single path message per topic (all poses at once)
            if out_poses:
                path_poses = [(stamp_ns, pos, rot) for _, stamp_ns, pos, rot in out_poses]
                first_ts, first_stamp_ns = out_poses[0][0], out_poses[0][1]
                writer.write(conn_path, first_ts, typestore.serialize_cdr(
                    make_path_msg(first_stamp_ns, FRAME_ID, path_poses), PATH_TYPE))

            if out_aligned:
                path_poses_al = [(stamp_ns, pos, rot) for _, stamp_ns, pos, rot in out_aligned]
                first_ts_al, first_stamp_ns_al = out_aligned[0][0], out_aligned[0][1]
                writer.write(conn_path_al, first_ts_al, typestore.serialize_cdr(
                    make_path_msg(first_stamp_ns_al, FRAME_ID, path_poses_al), PATH_TYPE))

            if conn_vio_path and posimus:
                vio_path_poses = []
                for vio_ts, pm in posimus:
                    pos = [pm.pose.pose.position.x, pm.pose.pose.position.y, pm.pose.pose.position.z]
                    rot = Rotation.from_quat([pm.pose.pose.orientation.x, pm.pose.pose.orientation.y,
                                              pm.pose.pose.orientation.z, pm.pose.pose.orientation.w])
                    stamp_ns = pm.header.stamp.sec * 10**9 + pm.header.stamp.nanosec
                    vio_path_poses.append((stamp_ns, pos, rot))
                writer.write(conn_vio_path, posimus[0][0], typestore.serialize_cdr(
                    make_path_msg(vio_path_poses[0][0], FRAME_ID, vio_path_poses), PATH_TYPE))

        else:
            # Growing path message at every fix timestamp
            path_poses = []
            for fix_ts, stamp_ns, pos, rot in out_poses:
                path_poses.append((stamp_ns, pos, rot))
                writer.write(conn_pose, fix_ts, typestore.serialize_cdr(
                    make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
                writer.write(conn_path, fix_ts, typestore.serialize_cdr(
                    make_path_msg(stamp_ns, FRAME_ID, path_poses), PATH_TYPE))

            path_poses_al = []
            for fix_ts, stamp_ns, pos, rot in out_aligned:
                path_poses_al.append((stamp_ns, pos, rot))
                writer.write(conn_pose_al, fix_ts, typestore.serialize_cdr(
                    make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
                writer.write(conn_path_al, fix_ts, typestore.serialize_cdr(
                    make_path_msg(stamp_ns, FRAME_ID, path_poses_al), PATH_TYPE))

            pose_buf = bytearray()
            for k, (vio_ts, pm) in enumerate(posimus, 1):
                stamp_ns = pm.header.stamp.sec * 10 ** 9 + pm.header.stamp.nanosec
                pos = (pm.pose.pose.position.x, pm.pose.pose.position.y, pm.pose.pose.position.z)
                q   = (pm.pose.pose.orientation.x, pm.pose.pose.orientation.y,
                       pm.pose.pose.orientation.z, pm.pose.pose.orientation.w)
                pose_buf.extend(_pose_cdr_bytes(stamp_ns, pos, q))
                writer.write(conn_vio_path, vio_ts,
                             _path_header_cdr(stamp_ns, k) + bytes(pose_buf))

    print(f'Output bag written to: {out_dir}')
