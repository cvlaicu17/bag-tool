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
import sys
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore
from scipy.spatial.transform import Rotation

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
def run(input_bag: str, output_bag: str, vio_topic: str, stores_enum) -> None:
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
                yaws.append((timestamp, msg.data))
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
            out_aligned.append((fix_ts, stamp_ns, p_al, Rotation.from_quat(q_al_arr)))

    print(f'Emitted {len(out_poses)} RTK poses, {len(out_aligned)} aligned poses')

    # ------------------------------------------------------------------
    # Phase 3: write output bag
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
        conn_pose    = writer.add_connection('/ov_srvins/rtk/pose',         POSE_TYPE, typestore=typestore)
        conn_path    = writer.add_connection('/ov_srvins/rtk/path',         PATH_TYPE, typestore=typestore)
        conn_pose_al = writer.add_connection('/ov_srvins/rtk/pose_aligned', POSE_TYPE, typestore=typestore)
        conn_path_al = writer.add_connection('/ov_srvins/rtk/path_aligned', PATH_TYPE, typestore=typestore)
        conn_vio     = writer.add_connection(vio_topic,                     POSE_TYPE, typestore=typestore)

        path_poses         = []   # (stamp_ns, pos, rot)
        path_poses_aligned = []

        for fix_ts, stamp_ns, pos, rot in out_poses:
            path_poses.append((stamp_ns, pos, rot))

            pose_msg = make_pose_msg(stamp_ns, FRAME_ID, pos, rot)
            path_msg = make_path_msg(stamp_ns, FRAME_ID, path_poses)

            writer.write(conn_pose, fix_ts, typestore.serialize_cdr(pose_msg, POSE_TYPE))
            writer.write(conn_path, fix_ts, typestore.serialize_cdr(path_msg, PATH_TYPE))

        for fix_ts, stamp_ns, pos, rot in out_aligned:
            path_poses_aligned.append((stamp_ns, pos, rot))

            pose_msg = make_pose_msg(stamp_ns, FRAME_ID, pos, rot)
            path_msg = make_path_msg(stamp_ns, FRAME_ID, path_poses_aligned)

            writer.write(conn_pose_al, fix_ts, typestore.serialize_cdr(pose_msg, POSE_TYPE))
            writer.write(conn_path_al, fix_ts, typestore.serialize_cdr(path_msg, PATH_TYPE))

        for ts, msg in posimus:
            writer.write(conn_vio, ts, typestore.serialize_cdr(msg, POSE_TYPE))

    print(f'Output bag written to: {out_dir}')
