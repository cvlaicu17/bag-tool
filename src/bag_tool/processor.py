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
    """Per-frame RTE-D from ATE records: |ATE(t)| - |ATE(t - window)| (the pre-2026-11-16 "RTE"; alignment-dependent).
    RTE proper is bag_tool.trajectory.rte_relative_pose."""
    # Records may be 3-tuples (ts, stamp, ate) or 4-tuples with a trailing
    # error VECTOR (eval mode; used for segment-anchored phase ATE).
    ate_header_stamps = [r[1] for r in ate_records]
    n_ate = len(ate_records)
    rte_values: list[float] = []
    for rec in ate_records:
        vio_stamp_ns, ate = rec[1], rec[2]
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

# --- INIT phase: ground -> INIT_TOP_M of the first ascent ------------------
# Rationale (user, 2026-08-15): "even tiny problems at init can destroy the
# future trajectory" -- so measure the init directly instead of inferring it
# from whole-flight numbers. This window is the estimator's bootstrap: the
# climb from the launch pad to INIT_TOP_M, where scale is acquired and where
# a visual-inertial initializer either earns its metric footing or bakes in
# an error the rest of the flight inherits. Reported ALONGSIDE (not instead
# of) the ascent phase, and segment-anchored the same way.
INIT_TOP_M = 6.0


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
    """Return (rms_rte, jump_penalty) from a list of per-frame RTE values (NaN = not scored, skipped)."""
    rte_arr = np.array(rte_values, dtype=float)
    rte_arr = rte_arr[np.isfinite(rte_arr)]
    if not len(rte_arr):
        return float('nan'), 0.0
    rms_rte = float(np.sqrt(np.mean(rte_arr ** 2)))
    if len(rte_arr) > 1:
        excess = np.maximum(0.0, np.abs(np.diff(rte_arr)) - JUMP_THRESHOLD)
        jump_penalty = float(np.sqrt(np.sum(excess ** 2)))
    else:
        jump_penalty = 0.0
    return rms_rte, jump_penalty


# Alignment method/diagnostics of the last compute_alignment call (merged into the eval metrics).
LAST_ALIGN_INFO: dict = {'align_method': 'first-fix'}

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
    '/ov_srvins/rte_d',
    '/ov_srvins/eval_rms_rte',
    '/ov_srvins/eval_rms_rte_d',
    '/ov_srvins/eval_jump_penalty',
    '/ov_srvins/eval_avg_slam_feats',
    '/ov_srvins/eval_ascent_ate',
    '/ov_srvins/eval_ascent_rte',
    '/ov_srvins/eval_descent_ate',
    '/ov_srvins/eval_descent_rte',
    '/ov_srvins/eval_cruise_ate',
    '/ov_srvins/eval_cruise_rte',
    '/ov_srvins/eval_init_ate',
    '/ov_srvins/eval_init_rte',
    '/ov_srvins/eval_ascent_rte_d',
    '/ov_srvins/eval_descent_rte_d',
    '/ov_srvins/eval_cruise_rte_d',
    '/ov_srvins/eval_init_rte_d',
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


def _navsatfix_records_noyaw(fixes):
    """NavSatFix lat/lon/alt -> ENU records about the first fix, identity orientation (no heading in the GT)."""
    records = []
    ref = None
    for fix_ts, m in fixes:
        if (m.latitude == 0.0 and m.longitude == 0.0) or not math.isfinite(m.latitude) or m.status.status < 0:
            continue
        if ref is None:
            ref = (m.latitude, m.longitude, geodetic_to_ecef(m.latitude, m.longitude, m.altitude))
            print(f'ENU origin set: lat={m.latitude:.7f} lon={m.longitude:.7f} alt={m.altitude:.3f}')
        cx, cy, cz = geodetic_to_ecef(m.latitude, m.longitude, m.altitude)
        enu = np.array(ecef_to_enu(cx - ref[2][0], cy - ref[2][1], cz - ref[2][2], ref[0], ref[1]))
        stamp_ns = m.header.stamp.sec * 10**9 + m.header.stamp.nanosec or fix_ts
        records.append((fix_ts, stamp_ns, enu, Rotation.identity()))
    return records


def ground_mag_alignment(vio, gps_records, mags, platform) -> dict:
    """Yaw (VIO world -> ENU) and translation from the static ground window only: no trajectory fit.

    Window = the first platform.ground_window_s of VIO output (raven: the pre-start static pose,
    motors off). Magnetometer -> IMU frame with the platform calibration, rotated into the VIO world
    with the VIO attitude (gravity-aligned roll/pitch), averaged; magnetic north = its horizontal
    direction; true north = + declination. Translation maps the mean GNSS ground fix onto the mean
    VIO ground position."""
    t_v = np.array([sn for _, sn, _, _ in vio]); t0 = t_v[0]; t1 = t0 + int(platform.ground_window_s * 1e9)
    iv = np.where(t_v <= t1)[0]
    R_m = Rotation.from_rotvec(platform.mag_to_imu_rotvec); b = np.array(platform.mag_hard_iron_ut)
    sel = (mags[:, 0] >= t0) & (mags[:, 0] <= t1)
    if sel.sum() < 10:
        raise ValueError(f'ground-mag: only {int(sel.sum())} magnetometer samples in the first {platform.ground_window_s} s of VIO output')
    m_imu = platform.mag_sign * R_m.apply(mags[sel, 1:4] * 1e6 - b)
    # VIO attitude at each mag sample (nearest pose)
    j = np.clip(np.searchsorted(t_v, mags[sel, 0]), 0, len(vio) - 1)
    m_w = np.array([vio[k][3].apply(v) for k, v in zip(j, m_imu)])
    az = np.arctan2(m_w[:, 1], m_w[:, 0])                       # magnetic north in the VIO world, per sample
    alpha = math.atan2(np.sin(az).mean(), np.cos(az).mean())
    spread = math.degrees(np.sqrt(np.mean(np.angle(np.exp(1j * (az - alpha))) ** 2)))
    theta = math.radians(90.0 - platform.mag_declination_deg) - alpha
    theta = (theta + math.pi) % (2 * math.pi) - math.pi
    g = np.array([p for _, sn, p, _ in gps_records if t0 <= sn <= t1])
    if len(g) < 3:
        raise ValueError(f'ground-mag: only {len(g)} GNSS fixes in the ground window')
    p_gnss = g.mean(0); p_vio = np.mean([vio[k][2] for k in iv], axis=0)
    trans = p_vio - Rotation.from_euler('z', -theta).apply(p_gnss)
    print(f'Ground-mag alignment: yaw (VIO world -> ENU) {math.degrees(theta):+.2f} deg (mag spread {spread:.2f} deg, '
          f'{int(sel.sum())} samples, decl {platform.mag_declination_deg:+.1f}); origin = mean of {len(g)} GNSS fixes '
          f'(std {np.linalg.norm(g.std(0)[:2]):.2f} m horiz) over the first {platform.ground_window_s:.0f} s')
    return {'ok': True, 'theta_rad': theta, 'trans': trans, 'lag_s': 0.0, 'yaw_spread_deg': spread,
            'window_s': platform.ground_window_s, 'gnss_std_m': float(np.linalg.norm(g.std(0)[:2])),
            'n_mag': int(sel.sum()), 'n_fix': int(len(g))}


def _trajectory_from_records(out_aligned, out_poses, has_attitude, source, align_rot_pos, posimus, rte_window_ns):
    """Legacy alignments (first-fix, dynamic, ground-mag) -> AlignedTrajectory. Positions are unchanged (ATE and RTE-D
    are bit-identical to before). With a full-pose GT (PoseStamped) the attitude becomes the VIO body's attitude in the
    VIO world: Rw * R_gt * Rx, Rw = the alignment's world yaw (the one applied to positions), R_gt = the raw GT
    attitude, Rx = constant VIO body -> GT body rotation from a hand-eye on the relative rotations over rte_window.
    Position-only GT keeps the legacy placeholder attitude and has_attitude False (RTE not scorable)."""
    from bag_tool.trajectory import AlignedTrajectory, handeye_rotation, window_partner
    from scipy.spatial.transform import Slerp
    traj = AlignedTrajectory.from_records(out_aligned, has_attitude, source)
    if not traj.has_attitude:
        return traj
    if len(traj) < 2 or align_rot_pos is None or not posimus or len(out_poses) != len(out_aligned):
        traj.has_attitude = False
        return traj
    raw = align_rot_pos * Rotation.concatenate([r for _, _, _, r in out_poses])
    vf = _decode_posimus(posimus)
    t_v = np.array([sn for _, sn, _, _ in vf], np.int64); R_v = Rotation.concatenate([r for _, _, _, r in vf])
    tg = traj.stamp_ns * 1e-9; tv = t_v * 1e-9
    inside = (tv >= tg[0]) & (tv <= tg[-1])
    Rg_v = Slerp(tg, raw)(np.clip(tv, tg[0], tg[-1]))
    j = window_partner(t_v, rte_window_ns)
    pair = inside & inside[j] & (j < np.arange(len(t_v)))
    a = (R_v[j].inv() * R_v).as_rotvec(); b = (Rg_v[j].inv() * Rg_v).as_rotvec(); a[~pair] = np.nan
    M, he = handeye_rotation(a, b)
    traj.rot = raw * Rotation.from_matrix(M.T)
    LAST_ALIGN_INFO.update({'body_handeye_angle_deg': he['handeye_angle_deg'], 'body_handeye_pairs': he['handeye_pairs']})
    return traj


def _fixes_for_position_only(fixes, platform, typestore, gt_bag, reader_path):
    """Position-only GPS platforms (orqa): take /fc/gps from --gt-bag when the VIO bag has none, then move every fix into the sensor
    clock with the IMU (header stamp - log time) offset, see PlatformConfig.gt_clock_imu_topic."""
    import struct
    src = Path(gt_bag) if gt_bag else None
    if src is not None:
        src = src.parent if src.is_file() else src
    need = [] if fixes else [platform.gps_topic]
    imu_topic = platform.gt_clock_imu_topic
    lg, of = [], []
    for rp in [r for r in (src, Path(reader_path)) if r is not None]:
        with Reader(rp) as r:
            topics = {c.topic for c in r.connections}
            cons = [c for c in r.connections if (c.topic in need and not fixes) or (imu_topic and c.topic == imu_topic and not lg)]
            if not cons:
                continue                                            # an empty list would make rosbags read EVERY topic
            for c, t_ns, raw in r.messages(connections=cons):
                if c.topic == imu_topic:
                    s_, n_ = struct.unpack_from('<iI', raw, 4)         # header stamp straight from the CDR bytes
                    lg.append(t_ns * 1e-9); of.append(s_ + n_ * 1e-9 - t_ns * 1e-9)
                else:
                    fixes.append((t_ns, typestore.deserialize_cdr(raw, c.msgtype)))
    if fixes and lg:
        lg, of = np.array(lg[::50]), np.array(of[::50])
        for fix_ts, m in fixes:
            ns = int(round(fix_ts + float(np.interp(fix_ts * 1e-9, lg, of)) * 1e9))
            m.header.stamp.sec, m.header.stamp.nanosec = ns // 10**9, ns % 10**9
        print(f'GPS stamps moved into the sensor clock (IMU offset {min(of):+.3f}..{max(of):+.3f} s)')
    elif fixes and imu_topic:
        print(f'WARNING: no {imu_topic} to map the GPS clock into the sensor clock (set --gt-bag to the original recording)')
    return fixes


def gnss_ins_alignment(vio, gt_bag, platform, mags=None, gps_records=None, rte_window_ns=1_000_000_000, height=None):
    """'gnss-ins' alignment: the ground truth is a GNSS/INS smoother over the ORIGINAL recording (bag_tool.gnss_ins,
    nothing from the VIO enters it), i.e. a full 6-DoF trajectory. Steps:
      1. VIO body -> GT body rotation (constant; the VIO may run on another IMU of the rig): hand-eye on the
         rotations each trajectory makes over rte_window.
      2. World alignment from the ground window only (first platform.ground_window_s of VIO output, like ground-mag):
         yaw = mean heading of Rg Rx Rv^T, i.e. the GNSS/INS heading instead of the magnetometer; translation maps
         the mean GT position onto the mean VIO position. No trajectory fit.
      3. Output: the whole GT trajectory as VIO-body poses in the VIO world, at platform.gnss_ins_out_hz.
    Returns (AlignedTrajectory, out_poses (raw ENU records), info)."""
    from bag_tool.gnss_ins import ground_truth
    from bag_tool.trajectory import AlignedTrajectory, handeye_rotation, window_partner
    from scipy.spatial.transform import Slerp
    height = height or platform.gnss_ins_height
    gt = ground_truth(gt_bag, platform.gt_imu_topic, platform.gt_pvt_topic, height=height)
    tg = np.asarray(gt['t']); Rg_all = Rotation.from_quat(gt['q'])
    slg = Slerp(tg, Rg_all)
    t_v = np.array([sn for _, sn, _, _ in vio], np.int64); P_v = np.array([p for _, _, p, _ in vio])
    R_v = Rotation.concatenate([r for _, _, _, r in vio])
    tv = t_v * 1e-9
    inside = (tv >= tg[0]) & (tv <= tg[-1])
    if inside.sum() < 10:
        raise ValueError(f'gnss-ins: the GNSS/INS ground truth ({tg[0]:.1f}..{tg[-1]:.1f}) does not cover the VIO output '
                         f'({tv[0]:.1f}..{tv[-1]:.1f}); wrong --gt-bag?')
    Rg_v = slg(np.clip(tv, tg[0], tg[-1]))
    j = window_partner(t_v, rte_window_ns)
    pair = inside & inside[j] & (j < np.arange(len(t_v)))
    a = (R_v[j].inv() * R_v).as_rotvec(); b = (Rg_v[j].inv() * Rg_v).as_rotvec()
    a[~pair] = np.nan
    M, he = handeye_rotation(a, b)                              # a ~= M b ; Rx = M^T (VIO body -> GT body)
    R_x = Rotation.from_matrix(M.T)
    if he['handeye_angle_deg'] is None:
        print(f"WARNING: gnss-ins: only {he['handeye_pairs']} rotations > 3 deg for the body hand-eye -> VIO body taken = GT body")
    w = inside & (tv <= tv[0] + platform.ground_window_s)
    if w.sum() < 3:
        raise ValueError('gnss-ins: no VIO output inside the ground window covered by the ground truth')
    A = Rg_v[w] * R_x * R_v[w].inv()                            # VIO world -> ENU, per ground sample
    theta = float(np.angle(np.mean(np.exp(1j * A.as_euler('zyx')[:, 0]))))
    Rw = Rotation.from_euler('z', -theta)                       # ENU -> VIO world
    pg_w = np.stack([np.interp(tv[w], tg, gt['p'][:, c]) for c in range(3)], 1)
    trans = P_v[w].mean(0) - Rw.apply(pg_w.mean(0))
    # output trajectory
    step = max(1, int(round(1.0 / np.median(np.diff(tg)) / platform.gnss_ins_out_hz)))
    k = np.arange(0, len(tg), step)
    stamp = np.round(tg[k] * 1e9).astype(np.int64)
    off = int(np.median(np.array([bt for bt, _, _, _ in vio], np.int64) - t_v))   # header clock -> bag time
    pos_al = Rw.apply(gt['p'][k]) + trans
    rot_al = Rw * Rg_all[k] * R_x
    traj = AlignedTrajectory(stamp + off, stamp, pos_al, rot_al, True, 'gnss-ins')
    out_poses = [(int(stamp[i] + off), int(stamp[i]), gt['p'][kk] - gt['p'][k[0]], Rg_all[kk]) for i, kk in enumerate(k)]
    sd_h = np.degrees(np.asarray(gt['sd_att'])[:, 2])
    info = {'align_method': 'gnss-ins', 'align_yaw_world_to_enu_deg': math.degrees(theta), 'gnss_ins_height': height,
            'align_ground_window_s': platform.ground_window_s, 'gt_rate_hz': float(1.0 / np.median(np.diff(tg[k]))),
            'gnss_ins_gnss_lag_s': float(gt['lag']), 'gnss_ins_seed_coherence': float(gt['seed_coherence']),
            'gnss_ins_heading_sd_median_deg': float(np.median(sd_h)), 'gnss_ins_heading_sd_max_deg': float(sd_h.max()),
            'gnss_ins_nis_vel': float(gt['nis_vel']), 'gnss_ins_nis_pos': float(gt['nis_pos']),
            'body_handeye_angle_deg': he['handeye_angle_deg'], 'body_handeye_pairs': he['handeye_pairs'],
            'body_handeye_resid_deg': he.get('handeye_resid_deg')}
    print(f"GNSS/INS alignment: yaw (VIO world -> ENU) {math.degrees(theta):+.2f} deg from the GNSS/INS attitude over the first "
          f"{platform.ground_window_s:.0f} s (heading sd median {np.median(sd_h):.2f}, max {sd_h.max():.2f} deg); VIO->GT body rotation "
          f"{'n/a' if he['handeye_angle_deg'] is None else format(he['handeye_angle_deg'], '.2f')} deg (hand-eye, {he['handeye_pairs']} pairs"
          f"{'' if he.get('handeye_resid_deg') is None else ', resid ' + format(he['handeye_resid_deg'], '.2f') + ' deg'}); "
          f"GT {len(k)} poses at {info['gt_rate_hz']:.0f} Hz, height from {'Doppler' if height == 'doppler' else 'GNSS altitude'}")
    # magnetometer cross-check (ezio): how far the ground-mag heading is from the GNSS/INS one
    if mags is not None and len(mags) and gps_records:
        try:
            gm = ground_mag_alignment(vio, gps_records, mags, platform)
            d = (math.degrees(gm['theta_rad'] - theta) + 180) % 360 - 180
            info['mag_heading_error_deg'] = d
            print(f'Magnetometer heading check: ground-mag minus GNSS/INS = {d:+.2f} deg')
        except ValueError as e:
            print(f'Magnetometer heading check skipped: {e}')
    return traj, out_poses, info


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
    align_method: str = 'first-fix',
    align_origin_s: float = 5.0,
    gt_bag: str | None = None,
    rte_window_ns: int = 1_000_000_000,
    gt_height: str | None = None,
) -> tuple:
    """Load ground-truth + VIO data and compute raw + aligned trajectories.

    align_method : 'first-fix' (default; needs the vehicle heading, or the platform constant) or
                   'dynamic' (bag_tool.dynamic_align: the yaw between the two trajectories is recovered from
                   the paths themselves, no heading needed; also finds and removes the GT-vs-VIO clock offset).
                   Falls back to first-fix, loudly, if the flight is too short / straight to determine it.

    Returns (out_poses, out_aligned, posimus, typestore, reader_path,
             input_start_ns, input_end_ns, diag_tracking, platform).
    out_poses     : list of (gt_ts, stamp_ns, enu_pos, rot)    — raw ENU
    gt_traj       : AlignedTrajectory — the whole ground truth as VIO-body poses in the VIO world (every method
                    returns a full trajectory; has_attitude says whether the attitude is real)
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
    mags          = []  # (stamp_ns, Bx, By, Bz) tesla (ezio, yaw_source mag_ground)
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
                if platform.yaw_source == 'mag_ground':
                    f_ = msg.magnetic_field
                    mags.append((msg.header.stamp.sec * 10**9 + msg.header.stamp.nanosec, f_.x, f_.y, f_.z))
                else:
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

    if platform.yaw_source == 'none':
        fixes = _fixes_for_position_only(fixes, platform, typestore, gt_bag, reader_path)
    fixes.sort(key=lambda x: x[0])
    yaws.sort(key=lambda x: x[0])
    gt_poses.sort(key=lambda x: x[0])
    gt_points.sort(key=lambda x: x[0])
    posimus.sort(key=lambda x: x[0])

    global LAST_ALIGN_INFO
    if align_method == 'gnss-ins':
        if gt_bag is None:
            raise ValueError(f"--align-method gnss-ins needs --gt-bag <original recording> (IMU {platform.gt_imu_topic} + "
                             f"PVT {platform.gt_pvt_topic}); use --align-method ground-mag for the magnetometer alignment")
        if not posimus:
            raise ValueError('gnss-ins: no VIO poses')
        vf = _decode_posimus(posimus)
        recs = _navsatfix_records_noyaw(fixes) if fixes else []
        traj, out_poses, info = gnss_ins_alignment(vf, gt_bag, platform, np.array(mags) if mags else None, recs, rte_window_ns,
                                                   height=gt_height)
        LAST_ALIGN_INFO = info
        print(f'Emitted {len(out_poses)} ground-truth poses, {len(traj)} aligned poses')
        return (out_poses, traj, posimus, typestore, reader_path,
                input_start_ns, input_end_ns, diag_tracking, platform)

    # Dispatch by which list is populated (driven by the actual bag's msgtype, not the
    # platform's default). A bag should contain only one of these — if both poses and
    # points exist, prefer poses (more information).
    if fixes and platform.yaw_source == 'none':
        print(f'Loaded {len(fixes)} fixes (position-only, no heading), {len(posimus)} VIO poses')
        gps_records = _navsatfix_records_noyaw(fixes)
    elif fixes and platform.yaw_source == 'mag_ground':
        print(f'Loaded {len(fixes)} fixes, {len(mags)} magnetometer samples, {len(posimus)} VIO poses')
        gps_records = _navsatfix_records_noyaw(fixes)
    elif fixes:
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

    # Dynamic alignment (no heading): rotation + clock offset + origin from the two trajectories.
    LAST_ALIGN_INFO = {'align_method': 'first-fix'}
    dyn = None
    if align_method == 'ground-mag':
        if not (gps_records and posimus and mags):
            raise ValueError('--align-method ground-mag needs GNSS fixes, VIO poses and magnetometer samples '
                             f'(have {len(gps_records)}, {len(posimus)}, {len(mags)}); pass the magnetometer through to the result bag')
        dyn = ground_mag_alignment(_decode_posimus(posimus), gps_records, np.array(mags), platform)
        LAST_ALIGN_INFO = {'align_method': 'ground-mag', 'align_yaw_world_to_enu_deg': math.degrees(dyn['theta_rad']),
                           'align_yaw_spread_deg': dyn['yaw_spread_deg'], 'align_ground_window_s': dyn['window_s'],
                           'align_gnss_ground_std_m': dyn['gnss_std_m'], 'align_n_mag': dyn['n_mag'], 'align_n_fix': dyn['n_fix']}
    if align_method == 'gps-early':
        if not (gps_records and posimus):
            print('WARNING: --align-method gps-early needs ground truth and VIO poses -> using first-fix')
        else:
            from bag_tool.gps_early_align import estimate_alignment as _early
            vf = _decode_posimus(posimus)
            cov = [m.position_covariance[0] for _, m in fixes if m.position_covariance_type >= 2 and m.position_covariance[0] > 0]
            sig_h = float(np.sqrt(np.median(cov))) if cov else None
            dyn = _early(np.array([sn for _, sn, _, _ in vf]) * 1e-9, np.array([p for _, _, p, _ in vf]),
                         np.array([sn for _, sn, _, _ in gps_records]) * 1e-9, np.array([p for _, _, p, _ in gps_records]), sigma_h=sig_h)
            if dyn['ok']:
                print(f"GPS early-prefix alignment: yaw (VIO world -> ENU) {math.degrees(dyn['theta_rad']):+.2f}° (whole-flight fit "
                      f"{dyn['yaw_global_deg']:+.2f}°), fitted on the first {dyn['fit_end_s']:.0f} s of {dyn['flight_s']:.0f} s "
                      f"(fit rms {dyn['fit_rms_m']:.2f} m); gate at {dyn['gate_t_s']:.0f} s, sigma_h {dyn['sigma_h']:.2f} m; stop: {dyn['stop']}")
                LAST_ALIGN_INFO = {'align_method': 'gps-early', 'align_yaw_world_to_enu_deg': math.degrees(dyn['theta_rad']),
                                   'align_yaw_global_deg': dyn['yaw_global_deg'], 'align_fit_end_s': dyn['fit_end_s'],
                                   'align_flight_s': dyn['flight_s'], 'align_fit_frac': dyn['fit_frac'], 'align_fit_rms_m': dyn['fit_rms_m'],
                                   'align_gate_t_s': dyn['gate_t_s'], 'align_sigma_h_m': dyn['sigma_h'], 'align_stop': dyn['stop']}
            else:
                reason = dyn['reason']
                print(f"WARNING: gps-early alignment not possible ({reason}) -> falling back to first-fix"); dyn = None
                LAST_ALIGN_INFO = {'align_method': 'first-fix (gps-early failed)', 'align_failure': reason}
    if align_method == 'dynamic':
        if not (gps_records and posimus):
            print('WARNING: --align-method dynamic needs ground truth and VIO poses -> using first-fix')
        else:
            from bag_tool.dynamic_align import estimate_alignment
            vf = _decode_posimus(posimus)
            dyn = estimate_alignment(np.array([sn for _, sn, _, _ in vf]) * 1e-9, np.array([p for _, _, p, _ in vf]),
                                     np.array([sn for _, sn, _, _ in gps_records]) * 1e-9, np.array([p for _, _, p, _ in gps_records]),
                                     origin_s=align_origin_s)
            if dyn['ok']:
                lag_ns = int(round(dyn['lag_s'] * 1e9))
                if lag_ns:
                    gps_records = [(ts, sn + lag_ns, p, r) for ts, sn, p, r in gps_records]
                sd = f"{dyn['sd_deg']:.2f}" if dyn['sd_deg'] is not None else 'n/a'
                print(f"Dynamic alignment: yaw (VIO world -> ENU) {math.degrees(dyn['theta_rad']):+.2f}° ± {sd}° (bootstrap), "
                      f"{dyn['windows']} windows, coherence {dyn['coherence']:.2f}; GT stamps shifted by {dyn['lag_s']:+.2f} s; start position from the {dyn['origin']}")
                LAST_ALIGN_INFO = {'align_method': 'dynamic', 'align_yaw_world_to_enu_deg': math.degrees(dyn['theta_rad']),
                                   'align_yaw_sd_deg': dyn['sd_deg'], 'align_gt_lag_s': dyn['lag_s'], 'align_windows': dyn['windows'],
                                   'align_coherence': dyn['coherence'], 'align_origin': dyn['origin'], 'align_origin_n': dyn['origin_n']}
            else:
                reason = dyn['reason']
                print(f"WARNING: dynamic alignment not possible ({reason}) -> falling back to first-fix"); dyn = None
                LAST_ALIGN_INFO = {'align_method': 'first-fix (dynamic failed)', 'align_failure': reason}

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
        if rtk_init_rot is None and dyn is not None:
            rtk_init_rot = rot
            align_rot     = Rotation.from_euler('z', -dyn['theta_rad'])
            align_rot_pos = align_rot
            align_trans   = dyn['trans']
        elif rtk_init_rot is None:
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
    gt_traj = _trajectory_from_records(out_aligned, out_poses, has_attitude=bool(gt_poses) and not fixes,
                                       source='pose' if gt_poses else ('point' if gt_points else 'gnss-fix'),
                                       align_rot_pos=align_rot_pos, posimus=posimus, rte_window_ns=rte_window_ns)
    return (out_poses, gt_traj, posimus, typestore, reader_path,
            input_start_ns, input_end_ns, diag_tracking, platform)


# ---------------------------------------------------------------------------
# Phase 3: register computed connections on an open Writer and write messages
# ---------------------------------------------------------------------------
def write_alignment_topics(
    writer,
    typestore,
    out_poses: list,
    gt_traj,
    posimus: list,
    quick: bool,
    ts_offset: int = 0,
    rte_window_ns: int = 1_000_000_000,
    eval_mode: bool = False,
    diag_tracking: list | None = None,
    shrink: bool = False,
    platform: PlatformConfig | None = None,
) -> None:
    """Register computed alignment connections and write all their messages.

    gt_traj: AlignedTrajectory from compute_alignment (the alignment's full GT trajectory in the VIO world). The scorer
    compares it with the VIO trajectory: ATE, RTE-D (the |ATE| rate, formerly "RTE") and RTE (frame-invariant
    relative-pose drift over rte_window; needs gt_traj.has_attitude, NaN otherwise). See bag_tool.trajectory."""
    from bag_tool.trajectory import rte_relative_pose
    out_aligned = gt_traj.records()

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
        conn_rte_d         = _computed_conn('/ov_srvins/rte_d',               'std_msgs/msg/Float64')
        conn_rms_rte_d     = _computed_conn('/ov_srvins/eval_rms_rte_d',      'std_msgs/msg/Float64')
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
            gt_traj       = gt_traj.until(landing_stamp_ns)
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
            ate_records: list[tuple] = []
            for vio_ts, vio_stamp_ns, vio_pos, _ in vio_frames:
                err = vio_pos - al_positions[_nearest_al_eval(vio_stamp_ns)]
                ate_records.append((vio_ts, vio_stamp_ns, float(np.linalg.norm(err)), err))

            # VIO-failure extension: if VIO stopped before landing, freeze last position.
            pose_recs = [(pos, rot) for _, _, pos, rot in vio_frames]
            n_frozen = 0
            if vio_frames and landing_stamp_ns is not None:
                last_vio_stamp_ns = vio_frames[-1][1]
                if last_vio_stamp_ns < landing_stamp_ns:
                    frozen_pos = vio_frames[-1][2]
                    frozen_rot = vio_frames[-1][3]
                    for fix_ts, stamp_ns, _, _ in out_aligned:
                        if stamp_ns <= last_vio_stamp_ns:
                            continue
                        err = frozen_pos - al_positions[_nearest_al_eval(stamp_ns)]
                        ate_records.append((fix_ts, stamp_ns, float(np.linalg.norm(err)), err))
                        pose_recs.append((frozen_pos, frozen_rot))
                        writer.write(conn_vio_pose, fix_ts + ts_offset, typestore.serialize_cdr(
                            make_pose_msg(stamp_ns, FRAME_ID, frozen_pos, frozen_rot), POSE_TYPE))
                        n_frozen += 1
                    if n_frozen:
                        print(f'VIO failure detected — {n_frozen} frozen frames appended to landing')

            # RTE-D: the |ATE| rate (the pre-2026-11-16 "RTE"), alignment-dependent. RTE: frame-invariant drift.
            rte_d_values = rte_values_from_ate(ate_records, rte_window_ns)
            rec_stamps = np.array([rec[1] for rec in ate_records], np.int64)
            rte_values = [float(x) for x in rte_relative_pose(
                rec_stamps, np.array([p for p, _ in pose_recs], float), Rotation.concatenate([r for _, r in pose_recs]),
                gt_traj, rte_window_ns)]
            for rec, rte, rte_d in zip(ate_records, rte_values, rte_d_values):
                vio_ts = rec[0]
                writer.write(conn_rte, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte))
                writer.write(conn_rte_d, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte_d))
            rms_rte, _ = rms_jump_penalty(rte_values)
            rms_rte_d, jump_penalty = rms_jump_penalty(rte_d_values)     # jump penalty: on RTE-D, as before
            n_rte = int(np.isfinite(rte_values).sum())
            first_ts = ate_records[0][0]
            writer.write(conn_rms_rte,      first_ts + ts_offset, _ENCAP + struct.pack('<d', rms_rte))
            writer.write(conn_rms_rte_d,    first_ts + ts_offset, _ENCAP + struct.pack('<d', rms_rte_d))
            writer.write(conn_jump_penalty, first_ts + ts_offset, _ENCAP + struct.pack('<d', jump_penalty))
            if gt_traj.has_attitude:
                print(f'RMS RTE      : {rms_rte:.4f} m  (1-s relative-pose drift, {n_rte}/{len(ate_records)} frames, GT {gt_traj.source})')
            else:
                print(f'RMS RTE      : n/a  (ground truth {gt_traj.source} has no attitude; use an alignment with a full-pose GT)')
            print(f'RMS RTE-D    : {rms_rte_d:.4f} m  (|ATE| rate, alignment-dependent)')
            # whole-flight ATE (first-fix aligned, the same per-fix errors as the /ate topic)
            _ate = np.array([rec[2] for rec in ate_records])
            rms_ate, max_ate = float(np.sqrt(np.mean(_ate ** 2))), float(_ate.max())
            print(f'RMS ATE      : {rms_ate:.4f} m  (max {max_ate:.3f} m, {LAST_ALIGN_INFO["align_method"]} aligned)')
            print(f'Jump penalty : {jump_penalty:.4f} m  (threshold={JUMP_THRESHOLD}m)')

            # Phase-specific metrics: ATE/RTE per flight phase. Vertical
            # (ascent/descent) segments are detected from GT; CRUISE is their
            # complement over the record range, so the partition covers the
            # whole (touchdown-truncated) flight and divergence is localized
            # to exactly one phase. Phase ATE is SEGMENT-ANCHORED: each
            # segment is re-aligned at its first frame (the error vector at
            # segment start is subtracted), so a phase measures only drift
            # accrued WITHIN it — error inherited from earlier flight cancels.
            # RTE is the same per-frame series as the global metric, subset.
            seg_fixes = [(sn, np.asarray(p, dtype=float)) for _, sn, p, _ in out_aligned]
            segments = detect_vertical_segments(seg_fixes)
            phase_metrics: dict = {}
            vert_iv = sorted((a, b) for _, a, b in segments)
            cruise_iv = []
            if ate_records:
                lo, hi = ate_records[0][1], ate_records[-1][1]
                cur = lo
                for a, b in vert_iv:
                    if a > cur:
                        cruise_iv.append((cur, a - 1))
                    cur = max(cur, b + 1)
                if cur < hi:
                    cruise_iv.append((cur, hi))
            # INIT window: from the first ascent segment's start until GT
            # altitude first exceeds INIT_TOP_M above the launch height.
            init_iv = []
            asc = sorted((a, b) for d_, a, b in segments if d_ == 'ascent')
            if asc:
                a0, b0 = asc[0]
                z_by_stamp = {sn: float(p[2]) for _, sn, p, _ in out_aligned}
                base_z = None
                top_stamp = None
                for sn in sorted(z_by_stamp):
                    if sn < a0:
                        continue
                    if base_z is None:
                        base_z = z_by_stamp[sn]
                    if z_by_stamp[sn] - base_z >= INIT_TOP_M:
                        top_stamp = sn
                        break
                if top_stamp is not None:
                    init_iv = [(a0, top_stamp)]
                elif b0 > a0:
                    init_iv = [(a0, b0)]   # never reached INIT_TOP_M: whole ascent

            phases = [('init', init_iv),
                      ('ascent', [(a, b) for d_, a, b in segments if d_ == 'ascent']),
                      ('descent', [(a, b) for d_, a, b in segments if d_ == 'descent']),
                      ('cruise', cruise_iv)]
            for name, ivs in phases:
                vals_ate = []; vals_rte = []; vals_rte_d = []
                for a, b in ivs:
                    idx = [k for k, rec in enumerate(ate_records) if a <= rec[1] <= b]
                    if not idx:
                        continue
                    ref = ate_records[idx[0]][3]        # error vector at segment start
                    vals_ate += [float(np.linalg.norm(ate_records[k][3] - ref)) for k in idx]
                    vals_rte += [rte_values[k] for k in idx if np.isfinite(rte_values[k])]
                    vals_rte_d += [rte_d_values[k] for k in idx]
                if not vals_ate:
                    continue
                p_ate = float(np.sqrt(np.mean(np.square(vals_ate))))
                p_rte = float(np.sqrt(np.mean(np.square(vals_rte)))) if vals_rte else float('nan')
                p_rte_d = float(np.sqrt(np.mean(np.square(vals_rte_d))))
                phase_metrics[f'{name}_ate'] = p_ate
                phase_metrics[f'{name}_rte'] = p_rte if math.isfinite(p_rte) else None
                phase_metrics[f'{name}_rte_d'] = p_rte_d
                phase_metrics[f'{name}_segments'] = len(ivs)
                dur = sum((b - a) for a, b in ivs) / 1e9
                print(f'{name.capitalize():7s} ({len(ivs)} seg, {dur:.0f}s): '
                      f'ATE {p_ate:.3f} m  RTE {p_rte:.4f} m  RTE-D {p_rte_d:.4f} m   (segment-anchored)')
                first_ts = ate_records[0][0]
                for key, val in ((f'/ov_srvins/eval_{name}_ate', p_ate),
                                 (f'/ov_srvins/eval_{name}_rte', p_rte),
                                 (f'/ov_srvins/eval_{name}_rte_d', p_rte_d)):
                    conn = _computed_conn(key, 'std_msgs/msg/Float64')
                    writer.write(conn, first_ts + ts_offset, _ENCAP + struct.pack('<d', val))
            if not segments:
                print('No ascent/descent segments detected — cruise = whole flight')
            if diag_tracking:
                avg_slam_feats = float(np.mean([c for _, c in diag_tracking]))
                writer.write(conn_avg_slam, first_ts + ts_offset, _ENCAP + struct.pack('<d', avg_slam_feats))
                print(f'Avg SLAM feats: {avg_slam_feats:.1f} features/frame')
            else:
                avg_slam_feats = None
                print('WARNING: diag/tracking not in bag — avg_slam_feats skipped')
            metrics: dict = {'rms_rte': rms_rte if math.isfinite(rms_rte) else None, 'rms_rte_d': rms_rte_d,
                             'rms_ate': rms_ate, 'max_ate': max_ate, 'jump_penalty': jump_penalty,
                             'rte_frames': n_rte, 'gt_source': gt_traj.source, 'gt_has_attitude': gt_traj.has_attitude,
                             **phase_metrics, **LAST_ALIGN_INFO}
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
    conn_rte_d    = None if quick    else _computed_conn('/ov_srvins/rte_d', 'std_msgs/msg/Float64')

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
        # a cumulative Path per sample grows quadratically: high-rate GT (gnss-ins, 50 Hz) is pathed at <= ~5 Hz
        path_every = 1
        if len(out_aligned) > 2:
            hz = 1e9 / float(np.median(np.diff([sn for _, sn, _, _ in out_aligned])))
            path_every = max(1, int(round(hz / 5.0)))
        n_path = 0
        for k, (fix_ts, stamp_ns, pos, rot) in enumerate(out_aligned, 1):
            writer.write(conn_pose_al, fix_ts + ts_offset, typestore.serialize_cdr(
                make_pose_msg(stamp_ns, FRAME_ID, pos, rot), POSE_TYPE))
            if conn_path_al is not None and (k - 1) % path_every == 0:
                al_buf.extend(_pose_cdr_bytes(stamp_ns, pos, rot.as_quat())); n_path += 1
                writer.write(conn_path_al, fix_ts + ts_offset,
                             _path_header_cdr(stamp_ns, n_path) + bytes(al_buf))

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

        # Second pass: RTE-D = ate(t) - ate(t - rte_window_ns); RTE = frame-invariant relative-pose drift (NaN without GT attitude)
        from bag_tool.trajectory import rte_relative_pose
        rte = rte_relative_pose(np.array([sn for _, sn, _, _ in vio_frames], np.int64), np.array([p for _, _, p, _ in vio_frames]),
                                Rotation.concatenate([r for _, _, _, r in vio_frames]), gt_traj, rte_window_ns)
        for (vio_ts, _, _), rte_d, r in zip(ate_records, rte_values_from_ate(ate_records, rte_window_ns), rte):
            writer.write(conn_rte_d, vio_ts + ts_offset, _ENCAP + struct.pack('<d', rte_d))
            writer.write(conn_rte, vio_ts + ts_offset, _ENCAP + struct.pack('<d', float(r)))


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

    (out_poses, gt_traj, posimus, typestore, reader_path,
     _, _, diag_tracking, platform) = compute_alignment(
        input_bag, vio_topic, stores_enum, aruco_yaw_rad=aruco_yaw_rad,
    )

    out_dir = Path(output_bag)
    if out_dir.exists():
        print(f'ERROR: output path {out_dir} already exists — remove it first', file=sys.stderr)
        sys.exit(1)

    with Writer(out_dir, version=9, storage_plugin=StoragePlugin.MCAP) as writer:
        write_alignment_topics(writer, typestore, out_poses, gt_traj, posimus, quick,
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
