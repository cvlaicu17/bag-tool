"""Per-feature depth ground-truthing for the estimator's landmarks.

Joins the estimator's id-keyed landmark topics against an offline ground truth
built from the sim depth camera, per feature id, per frame:

  depth_est(id, t) = camera-axis z of (P_est(id) - p_vio(t))   [VIO global frame]
  depth_gt (id, t) = camera-axis z of (P_gt(id)  - p_gt(t))    [NED frame]

Depth along the camera optical axis is invariant to the VIO's global gauge
(yaw + translation), so the two are directly comparable with NO trajectory
alignment step.

Inputs (one result bag from the serial runner + the source mission bag):
  result bag: /ov_srvins/feat3d_slam, /ov_srvins/feat3d_msckf (id,x,y,z F64,
              VIO global), /ov_srvins/poseimu (VIO pose), /ov_srvins/feat2d
              (id,u,v,cam) when the run tracked images itself, and the GT
              passthrough /pf_geo_loc/fc_local_position (NED).
  source bag: /camera/depth (uint16 cm, euclidean ray range) for the GT
              landmark construction; /features/tracks as the 2D-obs fallback
              for feature-injected runs (their tracker is frozen, so feat2d
              is not meaningful there).

GT depth per observation: the depth image is sampled DIRECTLY at the observed
pixel (same resolution and 1:1 stamps as the mono frames) and converted from
euclidean ray range to camera-axis z via z = range/sqrt(1+x^2+y^2). No landmark
construction, no pose interpolation. Note the semantics: for drifted tracks
this is the depth of the surface under the CURRENT pixel (what generates the
measurement), not of the birth landmark; for drift-free arms the two coincide.

Output: per-phase (ascent/descent/cruise) depth-error stats, split by feature
type (MSCKF / SLAM), plus an optional npz dump of the raw per-observation join.
"""
from __future__ import annotations

import glob
import os
import struct

import numpy as np
from scipy.spatial.transform import Rotation, Slerp

from bag_tool.processor import detect_vertical_segments

FEAT3D_SLAM = '/ov_srvins/feat3d_slam'
FEAT3D_MSCKF = '/ov_srvins/feat3d_msckf'
FEAT2D = '/ov_srvins/feat2d'
POSEIMU = '/ov_srvins/poseimu'
GT_T = '/pf_geo_loc/fc_local_position'
DEPTH_T = '/camera/depth'
TRACKS_T = '/features/tracks'

# Camera extrinsic (R_ItoC) per platform. alexios: verified nadir mount,
# zero lever arm (config/alexios/kalibr_imucam_chain.yaml).
R_ITOC = {
    'alexios': np.array([[0.0, 1.0, 0.0], [-1.0, 0.0, 0.0], [0.0, 0.0, 1.0]]),
}
INTRINSICS = {
    'alexios': (1138.603, 1138.603, 640.0, 360.0),
}


def _mcap(path):
    if os.path.isdir(path):
        return sorted(glob.glob(os.path.join(path, '*.mcap')))[0]
    return path


def _read_result_bag(result_bag):
    """One pass over the result bag: id-clouds, poses, GT passthrough."""
    from mcap_ros2.reader import read_ros2_messages
    clouds = {FEAT3D_SLAM: {}, FEAT3D_MSCKF: {}}   # tns -> {id: (x,y,z)}
    feat2d = {}                                     # tns -> {id: (u,v)}
    vio = []                                        # (t, p(3), q_GtoI xyzw JPL)
    gt = []                                         # (t, p_ned(3), q xyzw Ham)
    for m in read_ros2_messages(_mcap(result_bag),
                                topics=[FEAT3D_SLAM, FEAT3D_MSCKF, FEAT2D,
                                        POSEIMU, GT_T]):
        tp = m.channel.topic
        if tp in (FEAT3D_SLAM, FEAT3D_MSCKF):
            pc = m.ros_msg
            hs = pc.header.stamp
            tns = hs.sec * 10**9 + hs.nanosec
            raw = bytes(pc.data)
            d = {}
            for k in range(pc.width):
                fid, x, y, z = struct.unpack_from('<dddd', raw, k * pc.point_step)
                d[int(fid)] = (x, y, z)
            clouds[tp][tns] = d
        elif tp == FEAT2D:
            pc = m.ros_msg
            hs = pc.header.stamp
            tns = hs.sec * 10**9 + hs.nanosec
            raw = bytes(pc.data)
            d = {}
            for k in range(pc.width):
                fid, u, v, _cam = struct.unpack_from('<dddd', raw, k * pc.point_step)
                d[int(fid)] = (u, v)
            feat2d[tns] = d
        elif tp == POSEIMU:
            p = m.ros_msg.pose.pose.position
            q = m.ros_msg.pose.pose.orientation
            hs = m.ros_msg.header.stamp
            vio.append((hs.sec + hs.nanosec * 1e-9,
                        (p.x, p.y, p.z), (q.x, q.y, q.z, q.w)))
        else:
            p = m.ros_msg.pose.position
            q = m.ros_msg.pose.orientation
            hs = m.ros_msg.header.stamp
            gt.append((hs.sec + hs.nanosec * 1e-9,
                       (p.x, p.y, p.z), (q.x, q.y, q.z, q.w)))
    return clouds, feat2d, vio, gt


def _read_obs_fallback(bag):
    """2D observation stream (/features/tracks) for feature-injected runs.
    Arm bags are typically rosbags-written, whose schemas mcap_ros2's dynamic
    decoder cannot parse ('time' type) -- read with rosbags in that case."""
    out = {}

    def add(hs_sec, hs_nsec, width, point_step, raw):
        tns = hs_sec * 10**9 + hs_nsec
        d = {}
        for k in range(width):
            u, v, fid = struct.unpack_from('<ffI', raw, k * point_step)
            d[int(fid)] = (u, v)
        out[tns] = d

    try:
        from mcap_ros2.reader import read_ros2_messages
        for m in read_ros2_messages(_mcap(bag), topics=[TRACKS_T]):
            pc = m.ros_msg
            add(pc.header.stamp.sec, pc.header.stamp.nanosec, pc.width,
                pc.point_step, bytes(pc.data))
        return out
    except NotImplementedError:
        out.clear()
    from rosbags.rosbag2 import Reader
    from rosbags.typesys import get_typestore
    from bag_tool.ros2_detect import detect_stores_enum
    ts = get_typestore(detect_stores_enum())
    bagdir = bag if os.path.isdir(bag) else os.path.dirname(bag)
    with Reader(bagdir) as r:
        cs = [c for c in r.connections if c.topic == TRACKS_T]
        for c, t, raw in r.messages(connections=cs):
            pc = ts.deserialize_cdr(raw, c.msgtype)
            add(pc.header.stamp.sec, pc.header.stamp.nanosec, pc.width,
                pc.point_step, bytes(pc.data))
    return out


def _interp_pose(times_q, ts, ps, rots):
    """Interpolate positions + slerp orientations at times_q (clipped)."""
    tq = np.clip(times_q, ts[0], ts[-1])
    pos = np.column_stack([np.interp(tq, ts, ps[:, k]) for k in range(3)])
    rot = Slerp(ts, rots)(tq)
    return pos, rot


def run(result_bag: str, src_bag: str, platform: str = 'alexios',
        dump_npz: str | None = None, obs_from_src: bool | None = None,
        tracks_bag: str | None = None) -> dict:
    R_ItoC = R_ITOC[platform]
    fx, fy, cx, cy = INTRINSICS[platform]

    print('[feat-depth] reading result bag ...')
    clouds, feat2d, vio, gt = _read_result_bag(result_bag)
    n2d = sum(len(v) for v in feat2d.values())
    if obs_from_src is None:
        obs_from_src = n2d == 0 or len(feat2d) < 100
    if obs_from_src:
        tb = tracks_bag or src_bag
        print(f'[feat-depth] feat2d absent/frozen -> reading /features/tracks '
              f'from {tb} (injected run)')
        obs = _read_obs_fallback(tb)
    else:
        obs = feat2d
    print(f'[feat-depth] frames: obs={len(obs)} slam={len(clouds[FEAT3D_SLAM])} '
          f'msckf={len(clouds[FEAT3D_MSCKF])} poses={len(vio)} gt={len(gt)}')

    # ---- GT depth: sample the depth image at each observed pixel ---------
    # (same resolution / 1:1 stamps as mono; range -> camera-axis z)
    print('[feat-depth] streaming depth at observed pixels ...')
    from mcap_ros2.reader import read_ros2_messages
    stamps = np.array(sorted(obs.keys()), dtype=np.int64)
    depth_gt = {}                  # (tns, fid) -> camera-axis z (m)
    nfr = 0
    for m in read_ros2_messages(_mcap(src_bag), topics=[DEPTH_T]):
        t = m.log_time_ns
        j = int(np.searchsorted(stamps, t))
        cand = [c for c in (j - 1, j) if 0 <= c < len(stamps)]
        if not cand:
            continue
        j = min(cand, key=lambda c: abs(int(stamps[c]) - t))
        if abs(int(stamps[j]) - t) > 20_000_000:
            continue
        tns = int(stamps[j])
        msg = m.ros_msg
        D = np.frombuffer(bytes(msg.data), dtype=np.uint16).reshape(
            msg.height, msg.step // 2)[:, :msg.width]
        for fid, (u, v) in obs[tns].items():
            x = min(max(int(round(u)), 0), msg.width - 1)
            y = min(max(int(round(v)), 0), msg.height - 1)
            rng_m = float(D[y, x]) / 100.0
            if not (0.5 < rng_m < 600.0):
                continue
            xn, yn = (u - cx) / fx, (v - cy) / fy
            depth_gt[(tns, fid)] = rng_m / float(np.sqrt(1.0 + xn * xn + yn * yn))
        nfr += 1
    print(f'[feat-depth] GT depths: {len(depth_gt)} (obs pixels sampled, '
          f'{nfr} depth frames)')

    # ---- per-frame depth join ------------------------------------------
    vio_t = np.array([v[0] for v in vio])
    vio_p = np.array([v[1] for v in vio])
    # poseimu carries JPL q_GtoI as xyzw; those same numbers read as a Hamilton
    # quaternion give R_GtoI directly (the poseimu-jpl convention).
    vio_R_GtoI = Rotation.from_quat(np.array([v[2] for v in vio])).as_matrix()

    gt_t = np.array([g[0] for g in gt])
    segs = detect_vertical_segments(
        [(int(t * 1e9), np.array([p[1], p[0], -p[2]])) for t, p, _ in gt])

    def phase_of(tns):
        for d_, s, e in segs:
            if s <= tns <= e:
                return d_
        return 'cruise'

    rows = []  # (tns, fid, type, depth_est, depth_gt)
    for tp, tag in ((FEAT3D_MSCKF, 0), (FEAT3D_SLAM, 1)):
        for tns, d in clouds[tp].items():
            if not d:
                continue
            t = tns / 1e9
            i = int(np.clip(np.searchsorted(vio_t, t), 1, len(vio_t) - 1))
            i = i if abs(vio_t[i] - t) < abs(vio_t[i - 1] - t) else i - 1
            R_GtoI = vio_R_GtoI[i]
            p_I = vio_p[i]
            for fid, P in d.items():
                d_gt = depth_gt.get((tns, fid))
                if d_gt is None:
                    continue      # landmark not observed this frame / bad depth
                d_est = float((R_ItoC @ (R_GtoI @ (np.array(P) - p_I)))[2])
                rows.append((tns, fid, tag, d_est, d_gt))

    R = np.array(rows, dtype=np.float64)
    if len(R) == 0:
        print('[feat-depth] NO joinable observations -- check topics')
        return {}
    tns, tag = R[:, 0], R[:, 2]
    err = R[:, 3] - R[:, 4]
    rel = err / np.maximum(np.abs(R[:, 4]), 1e-6)
    phases = np.array([phase_of(int(x)) for x in tns], dtype=object)

    fids = R[:, 1].astype(np.int64)
    out = {}
    print(f'\n[feat-depth] {len(R)} joined (feature,frame) samples')
    print(f'{"type":<6s} {"phase":<8s} {"n":>8s} {"tracks":>7s} {"gt_dep":>7s} '
          f'{"BIAS":>8s} {"trk_scat":>9s} {"NOISE":>7s} {"rel_bias":>9s} '
          f'{"rel_scat":>9s} {"out>1m":>7s} {"out>5%":>7s}')
    for tg, nm in ((0, 'MSCKF'), (1, 'SLAM')):
        for ph in ('ascent', 'descent', 'cruise'):
            msk = (tag == tg) & (phases == ph)
            if msk.sum() < 30:
                continue
            e = err[msk]
            g = R[msk, 4]
            f_ = fids[msk]
            # Per-track decomposition: a landmark's error persists across
            # frames, so the pooled std conflates two diseases. Split into
            #   BIAS      = mean of per-track mean errors (systematic)
            #   trk_scat  = std of per-track mean errors (landmark-to-landmark)
            #   NOISE     = std of within-track residuals (frame-to-frame)
            order = np.argsort(f_, kind='stable')
            fs, es, gs = f_[order], e[order], g[order]
            first = np.ones(len(fs), bool)
            first[1:] = fs[1:] != fs[:-1]
            gidx = np.cumsum(first) - 1
            cnt = np.bincount(gidx)
            tmean = np.bincount(gidx, weights=es) / cnt
            tgt = np.bincount(gidx, weights=gs) / cnt
            resid = es - tmean[gidx]
            multi = cnt[gidx] > 1
            st = dict(
                n=int(msk.sum()), tracks=int(len(tmean)),
                gt_dep_p50=float(np.median(g)),
                bias=float(np.mean(tmean)),
                bias_med=float(np.median(tmean)),
                trk_scatter=float(np.std(tmean)),
                noise=float(np.std(resid[multi])) if multi.any() else 0.0,
                rel_bias=float(np.mean(tmean / np.maximum(np.abs(tgt), 1e-6))),
                rel_scatter=float(np.std(tmean / np.maximum(np.abs(tgt), 1e-6))),
                out_1m=float(np.mean(np.abs(e) > 1.0)),
                out_5pct=float(np.mean(np.abs(e) > 0.05 * np.abs(g))))
            out[(nm, ph)] = st
            print(f'{nm:<6s} {ph:<8s} {st["n"]:8d} {st["tracks"]:7d} '
                  f'{st["gt_dep_p50"]:7.1f} {st["bias"]:+8.2f} '
                  f'{st["trk_scatter"]:9.2f} {st["noise"]:7.2f} '
                  f'{st["rel_bias"]:+9.3f} {st["rel_scatter"]:9.3f} '
                  f'{100*st["out_1m"]:6.1f}% {100*st["out_5pct"]:6.1f}%')
    if dump_npz:
        np.savez_compressed(dump_npz, rows=R, phases=phases)
        print(f'[feat-depth] raw join -> {dump_npz}')
    return out
