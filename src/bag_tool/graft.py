"""GRAFT: drift-free (ground-truth) feature-injection arm generation.

Takes an offline-tracked feature bag (run_offline_klt output: /features/tracks
+ IMU/GT/baro passthrough + init-window images) and writes two sibling bags
with the SAME observation set (one keep-mask applied to both), differing ONLY
in pixel values:

  <out>_realm : the tracker's own pixels        (control -- reproduces baseline)
  <out>_gt    : drift-free GT reprojections     (the experiment)

Construction (validated in the d03/d07/fleet GT experiments -- birth diff
exactly 0.00 px, cruise sub-pixel, cross-implementation triangulation residual
0.000 px):
  birth pixel + depth-image range at the birth pixel (uint16 cm, euclidean ray
  range) through the interpolated GT pose -> rigid world landmark; reproject
  through the GT pose at every later observation. Occlusion is tested against
  the depth image sampled at the GT-PROJECTED pixel.

Invariants this generation MUST keep (each cost a debugging cycle once):
  * stamps carried as EXACT integer ns end-to-end (FeatureDatabase matches
    observation times by double equality; a 1 ns slip cost 370 m descent ATE)
  * feature ids unchanged from the offline tracker (injection starts after the
    estimator's image-based init, which re-derives the same ids)
  * identical keep-mask on both arms, so the arms differ in exactly one thing
  * NEVER add iid pixel noise by default (0.5 px iid collapsed descent 100x;
    the real tracker's frame-to-frame error is ~0.1 px correlated)

The depth pass over the source bag is cached next to the output
(<out>_depthpass.npz) and reused on re-runs.

Run the arms with:  SRVINS_FEATURE_TOPIC=/features/tracks run_serial_msckf ...
"""
from __future__ import annotations

import glob
import os
import struct

import numpy as np
from scipy.spatial.transform import Rotation, Slerp

R_ITOC = {
    'alexios': np.array([[0.0, 1.0, 0.0], [-1.0, 0.0, 0.0], [0.0, 0.0, 1.0]]),
}
INTRINSICS = {
    'alexios': (1138.603, 1138.603, 640.0, 360.0),
}
GT_T = '/pf_geo_loc/fc_local_position'
FEAT_T = '/features/tracks'
DEPTH_T = '/camera/depth'
OCCL_TOL_M = 3.0


def _mcap(path):
    if os.path.isdir(path):
        return sorted(glob.glob(os.path.join(path, '*.mcap')))[0]
    return path


def _gt_rots(gt_t, gt_q):
    q = gt_q * np.sign(np.sum(gt_q * gt_q[0], axis=1))[:, None]
    return Rotation.from_quat(q)


def run(feat_bag: str, src_bag: str, out_prefix: str,
        platform: str = 'alexios', noise_px: float = 0.0) -> None:
    from mcap_ros2.reader import read_ros2_messages
    from rosbags.rosbag2 import Reader, Writer
    from rosbags.rosbag2.writer import StoragePlugin
    from rosbags.typesys import get_typestore
    from bag_tool.ros2_detect import detect_stores_enum

    R_ItoC = R_ITOC[platform]
    fx, fy, cx, cy = INTRINSICS[platform]
    okf = _mcap(feat_bag)

    # ---- 1. features + GT poses from the feature bag --------------------
    print('[graft] reading features + GT ...', flush=True)
    frames, obs_by_frame = [], []
    gt_t, gt_p, gt_q = [], [], []
    for m in read_ros2_messages(okf, topics=[FEAT_T, GT_T]):
        if m.channel.topic == GT_T:
            p, q = m.ros_msg.pose.position, m.ros_msg.pose.orientation
            gt_t.append(m.log_time_ns / 1e9)
            gt_p.append((p.x, p.y, p.z))
            gt_q.append((q.x, q.y, q.z, q.w))
            continue
        pc = m.ros_msg
        hs = pc.header.stamp
        raw = bytes(pc.data)
        frames.append(hs.sec * 10**9 + hs.nanosec)
        obs_by_frame.append([
            (int(i), float(u), float(v)) for (u, v, i) in
            (struct.unpack_from('<ffI', raw, k * pc.point_step)
             for k in range(pc.width))])
    gt_t = np.array(gt_t)
    gt_p = np.array(gt_p)
    gt_q = np.array(gt_q)
    frames_ns = np.array(frames, dtype=np.int64)
    ft = frames_ns / 1e9
    print(f'[graft] {len(frames)} feature frames, '
          f'{sum(len(o) for o in obs_by_frame)} obs, {len(gt_t)} GT poses')

    rot = Slerp(gt_t, _gt_rots(gt_t, gt_q))(np.clip(ft, gt_t[0], gt_t[-1]))
    fpos = np.column_stack([np.interp(ft, gt_t, gt_p[:, k]) for k in range(3)])
    Rc2n = rot.as_matrix() @ R_ItoC.T

    birth = {}
    for k, obs in enumerate(obs_by_frame):
        for (i, u, v) in obs:
            if i not in birth:
                birth[i] = (k, u, v)

    # ---- 2. depth pass (cached) -----------------------------------------
    cache = out_prefix + '_depthpass.npz'
    if os.path.exists(cache):
        print('[graft] loading cached depth pass ...', flush=True)
        cz = np.load(cache)
        keep = {(int(k), int(i)): bool(v) for k, i, v in
                zip(cz['keep_k'], cz['keep_i'], cz['keep_v'])}
        gt_px = {(int(k), int(i)): (float(u), float(v)) for k, i, (u, v) in
                 zip(cz['px_k'], cz['px_i'], cz['px_uv'])}
    else:
        print('[graft] streaming depth (single pass over the source bag) ...',
              flush=True)
        keep, gt_px, landmark = {}, {}, {}
        births_at = {}
        for i, (k, u, v) in birth.items():
            births_at.setdefault(k, []).append((i, u, v))
        obs_at = dict(enumerate(obs_by_frame))
        nf = 0
        for m in read_ros2_messages(_mcap(src_bag), topics=[DEPTH_T]):
            t = m.log_time_ns
            k = int(np.searchsorted(frames_ns, t))
            cand = [c for c in (k - 1, k) if 0 <= c < len(frames_ns)]
            if not cand:
                continue
            k = min(cand, key=lambda c: abs(int(frames_ns[c]) - t))
            if abs(int(frames_ns[k]) - t) > 20_000_000:
                continue
            msg = m.ros_msg
            D = np.frombuffer(bytes(msg.data), dtype=np.uint16).reshape(
                msg.height, msg.step // 2)[:, :msg.width]
            H, W = D.shape

            def dep_at(uu, vv):
                return float(D[min(max(int(round(vv)), 0), H - 1),
                               min(max(int(round(uu)), 0), W - 1)]) / 100.0

            for (i, u, v) in births_at.get(k, []):
                d_m = dep_at(u, v)
                if not (0.5 < d_m < 600.0):
                    continue
                ray = np.array([(u - cx) / fx, (v - cy) / fy, 1.0])
                ray /= np.linalg.norm(ray)
                landmark[i] = fpos[k] + Rc2n[k] @ (ray * d_m)

            for (i, u, v) in obs_at.get(k, []):
                P = landmark.get(i)
                if P is None:
                    keep[(k, i)] = False
                    continue
                pc_ = Rc2n[k].T @ (P - fpos[k])
                z = pc_[2]
                if z <= 0.5:
                    keep[(k, i)] = False
                    continue
                ug = fx * pc_[0] / z + cx
                vg = fy * pc_[1] / z + cy
                if not (2 < ug < W - 2 and 2 < vg < H - 2):
                    keep[(k, i)] = False
                    continue
                rng = float(np.linalg.norm(pc_))
                d_here = dep_at(ug, vg)
                if 0.5 < d_here < 600.0 and rng > d_here + OCCL_TOL_M:
                    keep[(k, i)] = False    # behind terrain at the GT pixel
                    continue
                gt_px[(k, i)] = (ug, vg)
                keep[(k, i)] = True
            nf += 1
            if nf % 1500 == 0:
                print(f'[graft]   {nf} depth frames', flush=True)
        np.savez_compressed(
            cache,
            keep_k=np.array([k for (k, i) in keep], np.int64),
            keep_i=np.array([i for (k, i) in keep], np.int64),
            keep_v=np.array(list(keep.values()), bool),
            px_k=np.array([k for (k, i) in gt_px], np.int64),
            px_i=np.array([i for (k, i) in gt_px], np.int64),
            px_uv=np.array(list(gt_px.values()), np.float64)
            if gt_px else np.zeros((0, 2)))

    tot = sum(len(o) for o in obs_by_frame)
    kept = sum(1 for v in keep.values() if v)
    print(f'[graft] keep {kept}/{tot} obs ({100*kept/max(tot,1):.2f}%)')

    # ---- 3. write both arms ---------------------------------------------
    ts = get_typestore(detect_stores_enum())
    T = ts.types
    PF = T['sensor_msgs/msg/PointField']
    rng_n = np.random.default_rng(12345)
    frame_idx = {int(t): k for k, t in enumerate(frames_ns)}
    for arm in ('realm', 'gt'):
        outdir = f'{out_prefix}_{arm}'
        if os.path.exists(outdir):
            import shutil
            shutil.rmtree(outdir)
        print(f'[graft] writing {outdir} ...', flush=True)
        n_f = n_o = 0
        with Reader(feat_bag) as r, Writer(outdir, version=9,
                                           storage_plugin=StoragePlugin.MCAP) as w:
            conns = {}
            for c in r.connections:
                # C++-written bags don't carry an RIHS digest on every
                # connection; regenerate msgdef+digest from the typestore
                md, rihs = ts.generate_msgdef(c.msgtype)
                conns[c.id] = w.add_connection(
                    c.topic, c.msgtype, msgdef=md, rihs01=rihs,
                    serialization_format='cdr', offered_qos_profiles='')
            for c, t, raw in r.messages():
                if c.topic != FEAT_T:
                    w.write(conns[c.id], t, raw)     # byte-identical passthrough
                    continue
                msg = ts.deserialize_cdr(raw, c.msgtype)
                hs = msg.header.stamp
                tns = hs.sec * 10**9 + hs.nanosec
                k = frame_idx.get(tns)
                if k is None:
                    continue
                lst = []
                for (i, u, v) in obs_by_frame[k]:
                    if not keep.get((k, i), False):
                        continue
                    if arm == 'gt':
                        gu, gv = gt_px[(k, i)]
                        if noise_px > 0:
                            gu += rng_n.normal(0, noise_px)
                            gv += rng_n.normal(0, noise_px)
                        lst.append((gu, gv, i))
                    else:
                        lst.append((u, v, i))
                if not lst:
                    continue
                buf = bytearray()
                for (u, v, i) in lst:
                    buf += struct.pack('<ffI', u, v, i)
                out = T['sensor_msgs/msg/PointCloud2'](
                    header=T['std_msgs/msg/Header'](
                        stamp=T['builtin_interfaces/msg/Time'](
                            sec=int(tns // 10**9), nanosec=int(tns % 10**9)),
                        frame_id='cam0'),
                    height=1, width=len(lst),
                    fields=[PF(name='u', offset=0, datatype=7, count=1),
                            PF(name='v', offset=4, datatype=7, count=1),
                            PF(name='id', offset=8, datatype=6, count=1)],
                    is_bigendian=False, point_step=12, row_step=12 * len(lst),
                    data=np.frombuffer(bytes(buf), dtype=np.uint8),
                    is_dense=True)
                w.write(conns[c.id], t, ts.serialize_cdr(out, c.msgtype))
                n_f += 1
                n_o += len(lst)
        print(f'[graft]   {outdir}: {n_f} feature frames, {n_o} obs')

    # sanity: the |GT - tracked| distribution should read as the measured
    # drift (sub-px cruise, growing with age in VDM)
    diffs = []
    for (k, i), ok in keep.items():
        if not ok:
            continue
        for (ii, u, v) in obs_by_frame[k]:
            if ii == i:
                gu, gv = gt_px[(k, i)]
                diffs.append(float(np.hypot(gu - u, gv - v)))
                break
        if len(diffs) >= 200000:
            break
    if diffs:
        d = np.array(diffs)
        print(f'[graft] |GT - tracked| px: median {np.median(d):.2f}  '
              f'p90 {np.percentile(d, 90):.2f}  p99 {np.percentile(d, 99):.2f}'
              f'  (sampled {len(d)})')
