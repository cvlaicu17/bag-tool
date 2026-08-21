"""sim tracker-GT benchmark: generate GT correspondences for a tracker run and
evaluate the tracker against them, all inside one self-contained result bag.

    bag-tool sim generate-tracker-gt <src_bag> <out_bag> --tracker-dump <dump>
    bag-tool sim evaluate-tracker <out_bag> [--json out.json]

The tracker dump is the VINS_TRACK_DUMP binary written by vulture's
FeatureTracker (see vulture benchmark/apply_track_dump.py): per frame pair,
every point that entered tracking with its previous/current pixel, feature id,
track age and fate (0 kept / 1 lk_fail / 2 fb_reject / 3 border / 4 ransac).

generate-tracker-gt writes an output bag containing:
  /tracker_eval/points  PointCloud2 (u,v = tracked cur px; pu,pv = prev px;
                        id, age, fate) -- the tracker's output
  /tracker_eval/gt      PointCloud2 (u,v = TRUE cur px from depth + GT pose;
                        id, validity) -- ground truth, 1:1 with /points
  /pf_geo_loc/...       GT pose passthrough (raw), so evaluate-tracker can
                        classify flight phase / yaw rate without the source bag
Both clouds share stamps and per-message point order, and render directly in
Foxglove (pixel space, z=0) for visual overlay of tracked-vs-true.

GT construction (conventions validated on pas_d01_vib3, 2026-08-21, photometric
warp x5.4 over identity; see vulture benchmark/README.md):
  P = ray(u) * depth_{n-1}(u);  X = T_rel * P;  u' = project(X)
Depth semantics auto-detected by encoding: 32FC1 = planar-Z metres (vib3+),
16UC1 = euclidean ray range in centimetres (vib2 generation, per graft.py).
Validity: 0 VALID / 1 NO_DEPTH / 2 OUT_OF_FRAME / 3 OCCLUDED / 4 BEHIND.
Occlusion = depth at the GT-projected pixel more than OCCL_TOL_M nearer than
the transformed point (static scene).

evaluate-tracker reproduces the vulture benchmark metric table (endpoint error
percentiles overall / per phase / per yaw bucket, drift vs track age, gate
quality vs truth) and writes a score JSON comparable across tracker candidates.
"""
from __future__ import annotations

import json
import struct
import sys
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader, Writer
from rosbags.rosbag2.writer import StoragePlugin
from rosbags.typesys import get_typestore
from scipy.spatial.transform import Rotation, Slerp

from bag_tool.graft import R_ITOC, INTRINSICS, GT_T, DEPTH_T, _mcap
from bag_tool.ros2_detect import detect_stores_enum

POINTS_T = '/tracker_eval/points'
GTOUT_T = '/tracker_eval/gt'
FATES = ['kept', 'lk_fail', 'fb_reject', 'border', 'ransac_reject']
VNAMES = ['VALID', 'NO_DEPTH', 'OUT_OF_FRAME', 'OCCLUDED', 'BEHIND']
OCCL_TOL_M = 3.0
BORDER_PX = 1.0
W, H = 1280, 720


# ---------------------------------------------------------------- dump reader
def read_dump(path):
    recs = []
    with open(path, 'rb') as f:
        while True:
            head = f.read(20)
            if len(head) < 20:
                break
            cur_t, prev_t, n = struct.unpack('<ddi', head)
            b = np.frombuffer(f.read(25 * n), dtype=np.uint8).reshape(n, 25)
            recs.append((cur_t, prev_t,
                         b[:, 0:4].copy().view(np.int32).ravel(),
                         b[:, 4:8].copy().view(np.int32).ravel(),
                         b[:, 8:24].copy().view(np.float32).reshape(n, 4),
                         b[:, 24].copy()))
    return recs


# ---------------------------------------------------------------- GT machinery
class Poses:
    def __init__(self, bag):
        t, p, q = [], [], []
        ts = get_typestore(detect_stores_enum())
        with Reader(Path(_mcap(bag)).parent if not Path(bag).is_dir() else Path(bag)) as r:
            conns = [c for c in r.connections if c.topic == GT_T]
            for c, _, raw in r.messages(connections=conns):
                m = ts.deserialize_cdr(raw, c.msgtype)
                t.append(m.header.stamp.sec + m.header.stamp.nanosec * 1e-9)
                p.append([m.pose.position.x, m.pose.position.y, m.pose.position.z])
                q.append([m.pose.orientation.x, m.pose.orientation.y,
                          m.pose.orientation.z, m.pose.orientation.w])
        o = np.argsort(t)
        self.t = np.array(t)[o]
        self.p = np.array(p)[o]
        self.rot = Rotation.from_quat(np.array(q)[o])
        self.slerp = Slerp(self.t, self.rot)

    def T_w_b(self, tt):
        if tt < self.t[0] or tt > self.t[-1]:
            return None
        i = int(np.clip(np.searchsorted(self.t, tt), 1, len(self.t) - 1))
        a = (tt - self.t[i - 1]) / max(self.t[i] - self.t[i - 1], 1e-12)
        p = (1 - a) * self.p[i - 1] + a * self.p[i]
        return self.slerp([tt]).as_matrix()[0], p


def _decode_depth(msg):
    if msg.encoding == '32FC1':      # vib3+: planar-Z metres
        d = np.frombuffer(msg.data, dtype=np.float32).reshape(msg.height, msg.width)
        return d.copy(), 'planar'
    if msg.encoding == '16UC1':      # vib2 generation: ray range, centimetres
        d = np.frombuffer(msg.data, dtype=np.uint16).reshape(msg.height, msg.width)
        d = d.astype(np.float32) * 1e-2
        d[d <= 0] = np.nan
        return d, 'ray'
    raise SystemExit(f'unexpected depth encoding {msg.encoding}')


def _bilinear(img, x, y):
    x0 = np.clip(np.floor(x).astype(int), 0, img.shape[1] - 2)
    y0 = np.clip(np.floor(y).astype(int), 0, img.shape[0] - 2)
    fx, fy = x - x0, y - y0
    return (img[y0, x0] * (1 - fx) * (1 - fy) + img[y0, x0 + 1] * fx * (1 - fy)
            + img[y0 + 1, x0] * (1 - fx) * fy + img[y0 + 1, x0 + 1] * fx * fy)


def _gt_points(px, py, dep_prev, dep_cur, Rr, tr, fx, fy, cx, cy, semantics):
    """Per-point GT endpoints + validity for prev pixels (px,py)."""
    d = _bilinear(dep_prev, px, py)
    okd = np.isfinite(d) & (d > 0.5)
    mx, my = (px - cx) / fx, (py - cy) / fy
    if semantics == 'planar':
        P = np.stack([mx * d, my * d, d], axis=-1)
    else:
        rn = np.sqrt(mx * mx + my * my + 1.0)
        z = d / rn
        P = np.stack([mx * z, my * z, z], axis=-1)
    X = P @ Rr.T + tr
    z1 = X[:, 2]
    behind = z1 <= 0.05
    gu = X[:, 0] / np.maximum(z1, 1e-9) * fx + cx
    gv = X[:, 1] / np.maximum(z1, 1e-9) * fy + cy
    inb = (gu >= BORDER_PX) & (gu < W - BORDER_PX) & (gv >= BORDER_PX) & (gv < H - BORDER_PX)
    validity = np.zeros(len(px), dtype=np.uint8)
    validity[~okd] = 1
    validity[okd & behind] = 4
    validity[okd & ~behind & ~inb] = 2
    if dep_cur is not None:
        m = validity == 0
        if m.any():
            ds = _bilinear(dep_cur, np.clip(gu[m], 0, W - 1), np.clip(gv[m], 0, H - 1))
            if semantics == 'ray':
                mn = np.sqrt(((gu[m] - cx) / fx) ** 2 + ((gv[m] - cy) / fy) ** 2 + 1.0)
                ds = ds / mn
            occ = np.isfinite(ds) & (z1[m] - ds > OCCL_TOL_M)
            vi = np.where(m)[0][occ]
            validity[vi] = 3
    return gu, gv, validity


# ---------------------------------------------------------------- generate
def generate(src_bag, out_bag, dump_path, platform='alexios'):
    fx, fy, cx, cy = INTRINSICS[platform]
    R_bc = R_ITOC[platform].T            # cam->body
    recs = read_dump(dump_path)
    by_prev = {}
    for r in recs:
        by_prev.setdefault(round(r[1], 4), []).append(r)
    print(f'[sim-gt] {len(recs)} tracker records from {dump_path}')

    poses = Poses(src_bag)
    print(f'[sim-gt] {len(poses.t)} GT poses')

    ts = get_typestore(detect_stores_enum())
    T = ts.types
    PF = T['sensor_msgs/msg/PointField']

    def cloud(stamp, fields, step, buf, n):
        return T['sensor_msgs/msg/PointCloud2'](
            header=T['std_msgs/msg/Header'](
                stamp=T['builtin_interfaces/msg/Time'](
                    sec=int(stamp), nanosec=int(round((stamp % 1.0) * 1e9)) % 10**9),
                frame_id='cam0'),
            height=1, width=n, fields=fields, is_bigendian=False,
            point_step=step, row_step=step * n,
            data=np.frombuffer(bytes(buf), dtype=np.uint8), is_dense=True)

    pts_fields = [PF(name='u', offset=0, datatype=7, count=1),
                  PF(name='v', offset=4, datatype=7, count=1),
                  PF(name='pu', offset=8, datatype=7, count=1),
                  PF(name='pv', offset=12, datatype=7, count=1),
                  PF(name='id', offset=16, datatype=6, count=1),
                  PF(name='age', offset=20, datatype=6, count=1),
                  PF(name='fate', offset=24, datatype=2, count=1)]
    gt_fields = [PF(name='u', offset=0, datatype=7, count=1),
                 PF(name='v', offset=4, datatype=7, count=1),
                 PF(name='id', offset=8, datatype=6, count=1),
                 PF(name='validity', offset=12, datatype=2, count=1)]

    out_path = Path(out_bag)
    if out_path.exists():
        raise SystemExit(f'output exists: {out_bag}')
    src_rpath = Path(src_bag) if Path(src_bag).is_dir() else Path(_mcap(src_bag))

    n_pairs = 0
    vhist = np.zeros(5, dtype=np.int64)
    with Reader(src_rpath if src_rpath.is_dir() else src_rpath.parent) as r, \
         Writer(out_path, version=9, storage_plugin=StoragePlugin.MCAP) as w:
        md, rihs = ts.generate_msgdef('sensor_msgs/msg/PointCloud2')
        c_pts = w.add_connection(POINTS_T, 'sensor_msgs/msg/PointCloud2',
                                 msgdef=md, rihs01=rihs,
                                 serialization_format='cdr', offered_qos_profiles='')
        c_gt = w.add_connection(GTOUT_T, 'sensor_msgs/msg/PointCloud2',
                                msgdef=md, rihs01=rihs,
                                serialization_format='cdr', offered_qos_profiles='')
        gt_conn_src = [c for c in r.connections if c.topic == GT_T]
        c_pose = None
        if gt_conn_src:
            sc = gt_conn_src[0]
            mdp, rihsp = ts.generate_msgdef(sc.msgtype)
            c_pose = w.add_connection(GT_T, sc.msgtype, msgdef=mdp, rihs01=rihsp,
                                      serialization_format='cdr', offered_qos_profiles='')

        conns = [c for c in r.connections if c.topic in (DEPTH_T, GT_T)]
        prev_depth = None    # (stamp, dep, semantics)
        pending = []         # records waiting for the NEXT depth (occlusion test)
        for c, t, raw in r.messages(connections=conns):
            if c.topic == GT_T:
                if c_pose is not None:
                    w.write(c_pose, t, raw)
                continue
            m = ts.deserialize_cdr(raw, c.msgtype)
            dts = m.header.stamp.sec + m.header.stamp.nanosec * 1e-9
            dep, semantics = _decode_depth(m)
            # flush records whose prev-depth we hold and whose cur-depth just arrived
            for (rec, dprev, sem) in pending:
                cur_t, prev_t, ids, age, xy, fate = rec
                out = _score_record(rec, dprev, dep, sem, poses, R_bc, fx, fy, cx, cy)
                if out is None:
                    continue
                gu, gv, validity = out
                n = len(ids)
                b1 = bytearray()
                b2 = bytearray()
                for i in range(n):
                    b1 += struct.pack('<ffffIIB', xy[i, 2], xy[i, 3], xy[i, 0],
                                      xy[i, 1], ids[i], age[i], fate[i])
                    b2 += struct.pack('<ffIB', gu[i], gv[i], ids[i], validity[i])
                tns = int(cur_t * 1e9)
                w.write(c_pts, tns, ts.serialize_cdr(
                    cloud(cur_t, pts_fields, 25, b1, n), 'sensor_msgs/msg/PointCloud2'))
                w.write(c_gt, tns, ts.serialize_cdr(
                    cloud(cur_t, gt_fields, 13, b2, n), 'sensor_msgs/msg/PointCloud2'))
                n_pairs += 1
                for k in range(5):
                    vhist[k] += int((validity == k).sum())
            pending = []
            key = round(dts, 4)
            for rec in by_prev.pop(key, []):
                pending.append((rec, dep, semantics))
            prev_depth = (dts, dep, semantics)
        # tail: records with no following depth -> no occlusion test
        for (rec, dprev, sem) in pending:
            out = _score_record(rec, dprev, None, sem, poses, R_bc, fx, fy, cx, cy)
            # (skipped for simplicity; a single tail pair at most)

    tot = max(vhist.sum(), 1)
    print(f'[sim-gt] wrote {n_pairs} pairs -> {out_bag}')
    print('[sim-gt] GT validity: ' + '  '.join(
        f'{VNAMES[i]} {100.0*vhist[i]/tot:.2f}%' for i in range(5)))
    if by_prev:
        print(f'[sim-gt] WARNING: {sum(len(v) for v in by_prev.values())} records '
              f'had no matching depth frame')


def _score_record(rec, dep_prev, dep_cur, semantics, poses, R_bc, fx, fy, cx, cy):
    cur_t, prev_t, ids, age, xy, fate = rec
    P0 = poses.T_w_b(prev_t)
    P1 = poses.T_w_b(cur_t)
    if P0 is None or P1 is None:
        return None
    R0, p0 = P0
    R1, p1 = P1
    Rwc0, Rwc1 = R0 @ R_bc, R1 @ R_bc
    Rr = Rwc1.T @ Rwc0
    tr = Rwc1.T @ (p0 - p1)
    return _gt_points(xy[:, 0], xy[:, 1], dep_prev, dep_cur, Rr, tr,
                      fx, fy, cx, cy, semantics)


# ---------------------------------------------------------------- evaluate
def evaluate(bag, json_out=None):
    ts = get_typestore(detect_stores_enum())
    pts_by_t, gt_by_t = {}, {}
    poses_t, poses_p, poses_q = [], [], []
    rpath = Path(bag) if Path(bag).is_dir() else Path(_mcap(bag)).parent
    with Reader(rpath) as r:
        conns = [c for c in r.connections if c.topic in (POINTS_T, GTOUT_T, GT_T)]
        for c, t, raw in r.messages(connections=conns):
            m = ts.deserialize_cdr(raw, c.msgtype)
            if c.topic == GT_T:
                poses_t.append(m.header.stamp.sec + m.header.stamp.nanosec * 1e-9)
                poses_p.append([m.pose.position.x, m.pose.position.y, m.pose.position.z])
                q = m.pose.orientation
                poses_q.append([q.x, q.y, q.z, q.w])
                continue
            stamp = m.header.stamp.sec + m.header.stamp.nanosec * 1e-9
            b = np.frombuffer(m.data, dtype=np.uint8).reshape(m.width, m.point_step)
            if c.topic == POINTS_T:
                pts_by_t[round(stamp, 4)] = (
                    b[:, 0:16].copy().view(np.float32).reshape(m.width, 4),
                    b[:, 16:20].copy().view(np.uint32).ravel(),
                    b[:, 20:24].copy().view(np.uint32).ravel(),
                    b[:, 24].copy())
            else:
                gt_by_t[round(stamp, 4)] = (
                    b[:, 0:8].copy().view(np.float32).reshape(m.width, 2),
                    b[:, 12].copy())

    o = np.argsort(poses_t)
    pt = np.array(poses_t)[o]
    pp = np.array(poses_p)[o]
    rots = Rotation.from_quat(np.array(poses_q)[o])
    vz = np.gradient(-pp[:, 2], pt)
    yaw = np.unwrap(rots.as_euler('ZYX')[:, 0])
    yawrate = np.degrees(np.abs(np.gradient(yaw, pt)))

    def klass(t):
        i = int(np.clip(np.searchsorted(pt, t), 1, len(pt) - 1))
        ph = 'climb' if vz[i] > 1.0 else ('descent' if vz[i] < -1.0 else 'cruise')
        w = yawrate[i]
        yb = 'yaw<2' if w < 2 else ('yaw2-10' if w < 10 else 'yaw>10')
        return ph, yb

    err_by, drift = {}, {}
    kept_all, fb_err = [], []
    fate_hist = np.zeros(5, dtype=np.int64)
    n_novalid = 0
    n_pairs = 0

    def agebin(a):
        for hi, name in ((2, 'age1-2'), (5, 'age3-5'), (10, 'age6-10'), (20, 'age11-20')):
            if a <= hi:
                return name
        return 'age>20'

    for k, (xy, ids, age, fate) in pts_by_t.items():
        if k not in gt_by_t:
            continue
        gxy, validity = gt_by_t[k]
        if len(validity) != len(fate):
            continue
        n_pairs += 1
        valid = validity == 0
        err = np.hypot(xy[:, 0] - gxy[:, 0], xy[:, 1] - gxy[:, 1])
        for f in range(5):
            fate_hist[f] += int((fate == f).sum())
        n_novalid += int((~valid).sum())
        kept = (fate == 0) & valid
        ph, yb = klass(k)
        if kept.any():
            e = err[kept]
            kept_all.append(e)
            err_by.setdefault(ph, []).append(e)
            err_by.setdefault(yb, []).append(e)
            for a, ee in zip(age[kept], e):
                drift.setdefault(agebin(a), []).append(ee)
        fbm = (fate == 2) & valid
        if fbm.any():
            fb_err.append(err[fbm])

    def pj(v):
        v = np.concatenate(v)
        return {'p50': round(float(np.percentile(v, 50)), 3),
                'p90': round(float(np.percentile(v, 90)), 3),
                'p99': round(float(np.percentile(v, 99)), 3),
                'max': round(float(v.max()), 2), 'n': int(len(v))}

    out = {'pairs_scored': n_pairs,
           'fate_pct': {FATES[i]: round(100.0 * fate_hist[i] / max(fate_hist.sum(), 1), 2)
                        for i in range(5)},
           'no_valid_gt_pct': round(100.0 * n_novalid / max(fate_hist.sum(), 1), 2),
           'kept_endpoint_error_px': {'overall': pj(kept_all)},
           'drift_vs_age_px': {}, 'gate_quality': {}}
    for k in ('climb', 'cruise', 'descent', 'yaw<2', 'yaw2-10', 'yaw>10'):
        if k in err_by:
            out['kept_endpoint_error_px'][k] = pj(err_by[k])
    for k in ('age1-2', 'age3-5', 'age6-10', 'age11-20', 'age>20'):
        if k in drift:
            v = np.array(drift[k])
            out['drift_vs_age_px'][k] = {'p50': round(float(np.percentile(v, 50)), 3),
                                         'p90': round(float(np.percentile(v, 90)), 3),
                                         'n': int(len(v))}
    ke = np.concatenate(kept_all)
    gq = {'false_accept_pct_gt1px': round(100.0 * float((ke > 1).mean()), 3),
          'false_accept_pct_gt3px': round(100.0 * float((ke > 3).mean()), 4),
          'false_accept_pct_gt10px': round(100.0 * float((ke > 10).mean()), 4)}
    if fb_err:
        fe = np.concatenate(fb_err)
        gq.update({'fb_rejected_n': int(len(fe)),
                   'fb_rejected_gt_err_p50_px': round(float(np.percentile(fe, 50)), 2),
                   'false_reject_pct_lt0p5px': round(100.0 * float((fe < 0.5).mean()), 1)})
    out['gate_quality'] = gq

    print(f"=== tracker evaluation: {n_pairs} pairs ===")
    print('fate: ' + '  '.join(f"{k} {v}%" for k, v in out['fate_pct'].items()))
    for k, v in out['kept_endpoint_error_px'].items():
        print(f"  {k:10s} p50 {v['p50']:6.3f}  p90 {v['p90']:6.3f}  "
              f"p99 {v['p99']:6.3f}  max {v['max']:8.2f}  (n={v['n']})")
    print('drift vs age: ' + '  '.join(
        f"{k} p50 {v['p50']}" for k, v in out['drift_vs_age_px'].items()))
    print('gate: ' + json.dumps(gq))

    if json_out is None:
        json_out = str(Path(bag)) + '.tracker_score.json'
    with open(json_out, 'w') as f:
        json.dump(out, f, indent=1)
    print(f'score JSON -> {json_out}')
    return out
