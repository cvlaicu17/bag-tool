"""scale-eval: split trajectory error into SCALE error and SHAPE error.

A VIO trajectory can be wrong in two independent ways:

  * SHAPE  -- the relative geometry is inconsistent (poses/structure disagree
              with each other).  This is what long-baseline visual optimisation
              fixes, and it is what reprojection error can see.
  * SCALE  -- the shape is right but the metre-per-unit conversion is wrong.
              Only the accelerometer and the barometer carry this information,
              and only while they are excited.  Reprojection error is BLIND to
              it: scaling poses and structure together leaves every reprojection
              unchanged.

Pooling both into one ATE/RTE number hides which one is failing.  This module
reports them separately by aligning ground truth to VIO twice:

    rigid  (SE3, Umeyama without scale)  -> total error
    scaled (Sim3, Umeyama WITH scale)    -> shape error, plus the scale factor s

The gap between them is the part of the error that a single scale factor
explains.  Global numbers are reported for reference, but the informative view
is WINDOWED: scale drifts during a flight, so a per-window s(t) shows when and
where the metric channel degrades, independently of shape quality.

Usage:
    bag-tool eval <aligned_bag> --scale
    bag-tool align <bag> --eval --scale
"""

from __future__ import annotations

import bisect
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader
from rosbags.typesys import get_typestore

from bag_tool.add_topics import _reader_path
from bag_tool.processor import detect_vertical_segments

_GT_TOPIC = '/ov_srvins/gt/aligned'
_VIO_TOPIC = '/ov_srvins/vio/pose'


def umeyama(src: np.ndarray, dst: np.ndarray, with_scale: bool):
    """Least-squares similarity that maps src onto dst.

    src, dst: (N,3).  Returns (R, t, s) with  dst ~= s * R @ src + t.
    Umeyama (1991); the scale term is the ratio of the correlation trace to the
    source variance, which is exactly the quantity a Sim3 alignment absorbs.
    """
    mu_s = src.mean(axis=0)
    mu_d = dst.mean(axis=0)
    S = src - mu_s
    D = dst - mu_d
    C = (D.T @ S) / len(src)
    U, sig, Vt = np.linalg.svd(C)
    W = np.eye(3)
    if np.linalg.det(U) * np.linalg.det(Vt) < 0:      # reflection guard
        W[2, 2] = -1.0
    R = U @ W @ Vt
    if with_scale:
        var_s = (S ** 2).sum() / len(src)
        s = float((sig * np.diag(W).astype(float)).sum() / var_s) if var_s > 1e-12 else 1.0
    else:
        s = 1.0
    t = mu_d - s * (R @ mu_s)
    return R, t, s


def _apply(R, t, s, pts):
    return (s * (R @ pts.T)).T + t


def _err(src, dst, with_scale):
    """Align src->dst and return (rms error, max error, scale)."""
    R, t, s = umeyama(src, dst, with_scale)
    e = np.linalg.norm(_apply(R, t, s, src) - dst, axis=1)
    return float(np.sqrt((e ** 2).mean())), float(e.max()), s


def load_aligned(input_bag: str, stores_enum):
    """Read (stamps, gt_pos, vio_pos) from an aligned bag, GT resampled to VIO stamps."""
    typestore = get_typestore(stores_enum)
    reader_path = _reader_path(Path(input_bag))
    gt: list[tuple[int, np.ndarray]] = []
    vio: list[tuple[int, np.ndarray]] = []
    with Reader(reader_path) as reader:
        relevant = [c for c in reader.connections if c.topic in {_GT_TOPIC, _VIO_TOPIC}]
        if not relevant:
            return None
        for conn, _ts, raw in reader.messages(connections=relevant):
            msg = typestore.deserialize_cdr(raw, conn.msgtype)
            ns = msg.header.stamp.sec * 10 ** 9 + msg.header.stamp.nanosec
            p = np.array([msg.pose.pose.position.x,
                          msg.pose.pose.position.y,
                          msg.pose.pose.position.z])
            (gt if conn.topic == _GT_TOPIC else vio).append((ns, p))
    if not gt or not vio:
        return None
    gt.sort(key=lambda r: r[0])
    vio.sort(key=lambda r: r[0])
    g_ns = [r[0] for r in gt]
    g_p = np.array([r[1] for r in gt])
    v_ns = np.array([r[0] for r in vio])
    v_p = np.array([r[1] for r in vio])
    # nearest-neighbour GT for each VIO stamp (bags are 1:1 in practice)
    idx = []
    n = len(g_ns)
    for s in v_ns:
        i = bisect.bisect_left(g_ns, s)
        if i == 0:
            j = 0
        elif i >= n:
            j = n - 1
        else:
            j = i if (g_ns[i] - s) <= (s - g_ns[i - 1]) else i - 1
        idx.append(j)
    return v_ns, g_p[np.array(idx)], v_p


def run(input_bag: str, stores_enum, windows_s=(4.0, 10.0)) -> dict:
    """Report the SCALE / SHAPE decomposition for an aligned bag."""
    data = load_aligned(input_bag, stores_enum)
    if data is None:
        print(f'ERROR: aligned bag is missing {_GT_TOPIC!r} or {_VIO_TOPIC!r}')
        return {}
    v_ns, gt_p, vio_p = data
    t = (v_ns - v_ns[0]) / 1e9
    print(f'Loaded {len(v_ns)} matched poses, {t[-1]:.1f} s')

    # ---- global ----------------------------------------------------------
    rms_rigid, max_rigid, _ = _err(gt_p, vio_p, with_scale=False)
    rms_sim3, max_sim3, s_glob = _err(gt_p, vio_p, with_scale=True)
    print()
    print('GLOBAL alignment (ground truth -> VIO)')
    print(f'  rigid  SE3 : rms {rms_rigid:8.2f} m   max {max_rigid:9.2f} m   <- total error')
    print(f'  scaled Sim3: rms {rms_sim3:8.2f} m   max {max_sim3:9.2f} m   <- SHAPE error')
    print(f'  scale s    : {s_glob:8.4f}          ({100 * (s_glob - 1):+.2f} %)  <- SCALE error')
    if rms_rigid > 1e-9:
        print(f'  shape/total: {rms_sim3 / rms_rigid:8.2f}   '
              f'({"scale-dominated" if rms_sim3 < 0.5 * rms_rigid else "shape-dominated"})')

    # ---- phases ----------------------------------------------------------
    # NOTE: poses in an aligned bag are already in the VIO frame, which is z-UP,
    # and detect_vertical_segments expects z-UP. Do NOT apply a NED conversion
    # here -- doing so silently swaps ascent and descent.
    fixes = [(int(ns), np.asarray(p, dtype=float)) for ns, p in zip(v_ns, gt_p)]
    segs = detect_vertical_segments(fixes)
    phase_of = []
    for ns in v_ns:
        lab = 'cruise'
        for d, s_ns, e_ns in segs:
            if s_ns <= ns < e_ns:
                lab = d
                break
        phase_of.append(lab)
    phase_of = np.array(phase_of)

    out = {'global': {'rms_rigid': rms_rigid, 'rms_shape': rms_sim3, 'scale': s_glob}}

    # ---- per-phase, WHOLE SEGMENT as a single alignment -------------------
    # Sits between GLOBAL (one fit over the whole mission) and WINDOWED (one fit
    # per 4-10s slice): here each flight phase (e.g. the whole cruise segment,
    # commonly several minutes) gets exactly ONE rigid vs Sim3 fit. This answers
    # "how much of THIS PHASE's error does a single scale correction explain",
    # without the windowed view's within-phase averaging, and without the global
    # fit's cross-phase mixing (a cruise-only scale error can be diluted by
    # ascent/descent in the global number).
    print()
    print('PER-PHASE (entire phase as one window)')
    print(f'  {"phase":9s} {"n":>5s} | {"rigid rms":>10s} {"rigid max":>10s} | '
          f'{"shape rms":>10s} {"shape max":>10s} | {"scale s":>10s} | shape/total')
    out['per_phase'] = {}
    for ph in ('ascent', 'cruise', 'descent'):
        sel = phase_of == ph
        if sel.sum() < 8:
            continue
        g, v = gt_p[sel], vio_p[sel]
        r_rig, m_rig, _ = _err(g, v, False)
        r_shp, m_shp, s_ph = _err(g, v, True)
        ratio = r_shp / r_rig if r_rig > 1e-9 else float('nan')
        dur = t[sel].max() - t[sel].min()
        print(f'  {ph:9s} {sel.sum():5d} | {r_rig:10.3f} {m_rig:10.3f} | '
              f'{r_shp:10.3f} {m_shp:10.3f} | {s_ph:7.4f} ({100*(s_ph-1):+.2f}%) | {ratio:6.3f}'
              f'   ({dur:.0f}s)')
        out['per_phase'][ph] = {'n': int(sel.sum()), 'duration_s': float(dur),
                                 'rigid_rms': r_rig, 'rigid_max': m_rig,
                                 'shape_rms': r_shp, 'shape_max': m_shp,
                                 'scale': s_ph, 'shape_over_total': float(ratio)}

    # ---- windowed --------------------------------------------------------
    for win in windows_s:
        rows = []
        step = max(1, int(round(0.5 * win / max(np.median(np.diff(t)), 1e-6))))
        n_min = 8
        i = 0
        while i < len(t):
            j = np.searchsorted(t, t[i] + win)
            if j - i < n_min:
                break
            g = gt_p[i:j]
            v = vio_p[i:j]
            # drop degenerate windows (vehicle nearly stationary -> scale meaningless)
            span = float(np.linalg.norm(g - g.mean(axis=0), axis=1).max())
            if span > 0.5:
                r_rig, _, _ = _err(g, v, False)
                r_shp, _, s_w = _err(g, v, True)
                rows.append((t[i] + 0.5 * win, phase_of[(i + j) // 2], r_rig, r_shp, s_w, span))
            i += step
        if not rows:
            continue
        print()
        print(f'WINDOWED ({win:.0f} s windows, {len(rows)} windows)')
        print(f'  {"phase":9s} {"n":>4s} | {"total rms":>18s} | {"SHAPE rms":>18s} | '
              f'{"SCALE s":>20s} | shape/total')
        print(f'  {"":9s} {"":>4s} | {"p50":>8s} {"p90":>9s} | {"p50":>8s} {"p90":>9s} | '
              f'{"p50":>7s} {"p10-p90":>12s} |')
        key = f'win{int(win)}'
        out[key] = {}
        for ph in ('ascent', 'descent', 'cruise', 'ALL'):
            sel = [r for r in rows if ph == 'ALL' or r[1] == ph]
            if len(sel) < 3:
                continue
            rig = np.array([r[2] for r in sel])
            shp = np.array([r[3] for r in sel])
            sc = np.array([r[4] for r in sel])
            ratio = np.median(shp) / max(np.median(rig), 1e-9)
            print(f'  {ph:9s} {len(sel):4d} | {np.median(rig):8.2f} {np.percentile(rig, 90):9.2f} | '
                  f'{np.median(shp):8.2f} {np.percentile(shp, 90):9.2f} | '
                  f'{np.median(sc):7.3f} {np.percentile(sc, 10):5.3f}-{np.percentile(sc, 90):<6.3f} | '
                  f'{ratio:6.2f}')
            out[key][ph] = {'n': len(sel), 'rigid_p50': float(np.median(rig)),
                            'shape_p50': float(np.median(shp)),
                            'scale_p50': float(np.median(sc)),
                            'scale_p10': float(np.percentile(sc, 10)),
                            'scale_p90': float(np.percentile(sc, 90)),
                            'shape_over_total': float(ratio)}
    print()
    print('  Reading: shape/total near 0 => the error is almost entirely SCALE (a single')
    print('  scale factor explains it).  Near 1 => the relative geometry itself is wrong.')
    print('  Scale spread (p10-p90) far from 1.0 => the metric channel is unreliable there.')
    return out
