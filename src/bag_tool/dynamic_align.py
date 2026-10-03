"""dynamic_align.py -- ground-truth <-> VIO yaw from the two trajectories themselves (no heading needed).

The first-fix alignment needs the heading of the vehicle at the first fix. For a position-only ground truth (altair PointStamped, GPS NavSatFix
without a compass) it falls back to a constant measured on one rig (platform.yaw_correction_rad: -133.954 deg, valid only for the Day20 take-off
heading of ~225 deg; on orqa it is 76-93 deg off, ATE 52-598 m on trajectories that are 1.3-14 m off).

This method recovers the rotation between the VIO world and the ground-truth ENU frame from the paths:
  p_k = E_k + i N_k (GT, mean-removed per window), s_k = x_k + i y_k (VIO at the same instants), ENU = exp(i theta) * world, so over a set of windows
  cost(theta) = sum |p - exp(i theta) s|^2  ->  theta = -arg( sum conj(p) s )            (closed form, no iteration)
Windows of `window_s` seconds starting at take-off (VIO height +2 m) so that the pad and the climb (where the paths are not a rigid pair) are left
out. The time offset between the GT stamps and the VIO clock (GPS latency; ~0.3 s on orqa, ~0 on Day20) is searched on [-lag_range, lag_range] by the
minimum of the pooled cost. Translation: the VIO and GT means over the pre-take-off pad (>= 5 GT fixes) so a 1.2 m GPS first fix does not shift
the whole trajectory; else the first fix. Uncertainty: block bootstrap over windows.

Validated (flock tools/yaw_align.py, `trajectory` method): 0.61 deg rms against the Day20 landmark georeference (6 flights), unchanged with raw-GPS noise
(0.62 deg); on 9 orqa flights it agrees to 0.41 deg rms with an inertial (IMU accelerations + attitude vs GPS) estimate. Cost: 15-40 ms per bag.
Needs ~90 s of flight after take-off (>= 3 windows of 60 s step 30 s). It does not need manoeuvres: in the plane a straight line already fixes the yaw
(unlike an inertial estimate, which needs accelerations); GPS noise limits it when the covered distance is short. The coherence check refuses a GT that is
not the same path as the VIO (VIO failure, wrong GT, wrong clock).
"""
from __future__ import annotations
import math
import numpy as np


def _windows(t_lo, t_hi, w, step):
    return [(s, s + w) for s in np.arange(t_lo, t_hi - w + 1e-9, step)]


def _parts(vio_t, vio_p, gt_t, gt_p, wins, lag):
    out = []
    for w0, w1 in wins:
        m = (gt_t >= w0) & (gt_t < w1)
        if m.sum() < 6:
            continue
        tk = np.clip(gt_t[m] + lag, vio_t[0], vio_t[-1])
        p = gt_p[m, 0] + 1j * gt_p[m, 1]
        s = np.interp(tk, vio_t, vio_p[:, 0]) + 1j * np.interp(tk, vio_t, vio_p[:, 1])
        q, s = p - p.mean(), s - s.mean()
        out.append((np.sum(np.conj(q) * s), float(np.sum(np.abs(s) ** 2)), float(np.sum(np.abs(q) ** 2)),
                    float(np.sqrt(np.sum(np.abs(q) ** 2) * np.sum(np.abs(s) ** 2)))))
    return out


def _solve(parts):
    Z = np.array([p[0] for p in parts])
    return -np.angle(Z.sum()), sum(p[1] + p[2] for p in parts) - 2 * abs(Z.sum())


def estimate_alignment(vio_t, vio_p, gt_t, gt_p, *, window_s=60.0, step_s=30.0, lag_range=1.0, lag_step=0.05,
                       min_windows=3, nboot=300, seed=0, min_coherence=0.8, apply_lag=True):
    """vio_t, gt_t: seconds (float, same clock); vio_p, gt_p: (N,3) positions (GT already in ENU, relative to its first fix).

    Returns a dict. ok=False with a `reason` if it cannot be trusted (too short, no manoeuvres, incoherent windows); otherwise
      theta_rad  rotation world -> ENU (ENU = Rz(theta) * world); the GT -> VIO rotation is Rz(-theta)
      lag_s      GT stamp -> VIO clock offset (VIO time = GT stamp + lag_s); applied by the caller when apply_lag
      sd_deg     block-bootstrap std of theta;  windows;  coherence (pooled |Z| / sum |p||s|, 1 = the paths are a perfect rigid pair)
      trans      translation GT -> VIO after the rotation (3,)
    """
    vio_t, gt_t = np.asarray(vio_t, float), np.asarray(gt_t, float)
    vio_p, gt_p = np.asarray(vio_p, float), np.asarray(gt_p, float)
    res = dict(ok=False, reason="", method="dynamic")
    if len(vio_t) < 50 or len(gt_t) < 20:
        res["reason"] = "too few VIO poses / GT fixes"; return res
    h = vio_p[:, 2]; up = np.nonzero(h > np.median(h[:20]) + 2.0)[0]
    t_take = float(vio_t[up[0]]) if len(up) else float(vio_t[0])
    t_lo, t_hi = max(t_take, gt_t[0], vio_t[0]), min(gt_t[-1], vio_t[-1])
    wins = _windows(t_lo, t_hi, window_s, step_s)
    best = None
    for lag in np.arange(-lag_range, lag_range + 1e-9, lag_step):
        parts = _parts(vio_t, vio_p, gt_t, gt_p, wins, lag)
        if len(parts) < min_windows:
            continue
        th, c = _solve(parts)
        if best is None or c < best[0]:
            best = (c, float(lag), th, parts)
    if best is None:
        res["reason"] = f"fewer than {min_windows} usable {window_s:.0f} s windows after take-off (need ~{window_s + (min_windows - 1) * step_s:.0f} s of flight)"; return res
    _, lag, th, parts = best
    Z = np.array([p[0] for p in parts]); coherence = float(abs(Z.sum()) / max(sum(p[3] for p in parts), 1e-9))
    rng = np.random.default_rng(seed); n = len(parts); block = 2 if n < 8 else 4; sd = None
    if n >= block + 1:
        tb = []
        for _ in range(nboot):
            idx = np.concatenate([np.arange(s, min(s + block, n)) for s in rng.integers(0, n - block + 1, size=int(math.ceil(n / block)))])[:n]
            tb.append(-np.angle(Z[idx].sum()))
        sd = float(np.degrees(np.std((np.array(tb) - th + math.pi) % (2 * math.pi) - math.pi)))
    res.update(theta_rad=float(th), lag_s=lag if apply_lag else 0.0, lag_found_s=lag, sd_deg=sd, windows=n, coherence=coherence, takeoff_s=t_take)
    if coherence < min_coherence:
        res["reason"] = f"windows are not a rigid pair (coherence {coherence:.2f} < {min_coherence}): straight flight, VIO failure or a wrong GT"; return res
    # translation GT -> VIO: pad mean before take-off (>= 5 GT fixes), else the first fix
    R = np.array([[math.cos(-th), -math.sin(-th), 0], [math.sin(-th), math.cos(-th), 0], [0, 0, 1]])
    tg = gt_t + (lag if apply_lag else 0.0)
    pad = (tg >= vio_t[0]) & (tg < t_take - 1.0)
    if pad.sum() >= 5:
        vp = np.c_[[np.interp(tg[pad], vio_t, vio_p[:, k]) for k in range(3)]].T
        res["trans"] = vp.mean(0) - R @ gt_p[pad].mean(0); res["origin"] = f"pad mean ({int(pad.sum())} fixes)"
    else:
        k = int(np.argmin(np.abs(tg - vio_t[0]))); vp = np.array([np.interp(tg[k], vio_t, vio_p[:, j]) for j in range(3)])
        res["trans"] = vp - R @ gt_p[k]; res["origin"] = "first fix"
    res["ok"] = True
    return res
