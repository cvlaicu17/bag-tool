"""gps_early_align.py -- VIO <-> position-only GPS alignment fitted on an EARLY prefix of the flight (no heading needed).

Why: a whole-flight fit absorbs the VIO's later drift into the alignment (it minimises ATE by construction), and a GPS-only
ground truth (orqa: /fc/gps NavSatFix) has no attitude to anchor yaw. Drift grows with time, so the alignment should be decided by the
part of the flight where the VIO is still good, and only as soon as there is enough geometry to observe yaw.

Unknowns: yaw about +z and a 3-D translation (the VIO world is gravity-aligned, so tilt is NOT fitted and cannot absorb tilt error).
  1. Airborne start = first GPS fix that moved > `move_m` from the start (ground time carries no information).
  2. First prefix (dynamic gate): the earliest prefix whose yaw uncertainty  sigma_yaw = sigma_h / sqrt(sum |p - pmean|^2)  (horizontal GPS
     points, sigma_h = median NavSatFix position sigma, else `sigma_h_default`) is small enough that the yaw-induced error at the flight's
     farthest point stays <= kappa * sigma_h, i.e. sigma_yaw <= kappa * sigma_h / R_far.
  3. Prefixes then grow geometrically (x `grow`) and A_k is fitted on each. If A_k makes any earlier prefix j worse than its own fit by more than
     max(tol_abs, tol_rel * own_j), the later trajectory is pulling the fit: stop and keep A_{k-1}.
Validated on orqa_10..19 (raven altair-aggressive, 2026-11-20): early and whole-flight yaw agree to 0.4-0.7 deg on average (1.3 deg worst), mean
early-fit ATE 7.4 m vs 4.6 m whole-flight-fit (the difference is the drift the global fit hides)."""
from __future__ import annotations
import math
import numpy as np


def _fit4(P, G):
    """yaw (VIO -> GT) + translation, least squares: G ~ Rz(theta) P + t."""
    pm, gm = P.mean(0), G.mean(0)
    p, q = (P - pm)[:, :2], (G - gm)[:, :2]
    th = math.atan2((p[:, 0] * q[:, 1] - p[:, 1] * q[:, 0]).sum(), (p * q).sum())
    c, s = math.cos(th), math.sin(th)
    R = np.array([[c, -s, 0], [s, c, 0], [0, 0, 1.0]])
    return R, gm - R @ pm, th


def _res(R, t, P, G):
    return float(np.sqrt((((P @ R.T + t) - G) ** 2).sum(1).mean()))


def estimate_alignment(t_vio, p_vio, t_gt, p_gt, sigma_h=None, kappa=1.0, grow=1.15, tol_rel=0.5, tol_abs=0.3,
                       move_m=3.0, sigma_h_default=1.5):
    """t_* in seconds, p_* (N,3) metres (GT in ENU). Returns a dict; ok=False + reason when yaw cannot be determined.
    On success: theta_rad (VIO world -> ENU yaw), trans (so that the GT in the VIO frame is Rz(-theta) @ enu + trans, the convention of
    bag_tool.dynamic_align), plus diagnostics."""
    t_vio, p_vio, t_gt, p_gt = map(np.asarray, (t_vio, p_vio, t_gt, p_gt))
    sig = float(sigma_h) if sigma_h and sigma_h > 0 else sigma_h_default
    m = (t_gt >= t_vio[0]) & (t_gt <= t_vio[-1])
    tg, gt = t_gt[m], p_gt[m]
    if len(tg) < 20:
        return dict(ok=False, reason=f'only {len(tg)} GPS fixes inside the VIO span')
    vi = np.stack([np.interp(tg, t_vio, p_vio[:, k]) for k in range(3)], 1)
    moved = np.linalg.norm(gt - gt[0], axis=1) > move_m
    if not moved.any():
        return dict(ok=False, reason='the vehicle never moved')
    i0 = int(np.argmax(moved))
    r_far = float(np.linalg.norm(gt[:, :2] - gt[0, :2], axis=1).max())
    th_target = kappa * sig / max(r_far, 1.0)

    def sig_yaw(G):
        h = G[:, :2] - G[:, :2].mean(0)
        return sig / max(math.sqrt((h ** 2).sum()), 1e-9)
    k_end = None
    for i in range(i0 + 5, len(tg)):
        if sig_yaw(gt[:i + 1]) <= th_target:
            k_end = i
            break
    if k_end is None:
        return dict(ok=False, reason=f'never enough horizontal baseline for yaw (R_far {r_far:.0f} m, sigma_h {sig:.2f} m)')
    ends, t_end = [], tg[k_end]
    while t_end < tg[-1]:
        ends.append(int(np.searchsorted(tg, t_end)))
        t_end = tg[0] + (t_end - tg[0]) * grow
    ends.append(len(tg) - 1)
    ends = sorted(set(ends))
    fits = [_fit4(vi[:e + 1], gt[:e + 1]) for e in ends]
    own = [_res(R, t, vi[:e + 1], gt[:e + 1]) for (R, t, _), e in zip(fits, ends)]
    best, why = 0, 'never degraded (= whole-flight fit)'
    for k in range(1, len(ends)):
        R, t, _ = fits[k]
        bad = None
        for j in range(k):
            rj = _res(R, t, vi[:ends[j] + 1], gt[:ends[j] + 1])
            if rj - own[j] > max(tol_abs, tol_rel * own[j]):
                bad = (j, rj, own[j])
                break
        if bad:
            why = f'prefix {k} (t={tg[ends[k]] - tg[0]:.0f}s) degrades prefix {bad[0]} ({bad[1]:.2f} vs own {bad[2]:.2f} m)'
            break
        best = k
    R, t, th = fits[best]
    e_fit = ends[best]
    Rg, tg_, thg = _fit4(vi, gt)
    return dict(ok=True, theta_rad=th, trans=-(R.T @ t), lag_s=0.0, fit_end_s=float(tg[e_fit] - tg[0]), flight_s=float(tg[-1] - tg[0]),
                fit_frac=float((tg[e_fit] - tg[0]) / (tg[-1] - tg[0])), fit_rms_m=_res(R, t, vi[:e_fit + 1], gt[:e_fit + 1]),
                sigma_h=sig, r_far=r_far, gate_t_s=float(tg[k_end] - tg[0]), gate_sigma_yaw_deg=math.degrees(sig_yaw(gt[:k_end + 1])),
                yaw_global_deg=math.degrees(thg), stop=why, n_prefixes=len(ends))
