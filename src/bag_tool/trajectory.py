"""AlignedTrajectory: what every alignment method hands to the scorer.

An alignment's job is to express the ground truth as a full trajectory in the VIO world frame (position of the VIO
body and, when the ground truth carries it, the VIO body's attitude). The scorer then compares two trajectories:

  ATE   |pv(t) - pg(t)|                                    (needs positions only)
  RTE-D |ATE(t)| - |ATE(t - w)|                            (needs positions only; the pre-2026-11-16 "RTE")
  RTE   |Rv(t-w)^T (pv(t) - pv(t-w)) - Rg(t-w)^T (pg(t) - pg(t-w))|
        both trajectories put in the same frame at the start of each window, drift after w (needs full 6-DoF GT)
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from scipy.spatial.transform import Rotation, Slerp


@dataclass
class AlignedTrajectory:
    bag_ts: np.ndarray        # int64, bag receive time of each GT sample (for writing topics)
    stamp_ns: np.ndarray      # int64, header stamps (the clock the VIO stamps live in)
    pos: np.ndarray           # (N, 3) VIO-body position in the VIO world
    rot: Rotation | None      # (N) VIO-body attitude in the VIO world (placeholder when has_attitude is False; None if empty)
    has_attitude: bool        # False: position-only ground truth -> RTE cannot be scored
    source: str               # 'gnss-fix', 'pose', 'point', 'gnss-ins', ...

    def __len__(self) -> int:
        return len(self.stamp_ns)

    @classmethod
    def from_records(cls, records: list, has_attitude: bool, source: str) -> 'AlignedTrajectory':
        """From the legacy list of (bag_ts, stamp_ns, pos, Rotation)."""
        if not records:
            return cls(np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros((0, 3)), None, False, source)
        return cls(np.array([r[0] for r in records], np.int64), np.array([r[1] for r in records], np.int64),
                   np.array([r[2] for r in records], float), Rotation.concatenate([r[3] for r in records]),
                   has_attitude, source)

    def records(self) -> list:
        """Legacy (bag_ts, stamp_ns, pos, Rotation) list (topic writing)."""
        return [(int(self.bag_ts[k]), int(self.stamp_ns[k]), self.pos[k], self.rot[k]) for k in range(len(self))]

    def until(self, max_stamp_ns: int) -> 'AlignedTrajectory':
        m = self.stamp_ns <= max_stamp_ns
        return AlignedTrajectory(self.bag_ts[m], self.stamp_ns[m], self.pos[m], self.rot[np.where(m)[0]] if m.any() else None,
                                 self.has_attitude, self.source)

    def nearest_index(self, stamps_ns: np.ndarray) -> np.ndarray:
        """Index of the nearest GT sample (ATE pairing, unchanged from the original scorer: ties go to the later)."""
        s = self.stamp_ns; n = len(s)
        idx = np.searchsorted(s, stamps_ns)
        lo = np.clip(idx - 1, 0, n - 1); hi = np.clip(idx, 0, n - 1)
        take_lo = (idx > 0) & ((idx >= n) | ((s[hi] - stamps_ns) > (stamps_ns - s[lo])))
        return np.where(take_lo, lo, hi)

    def interpolate(self, stamps_ns: np.ndarray):
        """Linear position + slerp attitude at the given stamps; ok = inside the GT span."""
        t = self.stamp_ns.astype(float) * 1e-9; q = np.asarray(stamps_ns, float) * 1e-9
        ok = (q >= t[0]) & (q <= t[-1])
        qc = np.clip(q, t[0], t[-1])
        pos = np.stack([np.interp(qc, t, self.pos[:, c]) for c in range(3)], 1)
        rot = Slerp(t, self.rot)(qc) if self.has_attitude else None
        return pos, rot, ok


def handeye_rotation(rel_a: np.ndarray, rel_b: np.ndarray, min_angle_deg: float = 3.0):
    """Constant rotation M with a ~= M b between paired relative-rotation vectors (Kabsch on rotation axes):
    a in the VIO body, b in the GT body -> M = (VIO body -> GT body)^T. Returns (M, info)."""
    sel = (np.linalg.norm(rel_b, axis=1) > math.radians(min_angle_deg)) & np.all(np.isfinite(rel_a), axis=1) \
        & np.all(np.isfinite(rel_b), axis=1)
    info = {'handeye_pairs': int(sel.sum())}
    if sel.sum() < 20:
        info['handeye_angle_deg'] = None
        return np.eye(3), info
    H = rel_b[sel].T @ rel_a[sel]
    U, _, Vt = np.linalg.svd(H)
    D = np.diag([1.0, 1.0, np.sign(np.linalg.det(Vt.T @ U.T))])
    M = Vt.T @ D @ U.T
    res = rel_a[sel] - rel_b[sel] @ M.T
    info['handeye_angle_deg'] = math.degrees(np.linalg.norm(Rotation.from_matrix(M).as_rotvec()))
    info['handeye_resid_deg'] = math.degrees(float(np.sqrt(np.mean(np.sum(res ** 2, 1)))))
    return M, info


def window_partner(stamps_ns: np.ndarray, window_ns: int) -> np.ndarray:
    """For each frame, the frame nearest to (stamp - window) (same rule as the original RTE)."""
    n = len(stamps_ns)
    past = stamps_ns - window_ns
    j = np.clip(np.searchsorted(stamps_ns, past), 0, n - 1)
    jm = np.maximum(j - 1, 0)
    use_prev = (j > 0) & ((stamps_ns[j] - past) > (past - stamps_ns[jm]))
    return np.where(use_prev, jm, j)


def rte_relative_pose(stamps_ns: np.ndarray, vio_pos: np.ndarray, vio_rot: Rotation, gt: AlignedTrajectory,
                      window_ns: int) -> np.ndarray:
    """Per-frame RTE (see module doc). NaN where not scorable (no GT attitude, outside the GT span, or no
    earlier frame a window back)."""
    n = len(stamps_ns)
    if not gt.has_attitude or len(gt) < 2 or n == 0:
        return np.full(n, np.nan)
    j = window_partner(stamps_ns, window_ns)
    pg, Rg, ok = gt.interpolate(stamps_ns)
    ok = ok & ok[j] & (j < np.arange(n))
    dv = vio_rot[j].inv().apply(vio_pos - vio_pos[j])
    dg = Rg[j].inv().apply(pg - pg[j])
    vals = np.linalg.norm(dv - dg, axis=1)
    vals[~ok] = np.nan
    return vals
