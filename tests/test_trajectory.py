"""RTE (frame-invariant relative-pose drift) and the body hand-eye used by the alignments."""
import numpy as np
from scipy.spatial.transform import Rotation

from bag_tool.trajectory import AlignedTrajectory, handeye_rotation, rte_relative_pose, window_partner


def _flight(n=2000, hz=20.0, seed=0):
    rng = np.random.default_rng(seed)
    t = np.arange(n) / hz
    yaw = 0.3 * np.sin(0.05 * t) + 0.02 * t
    rot = Rotation.from_euler('zyx', np.c_[yaw, 0.1 * np.sin(0.3 * t), 0.08 * np.cos(0.2 * t)])
    vel = np.c_[8 * np.cos(yaw), 8 * np.sin(yaw), 0.5 * np.sin(0.1 * t)]
    pos = np.cumsum(vel / hz, 0) + rng.normal(scale=0.0, size=(n, 3))
    return (t * 1e9).astype(np.int64), pos, rot


def _gt(stamps, pos, rot):
    return AlignedTrajectory(stamps, stamps, pos, rot, True, 'test')


def test_rte_zero_for_identical_and_for_any_world_alignment():
    s, p, r = _flight()
    assert np.nanmax(rte_relative_pose(s, p, r, _gt(s, p, r), 1_000_000_000)) < 1e-9
    W = Rotation.from_euler('z', 37, degrees=True)                 # wrong heading + offset: RTE must not see it
    v = rte_relative_pose(s, W.apply(p) + [5, -3, 2], W * r, _gt(s, p, r), 1_000_000_000)
    assert np.nanmax(v) < 1e-9


def test_rte_measures_scale_drift():
    s, p, r = _flight()
    v = rte_relative_pose(s, 1.01 * p, r, _gt(s, p, r), 1_000_000_000)
    speed = np.linalg.norm(np.diff(p, axis=0), axis=1).mean() * 20
    assert abs(np.nanmedian(v) - 0.01 * speed) < 0.01 * speed * 0.05


def test_rte_nan_without_attitude():
    s, p, r = _flight(200)
    g = AlignedTrajectory(s, s, p, r, False, 'point')
    assert np.isnan(rte_relative_pose(s, p, r, g, 1_000_000_000)).all()


def test_handeye_recovers_body_rotation():
    s, p, r = _flight()
    Rx = Rotation.from_euler('xyz', [2, -1, 90], degrees=True)    # VIO body -> GT body
    rg = r * Rx.inv()                                              # GT body attitude
    j = window_partner(s, 1_000_000_000)
    a = (r[j].inv() * r).as_rotvec(); b = (rg[j].inv() * rg).as_rotvec()
    M, info = handeye_rotation(a, b, min_angle_deg=0.5)
    assert info['handeye_angle_deg'] is not None
    assert np.allclose(M.T, Rx.as_matrix(), atol=1e-6)
