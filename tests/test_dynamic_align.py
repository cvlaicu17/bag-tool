"""Synthetic checks of bag_tool.dynamic_align: recovery of a known yaw, GPS latency, and the cases where it must refuse (short / straight / wrong GT)."""
import math
import numpy as np
from bag_tool.dynamic_align import estimate_alignment


def make_flight(theta_deg=-49.0, lag=-0.3, seconds=260.0, noise=1.2, gps_hz=2.0, seed=0, straight=False):
    """VIO path: 40 s on the pad, climb 80 m, then a figure of curves at ~12 m/s (or a straight line). GT = ENU = Rz(theta) world + GPS noise, stamped `lag` late."""
    rng = np.random.default_rng(seed); dt = 0.05; t = np.arange(0, seconds, dt); x = np.zeros_like(t); y = np.zeros_like(t); z = np.zeros_like(t)
    for i in range(1, len(t)):
        ti = t[i]
        if ti < 40: v = 0.0; hd = 0.5
        else: v = 12.0; hd = 0.5 if straight else 0.5 + 0.35 * math.sin((ti - 40) / 22.0) * 3.0
        x[i] = x[i - 1] + v * math.cos(hd) * dt; y[i] = y[i - 1] + v * math.sin(hd) * dt; z[i] = min(80.0, max(0.0, (ti - 40) * 4.0)) if ti >= 40 else 0.0
    th = math.radians(theta_deg); E = math.cos(th) * x - math.sin(th) * y; N = math.sin(th) * x + math.cos(th) * y
    tg = np.arange(0, seconds, 1.0 / gps_hz); gE = np.interp(tg + lag, t, E) + rng.normal(0, noise, len(tg)); gN = np.interp(tg + lag, t, N) + rng.normal(0, noise, len(tg))
    gz = np.interp(tg + lag, t, z) + rng.normal(0, 1.5, len(tg))
    g = np.c_[gE, gN, gz]; return t, np.c_[x, y, z], tg, g - g[0]


def test_recovers_yaw_and_lag():
    for th in (-49.0, 76.0, 150.0, -170.0):
        for lag in (-0.3, 0.0, 0.45):
            vt, vp, gt, gp = make_flight(theta_deg=th, lag=lag, seed=int(th) % 7)
            r = estimate_alignment(vt, vp, gt, gp)
            assert r["ok"], r
            err = (math.degrees(r["theta_rad"]) - th + 180) % 360 - 180
            assert abs(err) < 0.7, (th, lag, err)
            assert abs(r["lag_s"] - lag) <= 0.1, (lag, r["lag_s"])


def test_translation_uses_the_pad_mean():
    vt, vp, gt, gp = make_flight(seed=3); r = estimate_alignment(vt, vp, gt, gp); assert r["ok"] and "pad mean" in r["origin"]
    R = np.array([[math.cos(-r["theta_rad"]), -math.sin(-r["theta_rad"]), 0], [math.sin(-r["theta_rad"]), math.cos(-r["theta_rad"]), 0], [0, 0, 1]])
    pad = gt < 38; resid = np.linalg.norm((R @ gp[pad].T).T[:, :2] + r["trans"][:2] - vp[0, :2], axis=1)
    assert resid.mean() < 2.0                                            # GPS noise 1.2 m per axis, averaged over the pad


def test_refuses_a_short_flight():
    vt, vp, gt, gp = make_flight(seconds=110.0); r = estimate_alignment(vt, vp, gt, gp); assert not r["ok"] and "windows" in r["reason"]


def test_a_straight_flight_still_gives_the_yaw():
    """in the plane a straight line fixes the yaw (it is the inertial estimate that needs manoeuvres)."""
    for th in (-49.0, 76.0):
        vt, vp, gt, gp = make_flight(theta_deg=th, straight=True, seed=2); r = estimate_alignment(vt, vp, gt, gp); assert r["ok"], r
        assert abs((math.degrees(r["theta_rad"]) - th + 180) % 360 - 180) < 1.0


def test_refuses_a_gt_that_is_not_the_same_path():
    vt, vp, gt, gp = make_flight(seed=1); rng = np.random.default_rng(5); r = estimate_alignment(vt, vp, gt, rng.normal(0, 80, gp.shape))
    assert not r["ok"] and "coherence" in r["reason"]


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"): fn(); print("ok", name)
