"""gps_early_align: yaw/translation recovered from the early part of a flight; later drift is not absorbed into the alignment."""
import math
import numpy as np
from bag_tool.gps_early_align import estimate_alignment


def _flight(drift_rate=0.0, yaw_deg=70.0, seed=0):
    rng = np.random.default_rng(seed)
    t = np.arange(0, 300, 0.05)
    # climb, then a long out-and-back in the VIO world (gravity-aligned, arbitrary yaw)
    x = np.where(t < 60, 0.0, 12.0 * (np.minimum(t, 200) - 60) - 12.0 * np.maximum(t - 200, 0) * 0.0)
    y = np.where(t > 200, 10.0 * (t - 200), 0.0) + 2.0 * np.sin(t / 20)
    z = np.minimum(t, 30) * 3.0
    vio = np.stack([x, y, z], 1)
    th = math.radians(yaw_deg)
    R = np.array([[math.cos(th), -math.sin(th), 0], [math.sin(th), math.cos(th), 0], [0, 0, 1]])
    gt = vio @ R.T + np.array([5.0, -3.0, 0.0])
    # the VIO drifts after t > 120 (scale error growing with time): the alignment must not chase it
    k = 1.0 + drift_rate * np.maximum(t - 120, 0)
    vio_meas = vio * k[:, None]
    tg = np.arange(0, 300, 0.5)
    gtn = np.stack([np.interp(tg, t, gt[:, c]) for c in range(3)], 1) + rng.normal(0, 1.0, (len(tg), 3))
    return t, vio_meas, tg, gtn, th


def test_recovers_yaw_without_drift():
    t, v, tg, g, th = _flight(0.0)
    d = estimate_alignment(t, v, tg, g, sigma_h=1.0)
    assert d['ok']
    assert abs(math.degrees(d['theta_rad'] - th)) < 1.0


def test_early_fit_ignores_late_drift():
    t, v, tg, g, th = _flight(0.004)
    d = estimate_alignment(t, v, tg, g, sigma_h=1.0)
    assert d['ok'] and d['fit_frac'] < 0.9                      # stopped before the end of the flight
    assert abs(math.degrees(d['theta_rad'] - th)) < 2.0


def test_translation_convention_maps_gt_into_vio_frame():
    t, v, tg, g, th = _flight(0.0)
    d = estimate_alignment(t, v, tg, g, sigma_h=1.0)
    c, s = math.cos(-d['theta_rad']), math.sin(-d['theta_rad'])
    Rz = np.array([[c, -s, 0], [s, c, 0], [0, 0, 1]])
    back = g @ Rz.T + d['trans']                                # GT in the VIO frame (dynamic_align convention)
    vi = np.stack([np.interp(tg, t, v[:, c_]) for c_ in range(3)], 1)
    m = tg < 100
    assert np.sqrt(((back[m] - vi[m]) ** 2).sum(1).mean()) < 3.0


def test_stationary_vehicle_is_refused():
    t = np.arange(0, 100, 0.05); v = np.zeros((len(t), 3)); tg = np.arange(0, 100, 0.5); g = np.random.default_rng(1).normal(0, 0.5, (len(tg), 3))
    assert not estimate_alignment(t, v, tg, g, sigma_h=1.0)['ok']
