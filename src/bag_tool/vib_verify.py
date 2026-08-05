"""vib-verify: phase-resolved vibration verification of a (sim) bag against a real
reference.

Checks that a bag's IMU vibration matches a reference, per flight phase (climb /
cruise / descent, classified from ground-truth vertical velocity) and per axis:
  - accel & gyro band-stds in 20-100 Hz and 100-195 Hz (the vibration bands at
    fs=400; body dynamics live below ~10 Hz and are excluded),
  - the cruise accel-z spectral peak family (at 400 Hz the motor-shaft harmonics of
    an FPV drone are ALIASES: real ~349 Hz shaft -> observed ~51.5/103.5/155 Hz),
  - ground-rest silence (motors off -> no injected vibration).

Reference targets come from --targets JSON (schema of day207_vib_targets_v2.json /
measure_vib_targets_v2.py), from --ref REAL_BAG (derived on the fly, optionally
--save-targets), or from the config-stored default (--set-targets persists one).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from scipy.signal import welch, medfilt, butter, sosfilt

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.vib_check import ok, warn, fail, hdr, SEP

_TS = get_typestore(Stores.ROS2_JAZZY)
IMU_TOPIC = "/imu/data_raw"
GT_TOPIC = "/pf_geo_loc/fc_local_position"
FLOOR_FREQS = np.linspace(5, 195, 48)


# ── bag reading ────────────────────────────────────────────────────────────────
def _read(bag_path: str, imu_topic: str, gt_topic: str):
    t_i, acc, gyr, t_g, z_g = [], [], [], [], []
    with Reader(Path(bag_path)) as reader:
        conns = [c for c in reader.connections if c.topic in (imu_topic, gt_topic)]
        for conn, ts, raw in reader.messages(connections=conns):
            msg = _TS.deserialize_cdr(raw, conn.msgtype)
            if conn.topic == imu_topic:
                t_i.append(ts)
                acc.append((msg.linear_acceleration.x, msg.linear_acceleration.y,
                            msg.linear_acceleration.z))
                gyr.append((msg.angular_velocity.x, msg.angular_velocity.y,
                            msg.angular_velocity.z))
            else:
                t_g.append(ts); z_g.append(msg.pose.position.z)
    if not t_i:
        raise SystemExit(f"ERROR: no {imu_topic} messages in {bag_path}")
    if not t_g:
        raise SystemExit(f"ERROR: no {gt_topic} messages in {bag_path} "
                         "(needed for phase classification)")
    t0 = t_i[0]
    return (np.array(t_i) - t0) / 1e9, np.array(acc), np.array(gyr), \
           (np.array(t_g) - t0) / 1e9, np.array(z_g)


# ── phases / spectra ──────────────────────────────────────────────────────────
def _phase_masks(ti, tg, zg):
    h = -(zg - zg[0])
    vz = np.gradient(h) / np.maximum(np.gradient(tg), 1e-6)
    k = max(3, int(2 / max(np.median(np.diff(tg)), 1e-3)))
    vz = np.convolve(vz, np.ones(k) / k, mode="same")
    vzi = np.interp(ti, tg, vz); hi = np.interp(ti, tg, h)
    flight = hi > 2.0
    return {"climb": flight & (vzi > 0.5),
            "cruise": flight & (np.abs(vzi) <= 0.3),
            "descent": flight & (vzi < -0.3)}, hi


def _band(x, lo, hi, fs):
    sos = butter(4, [lo, hi], btype="band", fs=fs, output="sos")
    return float(sosfilt(sos, x - x.mean()).std())


def _floor_env(x, fs):
    f, p = welch(x - x.mean(), fs=fs, nperseg=4096)
    w = int(15 / (f[1] - f[0])) | 1
    fl = medfilt(p, w)
    sel = (f >= 5) & (f <= 195)
    return np.interp(FLOOR_FREQS, f[sel], fl[sel])


def _peaks(x, fs, n=5):
    f, p = welch(x - x.mean(), fs=fs, nperseg=4096)
    idx = [i for i in range(1, len(p) - 1)
           if p[i] > p[i - 1] and p[i] > p[i + 1] and f[i] > 20]
    idx.sort(key=lambda i: -p[i])
    out = []
    for i in idx:
        if all(abs(f[i] - fo) > 4 for fo in out):
            out.append(float(f[i]))
        if len(out) >= n:
            break
    return sorted(out)


def measure_targets(bag_path: str, imu_topic: str = IMU_TOPIC,
                    gt_topic: str = GT_TOPIC) -> dict:
    """Derive phase-resolved targets from a reference bag (schema v2)."""
    ti, acc, gyr, tg, zg = _read(bag_path, imu_topic, gt_topic)
    fs = 1 / np.median(np.diff(ti))
    masks, _ = _phase_masks(ti, tg, zg)
    out = {"fs": float(fs), "source": str(bag_path),
           "floor_freqs": [float(v) for v in FLOOR_FREQS], "phases": {}}
    for pname, mk in masks.items():
        d = {"seconds": float(mk.sum() / fs)}
        for sname, arr in (("accel", acc), ("gyro", gyr)):
            for ax, lbl in enumerate("xyz"):
                x = arr[mk, ax]
                d[f"{sname}_{lbl}"] = {
                    "band20_100": _band(x, 20, 100, fs),
                    "band100_195": _band(x, 100, 195, fs),
                    "floor_psd": [float(v) for v in _floor_env(x, fs)],
                }
        out["phases"][pname] = d
    out["peaks_accz_cruise"] = _peaks(acc[masks["cruise"], 2], fs)
    return out


# ── verification ──────────────────────────────────────────────────────────────
def _grade(sim: float, real: float, tiny: float) -> tuple[str, str]:
    if (sim < tiny and real < tiny) or abs(sim - real) < tiny:
        return "ok", "· (Δ<tiny)"
    r = sim / max(real, 1e-12)
    if 0.8 <= r <= 1.25:
        return "ok", f"{r:5.2f}x"
    if 0.6 <= r <= 1.6:
        return "warn", f"{r:5.2f}x"
    return "fail", f"{r:5.2f}x"


def run(args) -> None:
    targets = json.load(open(args.targets))
    print(hdr(f"vib-verify: {args.input_bag}"))
    print(f"targets   : {args.targets} (source: {targets.get('source', 'n/a')})")
    print(SEP)
    ti, acc, gyr, tg, zg = _read(args.input_bag, args.imu_topic, args.gt_topic)
    fs = 1 / np.median(np.diff(ti))
    masks, hi = _phase_masks(ti, tg, zg)
    counts = {"ok": 0, "warn": 0, "fail": 0}
    for pname, mk in masks.items():
        if pname not in targets["phases"] or mk.sum() < fs * 5:
            continue
        print(hdr(f"\n[{pname}]  {mk.sum() / fs:.0f}s   "
                  f"{'20-100 Hz (sim/real)':>28s} {'100-195 Hz (sim/real)':>24s}"))
        # tolerance floors: differences below these are immaterial (gyro: ~2x the
        # MPU-6000 sensor noise floor at 400 Hz, 0.0003*sqrt(200) ~ 0.0045 rad/s)
        for sname, arr, tiny in (("accel", acc, 0.02), ("gyro", gyr, 0.01)):
            for ax, lbl in enumerate("xyz"):
                x = arr[mk, ax]
                tgt = targets["phases"][pname][f"{sname}_{lbl}"]
                cells = []
                for b, (lo, hi_) in (("band20_100", (20, 100)),
                                     ("band100_195", (100, 195))):
                    sim = _band(x, lo, hi_, fs)
                    g, txt = _grade(sim, tgt[b], tiny)
                    counts[g] += 1
                    mark = {"ok": "✔", "warn": "⚠", "fail": "✘"}[g]
                    cells.append(f"{sim:7.3f}/{tgt[b]:6.3f} {mark} {txt}")
                print(f"  {sname}_{lbl}:  {cells[0]:>34s}  {cells[1]:>30s}")
    # spectral line family
    pk = _peaks(acc[masks["cruise"], 2], fs)
    print(SEP)
    print("cruise accZ peaks :", " ".join(f"{v:.1f}" for v in pk), "Hz")
    if "peaks_accz_cruise" in targets:
        print("reference peaks   :",
              " ".join(f"{v:.1f}" for v in targets["peaks_accz_cruise"]), "Hz")
    # ground silence
    g = (ti < ti[0] + 30) & (hi < 1.0)
    if g.sum() > fs * 3:
        print(f"ground-rest accZ  : {acc[g, 2].std():.3f} m/s² "
              "(should be sensor-noise level: motors off ⇒ no vibration)")
    print(SEP)
    verdict = ok if counts["fail"] == 0 and counts["warn"] <= 6 else \
        (warn if counts["fail"] <= 2 else fail)
    print(verdict(f"{counts['ok']} ok / {counts['warn']} warn / "
                  f"{counts['fail']} fail band checks"))
