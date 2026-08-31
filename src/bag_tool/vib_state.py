"""vib-fit-state: fit IMU vibration amplitude to a CONTINUOUS flight-state model,
from a real reference bag.

Supersedes the three discrete phases (climb/cruise/descent) that measure_vib_targets_v2
uses. Measured on the full altair day20.7 bag (905 airborne 4s windows): a state model
(vertical speed split into climb/sink components, plus horizontal speed) explains MORE
variance in every harmonic band than the three-phase split, for the same parameter count
-- see the "Vibration Model v3" plan. RPM/RPM^2 explains almost none of it: over a mission
flown at roughly constant thrust, RPM barely moves, so this fit deliberately does NOT
include an RPM term. RPM stays in the SIMULATOR's synthesis as a fixed physical prior
(exponent 2, not fitted -- there is no mission data that could fit it), applied on top of
whatever this script measures.

Per harmonic order h in {1,2,3} and channel (accel/gyro x,y,z), fits in log space:

    log A_h(t) = log A0_h + beta_plus_h * vz_plus(t) + beta_minus_h * vz_minus(t)
                           + gamma_h * vh(t)

  vz_plus  = max(vz, 0)   (climbing component)
  vz_minus = min(vz, 0)   (sinking component -- propwash is asymmetric, so this gets its
                            own slope rather than sharing one with climbing)
  vh       = horizontal ground speed

Log space is not cosmetic: it matches the multiplicative form the synthesiser applies
(amplitude = reference * exp(state terms)), and it cannot produce a negative amplitude.

Harmonic frequencies are read at FIXED narrow bands around the real bag's own measured
line family (default 51.04 / 103.94 / 149.01 Hz -- the assumed real shaft family; see
"the shaft-line identity" risk in the vibration plan: a single peak tracker could not
confirm any one of the real bag's several close-spaced lines is genuinely the aliased
shaft, so this is an assumption inherited from the existing v2 targets, not a proven
identification). Per the "Frequencies come from the simulated props" decision, the
SIMULATOR side does not use these frequencies directly -- it aliases its own simulated
rotor speed and gets whatever line positions that produces. This fit only answers "how
does amplitude vary with state", at whichever real-bag frequencies best isolate each
harmonic; the resulting (A0, beta_plus, beta_minus, gamma) are applied to the simulator's
own line, wherever it lands.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.vib_check import ok, warn, fail, hdr, SEP

_TS = get_typestore(Stores.ROS2_JAZZY)
IMU_TOPIC = "/imu/data_raw"
GT_TOPIC = "/pf_geo_loc/fc_local_position"

# The assumed real aliased family (order -> Hz), inherited from
# measure_vib_targets_v2's peaks_accz_cruise / the vib_verify docstring
# (~349 Hz true shaft -> ~51.5/103.5/155 Hz observed). Overridable via CLI.
DEFAULT_HARMONICS = {1: 51.04, 2: 103.94, 3: 149.01}
DEFAULT_HALF_WIDTH_HZ = 4.0
WIN_S, HOP_S = 2.0, 0.5
MIN_HEIGHT_M = 2.0          # airborne threshold, matches vib_verify's phase masks
MIN_AIRBORNE_WINDOWS = 30   # below this a fit is noise, not a model


# ── bag reading ────────────────────────────────────────────────────────────────
def _read(bag_path: str, imu_topic: str, gt_topic: str):
    t_i, acc, gyr, t_g, pos = [], [], [], [], []
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
                t_g.append(ts)
                p = msg.pose.position
                pos.append((p.x, p.y, p.z))
    if not t_i:
        raise SystemExit(f"ERROR: no {imu_topic} messages in {bag_path}")
    if not t_g:
        raise SystemExit(f"ERROR: no {gt_topic} messages in {bag_path} "
                         "(needed for the state variables)")
    t0 = t_i[0]
    return ((np.array(t_i) - t0) / 1e9, np.array(acc), np.array(gyr),
             (np.array(t_g) - t0) / 1e9, np.array(pos))


def _smooth(x: np.ndarray, n: float) -> np.ndarray:
    n = max(3, int(n) | 1)
    return np.convolve(x, np.ones(n) / n, mode="same")


def _state_series(ti: np.ndarray, tg: np.ndarray, pos: np.ndarray):
    """Height (NED, up-positive) plus smoothed vertical/horizontal speed, resampled
    onto the IMU timeline. NED convention matches vib_verify._phase_masks and
    add_vibration.py -- kept consistent rather than re-derived per bag."""
    xg, yg, zg = pos[:, 0], pos[:, 1], pos[:, 2]
    h = -(zg - zg[0])
    dtg = max(np.median(np.diff(tg)), 1e-3)
    win = 2.0 / dtg
    vz = _smooth(np.gradient(h) / np.maximum(np.gradient(tg), 1e-6), win)
    vx = _smooth(np.gradient(xg) / np.maximum(np.gradient(tg), 1e-6), win)
    vy = _smooth(np.gradient(yg) / np.maximum(np.gradient(tg), 1e-6), win)
    vh = np.hypot(vx, vy)
    return (np.interp(ti, tg, h), np.interp(ti, tg, vz), np.interp(ti, tg, vh))


# ── windowed spectral amplitude at fixed harmonic bands ─────────────────────────
def _window_amplitudes(x: np.ndarray, fs: float, freqs_hz: dict[int, float],
                       half_width: float, win_s: float, hop_s: float):
    """FFT-windowed band-std around each harmonic's fixed frequency, per hop.
    A short bandpass filter rings at these narrow relative bandwidths, so this uses
    a Hann-windowed periodogram and integrates the PSD over each band instead
    (same approach validated in the P1 sweep)."""
    nwin, nhop = int(win_s * fs), int(hop_s * fs)
    if len(x) < nwin:
        return np.empty(0), {h: np.empty(0) for h in freqs_hz}
    w = np.hanning(nwin)
    freqs = np.fft.rfftfreq(nwin, 1.0 / fs)
    bands = {h: (freqs >= f0 - half_width) & (freqs < f0 + half_width)
             for h, f0 in freqs_hz.items()}
    centers, amps = [], {h: [] for h in freqs_hz}
    for s in range(0, len(x) - nwin, nhop):
        seg = (x[s:s + nwin] - x[s:s + nwin].mean()) * w
        psd = (np.abs(np.fft.rfft(seg)) ** 2) / (np.sum(w ** 2) * fs) * 2.0
        centers.append(s + nwin // 2)
        for h, m in bands.items():
            amps[h].append(float(np.sqrt(np.trapezoid(psd[m], freqs[m]))))
    return np.array(centers), {h: np.array(v) for h, v in amps.items()}


# ── fitting ──────────────────────────────────────────────────────────────────
def _fit_log_linear(y: np.ndarray, vzp: np.ndarray, vzn: np.ndarray, vh: np.ndarray):
    """log(y) ~ intercept + beta_plus*vzp + beta_minus*vzn + gamma*vh. Returns the
    coefficients plus R^2, so a fit that explains nothing is visible rather than
    silently accepted."""
    ylog = np.log(np.maximum(y, 1e-9))
    X = np.column_stack([np.ones(len(y)), vzp, vzn, vh])
    coef, *_ = np.linalg.lstsq(X, ylog, rcond=None)
    pred = X @ coef
    ss_res = float(((ylog - pred) ** 2).sum())
    ss_tot = float(((ylog - ylog.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return {"A0": float(np.exp(coef[0])), "beta_plus": float(coef[1]),
            "beta_minus": float(coef[2]), "gamma": float(coef[3]), "r2": float(r2)}


def fit_state(bag_path: str, imu_topic: str = IMU_TOPIC, gt_topic: str = GT_TOPIC,
             harmonics: dict[int, float] | None = None,
             half_width_hz: float = DEFAULT_HALF_WIDTH_HZ,
             win_s: float = WIN_S, hop_s: float = HOP_S) -> dict:
    """Derive the v3 state-model coefficients from a reference bag."""
    harmonics = harmonics or DEFAULT_HARMONICS
    ti, acc, gyr, tg, pos = _read(bag_path, imu_topic, gt_topic)
    fs = 1.0 / np.median(np.diff(ti))
    hgt, vz, vh = _state_series(ti, tg, pos)

    channels = {"accel_x": acc[:, 0], "accel_y": acc[:, 1], "accel_z": acc[:, 2],
               "gyro_x": gyr[:, 0], "gyro_y": gyr[:, 1], "gyro_z": gyr[:, 2]}

    out = {"fs": float(fs), "source": str(bag_path), "window_s": win_s, "hop_s": hop_s,
           "half_width_hz": half_width_hz,
           "harmonics_hz": {str(h): f for h, f in harmonics.items()},
           "channels": {}}

    fly = None
    for name, x in channels.items():
        centers, amps = _window_amplitudes(x, fs, harmonics, half_width_hz, win_s, hop_s)
        if len(centers) == 0:
            continue
        if fly is None:
            hgt_c = hgt[np.clip(centers, 0, len(hgt) - 1)]
            fly = hgt_c > MIN_HEIGHT_M
            if fly.sum() < MIN_AIRBORNE_WINDOWS:
                raise SystemExit(
                    f"ERROR: only {fly.sum()} airborne windows in {bag_path} "
                    f"(need >= {MIN_AIRBORNE_WINDOWS}); bag too short or GT height "
                    "sign looks wrong")
            vz_c, vh_c = vz[np.clip(centers, 0, len(vz) - 1)], vh[np.clip(centers, 0, len(vh) - 1)]
            vzp, vzn = np.maximum(vz_c, 0), np.minimum(vz_c, 0)
        out["channels"][name] = {
            str(h): _fit_log_linear(amps[h][fly], vzp[fly], vzn[fly], vh_c[fly])
            for h in harmonics
        }

    fly_idx = np.clip(centers, 0, len(vz) - 1)[fly]
    out["state_ranges"] = {
        "vz_min": float(vz[fly_idx].min()), "vz_max": float(vz[fly_idx].max()),
        "vh_min": float(vh[fly_idx].min()), "vh_max": float(vh[fly_idx].max()),
        "airborne_windows": int(fly.sum()),
    }
    return out


# ── CLI ──────────────────────────────────────────────────────────────────────
def run(args) -> str:
    """Returns the path the coefficients JSON was written to."""
    harmonics = DEFAULT_HARMONICS
    if getattr(args, "harmonics", None):
        harmonics = {}
        for i, tok in enumerate(args.harmonics.split(","), start=1):
            harmonics[i] = float(tok)

    print(hdr(f"vib-fit-state: {args.input_bag}"))
    result = fit_state(args.input_bag, args.imu_topic, args.gt_topic, harmonics,
                       args.half_width_hz, args.window_s, args.hop_s)
    sr = result["state_ranges"]
    print(f"fs {result['fs']:.2f} Hz   window {result['window_s']:.1f}s / "
         f"hop {result['hop_s']:.1f}s   airborne windows {sr['airborne_windows']}")
    print(f"harmonics (Hz): "
         + ", ".join(f"{h}x={f:.2f}" for h, f in result["harmonics_hz"].items()))
    print(f"calibrated state range: vz [{sr['vz_min']:.2f}, {sr['vz_max']:.2f}] m/s   "
         f"vh [{sr['vh_min']:.2f}, {sr['vh_max']:.2f}] m/s")
    print(SEP)
    print(f"{'channel':<10} {'h':>2}  {'A0':>9} {'beta+':>8} {'beta-':>8} "
         f"{'gamma':>8} {'r2':>6}")
    low_r2 = 0
    for name, hh in result["channels"].items():
        for h, c in hh.items():
            mark = "" if c["r2"] >= 0.10 else " (low)"
            if c["r2"] < 0.10:
                low_r2 += 1
            print(f"{name:<10} {h:>2}  {c['A0']:9.4f} {c['beta_plus']:8.3f} "
                 f"{c['beta_minus']:8.3f} {c['gamma']:8.3f} {c['r2']:6.3f}{mark}")
    print(SEP)
    verdict = ok if low_r2 == 0 else warn
    print(verdict(f"{low_r2} of {sum(len(v) for v in result['channels'].values())} "
                  "channel/harmonic fits explain <10% variance (state may not drive "
                  "that line -- check before trusting it)"))

    out_path = args.json or str(Path(args.input_bag).with_suffix("")) + "_vib_state_v3.json"
    with open(out_path, "w") as fh:
        json.dump(result, fh, indent=2)
    print(f"\nwrote {out_path}")
    return out_path
