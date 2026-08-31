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

Additionally, --harmonic-targets JSON (schema of measure_harmonic_targets / the
'vib-fit-harmonic-targets' subcommand) enables a HARMONIC-INDEXED check: each
harmonic is measured at wherever it actually sits -- the bag's OWN predicted line
position (from --rotor-npz if given, else the frequencies the targets file itself
assumes) -- rather than in a fixed 20-100/100-195 Hz bucket. This matters because the
"frequencies come from the simulated props" decision means a sim bag's aliased lines
do not sit at the same Hz as the real bag's: a fixed-bucket check silently changes
which harmonics it is even measuring once a line crosses a bucket boundary. A
bucket-agnostic total-energy check (20-195 Hz, wherever the lines are) is measured
alongside it, so realism can be judged even when the harmonic-position assumption
itself is wrong.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from scipy.signal import welch, medfilt, butter, sosfilt

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.vib_check import ok, warn, fail, hdr, SEP
from bag_tool.vib_state import state_series, DEFAULT_HARMONICS, DEFAULT_HALF_WIDTH_HZ

_TS = get_typestore(Stores.ROS2_JAZZY)
IMU_TOPIC = "/imu/data_raw"
GT_TOPIC = "/pf_geo_loc/fc_local_position"
FLOOR_FREQS = np.linspace(5, 195, 48)


# ── bag reading ────────────────────────────────────────────────────────────────
def _read(bag_path: str, imu_topic: str, gt_topic: str):
    """Returns (ti, acc, gyr, tg, pos) -- pos is (N,3) x/y/z, so callers that only need
    phase classification use pos[:,2] and callers that also need horizontal speed (the
    state envelope check) have x/y without a second pass over the bag."""
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
                         "(needed for phase classification)")
    t0 = t_i[0]
    return (np.array(t_i) - t0) / 1e9, np.array(acc), np.array(gyr), \
           (np.array(t_g) - t0) / 1e9, np.array(pos)


# ── phases / spectra ──────────────────────────────────────────────────────────
def _phase_masks(ti, tg, zg):
    """Tiled thresholds (0.3/0.3/-0.3), matching add_vibration.py's own phase masks --
    the old 0.5/0.3/-0.3 split left 0.3 < vz <= 0.5 claimed by no phase. Grading and
    synthesis must agree on what "climb" means, or a mission that spends time in that
    gap gets graded against a phase boundary the synthesiser doesn't actually use."""
    h = -(zg - zg[0])
    vz = np.gradient(h) / np.maximum(np.gradient(tg), 1e-6)
    k = max(3, int(2 / max(np.median(np.diff(tg)), 1e-3)))
    vz = np.convolve(vz, np.ones(k) / k, mode="same")
    vzi = np.interp(ti, tg, vz); hi = np.interp(ti, tg, h)
    flight = hi > 2.0
    return {"climb": flight & (vzi > 0.3),
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


# ── harmonic-indexed targets/measurement ────────────────────────────────────────
def _alias_fold(f: float, fs: float) -> float:
    """Fold a true frequency into [0, fs/2) the way discrete-time sampling actually
    does -- add_vibration.py evaluates continuous-time tones at the IMU sample times,
    so what lands in the bag is each harmonic's ALIAS, not its true frequency. A 2x/3x
    harmonic of a several-hundred-Hz shaft is typically several times fs itself."""
    fs2 = fs / 2.0
    m = f % fs
    return fs - m if m > fs2 else m


def _sim_own_harmonics(rotor_npz: str, fs: float) -> dict[int, float] | None:
    """The bag's OWN predicted harmonic frequencies, ALIASED at this bag's sample
    rate, from its rotor-speed sidecar (the same file add_vibration.py reads).
    Returns None if the sidecar has no meaningful motors-on time -- callers should
    fall back to an assumed family."""
    rs = np.load(rotor_npz)
    speeds = rs["speeds"]
    shaft = speeds.mean(axis=1) / (2 * np.pi)
    on = shaft > 50
    if on.sum() < 10:
        return None
    f0 = float(np.median(shaft[on]))
    return {h: _alias_fold(h * f0, fs) for h in (1, 2, 3)}


def measure_harmonic_targets(bag_path: str, harmonics: dict[int, float] | None = None,
                             half_width_hz: float = DEFAULT_HALF_WIDTH_HZ,
                             imu_topic: str = IMU_TOPIC, gt_topic: str = GT_TOPIC) -> dict:
    """Derive phase-resolved, HARMONIC-INDEXED targets from a reference bag: each
    harmonic measured at its own frequency (not a fixed bucket), plus a bucket-
    agnostic total-energy figure (20-195 Hz) that stays meaningful even if the
    harmonic-position assumption is later found wrong."""
    harmonics = harmonics or DEFAULT_HARMONICS
    ti, acc, gyr, tg, pos = _read(bag_path, imu_topic, gt_topic)
    fs = 1 / np.median(np.diff(ti))
    masks, _ = _phase_masks(ti, tg, pos[:, 2])
    out = {"fs": float(fs), "source": str(bag_path),
           "harmonics_hz": {str(h): f for h, f in harmonics.items()},
           "half_width_hz": half_width_hz, "phases": {}}
    for pname, mk in masks.items():
        d = {"seconds": float(mk.sum() / fs)}
        for sname, arr in (("accel", acc), ("gyro", gyr)):
            for ax, lbl in enumerate("xyz"):
                x = arr[mk, ax]
                ch = {str(h): _band(x, max(f0 - half_width_hz, 0.5), f0 + half_width_hz, fs)
                     for h, f0 in harmonics.items()}
                ch["total"] = _band(x, 20, 195, fs)
                d[f"{sname}_{lbl}"] = ch
        out["phases"][pname] = d
    return out


def measure_targets(bag_path: str, imu_topic: str = IMU_TOPIC,
                    gt_topic: str = GT_TOPIC) -> dict:
    """Derive phase-resolved targets from a reference bag (schema v2)."""
    ti, acc, gyr, tg, pos = _read(bag_path, imu_topic, gt_topic)
    fs = 1 / np.median(np.diff(ti))
    masks, _ = _phase_masks(ti, tg, pos[:, 2])
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


def _state_envelope_report(ti, tg, pos, state_path: str, sat_k: float) -> None:
    """Informational, not scored: how much of this bag's flight state falls outside the
    range day207_vib_state_v3.json was fitted over. add_vibration.py soft-saturates past
    that range rather than failing, so this is context for how much of the mission relied
    on that extrapolation -- not a pass/fail signal on its own."""
    state = json.load(open(state_path))
    SR = state["state_ranges"]
    hgt, vz, vh = state_series(ti, tg, pos)
    fly = hgt > 2.0
    if fly.sum() < 10:
        print(f"state envelope: too few airborne samples ({fly.sum()}) to report")
        return
    vzp, vzn, vhf = np.maximum(vz[fly], 0), np.minimum(vz[fly], 0), vh[fly]
    beyond = (vzp > SR["vz_max"]) | (vzn < SR["vz_min"]) | (vhf > SR["vh_max"])
    deep = (vzp > 2 * SR["vz_max"]) | (vzn < 2 * SR["vz_min"]) | (vhf > 2 * SR["vh_max"])
    print(f"state envelope ({Path(state_path).name}):")
    print(f"  calibrated : vz [{SR['vz_min']:.2f},{SR['vz_max']:.2f}] m/s   "
         f"vh_max {SR['vh_max']:.2f} m/s")
    print(f"  this bag   : vz [{vz[fly].min():.2f},{vz[fly].max():.2f}] m/s   "
         f"vh_max {vhf.max():.2f} m/s")
    print(f"  {beyond.sum()}/{fly.sum()} airborne samples ({100*beyond.mean():.1f}%) exceed "
         f"the calibrated range -- soft-saturated extrapolation there (ceiling "
         f"{sat_k:.1f}x the edge), not a direct measurement. {deep.sum()} "
         f"({100*deep.mean():.1f}%) are past 2x the edge.")


def _harmonic_report(ti, acc, gyr, tg, pos, masks, fs, harmonic_targets_path: str,
                     rotor_npz: str | None) -> dict:
    """Harmonic-indexed check: grade each harmonic where THIS bag actually put it
    (own rotor sidecar if given, else the assumed family the targets themselves
    used), plus a bucket-agnostic total-energy figure. Returns ok/warn/fail counts
    so the caller can fold them into the overall verdict."""
    htgt = json.load(open(harmonic_targets_path))
    own = _sim_own_harmonics(rotor_npz, fs) if rotor_npz else None
    if own is None:
        own = {int(k): v for k, v in htgt["harmonics_hz"].items()}
        src = "assumed family (targets file; no --rotor-npz given or motors never on)"
    else:
        src = f"this bag's own rotor sidecar ({rotor_npz})"
    hw = htgt.get("half_width_hz", DEFAULT_HALF_WIDTH_HZ)
    print(hdr(f"\n[harmonic-indexed]  measured at {src}"))
    print("  " + ", ".join(f"{h}x={f:.2f}Hz" for h, f in sorted(own.items())))
    counts = {"ok": 0, "warn": 0, "fail": 0}
    for pname, mk in masks.items():
        if pname not in htgt["phases"] or mk.sum() < fs * 5:
            continue
        print(f"  [{pname}]")
        for sname, arr, tiny in (("accel", acc, 0.02), ("gyro", gyr, 0.01)):
            for ax, lbl in enumerate("xyz"):
                x = arr[mk, ax]
                tgt = htgt["phases"][pname][f"{sname}_{lbl}"]
                cells = []
                for h, f0 in sorted(own.items()):
                    lo, hi_ = max(f0 - hw, 0.5), min(f0 + hw, fs / 2 - 0.5)
                    sim = _band(x, lo, hi_, fs)
                    g, txt = _grade(sim, tgt[str(h)], tiny)
                    counts[g] += 1
                    mark = {"ok": "✔", "warn": "⚠", "fail": "✘"}[g]
                    cells.append(f"{h}x {sim:6.3f}/{tgt[str(h)]:6.3f} {mark}{txt}")
                sim_tot = _band(x, 20, 195, fs)
                g, txt = _grade(sim_tot, tgt["total"], tiny)
                counts[g] += 1
                mark = {"ok": "✔", "warn": "⚠", "fail": "✘"}[g]
                cells.append(f"tot {sim_tot:6.3f}/{tgt['total']:6.3f} {mark}{txt}")
                print(f"    {sname}_{lbl}:  " + "  ".join(cells))
    return counts


def run(args) -> None:
    targets = json.load(open(args.targets))
    print(hdr(f"vib-verify: {args.input_bag}"))
    print(f"targets   : {args.targets} (source: {targets.get('source', 'n/a')})")
    print(SEP)
    ti, acc, gyr, tg, pos = _read(args.input_bag, args.imu_topic, args.gt_topic)
    fs = 1 / np.median(np.diff(ti))
    masks, hi = _phase_masks(ti, tg, pos[:, 2])
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
        # This is a TIME/HEIGHT proxy for "motors off", not a direct motor-speed check --
        # add_vibration.py now gates tones on the rotor sidecar's own "spinning" state, not
        # on this proxy, so a mission that arms and spools up before the first 30s/1m are
        # up (common) will show a small, EXPECTED non-zero reading here, not a bug.
        print(f"ground-rest accZ  : {acc[g, 2].std():.3f} m/s² "
              "(should be sensor-noise level if motors are truly off in this window; "
              "see caveat above if the mission arms/spools up early)")
    if getattr(args, "harmonic_targets", None):
        print(SEP)
        hcounts = _harmonic_report(ti, acc, gyr, tg, pos, masks, fs,
                                   args.harmonic_targets, getattr(args, "rotor_npz", None))
        for k in counts:
            counts[k] += hcounts[k]
    if getattr(args, "state", None):
        print(SEP)
        _state_envelope_report(ti, tg, pos, args.state, args.sat_k)
    print(SEP)
    verdict = ok if counts["fail"] == 0 and counts["warn"] <= 6 else \
        (warn if counts["fail"] <= 2 else fail)
    print(verdict(f"{counts['ok']} ok / {counts['warn']} warn / "
                  f"{counts['fail']} fail band checks"))
