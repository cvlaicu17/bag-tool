"""vib-realism: is this bag's IMU vibration structurally realistic?

A REFERENCE-FREE realism gate for (mostly simulated) mission bags. Unlike vib-verify,
which asks "does this bag match one specific real reference flight?" (right question at
calibration time, wrong one at mission-acceptance time), this asks "does this bag vibrate
the way real drones vibrate?" -- structural invariants only, with thresholds calibrated so
that EVERY real bag in the corpus passes and an unvibed or badly-vibed sim bag fails.

The checks map onto the layers add_vibration.py generates, and come in THREE TIERS. The
tier is the point: a mission is rejected only for things that mean "this bag is broken",
never for texture a VIO estimator barely sees.

 FAIL   (gate)   -- C5.finite / C5.magnitude   values finite and physically representable
                    C7.injected                the noise the vibration PASS added, as a VIO
                                               estimator sees it: (vib - raw) above 5 Hz,
                                               averaged over one 50 ms camera pre-integration
                                               interval, RMS. Bounded by the loudest real
                                               flight's TOTAL -- "did we add too much noise for
                                               a trajectory to survive". Sim only (needs raw).
                    C7.noise_eff (warn)        the same measure on the finished bag, real or
                                               sim alike; on sim it includes the simulated
                                               flight's own 5-15 Hz controller dynamics (raw
                                               fleet1 bags already carry 0.17-0.34 m/s^2 of
                                               it, the pass adds ~0.02) -- so it warns.
                    C7.snr (report)            ratio to the 0.1-5 Hz body-motion signal, dB.
                    C5.level                   vibration-band energy inside the loose real
                                               corpus range: catches a forgotten or
                                               150x-too-quiet pass AND a runaway floor
                    C5.lowband                 the < 5 Hz body-dynamics band -- the part
                                               VIO treats as truth -- is untouched vs the
                                               raw bag (sim only; needs --raw / auto)
ADVISE (warn)   -- C3.exist                   spectral lines stand above the floor. NOT the
                                               absence detector it was meant to be: a raw
                                               PAS bag carries its own narrow 23-27 Hz
                                               simulator line (7 dB, a tenth of a tone's
                                               energy) that the census locks onto, so raw
                                               passes it and a smeared genuine tone can
                                               fail it. C5.level is the absence detector.
                    C2.gaussian / C2.breathe   floor is heavy-tailed and breathes
                    C3.width / C3.responsive   line width sane; amplitude follows state
                    C4.contrast                some channel pairs locked, some not
                    These are texture: real, measurable, and they DID surface genuine L2
                    defects once -- but a mission that trips one is still usable, so they
                    advise instead of block.
 REPORT (inform) -- C1.* white level/flatness, C2.exists, C3.cluster / breathe / skew,
                    C4.gyro_ratio, C6 whitening. Either sanity bands, or metrics shown
                    to be unstable (C2.exists swings 15x between two spans of ONE real
                    flight) or non-discriminating (C6). Printed with their real-corpus
                    range so a human can read them; never scored.

Deliberately NOT checked (do not add back): exact line frequencies or the exact
harmonic family (drone-specific, and sim frequencies come from sim physics by
decision); amplitudes vs any particular reference bag; integer relationships between
lines (aliasing scrambles them); explicit H(f) (unidentifiable from mission data);
anything about the RPM law; the simulated flight controller's throttle activity (an
earlier C0.shaft_steady check compared the sim's rotor sidecar against a real-aircraft
number that cannot be measured without motor telemetry -- it explained failures away
instead of gating, and add_vibration.py now smooths the shaft it phase-integrates, so
the smear it excused is gone at the source); motors-off silence (sim missions never
have the motors off, so it could only ever skip).

Segmentation policy: NO legacy climb/cruise/descent phases. Where a quasi-stationary
stretch is needed (spectral surgery), the tool uses the longest contiguous airborne run
whose |vertical speed| stays below a data-driven quantile of the mission's own |vz|.
State-responsiveness is measured over the whole airborne record with no segmentation.

Isolation pipeline (validated on real day20.7 before this was written -- the whitened
residual there comes out at spectral flatness 0.981, kurtosis +0.20):
  census_lines -> excise_lines (FFT surgery; energy bookkeeping is exact) ->
  split_floor (white level read where the carpet has gone flat) -> whiten.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
from scipy.signal import welch, medfilt, find_peaks, butter, sosfilt, coherence
from scipy import stats as sstats

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.vib_check import ok, warn, fail, hdr, SEP

_TS = get_typestore(Stores.ROS2_JAZZY)
IMU_TOPIC = "/imu/data_raw"
# GT candidates tried in order; realism metrics degrade gracefully with no GT at all
# (only the airborne gate and the steady-block choice use it).
GT_CANDIDATES = (
    ("/pf_geo_loc/fc_local_position",
     ("geometry_msgs/msg/PoseStamped", "geometry_msgs/msg/PointStamped")),
    ("/m300/rtk/fix", ("sensor_msgs/msg/NavSatFix",)),
)

VIB_BAND = (20.0, 195.0)
WIN_S, HOP_S = 2.0, 1.0

# Initial thresholds, seeded from real day20.7 measurements with generous margins,
# then CALIBRATED on the full real-bag corpus (every real bag must pass; a threshold a
# real bag violates is a wrong threshold). Values live here rather than a config file
# so the calibration history is versioned with the code; --profile overrides for
# experiments.
DEFAULT_PROFILE = {
    # CALIBRATED on the 8-bag real altair corpus (Day20.1/2/3/5/6/7/8 + Day32), each
    # read twice (trimmed _jazzy conversion + full original), with v2/v3 sim bags and
    # purpose-built controls as negatives. Rule: every real bag must PASS the FAIL tier
    # with no warns; a threshold a real bag violates is a wrong threshold. Observed
    # ranges are quoted as [real | sim] so the next person can see the margin.
    # ---- FAIL tier ----
    "accel_abs_max": 250.0,
    "gyro_abs_max": 50.0,
    # vibration-band (20-195 Hz) std over the airborne record, main accel axis and
    # loudest gyro axis. Loose by design: 0.3x the quietest real bag to 3x the loudest.
    # Catches a forgotten/uncalibrated vibration pass (a stale test bag once measured
    # 150x too quiet) and a runaway floor, both of which the old white-level ratio
    # (C2.exists) flagged so unstably it had to be demoted.
    "band_std_accel": [0.35, 6.5],      # m/s^2  [real 1.15-2.12 | raw unvibed 0.11]
    "band_std_gyro":  [0.012, 0.18],    # rad/s  [real 0.040-0.060 | raw 0.006 | sim 0.037]
    # C7: noise that survives a 50 ms pre-integration window (content > 5 Hz, box-
    # averaged over preint_s, RMS over the airborne record).
    #   real corpus TOTAL: accel 0.088-0.223 m/s^2, gyro 0.0135-0.0285 rad/s
    #   fleet1 sim TOTAL:  accel 0.19-0.35 (raw flight alone 0.17-0.34!), gyro 0.012
    #   fleet1 sim INJECTED by the pass: accel 0.079-0.090, gyro 0.0105-0.0112
    # FAIL if what the pass ADDED exceeds the loudest real flight's total (1.0x);
    # WARN if the finished bag's total exceeds 1.5x it -- on sim that excess is the
    # simulated flight controller's own 5-15 Hz activity, not the vibration pass.
    "preint_s": 0.05,
    "injected_accel_max": 0.23,       # m/s^2   (fail tier, sim only)
    "injected_gyro_max": 0.029,       # rad/s
    "noise_eff_accel_max": 0.34,      # m/s^2   (warn tier, real and sim)
    "noise_eff_gyro_max": 0.043,      # rad/s
    # injected 0.05-4.5 Hz RMS relative to the finished bag's own white-noise RMS in
    # that band (its 178-198 Hz density). Sim only. NOT relative to the raw bag's
    # dynamics: the sim flies laterally far smoother than any real aircraft, so a
    # 0.013 m/s^2 leak (below the hiss) read as 0.5x of the raw's lateral motion.
    # 0.5 = the pass adds at most a quarter of the hiss power to the truth band.
    "lowband_leak_max": 0.5,
    # ---- WARN tier ----
    # C3.exist: real 5.1-9.0 dB; smoothed-shaft sim ~5.5 dB but often measured on the
    # simulator's own narrow 23-27 Hz line (raw d01: 7.4 dB, 40 lines -- so the raw
    # unvibed bag PASSES this; C5.level is what catches absence, at 0.111 vs 0.30).
    "lines_min_prom_db": 3.5,
    "lines_min_count": 5,              # real 21-40, sim 15-30
    "floor_kurtosis": [0.15, 8.0],     # [real 0.36-4.59 | pre-fix sim -0.02-0.05]
    "floor_breathe_cv_min": 0.13,      # [real 0.18-0.36 | pre-fix sim 0.06-0.09]
    "line_width_hz": [0.15, 9.0],      # [real 0.78-8.46 | unsmoothed-shaft sim 8-15]
    "tone_p90_p10_min": 2.20,          # [real 2.38-3.84 | sim 1.47-3.81]
    "coh_high_min": 0.60,              # [real max-pair 0.79-0.99 | smeared sim 0.10]
    "coh_low_max": 0.85,               # [real min-pair 0.07-0.77] Day32 couples all
                                        # channels at 0.77 -- only near-total lock is odd
    # ---- REPORT tier (ranges shown, never scored) ----
    "white_flatness_min": 0.15,        # [real 0.21-0.95]
    "white_accel_density": [1e-6, 3e-2],
    "white_gyro_density":  [1e-11, 1e-4],
    "colored_ratio": [1.30, 15.0],     # [real 1.47-94.5] segment-sensitive, see docstring
    "tone_sigma_log": [0.30, 1.5],     # [real 0.337-0.536] measured on a floor-dominated
    "tone_tau_s": [0.3, 20.0],         #   band, so it cannot isolate the tone modulation
    "gyro_accel_ratio": [3e-4, 0.5],
    "whiten_flatness_min": 0.75,       # [real 0.947-0.986 | sim 0.961-0.985] no contrast
    "whiten_kurtosis": [-1.0, 4.0],
    # segmentation
    "min_height_m": 2.0,
    "min_steady_s": 20.0,
}

# Which tier each check scores in. A check whose bound is violated gets this verdict;
# "report" checks are never scored at all (their value is printed with the range).
TIER = {
    "C5.finite": "fail", "C5.magnitude": "fail", "C5.level": "fail",
    "C5.lowband": "fail", "C7.injected": "fail", "SEG": "fail",
    "C7.noise_eff": "warn", "C7.snr": "report",
    "C3.exist": "warn", "C2.gaussian": "warn", "C2.breathe": "warn", "C3.width": "warn",
    "C3.responsive": "warn", "C4.contrast": "warn",
    "C1.white_flat": "report", "C1.white_level": "report", "C1.white_level_gyro": "report",
    "C2.exists": "report", "C3.cluster": "report", "C3.breathe": "report",
    "C3.skew": "report", "C4.gyro_ratio": "report", "C6.flat": "report",
}


# ── reading ──────────────────────────────────────────────────────────────────
def _read(bag_path: str, imu_topic: str = IMU_TOPIC, gt_topic: str | None = None):
    """Returns (ti, acc, gyr, tg, hgt, gt_name). GT is optional: hgt is None when no
    candidate topic exists -- callers degrade to energy-based gating."""
    t_i, acc, gyr = [], [], []
    t_g, hraw = [], []
    gt_name = None
    with Reader(Path(bag_path)) as reader:
        topics = {c.topic: c.msgtype for c in reader.connections}
        if gt_topic:
            cands = [(gt_topic, None)]
        else:
            cands = [(t, m) for t, m in GT_CANDIDATES]
        for t, _ in cands:
            if t in topics:
                gt_name = t
                break
        want = [imu_topic] + ([gt_name] if gt_name else [])
        conns = [c for c in reader.connections if c.topic in want]
        for conn, ts, raw in reader.messages(connections=conns):
            if conn.topic == imu_topic:
                msg = _TS.deserialize_cdr(raw, conn.msgtype)
                t_i.append(ts)
                acc.append((msg.linear_acceleration.x, msg.linear_acceleration.y,
                            msg.linear_acceleration.z))
                gyr.append((msg.angular_velocity.x, msg.angular_velocity.y,
                            msg.angular_velocity.z))
            else:
                msg = _TS.deserialize_cdr(raw, conn.msgtype)
                t_g.append(ts)
                if conn.msgtype.endswith("PoseStamped"):
                    hraw.append(-msg.pose.position.z)          # NED -> up-positive
                elif conn.msgtype.endswith("PointStamped"):
                    hraw.append(-msg.point.z)
                else:                                           # NavSatFix
                    hraw.append(msg.altitude)
    if not t_i:
        raise SystemExit(f"ERROR: no {imu_topic} messages in {bag_path}")
    t0 = t_i[0]
    ti = (np.array(t_i) - t0) / 1e9
    if t_g:
        tg = (np.array(t_g) - t0) / 1e9
        hgt = np.array(hraw, float)
        hgt -= np.median(hgt[: max(1, len(hgt) // 20)])         # zero at start
    else:
        tg, hgt = None, None
    return ti, np.array(acc), np.array(gyr), tg, hgt, gt_name


def _read_imu_only(bag_path: str, imu_topic: str = IMU_TOPIC):
    """(t_ns, acc, gyr) of one IMU topic -- for the raw (pre-vibration) bag."""
    t_i, acc, gyr = [], [], []
    with Reader(Path(bag_path)) as reader:
        conns = [c for c in reader.connections if c.topic == imu_topic]
        for conn, ts, raw in reader.messages(connections=conns):
            msg = _TS.deserialize_cdr(raw, conn.msgtype)
            t_i.append(ts)
            acc.append((msg.linear_acceleration.x, msg.linear_acceleration.y,
                        msg.linear_acceleration.z))
            gyr.append((msg.angular_velocity.x, msg.angular_velocity.y,
                        msg.angular_velocity.z))
    return np.array(t_i), np.array(acc), np.array(gyr)


def guess_raw_bag(bag_path: str) -> str | None:
    """A vibed PAS bag is <raw>_vib or <raw>_vib_<TAG> next to its raw. Returns the raw
    path if it exists, else None (real bags have no raw -- the check skips)."""
    p = Path(bag_path.rstrip("/"))
    name = p.name
    if "_vib" not in name:
        return None
    raw = p.with_name(name[: name.index("_vib")])
    return str(raw) if raw.exists() else None


def _smooth(x, n):
    n = max(3, int(n) | 1)
    return np.convolve(x, np.ones(n) / n, mode="same")


def segments(ti, acc, tg, hgt, fs, profile):
    """(active_mask, steady_slice, vz_on_imu_or_None).

    active: airborne if GT exists (height > min_height_m), else windows whose
    vibration-band energy clears half the p90 (motors clearly running).
    steady: longest contiguous ACTIVE run with |vz| <= median(|vz| over active) --
    a data-driven quantile, deliberately NOT the legacy named phases.
    """
    if hgt is not None and tg is not None and len(tg) > 10:
        hi = np.interp(ti, tg, hgt)
        active = hi > profile["min_height_m"]
        dtg = max(np.median(np.diff(tg)), 1e-3)
        vz = _smooth(np.gradient(hgt) / np.maximum(np.gradient(tg), 1e-6), 2.0 / dtg)
        vzi = np.interp(ti, tg, vz)
    else:
        sos = butter(4, list(VIB_BAND), btype="band", fs=fs, output="sos")
        e = _smooth(np.abs(sosfilt(sos, acc[:, 2] - acc[:, 2].mean())), int(2 * fs))
        active = e > 0.5 * np.percentile(e, 90)
        vzi = None

    if active.sum() < fs * profile["min_steady_s"]:
        return active, None, vzi

    def longest_run(mask):
        d = np.diff(mask.astype(int))
        starts = np.where(d == 1)[0] + 1
        ends = np.where(d == -1)[0] + 1
        if mask[0]:
            starts = np.r_[0, starts]
        if mask[-1]:
            ends = np.r_[ends, len(mask)]
        if len(starts) == 0:
            return None
        runs = sorted(zip(starts, ends), key=lambda ab: ab[0] - ab[1])
        return int(runs[0][0]), int(runs[0][1])

    # Adaptive |vz| quantile: start selective, loosen until a long-enough contiguous
    # run exists. A fixed quantile (or the legacy fixed thresholds) fragments flights
    # whose vz hovers around the cut -- a 30s scrap makes every spectral estimate
    # downstream unreliable. Target 60s; accept min_steady_s as the floor.
    target = max(60.0, profile["min_steady_s"])
    best = longest_run(active)
    if vzi is not None and best is not None:
        for q in (50, 60, 70, 80, 90):
            thr = np.percentile(np.abs(vzi[active]), q)
            r = longest_run(active & (np.abs(vzi) <= thr))
            if r is not None and (r[1] - r[0]) >= fs * target:
                best = r
                break
        else:
            # no quantile gives the target; take the longest run at q=70 or fall
            # back to the longest active run
            thr = np.percentile(np.abs(vzi[active]), 70)
            r = longest_run(active & (np.abs(vzi) <= thr))
            if r is not None and (r[1] - r[0]) > (best[1] - best[0]) * 0.5:
                best = r
    if best is None or (best[1] - best[0]) < fs * profile["min_steady_s"]:
        return active, None, vzi
    return active, best, vzi


# ── isolation pipeline ────────────────────────────────────────────────────────
def census_lines(x, fs, min_prom_db=3.0):
    """Detect spectral lines by height above the local (median-filtered) floor.
    Returns [(freq, prominence_db)] sorted by prominence, and the welch grid."""
    f, p = welch(x - x.mean(), fs=fs, nperseg=4096)
    lp = np.log(np.maximum(p, 1e-300))
    med = medfilt(lp, int(5 / (f[1] - f[0])) | 1)
    pk, _ = find_peaks(lp - med, prominence=min_prom_db * np.log(10) / 10)
    lines = [(float(f[i]), float((lp[i] - med[i]) / np.log(10) * 10))
             for i in pk if VIB_BAND[0] <= f[i] <= VIB_BAND[1]]
    lines.sort(key=lambda t: -t[1])
    return lines, (f, p)


def excise_lines(x, fs, line_freqs, hw=3.0):
    """FFT surgery: cut +-hw around each line. Returns (tone_series, floor_series,
    excised_mask_fn) -- energy bookkeeping is exact by Parseval."""
    x = x - x.mean()
    N = len(x)
    X = np.fft.rfft(x)
    fr = np.fft.rfftfreq(N, 1 / fs)
    mask = np.zeros(len(fr), bool)
    for fq in line_freqs:
        mask |= (fr >= fq - hw) & (fr <= fq + hw)
    tone = np.fft.irfft(np.where(mask, X, 0), n=N)
    floor = np.fft.irfft(np.where(mask, 0, X), n=N)

    def excised(fgrid):
        m = np.zeros(len(fgrid), bool)
        for fq in line_freqs:
            m |= (fgrid >= fq - hw - 1.0) & (fgrid <= fq + hw + 1.0)
        return m
    return tone, floor, excised


def split_floor(floor_series, fs, excised):
    """White level read where the carpet has flattened (line-free 178-198 Hz).
    Returns (f, p, white_level, colored_ratio_lowband)."""
    f, p = welch(floor_series - floor_series.mean(), fs=fs, nperseg=4096)
    holes = excised(f)
    hib = (f >= 178) & (f <= 198) & ~holes
    if hib.sum() < 5:
        hib = (f >= 170) & (f <= 199) & ~holes
    white = float(np.median(p[hib])) if hib.any() else float(np.median(p[(f >= 170)]))
    lob = (f >= 25) & (f <= 45) & ~holes
    if lob.sum() < 5:                       # excision holes swallowed the band
        lob = (f >= 20) & (f <= 70) & ~holes
    ratio = float(np.median(p[lob]) / max(white, 1e-300)) if lob.any() else np.nan
    return f, p, white, ratio


def whiten_test(floor_series, fs, excised):
    """Divide the floor by its own smooth envelope; measure flatness and kurtosis of
    what's left, over SURVIVING bins only (measuring inside the surgery holes was a
    bug once already -- see the session log)."""
    f, p = welch(floor_series - floor_series.mean(), fs=fs, nperseg=4096)
    holes = excised(f)
    keep = ~holes
    env = np.interp(f, f[keep], medfilt(p, 31)[keep])
    b = (f >= 25) & (f <= 190) & keep
    if b.sum() < 20:
        return np.nan, np.nan
    ratio = p[b] / np.maximum(env[b], 1e-300)
    flatness = float(np.exp(np.mean(np.log(ratio))) / np.mean(ratio))
    # time-domain kurtosis of the whitened series, surgery holes filled with matched
    # white noise so the statistics aren't hole-dominated
    N = len(floor_series)
    X = np.fft.rfft(floor_series - floor_series.mean())
    fr = np.fft.rfftfreq(N, 1 / fs)
    envF = np.interp(fr, f, env)
    holesF = excised(fr)
    rng = np.random.default_rng(0)
    fill = (rng.standard_normal(len(fr)) + 1j * rng.standard_normal(len(fr))) \
        * np.sqrt(np.maximum(envF, 0) * fs * N / 4)
    Xw = np.where(holesF, fill, X) / np.sqrt(np.maximum(envF * fs * N / 2, 1e-300))
    wt = np.fft.irfft(np.where((fr >= 20) & (fr <= 195), Xw, 0), n=N)
    sd = wt.std()
    kurt = float(sstats.kurtosis(wt / sd)) if sd > 0 else np.nan
    return flatness, kurt


def windowed_band_amp(x, fs, f_lo, f_hi, win_s=WIN_S, hop_s=HOP_S, mask=None):
    """Per-window band amplitude (Hann periodogram integral), optionally only for
    windows whose center is inside mask."""
    nwin, nhop = int(win_s * fs), int(hop_s * fs)
    if len(x) < 2 * nwin:
        return np.empty(0)
    w = np.hanning(nwin)
    freqs = np.fft.rfftfreq(nwin, 1 / fs)
    b = (freqs >= f_lo) & (freqs <= f_hi)
    out = []
    for s in range(0, len(x) - nwin, nhop):
        c = s + nwin // 2
        if mask is not None and not mask[c]:
            continue
        seg = (x[s:s + nwin] - x[s:s + nwin].mean()) * w
        psd = (np.abs(np.fft.rfft(seg)) ** 2) / (np.sum(w ** 2) * fs) * 2.0
        out.append(float(np.sqrt(np.trapezoid(psd[b], freqs[b]))))
    return np.array(out)


def line_width_3db(x, fs, f0, span=8.0):
    f, p = welch(x - x.mean(), fs=fs, nperseg=8192)
    b = (f >= f0 - span) & (f <= f0 + span)
    if b.sum() < 8:
        return np.nan
    fb, pb = f[b], p[b]
    pk = pb.max()
    above = pb >= pk / 2
    return float(fb[above][-1] - fb[above][0]) if above.sum() > 1 else np.nan


# ── the checklist ─────────────────────────────────────────────────────────────
def _in(v, lohi):
    return lohi[0] <= v <= lohi[1]


def analyze(bag_path: str, imu_topic: str = IMU_TOPIC, gt_topic: str | None = None,
            profile: dict | None = None, raw_bag: str | None = None) -> dict:
    """Run every realism check. Returns {"checks": [...], "info": {...}} where each
    check is {id, name, value, verdict ('pass'|'warn'|'fail'|'report'|'skip'), note}.
    The verdict of a violated check is its TIER; report-tier checks are never scored."""
    P = dict(DEFAULT_PROFILE, **(profile or {}))
    ti, acc, gyr, tg, hgt, gt_name = _read(bag_path, imu_topic, gt_topic)
    fs = 1.0 / np.median(np.diff(ti))
    active, steady, vzi = segments(ti, acc, tg, hgt, fs, P)

    checks: list[dict] = []
    info = {"bag": str(bag_path), "fs": float(fs), "gt_topic": gt_name,
            "active_s": float(active.sum() / fs),
            "steady_s": float((steady[1] - steady[0]) / fs) if steady else 0.0,
            "raw_bag": raw_bag}

    def add(cid, name, value, ok, note=""):
        """ok: True/False (scored by tier), or a verdict string ('skip'/'report')."""
        tier = TIER.get(cid, "warn")
        if isinstance(ok, str):
            verdict = ok
        elif tier == "report":
            verdict = "report"
        else:
            verdict = "pass" if ok else tier
        checks.append({"id": cid, "name": name, "value": value,
                       "verdict": verdict, "note": note})

    # C5 first (cheap, and everything else assumes finite data)
    finite = bool(np.isfinite(acc).all() and np.isfinite(gyr).all())
    add("C5.finite", "all samples finite", finite, finite)
    amax, gmax = float(np.abs(acc).max()), float(np.abs(gyr).max())
    add("C5.magnitude", "physically representable magnitudes",
        {"accel_max": round(amax, 1), "gyro_max": round(gmax, 2)},
        amax <= P["accel_abs_max"] and gmax <= P["gyro_abs_max"])
    if not finite:
        return {"checks": checks, "info": info}

    sos = butter(4, list(VIB_BAND), btype="band", fs=fs, output="sos")

    # ---- C5.level: is there a vibration layer at all, and is it sane in size? ----
    # Band std over the airborne record. Reference-free in spirit (a loose corpus band,
    # not a match), but it is THE check that catches an unvibed or uncalibrated bag.
    if active.sum() > fs * 10:
        a_std = [float(sosfilt(sos, acc[:, j] - acc[:, j].mean())[active].std()) for j in range(3)]
        g_std = [float(sosfilt(sos, gyr[:, j] - gyr[:, j].mean())[active].std()) for j in range(3)]
        a_lvl, g_lvl = max(a_std), max(g_std)
        add("C5.level", "vibration-band energy inside the real corpus range",
            {"accel_band_std": round(a_lvl, 3), "gyro_band_std": round(g_lvl, 4)},
            _in(a_lvl, P["band_std_accel"]) and _in(g_lvl, P["band_std_gyro"]),
            "an unvibed / uncalibrated bag is far too quiet; a runaway floor far too loud")
    else:
        add("C5.level", "vibration-band energy inside the real corpus range", None, "skip",
            "no airborne record")

    # ---- raw (pre-vibration) bag, sim only: shared by C7.injected and C5.lowband ----
    racc = rgyr = None
    raw_note = None
    if raw_bag:
        try:
            rt, racc, rgyr = _read_imu_only(raw_bag, imu_topic)
            if len(rt) != len(ti):
                raw_note = (f"IMU message counts differ (raw {len(rt)}, vib {len(ti)}) -- "
                            "not the same flight, cannot align")
                racc = rgyr = None
        except Exception as e:  # unreadable raw is a skip, not a verdict
            raw_note = f"raw bag unreadable: {e}"
    else:
        raw_note = "no raw bag (real recording, or pass --raw)"

    # ---- C7: signal-to-noise as the estimator sees it (real and sim alike) ----
    # signal = 0.1-5 Hz body motion; noise = > 5 Hz content after the same 50 ms box
    # average a 20 Hz camera pre-integration applies (sinc^2 weighting: a 43 Hz tone is
    # cut ~15x, the 5-15 Hz floor passes almost whole -- that is what a trajectory feels).
    # With the raw bag, the same measure on (vib - raw) isolates what the vibration PASS
    # added -- the direct answer to "did we add too much". Measured on fleet1: the raw
    # simulated flight already carries 0.17-0.34 m/s^2 of this noise from its own 5-15 Hz
    # controller dynamics, and the pass adds ~0.01-0.02 on top.
    if active.sum() > fs * 10:
        N = len(ti)
        fr = np.fft.rfftfreq(N, d=1.0 / fs)
        nbox = max(1, int(round(P["preint_s"] * fs)))
        box = np.ones(nbox) / nbox
        hi_sel = fr > 5.0
        sig_sel = (fr >= 0.1) & (fr <= 5.0)

        def _eff(x):
            X = np.fft.rfft(x - x.mean())
            hi = np.fft.irfft(np.where(hi_sel, X, 0), n=N)
            return float(np.convolve(hi, box, mode="same")[active].std())

        def _sig(x):
            X = np.fft.rfft(x - x.mean())
            return float(np.fft.irfft(np.where(sig_sel, X, 0), n=N)[active].std())

        tot, sig, inj = {}, {}, {}
        for lbl, V, R in (("accel", acc, racc), ("gyro", gyr, rgyr)):
            tot[lbl] = max(_eff(V[:, j]) for j in range(3))
            sig[lbl] = float(np.sqrt(np.mean([_sig(V[:, j]) ** 2 for j in range(3)])))
            if R is not None:
                inj[lbl] = max(_eff(V[:, j] - R[:, j]) for j in range(3))
        add("C7.noise_eff", "noise surviving a 50 ms pre-integration window",
            {"accel": round(tot["accel"], 4), "gyro": round(tot["gyro"], 5)},
            tot["accel"] <= P["noise_eff_accel_max"] and tot["gyro"] <= P["noise_eff_gyro_max"],
            "louder than every real flight the estimator copes with; on a sim bag this "
            "includes the simulated flight's own 5-15 Hz controller dynamics (see "
            "C7.injected for the vibration pass's share)")
        if inj:
            add("C7.injected", "pre-integration noise ADDED by the vibration pass",
                {"accel": round(inj["accel"], 4), "gyro": round(inj["gyro"], 5)},
                inj["accel"] <= P["injected_accel_max"] and inj["gyro"] <= P["injected_gyro_max"],
                "the pass alone adds more surviving noise than the loudest real flight "
                "carries in total -- too much for a trajectory")
        else:
            add("C7.injected", "pre-integration noise ADDED by the vibration pass", None,
                "skip", raw_note or "")
        add("C7.snr", "signal-to-noise after pre-integration (dB)",
            {"accel_db": round(float(20 * np.log10(max(sig["accel"], 1e-12) / max(tot["accel"], 1e-12))), 1),
             "gyro_db": round(float(20 * np.log10(max(sig["gyro"], 1e-12) / max(tot["gyro"], 1e-12))), 1),
             "signal_accel": round(sig["accel"], 3), "signal_gyro": round(sig["gyro"], 4)},
            True, "signal depends on how the flight was flown (a gentle sim route has "
                  "less of it), so the ratio informs and C7.injected gates")
    else:
        add("C7.noise_eff", "noise surviving a 50 ms pre-integration window", None, "skip",
            "no airborne record")

    # ---- C5.lowband: the truth band must be untouched (sim only, needs the raw bag) --
    # VIO integrates the < 5 Hz content as the actual motion. The vibration pass is
    # additive and by construction writes nothing there (L2 zeroes < 5 Hz, tones sit at
    # >= 20 Hz aliased); this measures that it really didn't -- on the actual output.
    if racc is not None:
        # brick-wall band energies from a long-window PSD (a 4th-order Butterworth
        # "5 Hz lowpass" let the injected 5-10 Hz floor through its skirt)
        lo_b, hi_b = 0.05, 4.5
        leaks = {}
        for lbl, V, R in (("accel", acc, racc), ("gyro", gyr, rgyr)):
            for j, ax in enumerate("xyz"):
                d = V[:, j] - R[:, j]
                fw, pdiff = welch(d - d.mean(), fs=fs, nperseg=16384)
                # the hiss the estimator actually sees is the FINISHED bag's high-band
                # level (the pass tops the sim's own L4 up to the real floor)
                _, pvib = welch(V[:, j] - V[:, j].mean(), fs=fs, nperseg=16384)
                b = (fw >= lo_b) & (fw <= hi_b)
                inj_rms = float(np.sqrt(np.trapezoid(pdiff[b], fw[b])))
                wb = (fw >= 178) & (fw <= 198)
                hiss = float(np.sqrt(np.median(pvib[wb]) * (hi_b - lo_b)))
                leaks[f"{lbl}_{ax}"] = inj_rms / max(hiss, 1e-15)
        worst = max(leaks, key=leaks.get)
        add("C5.lowband", "< 5 Hz body-dynamics band untouched vs raw",
            {"worst": f"{worst}={leaks[worst]:.3f}"},
            leaks[worst] <= P["lowband_leak_max"],
            "injected 0.05-4.5 Hz RMS over the sensor's own white-noise RMS in "
            "that band; the pass must not write into the band VIO treats as truth")
    else:
        add("C5.lowband", "< 5 Hz body-dynamics band untouched vs raw", None, "skip", raw_note)

    if steady is None:
        add("SEG", "steady analysis block", None, False,
            f"no contiguous low-|vz| active run >= {P['min_steady_s']}s -- "
            "cannot run spectral checks")
        return {"checks": checks, "info": info}
    s0, s1 = steady

    # main accel channel: the one with the most vibration-band energy (data-driven,
    # not assumed to be z)
    ax_energy = [sosfilt(sos, acc[s0:s1, j] - acc[s0:s1, j].mean()).std() for j in range(3)]
    main = int(np.argmax(ax_energy))
    info["main_accel_axis"] = "xyz"[main]
    x = acc[s0:s1, main].astype(float)

    # ---- isolation pipeline on the steady block ----
    # Census the lines on the SHARPEST 60s sub-window of the steady block: over a
    # long block the rotor speed wanders enough to smear the lines into each other
    # (measured: 3.2 Hz width on a calm 164s run vs 8.4 Hz over a 277s mixed run on
    # the same real flight), hiding the multi-line cluster structure the checks look
    # for. Data-driven: try each 60s window, keep the one whose top line stands
    # proudest. Floor/whitening still use the full block (more averages).
    sub_n = int(60 * fs)
    best_lines, best_off = None, 0
    if len(x) > sub_n:
        for off in range(0, len(x) - sub_n, sub_n // 2):
            cand, _ = census_lines(x[off:off + sub_n], fs, min_prom_db=3.0)
            if cand and (best_lines is None or cand[0][1] > best_lines[0][1]):
                best_lines, best_off = cand, off
    lines = best_lines if best_lines else census_lines(x, fs, min_prom_db=3.0)[0]
    x_sharp = x[best_off:best_off + sub_n] if len(x) > sub_n else x
    # excise at most the 40 most prominent lines: a short/turbulent block can sprout
    # spurious peaks, and excising hundreds of "lines" swallows the floor bands the
    # later checks measure in
    line_freqs = [fq for fq, _ in lines[:40]]
    tone, floor, excised = excise_lines(x, fs, line_freqs)
    f, p, white, colored_ratio = split_floor(floor, fs, excised)

    # ---- C3 tones ----
    n_ge3 = sum(1 for _, pr in lines if pr >= 3.0)
    top_prom = lines[0][1] if lines else 0.0
    add("C3.exist", "spectral lines stand above the floor",
        {"top_prom_db": round(top_prom, 1), "n_lines_ge3db": n_ge3},
        top_prom >= P["lines_min_prom_db"] and n_ge3 >= P["lines_min_count"],
        "an unvibed bag has no lines at all")
    if lines:
        f_star = lines[0][0]
        strong = [fq for fq, pr in lines[:20] if pr >= 3.0]
        cluster = [fq for fq in strong if abs(fq - f_star) <= 15.0]
        # Report-only: the "rotors never spin identically so expect >=2 lines" premise
        # does NOT hold universally -- Day20.6 and Day20.8 each resolve a single line
        # (well-synchronised rotors, or smearing merging the cluster). Informative,
        # not a gate.
        add("C3.cluster", "fundamental line cluster size",
            {"f_star": round(f_star, 1), "lines_within_15hz": len(cluster)}, "report")
        # windowed cluster amplitude over the WHOLE active record (no segmentation)
        clo = min(cluster) - 4.0
        chi = max(cluster) + 4.0
        amps = windowed_band_amp(acc[:, main].astype(float), fs, clo, chi, mask=active)
        if len(amps) >= 20:
            la = np.log(np.maximum(amps, 1e-12))
            sigma = float(la.std())
            rho = float(np.corrcoef(la[:-1], la[1:])[0, 1])
            tau = float(-HOP_S / np.log(min(max(rho, 1e-6), 0.995))) if rho > 0.05 else HOP_S
            p90p10 = float(np.percentile(amps, 90) / max(np.percentile(amps, 10), 1e-12))
            add("C3.breathe", "tone amplitude breathes",
                {"sigma_log": round(sigma, 3), "tau_s": round(tau, 1)},
                _in(sigma, P["tone_sigma_log"]) and _in(tau, P["tone_tau_s"]),
                f"real sigma {P['tone_sigma_log'][0]}-0.54; measured on a floor-dominated "
                "band so it cannot isolate the tone modulation -- informational")
            add("C3.responsive", "tone amplitude responds to flight state",
                {"p90_p10": round(p90p10, 2)},
                p90p10 >= P["tone_p90_p10_min"],
                "constant-amplitude buzz ignores what the aircraft is doing")
            add("C3.skew", "amplitude surges lean loud",
                {"log_skew": round(float(sstats.skew(la)), 2)}, "report")
        else:
            add("C3.breathe", "tone amplitude breathes", None, "skip", "record too short")
        wdt = line_width_3db(x_sharp, fs, f_star)
        add("C3.width", "strongest line width sane",
            {"width_hz": round(wdt, 2) if np.isfinite(wdt) else None},
            bool(np.isfinite(wdt) and _in(wdt, P["line_width_hz"])),
            "too wide = tone frequency smeared by rotor-speed wander")

    # ---- C2 colored floor ----
    add("C2.exists", "colored floor over white level",
        {"lowband_over_white": round(colored_ratio, 2) if np.isfinite(colored_ratio) else None},
        bool(np.isfinite(colored_ratio) and _in(colored_ratio, P["colored_ratio"])),
        "real 1.5-95: the white level is segment-sensitive (15x swing on one real "
        "flight), so this only informs; C5.level is the gate for floor size")
    fbp = sosfilt(sos, floor)
    fk = float(sstats.kurtosis(fbp))
    add("C2.gaussian", "floor is heavy-tailed like real turbulence",
        {"excess_kurtosis": round(fk, 2)},
        _in(fk, P["floor_kurtosis"]),
        "exactly-Gaussian (kurtosis 0) means synthetic noise, not turbulence")
    nw = int(2 * fs)
    wstd = np.array([fbp[i:i + nw].std() for i in range(0, len(fbp) - nw, nw)])
    if len(wstd) >= 8:
        cv = float(wstd.std() / max(wstd.mean(), 1e-12))
        add("C2.breathe", "floor energy breathes",
            {"cv": round(cv, 2),
             "lag1": round(float(np.corrcoef(wstd[:-1], wstd[1:])[0, 1]), 2)},
            cv >= P["floor_breathe_cv_min"],
            "a stationary injected floor does not breathe")

    # ---- C1 white ----
    holes = excised(f)
    hib = (f >= 150) & (f <= 198) & ~holes
    if hib.sum() >= 10:
        r = p[hib] / max(white, 1e-300)
        wflat = float(np.exp(np.mean(np.log(np.maximum(r, 1e-12)))) / np.mean(r))
        add("C1.white_flat", "high-band floor is flat (white)",
            {"flatness": round(wflat, 2)}, wflat >= P["white_flatness_min"],
            "real 0.21-0.95")
    add("C1.white_level", "white level plausible for a MEMS part",
        {"accel_density": float(f"{white:.3g}")}, _in(white, P["white_accel_density"]),
        "MEMS sanity band 1e-6..3e-2")
    gx = gyr[s0:s1, 0].astype(float)
    glines, _ = census_lines(gx, fs, min_prom_db=2.0)
    _, gfloor, gexc = excise_lines(gx, fs, [fq for fq, _ in glines])
    _, _, gwhite, _ = split_floor(gfloor, fs, gexc)
    add("C1.white_level_gyro", "gyro white level plausible",
        {"gyro_density": float(f"{gwhite:.3g}")}, _in(gwhite, P["white_gyro_density"]),
        "MEMS sanity band 1e-11..1e-4")

    # ---- C4 cross-channel ----
    if lines:
        chans = {"ax": acc[s0:s1, 0], "ay": acc[s0:s1, 1], "az": acc[s0:s1, 2],
                 "gx": gyr[s0:s1, 0], "gy": gyr[s0:s1, 1], "gz": gyr[s0:s1, 2]}
        names = list(chans)
        cohs = {}
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                fc, C = coherence(chans[a] - chans[a].mean(), chans[b] - chans[b].mean(),
                                  fs=fs, nperseg=2048)
                bb = (fc >= f_star - 3) & (fc <= f_star + 3)
                cohs[f"{a}-{b}"] = float(C[bb].max())
        hi_pair = max(cohs, key=cohs.get)
        lo_pair = min(cohs, key=cohs.get)
        add("C4.contrast", "some channel pairs locked, some not",
            {"max": f"{hi_pair}={cohs[hi_pair]:.2f}", "min": f"{lo_pair}={cohs[lo_pair]:.2f}"},
            cohs[hi_pair] >= P["coh_high_min"] and cohs[lo_pair] <= P["coh_low_max"],
            "all-locked and none-locked are both unphysical")
        ga = []
        for j in range(3):
            g_amp = windowed_band_amp(gyr[s0:s1, j].astype(float), fs, f_star - 4, f_star + 4)
            a_amp = windowed_band_amp(x, fs, f_star - 4, f_star + 4)
            if len(g_amp) and len(a_amp):
                ga.append(np.median(g_amp) / max(np.median(a_amp), 1e-12))
        if ga:
            gr = float(max(ga))
            add("C4.gyro_ratio", "gyro echo of the accel buzz plausible",
                {"ratio": float(f"{gr:.3g}")}, _in(gr, P["gyro_accel_ratio"]),
                "g-sensitivity sanity band 3e-4..0.5")

    # ---- C6 whitening ----
    flatness, wkurt = whiten_test(floor, fs, excised)
    add("C6.flat", "whitened residual is structureless",
        {"flatness": round(flatness, 3) if np.isfinite(flatness) else None,
         "kurtosis": round(wkurt, 2) if np.isfinite(wkurt) else None},
        bool(np.isfinite(flatness) and flatness >= P["whiten_flatness_min"]
             and _in(wkurt, P["whiten_kurtosis"])),
        "three layers should explain the whole signal; real 0.947-0.986, does not "
        "discriminate sim from real")

    return {"checks": checks, "info": info}


# ── CLI ──────────────────────────────────────────────────────────────────────
def print_report(res: dict, title: str | None = None) -> dict:
    """Print the check table for an analyze() result; returns the verdict counts."""
    info = res["info"]
    if title:
        print(hdr(title))
    print(f"fs {info['fs']:.1f} Hz   gt {info.get('gt_topic') or 'NONE (energy-gated)'}   "
          f"active {info['active_s']:.0f}s   steady block {info['steady_s']:.0f}s   "
          f"main axis {info.get('main_accel_axis', '?')}   "
          f"raw {info.get('raw_bag') or 'none'}")
    print(SEP)
    counts = {"pass": 0, "warn": 0, "fail": 0, "report": 0, "skip": 0}
    mark = {"pass": "✔", "warn": "⚠", "fail": "✘", "report": "·", "skip": "-"}
    for c in res["checks"]:
        counts[c["verdict"]] += 1
        v = c["value"]
        vs = json.dumps(v) if isinstance(v, dict) else str(v)
        note = f"   ({c['note']})" if c["note"] and c["verdict"] != "pass" else ""
        print(f"  {mark[c['verdict']]} {c['id']:<14} {c['name']:<50} {vs}{note}")
    print(SEP)
    verdict = ok if counts["fail"] == 0 else fail
    print(verdict(f"{counts['pass']} pass / {counts['warn']} warn / {counts['fail']} fail"
                  f" ({counts['report']} report-only, {counts['skip']} skipped)"))
    return counts


def run(args) -> int:
    profile = None
    if getattr(args, "profile", None):
        profile = json.load(open(args.profile))
    raw = getattr(args, "raw", None) or guess_raw_bag(args.input_bag)
    res = analyze(args.input_bag, args.imu_topic, args.gt_topic, profile, raw)
    counts = print_report(res, f"vib-realism: {args.input_bag}")
    if getattr(args, "json", None):
        with open(args.json, "w") as fh:
            json.dump(res, fh, indent=2)
        print(f"wrote {args.json}")
    return 1 if counts["fail"] else 0
