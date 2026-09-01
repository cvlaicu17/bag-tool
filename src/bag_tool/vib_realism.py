"""vib-realism: is this bag's IMU vibration structurally realistic?

A REFERENCE-FREE realism gate for (mostly simulated) mission bags. Unlike vib-verify,
which asks "does this bag match one specific real reference flight?" (right question at
calibration time, wrong one at mission-acceptance time), this asks "does this bag vibrate
the way real drones vibrate?" -- structural invariants only, with thresholds calibrated so
that EVERY real bag in the corpus passes and an unvibed or badly-vibed sim bag fails.

The checks map onto the layers add_vibration.py generates, and each is phrased as a
property of reality rather than a comparison to a reference:

 C1 white noise    -- the sensor's own hiss: flat spectrum where nothing else lives,
                      at a loudness a real MEMS part could produce.
 C2 colored noise  -- the turbulence carpet: present (low band louder than the white
                      level), smooth, near-Gaussian carrier, energy that breathes.
 C3 tones          -- spikes above the carpet when motors run: a fundamental CLUSTER
                      (real rotors never spin identically), amplitudes that breathe
                      with multi-second memory, respond to flight state, and have a
                      physically sane line width.
 C4 cross-channel  -- vibration travels through a rigid body: some channel pairs move
                      in lockstep, others don't (all-locked and none-locked are both
                      impossible), and the gyro echo of the accel buzz has a plausible
                      magnitude.
 C5 invariants     -- finite values, physically representable magnitudes.
 C6 whitening      -- after accounting for tones + colored floor + white hiss, nothing
                      is left: the whitened residual is flat and Gaussian. One number
                      that says the three layers explain the whole signal.

Deliberately NOT checked (do not add back): exact line frequencies or the exact
harmonic family (drone-specific, and sim frequencies come from sim physics by
decision); amplitudes vs any particular reference bag; integer relationships between
lines (aliasing scrambles them); explicit H(f) (unidentifiable from mission data);
anything about the RPM law.

Segmentation policy: NO legacy climb/cruise/descent phases. Where a quasi-stationary
stretch is needed (spectral surgery), the tool uses the longest contiguous airborne run
whose |vertical speed| stays below the mission's own median -- a data-driven quantile,
not a fixed threshold with a name. State-responsiveness is measured over the whole
airborne record with no segmentation at all (p90/p10 of the windowed tone amplitude).

Isolation pipeline (validated on real day20.7 before this was written -- the whitened
residual there comes out at spectral flatness 0.981, kurtosis +0.20):
  census_lines -> excise_lines (FFT surgery; energy bookkeeping is exact) ->
  split_floor (white level read where the carpet has gone flat) -> whiten.
"""
from __future__ import annotations

import json
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
    # CALIBRATED on the 8-bag real altair corpus (Day20.1/2/3/5/6/7/8 + Day32) with
    # 4 sim bags as negative controls. Rule: every real bag must PASS; a threshold a
    # real bag violates is a wrong threshold. Where real and sim ranges are disjoint
    # the bound sits in the gap; where they overlap the check is a loose sanity bound
    # or report-only. Observed ranges are quoted as [real | sim] so the next person
    # can see the margin they are widening.
    # C1
    "white_flatness_min": 0.15,        # [real 0.21-0.95] Day32 sits at 0.21
    "white_accel_density": [1e-6, 3e-2],   # sanity band for a MEMS part
    "white_gyro_density":  [1e-11, 1e-4],
    # C2
    # Lower bound gates (a colored floor must exist); the UPPER bound only warns.
    # The white level is read at 178-198 Hz and is segment-sensitive: the same real
    # flight (Day20.8) gives 6.45 over a 208s steady block and 94.45 over a different
    # 120s block of the same recording. A 15x swing on identical hardware is too
    # unstable to fail a mission on. Needs a more robust white-level estimator before
    # it can gate; until then sim's over-strong floor is caught by C2.gaussian and
    # C2.breathe, which ARE stable.
    "colored_ratio": [1.30, 15.0],     # [real 1.47-94.5 | sim 1.43-74.4]
    "floor_kurtosis": [0.15, 8.0],     # [real 0.36-4.59 | sim -0.02-0.05] DISJOINT:
                                        # real turbulence is heavy-tailed, FFT-shaped
                                        # Gaussian noise is exactly kurtosis 0
    "floor_breathe_cv_min": 0.13,      # [real 0.18-0.36 | sim 0.06-0.09] DISJOINT
    # C3
    "lines_min_prom_db": 5.0,          # [real 5.1-9.0 | sim 4.3-6.2]
    "lines_min_count": 2,              # sanity floor only (real 21-40, sim 15-23)
    "tone_sigma_log": [0.30, 1.5],     # [real 0.337-0.536 | sim 0.204-0.513]
    "tone_tau_s": [0.3, 20.0],         # overlapping (real 1.7-6.6, sim 1.4-2.4): sanity only
    "tone_p90_p10_min": 2.20,          # [real 2.38-3.84 | sim 1.47-3.81]
    "line_width_hz": [0.15, 9.0],      # [real 0.78-8.46 | sim 7.96-15.53] THIN MARGIN:
                                        # real tops out at 8.46 (Day20.8 original span),
                                        # so only ~6% headroom. Catches the smeared v2
                                        # bags; does NOT catch current v3 (7.96).
    # C4
    "coh_high_min": 0.60,              # [real max-pair 0.79-0.99 | sim 0.26-0.95]
    "coh_low_max": 0.85,               # [real min-pair 0.07-0.77] Day32 couples all
                                        # channels at 0.77 -- "some pair must be loose"
                                        # is NOT a universal law, only near-total lock is
    "gyro_accel_ratio": [3e-4, 0.5],
    # C5
    "accel_abs_max": 250.0,
    "gyro_abs_max": 50.0,
    # C6 -- validates the 3-layer decomposition; does NOT discriminate
    # (real 0.947-0.986, sim 0.961-0.985 both fine). Kept as a modelling sanity check.
    "whiten_flatness_min": 0.75,
    "whiten_kurtosis": [-1.0, 4.0],
    # segmentation
    "min_height_m": 2.0,
    "min_steady_s": 20.0,
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
            profile: dict | None = None) -> dict:
    """Run every realism check. Returns {"checks": [...], "info": {...}} where each
    check is {id, name, value, verdict ('pass'|'warn'|'fail'|'report'|'skip'), note}."""
    P = dict(DEFAULT_PROFILE, **(profile or {}))
    ti, acc, gyr, tg, hgt, gt_name = _read(bag_path, imu_topic, gt_topic)
    fs = 1.0 / np.median(np.diff(ti))
    active, steady, vzi = segments(ti, acc, tg, hgt, fs, P)

    checks: list[dict] = []
    info = {"bag": str(bag_path), "fs": float(fs), "gt_topic": gt_name,
            "active_s": float(active.sum() / fs),
            "steady_s": float((steady[1] - steady[0]) / fs) if steady else 0.0}

    def add(cid, name, value, verdict, note=""):
        checks.append({"id": cid, "name": name, "value": value,
                       "verdict": verdict, "note": note})

    # C5 first (cheap, and everything else assumes finite data)
    finite = bool(np.isfinite(acc).all() and np.isfinite(gyr).all())
    add("C5.finite", "all samples finite", finite, "pass" if finite else "fail")
    amax, gmax = float(np.abs(acc).max()), float(np.abs(gyr).max())
    add("C5.magnitude", "physically representable magnitudes",
        {"accel_max": round(amax, 1), "gyro_max": round(gmax, 2)},
        "pass" if (amax <= P["accel_abs_max"] and gmax <= P["gyro_abs_max"]) else "fail")
    if not finite:
        return {"checks": checks, "info": info}

    if steady is None:
        add("SEG", "steady analysis block", None, "fail",
            f"no contiguous low-|vz| active run >= {P['min_steady_s']}s -- "
            "cannot run spectral checks")
        return {"checks": checks, "info": info}
    s0, s1 = steady

    # main accel channel: the one with the most vibration-band energy (data-driven,
    # not assumed to be z)
    sos = butter(4, list(VIB_BAND), btype="band", fs=fs, output="sos")
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
        "pass" if (top_prom >= P["lines_min_prom_db"] and n_ge3 >= P["lines_min_count"])
        else "fail",
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
                "pass" if (_in(sigma, P["tone_sigma_log"]) and _in(tau, P["tone_tau_s"]))
                else "fail",
                "deterministic constant amplitude fails here")
            add("C3.responsive", "tone amplitude responds to flight state",
                {"p90_p10": round(p90p10, 2)},
                "pass" if p90p10 >= P["tone_p90_p10_min"] else "fail",
                "constant-amplitude buzz ignores what the aircraft is doing")
            add("C3.skew", "amplitude surges lean loud",
                {"log_skew": round(float(sstats.skew(la)), 2)}, "report")
        else:
            add("C3.breathe", "tone amplitude breathes", None, "skip", "record too short")
        wdt = line_width_3db(x_sharp, fs, f_star)
        add("C3.width", "strongest line width sane",
            {"width_hz": round(wdt, 2) if np.isfinite(wdt) else None},
            "pass" if (np.isfinite(wdt) and _in(wdt, P["line_width_hz"])) else "fail",
            "too wide = tone frequency smeared by unsmoothed rotor chatter")

    # ---- C2 colored floor ----
    if not np.isfinite(colored_ratio) or colored_ratio < P["colored_ratio"][0]:
        _v, _n = "fail", "no colored floor above the white level (white-noise-only)"
    elif colored_ratio > P["colored_ratio"][1]:
        _v, _n = "warn", ("injected floor looks over-strong, but this metric is "
                          "segment-sensitive -- see profile note, not a gate")
    else:
        _v, _n = "pass", ""
    add("C2.exists", "colored floor present, not overpowering",
        {"lowband_over_white": round(colored_ratio, 2) if np.isfinite(colored_ratio) else None},
        _v, _n)
    fbp = sosfilt(sos, floor)
    fk = float(sstats.kurtosis(fbp))
    add("C2.gaussian", "floor is heavy-tailed like real turbulence",
        {"excess_kurtosis": round(fk, 2)},
        "pass" if _in(fk, P["floor_kurtosis"]) else "fail",
        "exactly-Gaussian (kurtosis 0) means synthetic noise, not turbulence")
    nw = int(2 * fs)
    wstd = np.array([fbp[i:i + nw].std() for i in range(0, len(fbp) - nw, nw)])
    if len(wstd) >= 8:
        cv = float(wstd.std() / max(wstd.mean(), 1e-12))
        add("C2.breathe", "floor energy breathes",
            {"cv": round(cv, 2),
             "lag1": round(float(np.corrcoef(wstd[:-1], wstd[1:])[0, 1]), 2)},
            "pass" if cv >= P["floor_breathe_cv_min"] else "fail",
            "a stationary injected floor does not breathe")

    # ---- C1 white ----
    holes = excised(f)
    hib = (f >= 150) & (f <= 198) & ~holes
    if hib.sum() >= 10:
        r = p[hib] / max(white, 1e-300)
        wflat = float(np.exp(np.mean(np.log(np.maximum(r, 1e-12)))) / np.mean(r))
        add("C1.white_flat", "high-band floor is flat (white)",
            {"flatness": round(wflat, 2)},
            "pass" if wflat >= P["white_flatness_min"] else "warn")
    add("C1.white_level", "white level plausible for a MEMS part",
        {"accel_density": float(f"{white:.3g}")},
        "pass" if _in(white, P["white_accel_density"]) else "warn")
    gx = gyr[s0:s1, 0].astype(float)
    glines, _ = census_lines(gx, fs, min_prom_db=2.0)
    _, gfloor, gexc = excise_lines(gx, fs, [fq for fq, _ in glines])
    _, _, gwhite, _ = split_floor(gfloor, fs, gexc)
    add("C1.white_level_gyro", "gyro white level plausible",
        {"gyro_density": float(f"{gwhite:.3g}")},
        "pass" if _in(gwhite, P["white_gyro_density"]) else "warn")

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
            "pass" if (cohs[hi_pair] >= P["coh_high_min"] and cohs[lo_pair] <= P["coh_low_max"])
            else "fail",
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
                {"ratio": float(f"{gr:.3g}")},
                "pass" if _in(gr, P["gyro_accel_ratio"]) else "warn")

    # ---- C6 whitening ----
    flatness, wkurt = whiten_test(floor, fs, excised)
    add("C6.flat", "whitened residual is structureless",
        {"flatness": round(flatness, 3) if np.isfinite(flatness) else None,
         "kurtosis": round(wkurt, 2) if np.isfinite(wkurt) else None},
        "pass" if (np.isfinite(flatness) and flatness >= P["whiten_flatness_min"]
                   and _in(wkurt, P["whiten_kurtosis"])) else "warn",
        "three layers should explain the whole signal")

    return {"checks": checks, "info": info}


# ── CLI ──────────────────────────────────────────────────────────────────────
def run(args) -> int:
    profile = None
    if getattr(args, "profile", None):
        profile = json.load(open(args.profile))
    print(hdr(f"vib-realism: {args.input_bag}"))
    res = analyze(args.input_bag, args.imu_topic, args.gt_topic, profile)
    info = res["info"]
    print(f"fs {info['fs']:.1f} Hz   gt {info.get('gt_topic') or 'NONE (energy-gated)'}   "
          f"active {info['active_s']:.0f}s   steady block {info['steady_s']:.0f}s   "
          f"main axis {info.get('main_accel_axis', '?')}")
    print(SEP)
    counts = {"pass": 0, "warn": 0, "fail": 0, "report": 0, "skip": 0}
    mark = {"pass": "✔", "warn": "⚠", "fail": "✘", "report": "·", "skip": "-"}
    for c in res["checks"]:
        counts[c["verdict"]] += 1
        v = c["value"]
        vs = json.dumps(v) if isinstance(v, dict) else str(v)
        note = f"   ({c['note']})" if c["note"] and c["verdict"] != "pass" else ""
        print(f"  {mark[c['verdict']]} {c['id']:<18} {c['name']:<42} {vs}{note}")
    print(SEP)
    verdict = ok if counts["fail"] == 0 else fail
    print(verdict(f"{counts['pass']} pass / {counts['warn']} warn / {counts['fail']} fail"
                  f" ({counts['report']} report-only)"))
    if getattr(args, "json", None):
        with open(args.json, "w") as fh:
            json.dump(res, fh, indent=2)
        print(f"wrote {args.json}")
    return 1 if counts["fail"] else 0
