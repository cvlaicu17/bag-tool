"""gnss_ins: GNSS/INS smoother ground truth (position + full attitude) from the raw recording.

Used for a frame-invariant RTE on platforms whose ground truth is a single-antenna GNSS (positions and
Doppler velocity, no attitude), e.g. ezio. Nothing from the VIO under test enters it:

  * IMU (the platform's gt_imu_topic, sensor_msgs/Imu) + GNSS PVT (gt_pvt_topic: lat/lon/height, NED Doppler
    velocity, h/v accuracy) are read from the original recording;
  * tilt and gyro bias are seeded from the first second of IMU (motors off or idle on the ground);
  * the GNSS latency and the initial heading are found together by matching the IMU's 0.4-s velocity changes
    (gyro-propagated, level frame of arbitrary heading) to the Doppler velocity changes: vertical channel and
    horizontal magnitude fix the lag, the horizontal direction fixes the heading;
  * forward error-state Kalman filter + RTS backward pass (ENU, 100 Hz steps, raw IMU pre-integrated per step).
    State (18): position, velocity, attitude error (world), accel bias, gyro bias, GNSS antenna lever arm (body).
    Measurements: Doppler velocity and position at the antenna (5 Hz), zero velocity + zero rate while still with
    motors off. Earth rate and Coriolis included. Heading is observable through horizontal manoeuvres; the
    posterior sigma (sd_att) says where.

Result is cached per recording (~/.cache/bag-tool/gnss_ins/) since it does not depend on the VIO run.
Ported from basalt_ws/analysis/ezio_gnss_ins_smoother.py (2026-11-16), whose raven-seeded yaw/lag were replaced."""
from __future__ import annotations

import hashlib
import math
import os
from pathlib import Path

import numpy as np
from scipy.spatial.transform import Rotation as Rot

CACHE_VERSION = 1


def _skew(v):
    return np.array([[0, -v[2], v[1]], [v[2], 0, -v[0]], [-v[1], v[0], 0]])


def _exp(v):
    return Rot.from_rotvec(v).as_matrix()


def _log(R):
    return Rot.from_matrix(R).as_rotvec()


# ---------------------------------------------------------------------------------------------- input
def load_inputs(bag: str | Path, imu_topic: str, pvt_topic: str):
    """IMU (N x 7: t, ax, ay, az, gx, gy, gz; header stamps, s) and PVT (dict of arrays) from the recording."""
    from rosbags.rosbag2 import Reader
    from rosbags.typesys import Stores, get_typestore, get_types_from_msg
    path = Path(bag)
    rpath = path.parent if path.is_file() else path
    ts = get_typestore(Stores.ROS2_JAZZY)
    imu, pvt = [], []
    with Reader(rpath) as r:
        conns = [c for c in r.connections if c.topic in (imu_topic, pvt_topic)]
        if not any(c.topic == imu_topic for c in conns) or not any(c.topic == pvt_topic for c in conns):
            raise ValueError(f'gnss_ins: {rpath} lacks {imu_topic} or {pvt_topic}')
        for c in conns:
            if c.msgtype not in ts.types:
                ts.register(get_types_from_msg(c.msgdef.data, c.msgtype))
        for c, _, raw in r.messages(connections=conns):
            m = ts.deserialize_cdr(raw, c.msgtype)
            t = m.header.stamp.sec + m.header.stamp.nanosec * 1e-9
            if c.topic == imu_topic:
                a, w = m.linear_acceleration, m.angular_velocity
                imu.append((t, a.x, a.y, a.z, w.x, w.y, w.z))
            else:
                v = m.vel_ned_mps
                pvt.append((t, m.fix_type, m.lat_deg, m.lon_deg, m.height_m, m.h_acc_m, m.v_acc_m, v[0], v[1], v[2]))
    imu = np.array(imu); pvt = np.array(pvt)
    imu = imu[np.argsort(imu[:, 0], kind='stable')]; pvt = pvt[np.argsort(pvt[:, 0], kind='stable')]
    imu = imu[np.r_[True, np.diff(imu[:, 0]) > 0]]
    pvt = pvt[(pvt[:, 1] >= 3) & np.r_[True, np.diff(pvt[:, 0]) > 0]]          # 3D fix only
    keys = ('t', 'fix', 'lat', 'lon', 'h', 'h_acc', 'v_acc', 'vn', 've', 'vd')
    return imu, {k: pvt[:, i] for i, k in enumerate(keys)}


# ---------------------------------------------------------------------------------------------- seeding
def _preintegrate(imu, edges):
    """Per step [edges[k], edges[k+1]): summed rotation increments and sculling-corrected velocity increments (body)."""
    t_raw = imu[:, 0]
    idx = np.searchsorted(t_raw, edges)
    dt_raw = np.diff(t_raw, append=t_raw[-1] + np.median(np.diff(t_raw)))
    s0, s1 = idx[0], idx[-1]
    th = imu[s0:s1, 4:7] * dt_raw[s0:s1, None]; dv = imu[s0:s1, 1:4] * dt_raw[s0:s1, None]
    loc = idx - s0
    C_th = np.vstack([np.zeros((1, 3)), np.cumsum(th, 0)])
    before = C_th[:-1] - np.repeat(C_th[loc[:-1]], np.diff(loc), axis=0)    # rotation accumulated earlier in the step
    sc = dv + np.cross(before, dv)
    C_sc = np.vstack([np.zeros((1, 3)), np.cumsum(sc, 0)])
    return idx, C_th[loc[1:]] - C_th[loc[:-1]], C_sc[loc[1:]] - C_sc[loc[:-1]]


def _seed_lag_heading(t_steps, dth, dvb, R0, bg0, G, pvt_t, v_enu, lags=np.arange(0.0, 0.30, 0.005)):
    """GNSS latency + heading of the level frame from velocity changes; no VIO. Returns (lag, yaw, quality)."""
    N = len(dth)
    R = np.empty((N + 1, 3, 3)); R[0] = R0
    for k in range(N):                                   # gyro-only attitude, level frame of arbitrary heading
        R[k + 1] = R[k] @ _exp(dth[k] - bg0 * (t_steps[k + 1] - t_steps[k]))
    dv_w = np.einsum('kij,kj->ki', R[:-1], dvb)
    dv_w[:, 2] -= G * np.diff(t_steps)
    V = np.vstack([np.zeros(3), np.cumsum(dv_w, 0)])     # integrated velocity (drifts; only differences are used)
    i = np.arange(1, len(pvt_t) - 1)
    dvg = v_enu[i + 1] - v_enu[i - 1]
    moving = np.linalg.norm(dvg, axis=1) > 0.15          # changes worth matching (0.4 s apart at 5 Hz)
    if moving.sum() < 20:
        return 0.1, 0.0, 0.0
    best = None
    for lag in lags:
        ta, tb = pvt_t[i - 1] - lag, pvt_t[i + 1] - lag
        ok = moving & (ta > t_steps[0]) & (tb < t_steps[-1])
        dvi = np.stack([np.interp(tb[ok], t_steps, V[:, c]) - np.interp(ta[ok], t_steps, V[:, c]) for c in range(3)], 1)
        zi = dvi[:, 0] + 1j * dvi[:, 1]; zg = dvg[ok, 0] + 1j * dvg[ok, 1]
        rot = np.sum(np.conj(zi) * zg)
        yaw = np.angle(rot)
        res = np.sum(np.abs(zg - np.exp(1j * yaw) * zi) ** 2) + np.sum((dvg[ok, 2] - dvi[:, 2]) ** 2)
        if best is None or res < best[0]:
            best = (res, lag, yaw, abs(rot) / max(np.sum(np.abs(zi) * np.abs(zg)), 1e-9))
    return best[1], best[2], best[3]


# ---------------------------------------------------------------------------------------------- smoother
def smooth(imu, pvt, rate=100.0, acc_n=0.03, gyr_n=1e-3, rw_acc=3e-4, rw_gyr=3e-5,
           vel_sigma=(0.05, 0.10), pos_scale=2.0, verbose=True):
    lat0, lon0, h0 = math.radians(pvt['lat'][0]), math.radians(pvt['lon'][0]), pvt['h'][0]
    G = 9.780327 * (1 + 0.0053024 * math.sin(lat0) ** 2 - 0.0000058 * math.sin(2 * lat0) ** 2) - 3.086e-6 * h0
    OM = 7.2921159e-5 * np.array([0, math.cos(lat0), math.sin(lat0)])
    gw = np.array([0, 0, -G])
    RE = 6378137.0
    pg = np.stack([(np.radians(pvt['lon']) - lon0) * RE * math.cos(lat0), (np.radians(pvt['lat']) - lat0) * RE,
                   pvt['h'] - h0], 1)
    vg = np.stack([pvt['ve'], pvt['vn'], -pvt['vd']], 1)
    sig_p = np.stack([pvt['h_acc'], pvt['h_acc'], pvt['v_acc']], 1) * pos_scale

    t_raw = imu[:, 0]
    step = 1.0 / rate
    T0 = max(t_raw[0], pvt['t'][0] - 1.0); t_end = min(t_raw[-1], pvt['t'][-1] + 1.0)
    edges = np.arange(T0, t_end, step)
    idx, dth, dvb = _preintegrate(imu, edges)
    N = len(edges) - 1

    # motors-off stillness (1 s raw std), as in the analysis smoother
    w1 = max(2, int(round(1.0 / np.median(np.diff(t_raw)))))
    def rstd(x):
        c1 = np.cumsum(np.vstack([np.zeros((1, 3)), x]), 0); c2 = np.cumsum(np.vstack([np.zeros((1, 3)), x ** 2]), 0)
        s1 = (c1[w1:] - c1[:-w1]) / w1; s2 = (c2[w1:] - c2[:-w1]) / w1
        return np.sqrt(np.maximum(s2 - s1 ** 2, 0)).max(1)
    sa, sgy = rstd(imu[:, 1:4]), rstd(imu[:, 4:7])
    still_raw = np.zeros(len(imu), bool)
    still_raw[w1 // 2: w1 // 2 + len(sa)] = (sa < 0.07) & (sgy < 0.02)
    still = np.array([still_raw[idx[k]:idx[k + 1]].all() if idx[k + 1] > idx[k] else False for k in range(N)])

    # seed: tilt + gyro bias from the first second (motors off, or armed idle: averages out), then lag + heading
    first = (t_raw >= T0) & (t_raw < T0 + 1.0)
    a0 = imu[first, 1:4].mean(0); bg0 = imu[first, 4:7].mean(0)
    R_tilt = Rot.align_vectors([[0, 0, 1.0]], [a0 / np.linalg.norm(a0)])[0].as_matrix()
    bx = R_tilt[:, 0]; R_tilt = _exp([0, 0, -math.atan2(bx[1], bx[0])]) @ R_tilt
    lag, yaw, coh = _seed_lag_heading(edges, dth, dvb, R_tilt, bg0, G, pvt['t'], vg)
    R = _exp([0, 0, yaw]) @ R_tilt
    if verbose:
        print(f'gnss-ins: {N * step:.0f} s at {rate:.0f} Hz, still (motors off) {still.sum() * step:.1f} s | seed: GNSS lag '
              f'{lag * 1e3:.0f} ms, heading {math.degrees(yaw):+.1f} deg (coherence {coh:.2f}, from velocity changes, no VIO)')

    NS = 18
    P = np.diag(np.r_[[0.5] * 3, [0.05] * 3, np.radians([1, 1, 10]), [0.05] * 3, [0.01] * 3, [0.3] * 3] ** 2)
    tg = pvt['t'] - lag
    p = pg[np.argmin(np.abs(tg - T0))].copy(); v = np.zeros(3)
    ba = np.zeros(3); bg = bg0.copy(); l = np.zeros(3)
    Qc = np.zeros(NS); Qc[3:6] = acc_n ** 2; Qc[6:9] = gyr_n ** 2; Qc[9:12] = rw_acc ** 2; Qc[12:15] = rw_gyr ** 2
    gi = np.searchsorted(tg, edges[0])
    Xp = np.zeros((N + 1, 15)); Rp = np.zeros((N + 1, 3, 3)); Xm = np.zeros((N + 1, 15)); Rm = np.zeros((N + 1, 3, 3))
    Pp = np.zeros((N + 1, NS, NS)); Pm = np.zeros((N + 1, NS, NS)); F = np.zeros((N, NS, NS))
    nis = {'vel': [], 'pos': []}
    st = {'P': P, 'p': p, 'v': v, 'R': R, 'ba': ba, 'bg': bg, 'l': l}

    def update(r, H, Rn, kind=None):
        P_ = st['P']; S = H @ P_ @ H.T + Rn; K = P_ @ H.T @ np.linalg.inv(S); dx = K @ r
        if kind:
            nis[kind].append(r @ np.linalg.solve(S, r) / len(r))
        IKH = np.eye(NS) - K @ H; st['P'] = IKH @ P_ @ IKH.T + K @ Rn @ K.T
        st['p'] = st['p'] + dx[0:3]; st['v'] = st['v'] + dx[3:6]; st['R'] = _exp(dx[6:9]) @ st['R']
        st['ba'] = st['ba'] + dx[9:12]; st['bg'] = st['bg'] + dx[12:15]; st['l'] = st['l'] + dx[15:18]

    def pack():
        return np.r_[st['p'], st['v'], st['ba'], st['bg'], st['l']]

    Xp[0] = Xm[0] = pack(); Rp[0] = Rm[0] = st['R']; Pp[0] = Pm[0] = st['P']
    w_last = np.zeros(3)
    I3 = np.eye(3)
    for k in range(N):
        dt = edges[k + 1] - edges[k]
        R = st['R']; v = st['v']
        th = dth[k] - st['bg'] * dt - R.T @ OM * dt
        dvb_c = dvb[k] - st['ba'] * dt
        Rmid = R @ _exp(0.5 * th)
        dvw = Rmid @ dvb_c
        acc_w = dvw / dt + gw - 2 * np.cross(OM, v)
        Fk = np.eye(NS)
        Fk[0:3, 3:6] = I3 * dt; Fk[3:6, 6:9] = -_skew(dvw); Fk[3:6, 9:12] = -Rmid * dt; Fk[6:9, 12:15] = -Rmid * dt
        st['p'] = st['p'] + v * dt + 0.5 * acc_w * dt * dt; st['v'] = v + acc_w * dt; st['R'] = R @ _exp(th)
        w_last = th / dt
        Gq = np.zeros(NS); Gq[3:15] = Qc[3:15] * dt
        P_ = Fk @ st['P'] @ Fk.T + np.diag(Gq); st['P'] = 0.5 * (P_ + P_.T)
        F[k] = Fk; Xm[k + 1] = pack(); Rm[k + 1] = st['R']; Pm[k + 1] = st['P']
        tk = edges[k + 1]
        while gi < len(tg) and tg[gi] <= tk + 0.5 * step:
            if tg[gi] > tk - 0.5 * step:
                R = st['R']; l = st['l']
                Rl = R @ np.cross(w_last, l)
                H = np.zeros((3, NS)); H[:, 3:6] = I3; H[:, 6:9] = -_skew(Rl); H[:, 12:15] = R @ _skew(l)
                H[:, 15:18] = R @ _skew(w_last)
                update(vg[gi] - (st['v'] + Rl), H, np.diag([vel_sigma[0] ** 2] * 2 + [vel_sigma[1] ** 2]), 'vel')
                R = st['R']; Rl = R @ st['l']
                H = np.zeros((3, NS)); H[:, 0:3] = I3; H[:, 6:9] = -_skew(Rl); H[:, 15:18] = R
                update(pg[gi] - (st['p'] + Rl), H, np.diag(sig_p[gi] ** 2), 'pos')
            gi += 1
        if still[k] and k % 10 == 0:
            i0, i1 = idx[k - 9 if k >= 9 else 0], idx[k + 1]
            wm = imu[i0:i1, 4:7].mean(0); n = max(1, i1 - i0)
            H = np.zeros((3, NS)); H[:, 12:15] = I3
            update(wm - (st['bg'] + st['R'].T @ OM), H, I3 * (3e-3 ** 2 / n * 20 + 1e-8))
            H = np.zeros((3, NS)); H[:, 3:6] = I3
            update(-st['v'], H, I3 * 0.01 ** 2)
        Xp[k + 1] = pack(); Rp[k + 1] = st['R']; Pp[k + 1] = st['P']

    # RTS
    Xs = Xp.copy(); Rs = Rp.copy(); Ps = Pp.copy()
    for k in range(N - 1, -1, -1):
        C = Pp[k] @ F[k].T @ np.linalg.inv(Pm[k + 1])
        e = np.r_[Xs[k + 1, 0:6] - Xm[k + 1, 0:6], _log(Rs[k + 1] @ Rm[k + 1].T), Xs[k + 1, 6:15] - Xm[k + 1, 6:15]]
        d = C @ e
        Xs[k, 0:6] = Xp[k, 0:6] + d[0:6]; Rs[k] = _exp(d[6:9]) @ Rp[k]; Xs[k, 6:15] = Xp[k, 6:15] + d[9:18]
        Ps[k] = Pp[k] + C @ (Ps[k + 1] - Pm[k + 1]) @ C.T
    sd = np.sqrt(np.einsum('kii->ki', Ps))
    nis_v = float(np.mean(nis['vel'])) if nis['vel'] else float('nan')
    nis_p = float(np.mean(nis['pos'])) if nis['pos'] else float('nan')
    if verbose:
        print(f'gnss-ins: normalised innovation vel {nis_v:.2f} pos {nis_p:.2f} (want ~1) | lever arm '
              f'{np.round(Xs[-1, 12:15], 3)} m | heading sd median {np.degrees(np.median(sd[:, 8])):.2f} deg, '
              f'max {np.degrees(sd[:, 8].max()):.2f} deg')
    return {'t': edges, 'p': Xs[:, 0:3], 'v': Xs[:, 3:6], 'q': Rot.from_matrix(Rs).as_quat(),
            'ba': Xs[:, 6:9], 'bg': Xs[:, 9:12], 'lever': Xs[-1, 12:15], 'sd_att': sd[:, 6:9], 'still': np.r_[still, still[-1]],
            'lag': lag, 'yaw_seed': yaw, 'seed_coherence': coh, 'G': G, 'nis_vel': nis_v, 'nis_pos': nis_p,
            'origin_lla': np.array([pvt['lat'][0], pvt['lon'][0], h0])}


# ---------------------------------------------------------------------------------------------- cache
def ground_truth(bag: str | Path, imu_topic: str, pvt_topic: str, cache_dir: str | Path | None = None, verbose=True):
    """Smoothed GT for a recording (cached). Keys: t (s, IMU header clock), p (ENU, m, IMU position), q (xyzw, IMU body ->
    ENU), sd_att (rad, world-frame attitude sigma; [:, 2] = heading), plus diagnostics."""
    path = Path(bag)
    rpath = path.parent if path.is_file() else path
    files = sorted(rpath.glob('*.mcap')) if rpath.is_dir() else [rpath]
    key = hashlib.md5('|'.join([f'{f.name}:{f.stat().st_size}' for f in files] + [imu_topic, pvt_topic,
                                str(CACHE_VERSION)]).encode()).hexdigest()[:12]
    cdir = Path(cache_dir or os.path.join(os.path.expanduser('~'), '.cache', 'bag-tool', 'gnss_ins'))
    cdir.mkdir(parents=True, exist_ok=True)
    cfile = cdir / f'{rpath.name}_{key}.npz'
    if cfile.exists():
        z = np.load(cfile)
        if verbose:
            print(f'gnss-ins: cached ground truth {cfile}')
        return {k: z[k] for k in z.files}
    if verbose:
        print(f'gnss-ins: reading {imu_topic} + {pvt_topic} from {rpath} ...')
    imu, pvt = load_inputs(rpath, imu_topic, pvt_topic)
    out = smooth(imu, pvt, verbose=verbose)
    np.savez(cfile, **out)
    if verbose:
        print(f'gnss-ins: wrote {cfile}')
    return out
