"""viz3d: self-contained interactive 3D HTML of a VIO trajectory against ground truth.

One rigid (SE3, no scale) fit of the VIO to GT, both drawn in ENU metres (height above the
first GT sample), with a time scrubber, play button and per-sample 3D error readout. The page
loads Plotly from cdnjs (pinned 2.35.0; 2.35.2 is NOT on cdnjs) so it opens on a phone and
publishes as an Artifact / S3 presigned page unchanged.

Inputs
  run          a run dir (uses traj.tum, else the odometry topic of result/), or a result bag
  --input-bag  the original bag; needed when the result bag carries no GT

Ground truth, first match wins
  1. /pf_geo_loc/fc_local_position (NED PointStamped/PoseStamped) in the result or input bag
  2. /fc/gps (NavSatFix) in the input bag, converted to local NED (flat earth, origin = median of
     the first 5 fixes, z from GPS altitude). The orqa 2026-09 flights carry ONLY this.

Clock caveat handled for source 2: GPS header stamps follow the system (log) clock, while
IMU/camera/altimeter stamps are sensor-clock. On orqa_12 the system clock stepped +127.2 s at
log t=13.4 s, so the two disagree afterwards. Each GPS fix is therefore re-stamped with
(imu_header_stamp - imu_log_time) interpolated at the fix's log time (offset ~0 on normal bags).
Cost: one pass over the IMU topic of the input bag.
Also note: GPS is ~1-2 m noisy and looks ~0.3 s late (a -0.3 s GT shift cuts 1-s RTE 25-35%),
so the plotted error includes reference error.
"""
from __future__ import annotations

import json
import struct
from pathlib import Path

import numpy as np
from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

from bag_tool.add_topics import _reader_path

_TS = get_typestore(Stores.ROS2_JAZZY)
GT_TOPIC = "/pf_geo_loc/fc_local_position"
GPS_TOPIC = "/fc/gps"
IMU_TOPIC = "/imu/data_raw"
_R = 6378137.0


def _stamp(m):
    return m.header.stamp.sec + m.header.stamp.nanosec * 1e-9


def _read_vio(run: Path, vio_topic: str):
    tum = run / "traj.tum" if run.is_dir() else None
    if tum is not None and tum.exists() and tum.stat().st_size > 0:
        a = np.genfromtxt(tum, usecols=(0, 1, 2, 3), invalid_raise=False)
        return a[np.isfinite(a).all(1)]
    bag = run / "result" if (run / "result").is_dir() else run
    rows = []
    with Reader(_reader_path(bag)) as r:
        cons = [c for c in r.connections if c.topic == vio_topic]
        for c, _, raw in r.messages(connections=cons):
            m = _TS.deserialize_cdr(raw, c.msgtype)
            if hasattr(m, "pose") and hasattr(m.pose, "pose"):
                p = m.pose.pose.position
            else:
                p = m.pose.position
            rows.append((_stamp(m), p.x, p.y, p.z))
    return np.array(rows)


def _gt_from_topic(bag: Path, topic: str):
    rows = []
    with Reader(_reader_path(bag)) as r:
        cons = [c for c in r.connections if c.topic == topic]
        if not cons:
            return None
        for c, _, raw in r.messages(connections=cons):
            m = _TS.deserialize_cdr(raw, c.msgtype)
            p = m.pose.position if hasattr(m, "pose") else m.point
            rows.append((_stamp(m), p.y, p.x, -p.z))          # NED -> ENU
    return np.array(rows) if rows else None


def _gt_from_gps(bag: Path):
    fixes, lg, of = [], [], []
    with Reader(_reader_path(bag)) as r:
        topics = {c.topic for c in r.connections}
        if GPS_TOPIC not in topics:
            return None
        cons = [c for c in r.connections if c.topic in (GPS_TOPIC, IMU_TOPIC)]
        for c, t_ns, raw in r.messages(connections=cons):
            if c.topic == IMU_TOPIC:
                s, n = struct.unpack_from("<iI", raw, 4)      # header stamp straight from the CDR bytes
                lg.append(t_ns * 1e-9)
                of.append(s + n * 1e-9 - t_ns * 1e-9)
                continue
            m = _TS.deserialize_cdr(raw, c.msgtype)
            if m.status.status < 0 or (m.latitude == 0 and m.longitude == 0):
                continue
            fixes.append((t_ns * 1e-9, m.latitude, m.longitude, m.altitude))
    if not fixes:
        return None
    a = np.array(fixes)
    if lg:
        lg, of = np.array(lg[::50]), np.array(of[::50])
        a[:, 0] += np.interp(a[:, 0], lg, of)
    lat0, lon0, alt0 = (np.median(a[:5, k]) for k in (1, 2, 3))
    n = np.radians(a[:, 1] - lat0) * _R
    e = np.radians(a[:, 2] - lon0) * _R * np.cos(np.radians(lat0))
    return np.stack([a[:, 0], e, n, a[:, 3] - alt0], 1)       # ENU, up = altitude


def load_gt(run: Path, input_bag: Path | None):
    cands = []
    if run.is_dir():
        for sub in (run / "result", run):
            if (sub / "metadata.yaml").exists():
                cands.append(sub)
    if input_bag is not None:
        cands.append(input_bag)
    for b in cands:
        g = _gt_from_topic(b, GT_TOPIC)
        if g is not None:
            return g, f"{GT_TOPIC} ({b.name})"
    if input_bag is not None:
        g = _gt_from_gps(input_bag)
        if g is not None:
            return g, f"{GPS_TOPIC} converted to local ENU ({input_bag.name})"
    raise SystemExit("viz3d: no ground truth found (need /pf_geo_loc/fc_local_position or --input-bag with /fc/gps)")


def _fit(v, g):
    ma, mb = v.mean(0), g.mean(0)
    U, S, Vt = np.linalg.svd((v - ma).T @ (g - mb))
    D = np.diag([1, 1, np.sign(np.linalg.det(Vt.T @ U.T))])
    R = Vt.T @ D @ U.T
    return R, mb - R @ ma


def build(vio, gt, title, sub, gt_label, vio_label, note):
    m = (gt[:, 0] >= vio[0, 0]) & (gt[:, 0] <= vio[-1, 0])
    g = gt[m]
    if len(g) < 4:
        raise SystemExit("viz3d: GT and VIO time ranges do not overlap (check the clocks)")
    v = np.stack([np.interp(g[:, 0], vio[:, 0], vio[:, k]) for k in (1, 2, 3)], 1)
    R, t = _fit(v, g[:, 1:])
    vs = vio[:: max(1, len(vio) // 2000)]
    vline = vs[:, 1:4] @ R.T + t
    vg = v @ R.T + t
    err = np.linalg.norm(vg - g[:, 1:], axis=1)
    t0, z0 = vio[0, 0], g[0, 3]
    r = lambda x: np.round(x, 2).tolist()
    data = dict(gt=dict(t=r(g[:, 0] - t0), x=r(g[:, 1]), y=r(g[:, 2]), z=r(g[:, 3] - z0)),
                vio=dict(t=r(vs[:, 0] - t0), x=r(vline[:, 0]), y=r(vline[:, 1]), z=r(vline[:, 2] - z0)),
                at=dict(x=r(vg[:, 0]), y=r(vg[:, 1]), z=r(vg[:, 2] - z0), err=r(err)),
                ate=float(np.sqrt((err ** 2).mean())), dur=float(vio[-1, 0] - t0))
    html = (_TEMPLATE.replace("__TITLE__", title).replace("__SUB__", sub).replace("__GTLABEL__", gt_label)
            .replace("__VIOLABEL__", vio_label).replace("__NOTE__", note)
            .replace("__DATA__", json.dumps(data, separators=(",", ":"))))
    return html, data


def run(args) -> int:
    run_p = Path(args.run).expanduser()
    inp = Path(args.input_bag).expanduser() if args.input_bag else None
    vio = _read_vio(run_p, args.vio_topic)
    gt, gt_src = load_gt(run_p, inp)
    name = args.title or run_p.name
    from_gps = GPS_TOPIC in gt_src
    note = ("GPS is about 1 to 2 m noisy and looks about 0.3 s late, so part of the gap is the reference, not the VIO."
            if from_gps else "The reference has its own noise, so part of the gap is not VIO error.")
    sub = (f"{name}: flight of {vio[-1, 0] - vio[0, 0]:.0f} s. The {args.vio_label} track is aligned to the ground truth with one rigid fit. "
           "Axes are metres east, north and height above the pad.")
    html, data = build(vio, gt, name, sub, "GPS" if from_gps else "Ground truth", args.vio_label, note)
    out = Path(args.out) if args.out else (run_p if run_p.is_dir() else run_p.parent) / "viz3d.html"
    out.write_text(html)
    print(f"GT         : {gt_src}")
    print(f"VIO        : {len(vio)} poses, GT samples used {len(data['gt']['t'])}")
    print(f"ATE (RMS)  : {data['ate']:.2f} m   median err {np.median(data['at']['err']):.2f} m   max {max(data['at']['err']):.2f} m")
    print(f"wrote      : {out} ({out.stat().st_size / 1e3:.0f} kB)")
    return 0


_TEMPLATE = r"""<title>__TITLE__</title>
<style>
/* layout: full-width 3D view on top, scrub bar and readout below */
:root{--bg:#f6f7f9;--fg:#1b2330;--mute:#5d6878;--card:#ffffff;--line:#d5dae2;--gps:#1f6fd6;--vio:#d4590b;--mk:#1b2330;--font:"IBM Plex Sans",system-ui,sans-serif;--mono:"IBM Plex Mono",ui-monospace,monospace}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#0f141b;--fg:#e6eaf0;--mute:#97a3b5;--card:#171e28;--line:#2b3442;--gps:#5aa2ff;--vio:#ff8a47;--mk:#e6eaf0;color-scheme:dark}}
:root[data-theme="dark"]{--bg:#0f141b;--fg:#e6eaf0;--mute:#97a3b5;--card:#171e28;--line:#2b3442;--gps:#5aa2ff;--vio:#ff8a47;--mk:#e6eaf0;color-scheme:dark}
html,body{overflow-x:hidden}
body{background:var(--bg);color:var(--fg);font-family:var(--font);padding-inline:16px;padding-block:16px}
.wrap{max-width:1000px;margin:0 auto;display:flex;flex-direction:column;gap:12px}
h1{font-size:20px;margin:0;font-weight:600}
.sub{color:var(--mute);font-size:13px;margin:2px 0 0;line-height:1.4}
#plot{width:100%;max-width:100%;overflow:hidden;height:min(70vh,560px);min-height:340px;background:var(--card);border:1px solid var(--line);border-radius:6px}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:13px}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:18px;height:3px;display:inline-block}
.ctl{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.ctl input[type=range]{flex:1;min-width:160px}
button{font:inherit;font-size:13px;background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:8px 14px;min-height:40px}
button:focus-visible,input:focus-visible{outline:2px solid var(--gps);outline-offset:2px}
.read{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px}
.read div{background:var(--card);border:1px solid var(--line);border-radius:6px;padding:8px 10px;min-width:0}
.read b{display:block;font-family:var(--mono);font-size:17px;font-variant-numeric:tabular-nums;font-weight:500}
.read small{color:var(--mute);font-size:11px;letter-spacing:.04em;text-transform:uppercase}
.note{color:var(--mute);font-size:12px;line-height:1.5}
</style>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;600&display=swap">
<script src="https://cdnjs.cloudflare.com/ajax/libs/plotly.js/2.35.0/plotly.min.js"></script>
<div class="wrap">
 <div><h1>__TITLE__</h1>
 <p class="sub">__SUB__</p></div>
 <div id="plot" role="img" aria-label="3D plot of the ground-truth track and the VIO trajectory"></div>
 <div class="legend"><span><i style="background:var(--gps)"></i>__GTLABEL__</span><span><i style="background:var(--vio)"></i>__VIOLABEL__</span><span><i style="background:var(--mk);height:8px;width:8px;border-radius:50%"></i>position at slider time</span></div>
 <div class="ctl"><button id="play" type="button">Play</button><input id="sl" type="range" min="0" value="0" step="1" aria-label="Time"><span id="tt" style="font-family:var(--mono);font-size:13px;min-width:56px;font-variant-numeric:tabular-nums"></span></div>
 <div class="read"><div><small>Time</small><b id="rt"></b></div><div><small>Height (GT)</small><b id="rh"></b></div><div><small>Error now</small><b id="re"></b></div><div><small>ATE (RMS)</small><b id="ra"></b></div></div>
 <p class="note">Drag to rotate, pinch or scroll to zoom. The grey segment joins the two positions at the slider time. __NOTE__ Error is a straight 3D distance after the global fit.</p>
</div>
<script>
const D=__DATA__;
const css=n=>getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const N=D.gt.t.length, sl=document.getElementById('sl'); sl.max=N-1;
function traces(i){return [
 {type:'scatter3d',mode:'lines',x:D.gt.x,y:D.gt.y,z:D.gt.z,name:'__GTLABEL__',line:{color:css('--gps'),width:4},hoverinfo:'skip'},
 {type:'scatter3d',mode:'lines',x:D.vio.x,y:D.vio.y,z:D.vio.z,name:'__VIOLABEL__',line:{color:css('--vio'),width:4},hoverinfo:'skip'},
 {type:'scatter3d',mode:'lines',x:[D.gt.x[i],D.at.x[i]],y:[D.gt.y[i],D.at.y[i]],z:[D.gt.z[i],D.at.z[i]],line:{color:css('--mute'),width:6},hoverinfo:'skip',showlegend:false},
 {type:'scatter3d',mode:'markers',x:[D.gt.x[i],D.at.x[i]],y:[D.gt.y[i],D.at.y[i]],z:[D.gt.z[i],D.at.z[i]],marker:{size:5,color:[css('--gps'),css('--vio')],line:{color:css('--mk'),width:2}},hoverinfo:'skip',showlegend:false}]}
function layout(){const ax=t=>({title:{text:t},color:css('--mute'),gridcolor:css('--line'),zerolinecolor:css('--line'),backgroundcolor:'rgba(0,0,0,0)',showbackground:false});
 return {margin:{l:0,r:0,t:0,b:0},paper_bgcolor:css('--card'),font:{color:css('--mute'),size:11},showlegend:false,scene:{xaxis:ax('E (m)'),yaxis:ax('N (m)'),zaxis:ax('height (m)'),aspectmode:'manual',aspectratio:{x:1,y:1,z:0.4},camera:{eye:{x:1.9,y:-1.9,z:1.5},center:{x:0,y:0,z:-0.15}}}}}
let cam=null;
function draw(i){const p=document.getElementById('plot');const l=layout(); if(cam) l.scene.camera=cam; Plotly.react(p,traces(i),l,{responsive:true,displaylogo:false,modeBarButtonsToRemove:['toImage']});
 if(!p._h){p._h=1;p.on('plotly_relayout',e=>{if(e['scene.camera'])cam=e['scene.camera']})}
 const t=D.gt.t[i];document.getElementById('tt').textContent=t.toFixed(0)+' s';document.getElementById('rt').textContent=t.toFixed(0)+' s';
 document.getElementById('rh').textContent=D.gt.z[i].toFixed(0)+' m';document.getElementById('re').textContent=D.at.err[i].toFixed(1)+' m';}
document.getElementById('ra').textContent=D.ate.toFixed(2)+' m';
sl.value=Math.floor(N*0.55);draw(+sl.value);
sl.addEventListener('input',()=>draw(+sl.value));
let tm=null;const pb=document.getElementById('play');
pb.onclick=()=>{if(tm){clearInterval(tm);tm=null;pb.textContent='Play';return}pb.textContent='Pause';if(+sl.value>=N-1)sl.value=0;
 tm=setInterval(()=>{let v=+sl.value+2;if(v>=N-1){v=N-1;clearInterval(tm);tm=null;pb.textContent='Play'}sl.value=v;draw(v)},120)};
matchMedia('(prefers-color-scheme:dark)').addEventListener('change',()=>setTimeout(()=>draw(+sl.value),50));
</script>
"""
