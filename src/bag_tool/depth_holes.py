"""Landscape-seam dropouts ("holes") in a PAS depth stream -- the sim-check port of
projectairsim/native/analysis/detect_depth_holes.py (spatial rules; same constants).

The defect: UE rasterises the landscape per component, and at component boundaries a
pixel sample occasionally falls in a sub-pixel crack and sees the skydome instead of
the ground -- one or a few pixels reading ~10-15 km inside otherwise continuous
terrain. The 5.7 map build fixes it at the source, so a mission bag is expected to
contain ZERO accepted dropouts; any means the map regressed (the fill pass in
analysis/ is the repair, not this check).

What must NOT be flagged: genuine sky above a ridge line and real depth discontinuities
at silhouettes. A pixel is accepted as a dropout only when the surface there is
continuous and known: the unfilled blob is small, its surrounding ring is essentially
all filled, and that ring is locally smooth. The detector's temporal corroboration
(reprojection into neighbouring frames via GT) is deliberately NOT ported: it needs
camera intrinsics and mount geometry, and the original treats it as corroboration
only -- a spatially accepted dropout is a dropout either way.

Alongside, per frame: the SKY fraction (rays that hit nothing). That is coverage, not
a defect; the runner's check_frame_coverage.py scores it against the height grid.
Reported here only as information (max sky fraction, frames with any sky).

Validated against the detector's own 10 known-answer cases (test_detector.py) --
identical accept/reject verdicts with scipy.ndimage in place of cv2.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
from scipy import ndimage

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

_TS = get_typestore(Stores.ROS2_JAZZY)

# --- what counts as unfilled (see detect_depth_holes.py for the measurements) ---------
SKY_M = 5000.0          # terrain fills 0-1 km, the skydome sits at 8-15 km, nothing between
INVALID_MAX = 0.0       # <= this is also unfilled (no return)
# --- spatial acceptance -----------------------------------------------------------
MAX_HOLE_PX = 16        # a seam crack is 1-4 px; anything bigger is not this defect
RING_DILATE = 2         # ring thickness sampled around the blob
RING_MIN_FILL = 0.95    # the ring must be essentially all filled
RING_SPREAD_M = 5.0     # absolute smoothness floor
RING_SPREAD_RE = 0.10   # ... or 10% of the ring depth, whichever is larger
BORDER_PX = 2           # the depth render's own edge artifact; not our defect

_STRUCT8 = np.ones((3, 3), bool)


def unfilled_mask(d: np.ndarray) -> np.ndarray:
    return (d >= SKY_M) | (d <= INVALID_MAX)


def analyse_frame(d: np.ndarray):
    """Return (accepted, rejected) blob records for one float32 depth image (metres)."""
    h, w = d.shape
    mask = unfilled_mask(d)
    if not mask.any():
        return [], []
    labels, n = ndimage.label(mask, structure=_STRUCT8)
    accepted, rejected = [], []
    objs = ndimage.find_objects(labels)
    for i, sl in enumerate(objs, start=1):
        if sl is None:
            continue
        ys, xs = np.nonzero(labels[sl] == i)
        ys = ys + sl[0].start
        xs = xs + sl[1].start
        area = int(len(ys))
        x0, y0 = int(xs.min()), int(ys.min())
        bw, bh = int(xs.max() - x0 + 1), int(ys.max() - y0 + 1)
        rec = {"area": area, "bbox": [x0, y0, bw, bh],
               "pixels": [[int(a), int(b)] for a, b in zip(ys, xs)][:64]}
        if area > MAX_HOLE_PX:
            rec["reject"] = "blob too large (%d px) -- sky or a real void, not a seam crack" % area
            rejected.append(rec)
            continue
        if (xs.min() < BORDER_PX or ys.min() < BORDER_PX or
                xs.max() >= w - BORDER_PX or ys.max() >= h - BORDER_PX):
            rec["reject"] = "touches the image border (render edge artifact)"
            rejected.append(rec)
            continue
        # ring = dilated blob minus the blob itself
        pad = RING_DILATE + 1
        sub = np.zeros((bh + 2 * pad, bw + 2 * pad), bool)
        sub[(ys - y0) + pad, (xs - x0) + pad] = True
        k = np.ones((2 * RING_DILATE + 1, 2 * RING_DILATE + 1), bool)
        ring = ndimage.binary_dilation(sub, structure=k) & ~sub
        oy, ox = y0 - pad, x0 - pad
        ry, rx = np.nonzero(ring)
        gy, gx = ry + oy, rx + ox
        keep = (gy >= 0) & (gy < h) & (gx >= 0) & (gx < w)
        gy, gx = gy[keep], gx[keep]
        vals = d[gy, gx]
        filled = ~unfilled_mask(vals)
        frac = float(filled.mean()) if vals.size else 0.0
        rec["ring_px"] = int(vals.size)
        rec["ring_filled_frac"] = round(frac, 3)
        if frac < RING_MIN_FILL:
            rec["reject"] = "ring only %.0f%% filled -- edge of a larger void" % (100 * frac)
            rejected.append(rec)
            continue
        good = vals[filled]
        med = float(np.median(good))
        spread = float(np.percentile(good, 90) - np.percentile(good, 10))
        rec["ring_median_m"] = round(med, 2)
        rec["ring_spread_m"] = round(spread, 2)
        limit = max(RING_SPREAD_M, RING_SPREAD_RE * med)
        if spread > limit:
            rec["reject"] = ("ring spans %.1f m (limit %.1f) -- a silhouette, "
                             "depth there is genuinely ambiguous" % (spread, limit))
            rejected.append(rec)
            continue
        rec["hole_depth_m"] = round(float(np.median(d[ys, xs])), 1)
        rec["fill_spatial_m"] = round(med, 2)
        accepted.append(rec)
    return accepted, rejected


def _decode_depth(msg) -> np.ndarray | None:
    data = bytes(msg.data)
    if msg.encoding == "32FC1":
        return np.frombuffer(data, dtype=np.float32).reshape(msg.height, msg.step // 4)[:, :msg.width]
    if msg.encoding in ("16UC1", "mono16"):           # legacy bags: uint16 centimetres
        return (np.frombuffer(data, dtype=np.uint16).reshape(msg.height, msg.step // 2)[:, :msg.width]
                .astype(np.float32) / 100.0)
    return None


def scan_bag(bag_path: str, depth_topic: str = "/camera/depth", stride: int = 1,
             limit: int = 0, keep_examples: int = 5) -> dict:
    """Scan every `stride`-th depth frame. Returns a summary dict:
    frames, frames_with_dropouts, dropout_px, sky_frames, max_sky_frac, examples."""
    n = seen = 0
    frames_bad = 0
    px_bad = 0
    sky_frames = 0
    max_sky = 0.0
    examples = []
    encoding = None
    with Reader(Path(bag_path)) as reader:
        conns = [c for c in reader.connections if c.topic == depth_topic]
        if not conns:
            return {"frames": 0, "error": f"no {depth_topic} in bag"}
        for conn, ts, raw in reader.messages(connections=conns):
            n += 1
            if stride > 1 and (n - 1) % stride:
                continue
            msg = _TS.deserialize_cdr(raw, conn.msgtype)
            d = _decode_depth(msg)
            if d is None:
                return {"frames": 0, "error": f"unsupported depth encoding {msg.encoding}"}
            encoding = msg.encoding
            seen += 1
            sky = float((d >= SKY_M).mean())
            if sky > 0:
                sky_frames += 1
                max_sky = max(max_sky, sky)
            acc, _ = analyse_frame(d)
            if acc:
                frames_bad += 1
                px_bad += sum(a["area"] for a in acc)
                if len(examples) < keep_examples:
                    examples.append({"frame": n, "t_ns": int(ts), "n_blobs": len(acc),
                                     "first": {k: acc[0][k] for k in ("bbox", "hole_depth_m", "fill_spatial_m")}})
            if limit and seen >= limit:
                break
    return {"frames": seen, "frames_total": n, "stride": stride, "encoding": encoding,
            "frames_with_dropouts": frames_bad, "dropout_px": px_bad,
            "sky_frames": sky_frames, "max_sky_frac": max_sky, "examples": examples}
