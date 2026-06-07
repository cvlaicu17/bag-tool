#!/usr/bin/env python3
"""Regenerate the Day20 barometer baseline literal for bag_tool/baro_baseline.py.

Runs the exact vio-check core (baro_metrics_from_arrays) on each given bag and
prints the DAY20_BARO_PER_FLIGHT dict to paste into baro_baseline.py.

Usage:
  python tools/gen_baro_baseline.py <bag1> [bag2 ...] \
      [--baro /altimeter/range] [--gt /pf_geo_loc/fc_local_position] \
      [--name-from-mcap]   # derive flight name from the .mcap filename
"""
import argparse, sys
from pathlib import Path
import numpy as np
from bag_tool.vio_check import read_baro_gt, baro_metrics_from_arrays

KEYS = ["ground_bias_m", "scale_pct", "noise_sigma_m",
        "resid_sigma_m", "spikes_pct", "pct_on_ground"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bags", nargs="+")
    ap.add_argument("--baro", default="/altimeter/range")
    ap.add_argument("--gt", default="/pf_geo_loc/fc_local_position")
    ap.add_argument("--ground-h", type=float, default=1.0)
    args = ap.parse_args()
    print("DAY20_BARO_PER_FLIGHT = {")
    for b in args.bags:
        name = Path(b).stem.replace("_0", "")
        bt, br, gt, gv = read_baro_gt(b, args.baro, args.gt)
        r = baro_metrics_from_arrays(bt, br, gt, gv, ground_h=args.ground_h)
        if not r.get("available"):
            print(f"    # {name}: SKIPPED ({'; '.join(r['issues'])})", file=sys.stderr)
            continue
        vals = {k: round(float(r[k]), 4) for k in KEYS if r.get(k) is not None}
        print(f"    {name!r}: {vals!r},")
    print("}")

if __name__ == "__main__":
    main()
