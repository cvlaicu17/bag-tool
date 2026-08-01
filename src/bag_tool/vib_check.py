"""vib-check: IMU vibration analysis subcommand."""

from __future__ import annotations

import struct
from pathlib import Path

try:
    import numpy as np
    from scipy.signal import welch
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


# ── Colour helpers ────────────────────────────────────────────────────────────
import sys as _sys

def _color_supported() -> bool:
    return hasattr(_sys.stdout, 'isatty') and _sys.stdout.isatty()

class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    CYAN   = "\033[96m"

def _c(code: str, s: str) -> str:
    return f"{code}{s}{C.RESET}" if _color_supported() else s

def ok(s):   return (_c(C.GREEN,  f"✔ {s}") if _color_supported() else f"OK  {s}")
def warn(s): return (_c(C.YELLOW, f"⚠ {s}") if _color_supported() else f"WRN {s}")
def fail(s): return (_c(C.RED,    f"✘ {s}") if _color_supported() else f"ERR {s}")
def hdr(s):  return _c(C.BOLD + C.CYAN, s)

SEP = "─" * 70

# ── Thresholds ─────────────────────────────────────────────────────────────────
# Derived from empirical comparison: working drone RMS ~1.0 m/s², failing ~5+ m/s²
_THRESH_RMS_WARN = 2.0   # m/s²  — elevated but may still work
_THRESH_RMS_FAIL = 4.0   # m/s²  — likely to break VIO
_THRESH_HF_WARN  = 0.5   # m/s²  RMS in 100-200 Hz band — resonance starting
_THRESH_HF_FAIL  = 2.0   # m/s²  — strong resonance


# ── CDR helpers ────────────────────────────────────────────────────────────────
def _decode_imu_cdr(data: bytes) -> tuple[float, float, float, float, float, float] | None:
    """
    Extract (ax, ay, az, gx, gy, gz) from a CDR-serialised sensor_msgs/Imu.
    Returns None if the buffer is too short.

    CDR layout (little-endian):
      [0:4]   CDR header
      [4:8]   stamp.sec  (uint32)
      [8:12]  stamp.nanosec  (uint32)
      [12:16] frame_id string length (uint32)
      [16 : 16+len] frame_id bytes (incl. null terminator)
      padded to 8-byte boundary for first double field (orientation.x)
      [pad : pad+32]  orientation quaternion (4 × float64)
      [+72]           orientation_covariance (9 × float64)
      [+24]           angular_velocity  (3 × float64)   ← gx, gy, gz
      [+72]           angular_velocity_covariance
      [+24]           linear_acceleration  (3 × float64) ← ax, ay, az
    """
    if len(data) < 16:
        return None
    str_len = struct.unpack_from('<I', data, 12)[0]
    after_str = 16 + str_len
    # align to 8-byte boundary relative to offset 4 (start of CDR content)
    after_str_rel = after_str - 4
    pad = (8 - (after_str_rel % 8)) % 8
    base = after_str + pad

    # orientation (32) + orientation_cov (72) = 104 bytes before angular_velocity
    gyro_off = base + 104
    # angular_velocity (24) + angular_velocity_cov (72) = 96 bytes before linear_acceleration
    accel_off = gyro_off + 96

    needed = accel_off + 24
    if len(data) < needed:
        return None

    gx, gy, gz = struct.unpack_from('<ddd', data, gyro_off)
    ax, ay, az = struct.unpack_from('<ddd', data, accel_off)
    return ax, ay, az, gx, gy, gz


# ── Data extraction ────────────────────────────────────────────────────────────
def extract_imu(bag_path: str, imu_topic: str) -> dict | None:
    """Return arrays of ax/ay/az/gx/gy/gz and log_times from the bag. None if topic missing."""
    from rosbags.rosbag2 import Reader

    path = Path(bag_path)
    # Pass file path directly so we don't accidentally read sibling .mcap files;
    # fall back to parent only for ROS2 bag directories (no .mcap extension).
    reader_path = path if path.is_file() else path

    ax_l, ay_l, az_l, gx_l, gy_l, gz_l, ts_l = [], [], [], [], [], [], []

    with Reader(reader_path) as reader:
        conns = [c for c in reader.connections if c.topic == imu_topic]
        if not conns:
            return None
        for connection, timestamp, rawdata in reader.messages(connections=conns):
            vals = _decode_imu_cdr(rawdata)
            if vals is None:
                continue
            ax, ay, az, gx, gy, gz = vals
            ax_l.append(ax); ay_l.append(ay); az_l.append(az)
            gx_l.append(gx); gy_l.append(gy); gz_l.append(gz)
            ts_l.append(timestamp)

    if not ts_l:
        return None

    return {
        'time': np.array(ts_l, dtype=np.float64) * 1e-9,
        'ax': np.array(ax_l), 'ay': np.array(ay_l), 'az': np.array(az_l),
        'gx': np.array(gx_l), 'gy': np.array(gy_l), 'gz': np.array(gz_l),
    }


# ── Vibration analysis ─────────────────────────────────────────────────────────
def analyze(d: dict, fs: float = 400.0) -> dict:
    acc_mag = np.sqrt(d['ax']**2 + d['ay']**2 + d['az']**2)
    acc_ac  = acc_mag - np.mean(acc_mag)
    rms_vib = float(np.sqrt(np.mean(acc_ac**2)))

    # Estimate actual sample rate from timestamps
    dt = np.median(np.diff(d['time']))
    if dt > 0:
        fs = float(1.0 / dt)

    f, psd = welch(acc_ac, fs=fs, nperseg=min(2048, len(acc_ac) // 4))

    _trap = getattr(np, "trapezoid", None) or np.trapz  # numpy>=2.0 renamed trapz→trapezoid

    def band_rms(flo, fhi):
        mask = (f >= flo) & (f < fhi)
        return float(np.sqrt(_trap(psd[mask], f[mask])))

    low_rms   = band_rms(0,   20)
    motor_rms = band_rms(20,  100)
    hf_rms    = band_rms(100, 200)

    top_peaks = [float(f[i]) for i in np.argsort(psd)[-5:][::-1]]

    # Per-axis PSD for detailed report
    axis_psd = {}
    for name, sig in [('x', d['ax']), ('y', d['ay']), ('z', d['az'])]:
        _, p = welch(sig - np.mean(sig), fs=fs, nperseg=min(2048, len(sig) // 4))
        axis_psd[name] = p

    issues = []
    passed = True

    if rms_vib >= _THRESH_RMS_FAIL:
        issues.append(f'Overall RMS {rms_vib:.2f} m/s² exceeds FAIL threshold ({_THRESH_RMS_FAIL} m/s²)')
        passed = False
    elif rms_vib >= _THRESH_RMS_WARN:
        issues.append(f'Overall RMS {rms_vib:.2f} m/s² exceeds WARN threshold ({_THRESH_RMS_WARN} m/s²)')

    if hf_rms >= _THRESH_HF_FAIL:
        issues.append(f'High-freq band RMS {hf_rms:.2f} m/s² (100-200 Hz) exceeds FAIL threshold ({_THRESH_HF_FAIL} m/s²) — likely structural resonance')
        passed = False
    elif hf_rms >= _THRESH_HF_WARN:
        issues.append(f'High-freq band RMS {hf_rms:.2f} m/s² (100-200 Hz) exceeds WARN threshold — possible resonance')

    return {
        'n': len(d['ax']),
        'duration_s': float(d['time'][-1] - d['time'][0]),
        'fs': fs,
        'rms_vib': rms_vib,
        'std_x': float(d['ax'].std()),
        'std_y': float(d['ay'].std()),
        'std_z': float(d['az'].std()),
        'std_gx': float(d['gx'].std()),
        'std_gy': float(d['gy'].std()),
        'std_gz': float(d['gz'].std()),
        'low_rms':   low_rms,
        'motor_rms': motor_rms,
        'hf_rms':    hf_rms,
        'top_peaks': top_peaks,
        'f': f, 'psd': psd, 'axis_psd': axis_psd,
        'issues': issues,
        'pass': passed,
    }


# ── Report printer ─────────────────────────────────────────────────────────────
def print_report(label: str, r: dict):
    status = ok("PASS") if r['pass'] else fail("FAIL")

    print(f"\n{hdr(label)}  [{status}]")
    print(f"  Messages : {r['n']:,}   Duration : {r['duration_s']:.1f} s"
          f"   Sample rate : {r['fs']:.1f} Hz")
    print(f"  Accel std  X={r['std_x']:.3f}  Y={r['std_y']:.3f}  Z={r['std_z']:.3f}  m/s²")
    print(f"  Gyro  std  X={r['std_gx']:.4f}  Y={r['std_gy']:.4f}  Z={r['std_gz']:.4f}  rad/s")

    peaks_str = '  '.join(f'{p:.0f} Hz' for p in r['top_peaks'])
    print(f"  Top PSD peaks : {peaks_str}")

    for issue in r['issues']:
        lvl = fail if 'FAIL' in issue else warn
        print(f"  {lvl(issue)}")


def print_comparison_table(bag_label: str, r: dict, ref_label: str, ref_r: dict):
    """Side-by-side comparison table of input bag vs reference."""

    use_color = _color_supported()

    def _colorize(plain: str, val: float, warn_t: float, fail_t: float) -> str:
        if not use_color:
            return plain
        if val >= fail_t:   return f'{C.RED}{plain}{C.RESET}'
        if val >= warn_t:   return f'{C.YELLOW}{plain}{C.RESET}'
        return f'{C.GREEN}{plain}{C.RESET}'

    def _cell_abs(val, warn_t, fail_t):
        """Absolute-threshold cell: returns (plain_text, colored_text, ratio_str)."""
        plain = f'{val:.2f} m/s²'
        return plain, _colorize(plain, val, warn_t, fail_t)

    def _cell_ratio(val, ref_val, warn_ratio, fail_ratio):
        """Ratio-threshold cell."""
        ratio = val / ref_val if ref_val and ref_val > 0 else 0
        plain = f'{val:.2f} m/s²'
        return plain, _colorize(plain, ratio, warn_ratio, fail_ratio)

    W_BAND  = 24
    W_VAL   = 14
    W_REF   = 14
    W_RATIO = 8

    b_lbl = (bag_label[:W_VAL - 1] + '…') if len(bag_label) > W_VAL else bag_label
    r_lbl = (ref_label[:W_REF - 1] + '…') if len(ref_label) > W_REF else ref_label

    top    = f"┌─{'─'*W_BAND}─┬─{'─'*W_VAL}─┬─{'─'*W_REF}─┬─{'─'*W_RATIO}─┐"
    h_sep  = f"├─{'─'*W_BAND}─┼─{'─'*W_VAL}─┼─{'─'*W_REF}─┼─{'─'*W_RATIO}─┤"
    bottom = f"└─{'─'*W_BAND}─┴─{'─'*W_VAL}─┴─{'─'*W_REF}─┴─{'─'*W_RATIO}─┘"
    header = (f"│ {'Band':<{W_BAND}} │ {b_lbl:<{W_VAL}} │"
              f" {r_lbl:<{W_REF}} │ {'Ratio':<{W_RATIO}} │")

    def ratio_str(val, ref_val):
        return f'{val/ref_val:.1f}×' if ref_val and ref_val > 0 else '—'

    rows = [
        ('0–20 Hz   (motion)',
         *_cell_ratio(r['low_rms'],   ref_r['low_rms'],   3.0, 6.0),
         f"{ref_r['low_rms']:.2f} m/s²",
         ratio_str(r['low_rms'],   ref_r['low_rms'])),
        ('20–100 Hz (motor/prop)',
         *_cell_ratio(r['motor_rms'], ref_r['motor_rms'], 2.0, 4.0),
         f"{ref_r['motor_rms']:.2f} m/s²",
         ratio_str(r['motor_rms'], ref_r['motor_rms'])),
        ('100–200 Hz (resonance)',
         *_cell_abs(r['hf_rms'],  _THRESH_HF_WARN,  _THRESH_HF_FAIL),
         f"{ref_r['hf_rms']:.2f} m/s²",
         ratio_str(r['hf_rms'],   ref_r['hf_rms'])),
        ('Overall RMS vib',
         *_cell_abs(r['rms_vib'], _THRESH_RMS_WARN, _THRESH_RMS_FAIL),
         f"{ref_r['rms_vib']:.2f} m/s²",
         ratio_str(r['rms_vib'],  ref_r['rms_vib'])),
    ]

    print()
    print(top)
    print(header)
    for band, plain, colored, ref_str, rat in rows:
        print(h_sep)
        # Pad using the plain (no-ANSI) width so columns always align
        cell = colored + ' ' * (W_VAL - len(plain))
        print(f"│ {band:<{W_BAND}} │ {cell} │ {ref_str:<{W_REF}} │ {rat:<{W_RATIO}} │")
    print(bottom)


# ── Plotting ───────────────────────────────────────────────────────────────────
def plot_results(bags: list[tuple[str, dict, dict]], save_path: str | None = None):
    if not HAS_MATPLOTLIB:
        print("[INFO] matplotlib not available — skipping plot")
        return

    n = len(bags)
    fig, axes = plt.subplots(2, n, figsize=(7 * n, 10))
    if n == 1:
        axes = [[axes[0]], [axes[1]]]

    palette = [
        ['#d62728', '#ff7f0e', '#9467bd'],
        ['#e377c2', '#bcbd22', '#17becf'],
        ['#1f77b4', '#2ca02c', '#8c564b'],
    ]

    for col, (label, d, r) in enumerate(bags):
        colors = palette[col % len(palette)]
        f = r['f']
        status = "PASS" if r['pass'] else "FAIL"

        # PSD of |accel| AC
        ax1 = axes[0][col]
        ax1.semilogy(f, r['psd'], color=colors[0], linewidth=0.9)
        ax1.axvspan(100, 200, color='red', alpha=0.06, label='resonance band')
        for pi in np.argsort(r['psd'])[-3:][::-1]:
            ax1.axvline(f[pi], color='red', alpha=0.5, linestyle='--', linewidth=0.7)
            ax1.text(f[pi] + 0.5, r['psd'][pi] * 1.3, f"{f[pi]:.0f} Hz", fontsize=7, color='red')
        ax1.set_title(f'{label} [{status}]\nPSD of |accel| (gravity removed)')
        ax1.set_xlabel('Frequency (Hz)')
        ax1.set_ylabel('PSD (m/s²)²/Hz')
        ax1.set_xlim(0, 200)
        ax1.grid(True, alpha=0.3)
        ax1.legend(fontsize=7)

        # PSD per axis
        ax2 = axes[1][col]
        for sig_name, c in zip(['x', 'y', 'z'], colors):
            ax2.semilogy(f, r['axis_psd'][sig_name], alpha=0.85, label=sig_name.upper(),
                         color=c, linewidth=0.8)
        ax2.set_title(f'{label}\nPSD per accel axis')
        ax2.set_xlabel('Frequency (Hz)')
        ax2.set_ylabel('PSD (m/s²)²/Hz')
        ax2.set_xlim(0, 200)
        ax2.legend(fontsize=7)
        ax2.grid(True, alpha=0.3)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"[INFO] Plot saved to {save_path}")
    else:
        plt.show()


# ── Entry point ────────────────────────────────────────────────────────────────
def run(args) -> None:
    if not HAS_SCIPY:
        print("ERROR: scipy is required for vib-check (pip install scipy)")
        raise SystemExit(1)

    print(SEP)
    print("  IMU Vibration Report")
    print(f"  Bag    : {args.input_bag}")
    if args.ref:
        print(f"  Ref    : {args.ref}")
    print(f"  Topic  : {args.imu_topic}")
    print(SEP)

    print("[INFO] Reading IMU data …")
    d = extract_imu(args.input_bag, args.imu_topic)
    if d is None:
        print(fail(f"No messages found on topic '{args.imu_topic}'"))
        raise SystemExit(1)

    r = analyze(d)

    ref_r = None
    ref_d = None
    if args.ref:
        print("[INFO] Reading reference IMU data …")
        ref_d = extract_imu(args.ref, args.imu_topic)
        if ref_d is None:
            print(warn(f"No IMU messages found in reference bag — skipping comparison"))
        else:
            ref_r = analyze(ref_d)

    print(SEP)
    bag_label = Path(args.input_bag).name
    print_report(bag_label, r)

    if ref_r is not None:
        ref_label = Path(args.ref).name
        print_report(f"{ref_label} (reference)", ref_r)
        print_comparison_table(bag_label, r, ref_label, ref_r)

    all_pass = r['pass'] and (ref_r is None or ref_r['pass'])
    print(f"\n{SEP}")
    if r['pass']:
        print(f"  Overall: {ok('VIBRATION LEVELS OK')}")
    else:
        print(f"  Overall: {fail('EXCESSIVE VIBRATION — consider mechanical fix or filter-imu')}")
    print(SEP + "\n")

    if args.plot or args.save_plot:
        bags = [(bag_label, d, r)]
        if ref_d is not None:
            bags.append((f"{Path(args.ref).name} (ref)", ref_d, ref_r))
        plot_results(bags, save_path=args.save_plot or None)
