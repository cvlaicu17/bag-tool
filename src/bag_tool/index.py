"""Write indexed copies of a bag's MCAP files so Foxglove can open them.

lee_capture_node writes MCAP without chunks (so without chunk/message indexes) and splits at
2 GiB. Foxglove refuses unindexed files over 1 GB ("Unable to open unindexed MCAP file").
`mcap sort` rewrites one file chunked and indexed; `mcap merge` writes all of them into one.

The originals are never modified. Each output is written under a temporary name and renamed
only after its message count matches the input's, so an interrupted run never leaves a
file that looks finished.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


def find_mcap(explicit: str | None) -> str:
    for cand in (explicit, os.environ.get("MCAP_BIN"), shutil.which("mcap"),
                 str(Path.home() / "bin" / "mcap")):
        if cand and Path(cand).is_file() and os.access(cand, os.X_OK):
            return cand
    sys.exit("ERROR: the mcap CLI was not found (PATH, ~/bin/mcap, $MCAP_BIN or --mcap).\n"
             "  Run ./install.sh (installs it to ~/bin/mcap) or download it from\n"
             "  https://github.com/foxglove/mcap/releases (mcap-cli).")


def _natural_key(p: Path):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", p.name)]


def bag_files(bag: Path) -> list[Path]:
    if bag.is_file():
        return [bag]
    files = sorted(bag.glob("*.mcap"), key=_natural_key)
    if not files:
        sys.exit(f"ERROR: no .mcap files in {bag}")
    return files


def message_count(mcap: str, f: Path) -> int | None:
    """Messages per the file's summary (`mcap info`); None if it cannot be read."""
    r = subprocess.run([mcap, "info", str(f)], capture_output=True, text=True)
    m = re.search(r"^messages:\s*(\d+)", r.stdout, re.MULTILINE)
    return int(m.group(1)) if r.returncode == 0 and m else None


def _check_space(out_dir: Path, need: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(out_dir).free
    if free < need * 1.05:
        sys.exit(f"ERROR: {free / 1e9:.1f} GB free in {out_dir}, need ~{need / 1e9:.1f} GB")


def _mcap(mcap: str, cmd: list[str], out: Path) -> bool:
    r = subprocess.run([mcap, *cmd, "-o", str(out)], capture_output=True, text=True)
    if r.returncode != 0:
        out.unlink(missing_ok=True)
        print(f"  FAILED: mcap {cmd[0]}: {(r.stderr or r.stdout).strip()}", file=sys.stderr)
    return r.returncode == 0


def _write(mcap: str, cmd: list[str], out: Path, expect: int | None) -> bool:
    tmp = out.with_name(out.stem + ".partial" + out.suffix)
    tmp.unlink(missing_ok=True)
    if not _mcap(mcap, cmd, tmp):
        return False
    got = message_count(mcap, tmp)
    if got is None or (expect is not None and got != expect):
        print(f"  FAILED: {got} messages written, {expect} in the input; kept {tmp.name}",
              file=sys.stderr)
        return False
    tmp.rename(out)
    print(f"  ok: {got} messages")
    return True


def _recover_and_sort(mcap: str, src: Path, out: Path) -> bool:
    """A file without a readable summary (recording killed, Jetson lost power): `mcap recover`
    keeps every complete record, then `mcap sort` adds the indexes (DESIGN.md kill test)."""
    print("  no summary (unfinished file): recover, then sort")
    rec = out.with_name(out.stem + ".recovered" + out.suffix)
    if not _mcap(mcap, ["recover", str(src)], rec):
        return False
    try:
        return _write(mcap, ["sort", str(rec)], out, message_count(mcap, rec))
    finally:
        rec.unlink(missing_ok=True)


def run(input_bag: str, output: str | None, merge: bool, mcap_bin: str | None,
        force: bool) -> int:
    mcap = find_mcap(mcap_bin)
    bag = Path(input_bag).expanduser().resolve()
    files = bag_files(bag)
    total = sum(f.stat().st_size for f in files)
    counts = {f: message_count(mcap, f) for f in files}

    if merge:
        name = bag.stem if bag.is_file() else bag.name
        out = Path(output).expanduser() if output else bag.parent / f"{name}_indexed.mcap"
        if out.exists() and not force:
            print(f"{out} exists (use --force to rewrite)")
            return 0
        bad = [f.name for f, n in counts.items() if n is None]
        if bad:
            print(f"ERROR: unfinished file(s) {bad}: run without --merge (recovers them)",
                  file=sys.stderr)
            return 1
        _check_space(out.parent, total)
        print(f"merging {len(files)} file(s), {total / 1e9:.1f} GB -> {out}")
        # lee_capture_node repeats "lee.pad" filler (and one lee.rig_config per file): keep them all
        merged = _write(mcap, ["merge", "--allow-duplicate-metadata", *map(str, files)], out,
                        sum(counts.values()))
        return 0 if merged else 1

    out_dir = (Path(output).expanduser() if output
               else (bag.parent if bag.is_file() else bag) / "indexed")
    todo = [f for f in files if force or not (out_dir / f.name).exists()]
    _check_space(out_dir, sum(f.stat().st_size for f in todo))
    print(f"{len(files)} file(s), {total / 1e9:.1f} GB -> {out_dir}/ "
          f"({len(files) - len(todo)} already done)")
    ok = True
    for f in todo:
        print(f"{f.name}")
        if counts[f] is None:
            ok &= _recover_and_sort(mcap, f, out_dir / f.name)
        else:
            ok &= _write(mcap, ["sort", str(f)], out_dir / f.name, counts[f])
    meta = bag / "metadata.yaml"
    if bag.is_dir() and meta.is_file():
        shutil.copy2(meta, out_dir / "metadata.yaml")  # same file names: still a rosbag2 bag
    elif bag.is_dir():
        print("note: no metadata.yaml (recording did not finish): fine for Foxglove; "
              "for ros2 tools run `ros2 bag reindex` on the output")
    if ok:
        print(f"done: open all {len(files)} files in {out_dir}/ together in Foxglove")
    return 0 if ok else 1
