#!/usr/bin/env python3
import argparse, math
from pathlib import Path

def load_clean_lines(p: Path):
    out = []
    for raw in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out

def chunks_even(seq, parts):
    n = len(seq)
    base = n // parts
    rem = n % parts
    out = []
    i = 0
    for k in range(parts):
        sz = base + (1 if k < rem else 0)
        out.append(seq[i:i+sz])
        i += sz
    return out

def main():
    ap = argparse.ArgumentParser(description="Split wells.txt into N parts.")
    ap.add_argument("master", nargs="?", default="wells.txt", help="Path to master wells file")
    ap.add_argument("--outdir", default="wells_parts", help="Output directory for parts")
    ap.add_argument("--parts", type=int, default=30, help="Number of parts")
    args = ap.parse_args()

    master = Path(args.master)
    if not master.is_file():
        raise SystemExit(f"[error] not found: {master}")

    lines = load_clean_lines(master)
    if not lines:
        raise SystemExit("[error] no wells after cleaning (empty or only comments)")

    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    width = max(2, len(str(args.parts - 1)))  # zero-padding width

    groups = chunks_even(lines, args.parts)
    count = 0
    for i, g in enumerate(groups):
        outp = Path(args.outdir) / f"wells_{i:0{width}d}.txt"
        outp.write_text("\n".join(g) + "\n", encoding="utf-8")
        count += 1

    print(f"[ok] wrote {count} parts to {args.outdir}/ (zero-padded to width {width})")

if __name__ == "__main__":
    main()
