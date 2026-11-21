"""Build the native pipeline against the pinned PyArrow wheel's C++ libraries."""
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

import pyarrow

root = Path(__file__).resolve().parents[1]
pipeline = root / "pipeline"
libraries = Path(pyarrow.__file__).parent


options = sys.argv[3:]
executable = "--executable" in options
mode = ["-DNATIVE_MAIN", "-flto"] if executable else ["-fPIC", "-shared"]
if executable and platform.machine() == "x86_64":
    mode.append("-march=x86-64-v3")
rpath = "$ORIGIN" if executable else str(libraries)


def build(extra):
    subprocess.run([
        sys.argv[1], "-std=c++20", "-O3", "-pthread", "-Wall", "-Wextra", "-Werror",
        "-DAGGREGATE_LIBRARY", *mode, "-isystem", pyarrow.get_include(),
        pipeline / "aggregate.cpp", pipeline / "engine.cpp",
        "-L", libraries, f"-Wl,-rpath,{rpath}",
        *[f"-l:{next(libraries.glob(f'lib{name}.so.*')).name}" for name in ("parquet", "arrow")],
        *extra, "-o", sys.argv[2],
    ], check=True)


if "--rust-csv" in options:
    with tempfile.TemporaryDirectory() as folder:
        scanner = Path(folder) / "librust_csv.a"
        subprocess.run(["rustc", "--crate-type=staticlib", "-O", "-C", "panic=abort",
                        pipeline / "rust_csv.rs", "-o", scanner], check=True)
        build(["-DRUST_CSV", scanner, "-ldl", "-Wl,-z,defs"])
else:
    build([])
