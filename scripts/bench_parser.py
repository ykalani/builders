"""Run the parser-only benchmark using the same window as the full pipeline."""
import json
import subprocess
import sys
from pathlib import Path

binary, data, workers = sys.argv[1:]
manifest = json.loads((Path(data) / "manifest.json").read_text())
subprocess.run([binary, data, manifest["window_start"], manifest["window_end"], workers], check=True)
