#!/usr/bin/env python3
"""In-memory C++ ingestion, deduplication, sorting and aggregation; Arrow Parquet output."""
import json
import os
from datetime import datetime
from pathlib import Path

from native import run_pipeline

DATA = Path(os.environ.get('DATA_DIR', '/data'))
OUT = Path(os.environ.get('OUTPUT_DIR', '/output'))
NCPU = int(os.environ.get('NCPU', '8'))
manifest = json.loads((DATA / 'manifest.json').read_text())
start, end = (int(datetime.fromisoformat(manifest[k].replace('Z', '+00:00')).timestamp()) * 1000
              for k in ('window_start', 'window_end'))

run_pipeline(DATA, OUT, start, end, NCPU)
