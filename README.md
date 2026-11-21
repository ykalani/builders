# Shopping Sessions: native pipeline

From this repository root, run `make setup` once (Python venv, a C++20 compiler, Docker with buildx, and Rust 1.85+ required). Then use `make fixtures`, `make dev`, `make run-dev`, and `make check-dev`. `make validate` runs all four in order and stops at the first failure. The pipeline implements strict parsing, linked identities, sessions, attribution, and assists.

`pipeline/engine.cpp` memory-maps event and click inputs and schedules LF-aligned chunks across `NCPU` workers. An event-ID bitmap scan identifies slots seen once. Unique recognized IDs bypass deduplication, while repeated slots and other ID formats use exact deduplication, preserving invalid earliest copies. The slot function reverses the seed-independent suffix of the public generator's ID permutation, with collisions safely routed through exact comparisons. Valid clicks go directly to person shards because identical click copies leave attribution and distinct campaign assists unchanged. Link ingestion assigns dense person numbers with union-find and preserves the byte-wise smallest name in each component. Short lowercase alphanumeric IDs use lossless numeric keys. Other IDs retain exact byte equality. Linked rows go straight to their assigned shard and local person index. Unlinked IDs receive an index within their owner shard. Both routing and grouping use 24-byte records. Each shard sorts each person's timeline and calls the shared session aggregator. Product categories are looked up only for purchases.

Native workers write user Parquet parts directly from typed Arrow batches. C++ merges worker campaign totals and writes the campaign Parquet part. The submission starts a native executable that reads the manifest and uses the Rust CSV tokenizer. Its Arrow and Parquet libraries come from the pinned PyArrow wheel at build time. The local shared-library and standalone aggregation tools remain available for regression checks. Native timings include peak RSS.

Attribution uses a rolling seven-day click queue, the latest winning click, and active click counts per campaign. Session starts snapshot the distinct assisting campaigns. Native stage times are logged to stderr. Run `.venv/bin/python scripts/check_attribution.py` after setup to compare rolling attribution with exhaustive click lookups.

For local profiling, the native timing log reports wall time for ingestion, deduplication, user shards, and campaign output. Grouping, sorting, aggregation with Arrow appends, and Parquet writing inside user shards are reported as sums of per-shard elapsed times because shards run concurrently. Input is CSV.

To enable the submission’s Rust CSV tokenizer in a local shared-library build, build a separate shared library with `.venv/bin/python scripts/build_native.py g++ /path/to/prototype/aggregate.so --rust-csv`. Put copies of `pipeline.py` and `native.py` beside it and run that entry point with the same input and worker count as the C++ build. The Rust build requires `rustc` on PATH.

Run `make bench-parser BENCH_DATA=dev/benchmark-20m CPUS=8` to time the C++ tokenizer and shared row validation in isolation.

For submission, push this repository to a public GitHub repository and provide its commit SHA to the platform. The platform builds the root Dockerfile and runs its default command. Keep generated data, output, and the virtual environment untracked; `.gitignore` excludes them. Run `make validate` successfully before submitting a solution.

This repository contains a native C++ pipeline and the supplied fixture and dev-data checkers.

If this starter and `rules.md` ever disagree, `rules.md` wins. Ask the organizers if `rules.md` leaves you unsure.

## What to do first

- Read `rules.md` for the full rules.
- The pipeline entry point is `pipeline/pipeline.py`; native ingestion is in `pipeline/engine.cpp`.
- Build the image and test against all 36 edge-case fixtures and the 1M-event dev set before submitting.

## Run and test locally

You need Docker with buildx, and Python 3.10+ with `polars` for the test scripts. Install it with `pip install polars`.

Build the image and run all 36 fixtures:

```sh
make build        # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures     # runs the image on all 36 fixtures with scorer flags, diffs each against expected/
```

Build, run, and check the 1M-event dev set. `make dev` builds the included generator source (Rust 1.85 or newer).

```sh
make dev                                # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                            # runs your image on dev/ with scorer flags and times it
make check-dev                          # exact diff against dev-answer/, the track page's dev-answer.zip unzipped here
```

If your laptop cannot spare 8 cores and 24 GB, run with lower limits using `make run-dev CPUS=4 MEM=8g`. Your local times will not match the scorer anyway; only the board's times count.

- **x86 Linux and Windows machines.** Build and run the amd64 image natively.
- **Apple-silicon Macs.** Emulating amd64 is slow. For fast local loops, build a native image with `make build run-dev PLATFORM=linux/arm64`. The Dockerfile pins no platform, so it builds either way. Check the amd64 image before you submit.

To iterate without Docker:

```sh
make setup
SCRATCH_DIR=/tmp .venv/bin/python scripts/run_fixtures.py fixtures -- .venv/bin/python pipeline/pipeline.py
```

## Submitting and scoring

Submit your public GitHub repository on the Buildathon track page. Enter a commit SHA to score a specific revision, or leave it empty to score the newest commit on the default branch.

The image's `CMD` runs with no arguments. Exit code 0 means "done, check my output". Build time is never timed; only the run is timed. The track page lists the current practice limits and how the final is scored.

## Container limits

Scorers are Google Cloud n2 Intel Xeon machines, Cascade Lake or newer, with AVX-512 available. Your container gets:

| Resource | Setting |
| --- | --- |
| `/data` | The dataset, read-only |
| `/output` | Empty, writable. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet |
| `/scratch` | Empty, writable, fast local disk for spills and temp files |
| CPU / RAM | 8 vCPUs, hyperthreads of an Intel Xeon, 24 GB, no swap |
| Network | None |
| Env vars | `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `SCRATCH_DIR=/scratch`, `HOME=/scratch`, `NCPU=8`, `MEM_GB=24` |
| Filesystem | Root is read-only apart from `/tmp`, 64 MB in memory. The process runs as uid 1000, not root |

## Common pitfalls

- **Memory.** Native records and mapped input bytes remain available through aggregation. The implementation uses memory rather than ingestion spills. Validate new dataset sizes against the 24-GB container limit.
- **CSV parsing.** Native parsing preserves spaces and CR, accepts only whole-field quoting, and splits records only at LF.
- **No extension downloads.** The container has no network at run time. PyArrow is installed at build time.
- **`\d` is not `[0-9]`.** In several regex engines, including Rust's used by Polars, `\d` matches any Unicode digit. The rules mean ASCII digits only.
- **Extra files in output folders.** `_SUCCESS` and other non-Parquet files in the output folders are ignored.

## Self-checks

- Summed over all rows, `sessions` and `revenue_cents` are equal in both tables.
- `day` must be a Parquet `DATE`, not a string or timestamp.
- Row order and the number of Parquet files do not matter. Exact column names do, and there must be no extra columns.

## Files in this starter

- `pipeline/pipeline.py` holds your pipeline code.
- `Dockerfile` builds a `linux/amd64` image for the Intel Xeon scorers.
- `fixtures/` holds the 36 edge cases, each an `input/` folder and the `expected/` tables.
- `scripts/` runs your image the way the scorer does and diffs the output.
