"""
run_all_scrapers.py -- Orchestrator that runs all store scrapers simultaneously.

Launches all 6 scrapers as independent subprocesses and waits for them to finish.
"""

import subprocess
import sys
from datetime import datetime


SCRAPERS = [
    "calgary_coop_scraper",
    "freshco_scraper",
    "safeway_scraper",
    "saveonfoods_scraper",
    "sobeys_scraper",
    "superstore_scraper",
]


def run():
    print(f"\n{'='*60}")
    print(f"  Cartly Scraper Job -- {datetime.utcnow().isoformat()}Z")
    print(f"  Running all {len(SCRAPERS)} scrapers simultaneously")
    print(f"{'='*60}\n", flush=True)

    processes = {}
    for name in SCRAPERS:
        print(f"  Launching: {name}", flush=True)
        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", f"import {name}; {name}.main()"],
        )
        processes[name] = proc

    results = {}
    for name, proc in processes.items():
        proc.wait()
        if proc.returncode == 0:
            results[name] = "Success"
        else:
            results[name] = f"Failed: exit code {proc.returncode}"
        print(f"  Finished: {name:30s} {results[name]}", flush=True)

    # -- Summary -----------------------------------------------
    print(f"\n\n{'='*60}")
    print("  JOB SUMMARY")
    print(f"{'='*60}")
    for name, status in results.items():
        print(f"  {name:30s} {status}")

    failed = [n for n, s in results.items() if s.startswith("Failed")]
    if failed:
        print(f"\n  {len(failed)} scraper(s) failed.")
        sys.exit(1)
    else:
        print("\n  All scrapers completed successfully.")
        sys.exit(0)


if __name__ == "__main__":
    run()
