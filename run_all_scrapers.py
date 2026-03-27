"""
run_all_scrapers.py -- Orchestrator that runs all store scrapers simultaneously.

Launches all 6 scrapers as independent subprocesses.
All output is logged to /opt/cartly/scraper.log and also printed to console.
"""

import subprocess
import sys
import os
from datetime import datetime

LOG_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(LOG_DIR, "scraper.log")

SCRAPERS = [
    "calgary_coop_scraper",
    "freshco_scraper",
    "safeway_scraper",
    "saveonfoods_scraper",
    "sobeys_scraper",
    "superstore_scraper",
]


def log(msg):
    """Print to console and append to log file."""
    print(msg, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(msg + "\n")


def run():
    timestamp = datetime.utcnow().isoformat()

    log(f"\n{'='*60}")
    log(f"  Cartly Scraper Job -- {timestamp}Z")
    log(f"  Running all {len(SCRAPERS)} scrapers simultaneously")
    log(f"  Log file: {LOG_FILE}")
    log(f"{'='*60}\n")

    # Open individual log files for each scraper's output
    log_files = {}
    processes = {}
    for name in SCRAPERS:
        scraper_log = os.path.join(LOG_DIR, f"{name}.log")
        lf = open(scraper_log, "w")
        log_files[name] = lf
        log(f"  Launching: {name} (log: {scraper_log})")
        proc = subprocess.Popen(
            [sys.executable, "-u", "-c", f"import {name}; {name}.main()"],
            stdout=lf,
            stderr=subprocess.STDOUT,
        )
        processes[name] = proc

    results = {}
    for name, proc in processes.items():
        proc.wait()
        log_files[name].close()
        if proc.returncode == 0:
            results[name] = "Success"
        else:
            results[name] = f"Failed: exit code {proc.returncode}"
        log(f"  Finished: {name:30s} {results[name]}")

    # Combine all individual logs into the main log
    log(f"\n\n{'='*60}")
    log("  INDIVIDUAL SCRAPER LOGS")
    log(f"{'='*60}")
    for name in SCRAPERS:
        scraper_log = os.path.join(LOG_DIR, f"{name}.log")
        log(f"\n{'─'*50}")
        log(f"  {name}")
        log(f"{'─'*50}")
        try:
            with open(scraper_log, "r") as f:
                content = f.read()
            # Only include last 200 lines to keep the summary manageable
            lines = content.strip().split("\n")
            if len(lines) > 200:
                log(f"  ... ({len(lines) - 200} lines omitted, see {scraper_log} for full output)")
                for line in lines[-200:]:
                    log(f"  {line}")
            else:
                for line in lines:
                    log(f"  {line}")
        except Exception as e:
            log(f"  Could not read log: {e}")

    # -- Summary -----------------------------------------------
    log(f"\n\n{'='*60}")
    log("  JOB SUMMARY")
    log(f"{'='*60}")
    for name, status in results.items():
        log(f"  {name:30s} {status}")

    failed = [n for n, s in results.items() if s.startswith("Failed")]
    if failed:
        log(f"\n  {len(failed)} scraper(s) failed.")
        sys.exit(1)
    else:
        log("\n  All scrapers completed successfully.")
        sys.exit(0)


if __name__ == "__main__":
    run()
