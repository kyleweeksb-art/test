"""
patch_scrapers.py — Automated patcher for scraper files.

Run this once to update all five scrapers so they use the shared
Entra ID database connection module instead of password-based auth.

Usage:
    python patch_scrapers.py

What it does for each scraper:
  1. Removes the DB_CONFIG dict
  2. Replaces `mysql.connector.connect(**DB_CONFIG)` calls with
     `get_db_connection()` from db_connection.py
  3. Removes the unused `os.environ.get("MYSQL_PASSWORD")` import pattern
"""

import re
import sys
import os

SCRAPERS = [
    "calgary_coop_scraper.py",
    "freshco_scraper.py",
    "safeway_scraper.py",
    "saveonfoods_scraper.py",
    "sobeys_scraper.py",
    "superstore_scraper.py",
]


def patch_file(filepath: str) -> bool:
    """Apply all patches to a single scraper file. Returns True if modified."""
    with open(filepath, "r") as f:
        content = f.read()

    original = content

    # 1. Add the import for db_connection at the top (after existing imports)
    if "from db_connection import get_db_connection" not in content:
        # Insert after the last 'from ... import ...' or 'import ...' line
        lines = content.split("\n")
        last_import_idx = 0
        for i, line in enumerate(lines):
            if line.startswith("import ") or line.startswith("from "):
                last_import_idx = i
        lines.insert(last_import_idx + 1, "from db_connection import get_db_connection")
        content = "\n".join(lines)

    # 2. Remove the DB_CONFIG block
    content = re.sub(
        r'\n# ── Database config.*?DB_CONFIG\s*=\s*\{[^}]+\}\n',
        '\n',
        content,
        flags=re.DOTALL,
    )

    # 3. Replace mysql.connector.connect(**DB_CONFIG) with get_db_connection()
    content = content.replace(
        "mysql.connector.connect(**DB_CONFIG)",
        "get_db_connection()"
    )

    # 4. Remove unused import of mysql.connector if it's only used for connect
    # Keep it if Error is still imported
    if "from mysql.connector import Error" in content:
        # Error is still needed, just remove the direct mysql.connector import
        # if it's a separate line
        content = re.sub(r'^import mysql\.connector\n', '', content, flags=re.MULTILINE)
    # Note: We keep 'from mysql.connector import Error' since it's used in except blocks

    if content != original:
        with open(filepath, "w") as f:
            f.write(content)
        return True
    return False


def main():
    patched = 0
    for scraper in SCRAPERS:
        if not os.path.exists(scraper):
            print(f"    {scraper} not found — skipping")
            continue
        if patch_file(scraper):
            print(f"   Patched: {scraper}")
            patched += 1
        else:
            print(f"  ─  No changes needed: {scraper}")

    print(f"\nDone. {patched} file(s) patched.")
    if patched > 0:
        print("Remember to test locally before deploying!")


if __name__ == "__main__":
    main()
