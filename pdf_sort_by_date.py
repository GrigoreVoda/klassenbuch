#!/usr/bin/env python3
"""
PDF Renamer by Date
-------------------
Extracts dates (DDMMYYYY) from PDF filenames (between underscores),
converts to YYYYMMDD, and renames files with a sorted numeric prefix.

Usage:
    python rename_pdfs_by_date.py <folder_path> [--dry-run]

Examples:
    python rename_pdfs_by_date.py ./pdfs
    python rename_pdfs_by_date.py ./pdfs --dry-run   # preview only, no changes
"""

import os
import re
import sys
import argparse
from datetime import datetime
from pathlib import Path


DATE_PATTERN = re.compile(r'(?<![0-9])(\d{8})(?![0-9])')  # exactly 8 digits


def extract_date(filename: str) -> datetime | None:
    """
    Find the first valid date (DDMMYYYY) between underscores in the filename.
    Returns a datetime object or None if no valid date found.
    """
    stem = Path(filename).stem  # strip extension
    parts = stem.split('_')

    for part in parts:
        matches = DATE_PATTERN.findall(part)
        for match in matches:
            # Try to parse as DDMMYYYY
            try:
                day   = int(match[0:2])
                month = int(match[2:4])
                year  = int(match[4:8])
                dt = datetime(year, month, day)
                return dt
            except ValueError:
                continue

    return None


def rename_pdfs(folder: str, dry_run: bool = False) -> None:
    folder_path = Path(folder).resolve()

    if not folder_path.is_dir():
        print(f"[ERROR] Not a valid directory: {folder_path}")
        sys.exit(1)

    pdf_files = list(folder_path.glob('*.pdf')) + list(folder_path.glob('*.PDF'))

    if not pdf_files:
        print(f"[INFO] No PDF files found in: {folder_path}")
        return

    print(f"[INFO] Found {len(pdf_files)} PDF(s) in: {folder_path}")

    # Extract dates and sort
    dated_files = []
    skipped = []

    for pdf in pdf_files:
        dt = extract_date(pdf.name)
        if dt:
            dated_files.append((dt, pdf))
        else:
            skipped.append(pdf)
            print(f"  [SKIP] No valid date found: {pdf.name}")

    if not dated_files:
        print("[WARNING] No files with valid dates to process.")
        return

    # Sort by date, then by original name as tiebreaker
    dated_files.sort(key=lambda x: (x[0], x[1].name))

    padding = len(str(len(dated_files)))  # e.g. 3 for 001, 002...

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Renaming {len(dated_files)} file(s):\n")

    for idx, (dt, pdf) in enumerate(dated_files, start=1):
        date_prefix = dt.strftime('%Y%m%d')           # YYYYMMDD
        order_prefix = str(idx).zfill(padding)        # e.g. 001
        new_name = f"{date_prefix}_{pdf.name}"
        new_path = pdf.parent / new_name

        print(f"  {order_prefix}. {pdf.name}")
        print(f"     --> {new_name}")

        if not dry_run:
            if new_path.exists():
                print(f"     [WARNING] Target already exists, skipping!")
            else:
                pdf.rename(new_path)

    if skipped:
        print(f"\n[INFO] {len(skipped)} file(s) skipped (no date found):")
        for f in skipped:
            print(f"  - {f.name}")

    if dry_run:
        print("\n[DRY RUN] No files were changed. Remove --dry-run to apply.")
    else:
        print(f"\n[DONE] Renamed {len(dated_files)} file(s).")


def main():
    parser = argparse.ArgumentParser(
        description='Rename PDFs by date (DDMMYYYY) found in their filename.'
    )
    parser.add_argument(
        'folder',
        help='Path to the folder containing PDF files'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without renaming any files'
    )

    args = parser.parse_args()
    rename_pdfs(args.folder, dry_run=args.dry_run)


if __name__ == '__main__':
    main()