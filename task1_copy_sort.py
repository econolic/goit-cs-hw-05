#!/usr/bin/env python3
"""
Asynchronous recursive file copier & sorter by file extension.

Usage:
    python task1_copy_sort.py /path/to/source [/path/to/dest]

If *dest* is not provided, the default `./dist` folder is created in the current working directory.

Features:
    * Files are copied from the source directory into the destination directory, organized by file extension.
    * Files without an extension are copied into the ``no_extension`` subdirectory.
    * If a file with the same name exists in the target subdirectory:
        - If the content is identical (SHA-256 hash matches), the copy is skipped as a duplicate.
        - If the content differs, the file is copied with a hash suffix added to its name (e.g., `file_abcd1234.txt`).
    * Uses asynchronous I/O (asyncio, aiofiles) for efficient processing of many files.
    * Logs all operations: copied files, skipped identical duplicates, renamed copies for different files, and any errors.
"""

import argparse
import asyncio
import hashlib
import logging
from pathlib import Path
import aiofiles
import aiofiles.os

CHUNK_SIZE: int = 65536  # 64 KB
MAX_CONCURRENT: int = 100

def parse_args() -> argparse.Namespace:
    """Parse command-line parameters."""
    parser = argparse.ArgumentParser(
        description="Copies files from the *src* directory to *dst* by extension.",
    )
    parser.add_argument(
        "src",
        type=Path,
        help="Path to the source directory",
    )
    parser.add_argument(
        "dst",
        type=Path,
        nargs="?",
        default=Path("dist"),
        help="Path to the destination directory (default: ./dist)",
    )
    return parser.parse_args()

async def compute_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of the given file asynchronously."""
    hash_obj = hashlib.sha256()
    async with aiofiles.open(file_path, "rb") as f:
        while True:
            chunk = await f.read(CHUNK_SIZE)
            if not chunk:
                break
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

async def copy_file_task(src_path: Path, dst_root: Path, sem: asyncio.Semaphore) -> None:
    """Asynchronously copy a single file to the destination folder (by extension)."""
    async with sem:
        try:
            ext_folder: str = src_path.suffix.lower().lstrip(".") or "no_extension"
            dest_dir: Path = dst_root / ext_folder
            # Ensure destination subdirectory exists
            await aiofiles.os.makedirs(dest_dir, exist_ok=True)
            dest_file: Path = dest_dir / src_path.name
            if dest_file.exists():
                src_size = src_path.stat().st_size
                dst_size = dest_file.stat().st_size
                if src_size == dst_size:
                    src_hash = await compute_hash(src_path)
                    dst_hash = await compute_hash(dest_file)
                    if src_hash == dst_hash:
                        logging.info(f"Skipped duplicate (identical content): {src_path}")
                        return
                    else:
                        short_hash = src_hash[:8]
                        new_name = f"{src_path.stem}_{short_hash}{src_path.suffix}"
                        dest_file = dest_dir / new_name
                        logging.info(
                            f"Duplicate with different content, copying with new name: "
                            f"{src_path} -> {dest_file.name}"
                        )
                else:
                    src_hash = await compute_hash(src_path)
                    short_hash = src_hash[:8]
                    new_name = f"{src_path.stem}_{short_hash}{src_path.suffix}"
                    dest_file = dest_dir / new_name
                    logging.info(
                        f"Duplicate with different content (different size), copying with new name: "
                        f"{src_path} -> {dest_file.name}"
                    )
            else:
                logging.info(f"Copying file: {src_path} -> {dest_file}")
            async with aiofiles.open(src_path, "rb") as src_f, aiofiles.open(dest_file, "wb") as dst_f:
                while True:
                    data = await src_f.read(CHUNK_SIZE)
                    if not data:
                        break
                    await dst_f.write(data)
        except (PermissionError, OSError) as exc:
            logging.error(f"Error processing {src_path}: {exc}")

async def main_async(src: Path, dst: Path) -> None:
    """Main async workflow: gather files and initiate copy tasks."""
    src_path = src.resolve()
    dst_path = dst.resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"{src_path} is not a directory")
    dst_path.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    all_files = [p for p in src_path.rglob("*") if p.is_file()]
    files = [f for f in all_files if dst_path not in f.parents]
    # Pre-create extension directories
    ext_dirs = { (f.suffix.lower().lstrip(".") or "no_extension") for f in files }
    for ext in ext_dirs:
        await aiofiles.os.makedirs(dst_path / ext, exist_ok=True)
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [asyncio.create_task(copy_file_task(f, dst_path, sem)) for f in files]
    await asyncio.gather(*tasks)
    logging.info("Processing completed.")

def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args.src, args.dst))

if __name__ == "__main__":
    main()
