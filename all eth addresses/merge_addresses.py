from pathlib import Path
import shutil

# ============================================================
# CONFIG
# ============================================================

INPUT_DIR = Path("chunks_24mb")
INPUT_PATTERN = "eth_addresses_*.txt"

OUTPUT_FILE = Path("ethereum_addresses_merged.txt")

BUFFER_SIZE = 8 * 1024 * 1024      # 8 MB copy buffer

# ============================================================


def main():

    if not INPUT_DIR.exists():
        print(f"[ERROR] Folder not found:")
        print(INPUT_DIR.resolve())
        return

    files = sorted(INPUT_DIR.glob(INPUT_PATTERN))

    if not files:
        print("[ERROR] No chunk files found.")
        return

    print("=" * 70)
    print("Ethereum Address Merge")
    print("=" * 70)
    print(f"Input Folder : {INPUT_DIR.resolve()}")
    print(f"Files        : {len(files):,}")
    print(f"Output File  : {OUTPUT_FILE.resolve()}")
    print("=" * 70)
    print()

    total_bytes = 0

    with OUTPUT_FILE.open("wb") as outfile:

        for index, file in enumerate(files, start=1):

            size = file.stat().st_size

            print(
                f"[{index:>5}/{len(files)}] "
                f"{file.name} "
                f"({size / 1024 / 1024:.2f} MB)"
            )

            with file.open("rb") as infile:
                shutil.copyfileobj(
                    infile,
                    outfile,
                    length=BUFFER_SIZE
                )

            total_bytes += size

    print()
    print("=" * 70)
    print("MERGE FINISHED")
    print("=" * 70)
    print(f"Files merged : {len(files):,}")
    print(f"Output size  : {total_bytes / 1024 / 1024:.2f} MB")
    print(f"Saved as     : {OUTPUT_FILE.resolve()}")
    print("=" * 70)


if __name__ == "__main__":
    main()