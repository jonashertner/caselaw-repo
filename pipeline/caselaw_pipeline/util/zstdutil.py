from __future__ import annotations

from pathlib import Path
import zstandard as zstd


def compress_zst(src: Path, dst: Path, level: int = 10) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cctx = zstd.ZstdCompressor(level=level, threads=-1)
    with src.open("rb") as ifh, dst.open("wb") as ofh:
        cctx.copy_stream(ifh, ofh)


def decompress_zst(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dctx = zstd.ZstdDecompressor()
    with src.open("rb") as ifh, dst.open("wb") as ofh:
        dctx.copy_stream(ifh, ofh)
