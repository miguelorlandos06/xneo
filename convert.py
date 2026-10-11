#!/usr/bin/env python3
import os
import sys
import json
import asyncio
import subprocess
import shutil
from pathlib import Path

import aiohttp
import aiofiles
import aioboto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig


API_BASE   = "https://xneo-3znw.onrender.com"
API_SECRET = "7f3a9b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

S3_ENDPOINT    = "https://s3.todus.cu"
S3_BUCKET      = "stream"
S3_REGION      = "us-east-1"
S3_PUBLIC_BASE = "https://s3.todus.cu/stream"

JOB_ID = os.environ["JOB_ID"]
SOURCE = os.environ["SOURCE"]
META   = json.loads(os.environ.get("META", "{}"))

WORK = Path("/tmp/work")
WORK.mkdir(exist_ok=True)
INPUT = WORK / "input.mp4"
OUT = WORK / "out"

SEG_DURATION = 4
MAX_INPUT_SIZE = 2 * 1024 * 1024 * 1024
DOWNLOAD_CHUNK = 1024 * 1024
UPLOAD_CONCURRENCY = 16

QUALITIES = [
    ("240p", 426, 240, "300k", "64k"),
    ("360p", 640, 360, "700k", "96k"),
    ("480p", 854, 480, "1200k", "128k"),
    ("720p", 1280, 720, "2500k", "128k"),
]

_s3_cfg = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=20,
    connect_timeout=30,
    read_timeout=300,
)
_s3_session = aioboto3.Session()


def content_type(name: str) -> str:
    if name.endswith(".m3u8"):
        return "application/vnd.apple.mpegurl"
    if name.endswith(".m4s"):
        return "video/iso.segment"
    if name.endswith(".mp4"):
        return "video/mp4"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    return "application/octet-stream"


def cache_control(name: str) -> str:
    if name.endswith(".m3u8"):
        return "public, max-age=300"
    return "public, max-age=31536000, immutable"


async def download():
    timeout = aiohttp.ClientTimeout(total=3600, connect=30, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(SOURCE, headers={"User-Agent": "Mozilla/5.0"}) as r:
            if r.status >= 400:
                raise RuntimeError(f"HTTP {r.status}")
            total = int(r.headers.get("Content-Length", 0))
            if total and total > MAX_INPUT_SIZE:
                raise RuntimeError("Video demasiado grande")
            async with aiofiles.open(INPUT, "wb") as f:
                async for chunk in r.content.iter_chunked(DOWNLOAD_CHUNK):
                    await f.write(chunk)


async def extract_metadata(path: Path) -> dict:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries",
        "format=duration,size,bit_rate:stream=codec_type,codec_name,width,height",
        "-of", "json", str(path),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, _ = await proc.communicate()
    data = json.loads(out.decode())
    fmt = data.get("format", {})
    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), {})
    return {
        "duration": float(fmt.get("duration", 0)),
        "size": int(fmt.get("size", path.stat().st_size)),
        "bitrate": int(fmt.get("bit_rate", 0)),
        "width": video.get("width"),
        "height": video.get("height"),
        "vcodec": video.get("codec_name"),
    }


async def detect_fps() -> int:
    def _run():
        return subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=r_frame_rate", "-of", "csv=p=0", str(INPUT)],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
    raw = await asyncio.to_thread(_run)
    if "/" not in raw:
        return 30
    n, d = raw.split("/")
    return max(1, round(int(n) / int(d)))


async def make_thumbnail(path: Path, out: Path) -> bool:
    cmd = [
        "ffmpeg", "-y", "-ss", "3", "-i", str(path),
        "-frames:v", "1", "-vf", "scale=640:-2",
        "-q:v", "3", str(out),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return proc.returncode == 0 and out.exists()


async def upload_file(local: Path, key: str, ctype: str,
                     cache: str = "public, max-age=31536000, immutable") -> str:
    async with _s3_session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public",
        aws_secret_access_key="public",
        region_name=S3_REGION,
        config=_s3_cfg,
    ) as s3:
        with open(local, "rb") as f:
            await s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=f,
                ContentType=ctype,
                CacheControl=cache,
                ACL="public-read",
            )
    return f"{S3_PUBLIC_BASE}/{key}"


async def transcode():
    fps = await detect_fps()
    gop = fps * SEG_DURATION
    OUT.mkdir(exist_ok=True)
    for name, *_ in QUALITIES:
        (OUT / name).mkdir(exist_ok=True)

    n = len(QUALITIES)
    split = "".join(f"[v{i}]" for i in range(n))
    scale = ";".join(
        f"[v{i}]scale=w={w}:h={h}:force_original_aspect_ratio=decrease,"
        f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2[v{i}out]"
        for i, (_, w, h, _, _) in enumerate(QUALITIES)
    )
    fc = f"[0:v]split={n}{split};{scale}"

    cmd = ["ffmpeg", "-y", "-i", str(INPUT), "-filter_complex", fc]
    for i, (_, _, _, vb, _) in enumerate(QUALITIES):
        cmd += [
            "-map", f"[v{i}out]",
            f"-c:v:{i}", "libx264",
            f"-b:v:{i}", vb,
            f"-maxrate:v:{i}", vb,
            f"-bufsize:v:{i}", f"{int(vb[:-1])*2}k",
            "-preset", "veryfast",
            "-force_key_frames", f"expr:gte(t,n_forced*{SEG_DURATION})",
            "-g", str(gop), "-keyint_min", str(gop), "-sc_threshold", "0",
        ]
    for i, (_, _, _, _, ab) in enumerate(QUALITIES):
        cmd += ["-map", "a:0", f"-c:a:{i}", "aac", f"-b:a:{i}", ab, "-ac", "2"]

    var_map = " ".join(
        f"v:{i},a:{i},name:{name}" for i, (name, *_ ) in enumerate(QUALITIES)
    )

    cmd += [
        "-f", "hls",
        "-hls_time", str(SEG_DURATION),
        "-hls_playlist_type", "vod",
        "-hls_flags", "independent_segments",
        "-hls_segment_type", "fmp4",
        "-hls_fmp4_init_filename", "init.mp4",
        "-hls_segment_filename", f"{OUT}/%v/segment_%05d.m4s",
        "-master_pl_name", "master.m3u8",
        "-var_stream_map", var_map,
        f"{OUT}/%v/playlist.m3u8",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg: {err[-500:].decode(errors='ignore')}")


async def upload_all():
    sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
    files = sorted(f for f in OUT.rglob("*") if f.is_file())

    async with _s3_session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public",
        aws_secret_access_key="public",
        region_name=S3_REGION,
        config=_s3_cfg,
    ) as s3:
        async def up(f: Path):
            rel = f.relative_to(OUT).as_posix()
            key = f"hls/{JOB_ID}/{rel}"
            async with sem:
                with open(f, "rb") as body:
                    await s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=key,
                        Body=body,
                        ContentType=content_type(str(f)),
                        CacheControl=cache_control(str(f)),
                        ACL="public-read",
                    )
        await asyncio.gather(*(up(f) for f in files))
    return files


async def delete_tmp():
    async with _s3_session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public",
        aws_secret_access_key="public",
        region_name=S3_REGION,
        config=_s3_cfg,
    ) as s3:
        try:
            await s3.delete_object(Bucket=S3_BUCKET, Key=f"tmp/{JOB_ID}.mp4")
        except Exception as e:
            print("no pude borrar tmp:", e)


async def notify(status: str, master_url: str = None, error: str = None, stats: dict = None):
    payload = {
        "job_id": JOB_ID,
        "status": status,
        "master_url": master_url,
        "error": error,
        "stats": stats or {},
    }
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{API_BASE}/api/admin/videos/{JOB_ID}/finish",
            json=payload,
            headers={"X-Admin-Token": API_SECRET},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            print("callback:", r.status, await r.text())


async def main():
    try:
        print(f"[{JOB_ID}] descargando…")
        await download()

        print(f"[{JOB_ID}] extrayendo metadatos…")
        meta = await extract_metadata(INPUT)

        print(f"[{JOB_ID}] generando miniatura…")
        thumb_local = WORK / "thumb.jpg"
        thumb_url = None
        if await make_thumbnail(INPUT, thumb_local):
            thumb_url = await upload_file(thumb_local, f"thumbs/{JOB_ID}.jpg", "image/jpeg")

        print(f"[{JOB_ID}] transcodificando…")
        await transcode()

        print(f"[{JOB_ID}] subiendo HLS…")
        files = await upload_all()
        master_url = f"{S3_PUBLIC_BASE}/hls/{JOB_ID}/master.m3u8"

        await notify(
            "ready",
            master_url=master_url,
            stats={
                "files": len(files),
                "bytes": sum(f.stat().st_size for f in files),
                "meta": meta,
                "thumb_url": thumb_url,
            },
        )
        await delete_tmp()
        print(f"[{JOB_ID}] OK → {master_url}")
    except Exception as e:
        print(f"[{JOB_ID}] ERROR: {e}")
        await notify("error", error=str(e)[:500])
        sys.exit(1)
    finally:
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
