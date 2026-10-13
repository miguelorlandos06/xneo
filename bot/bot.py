#!/usr/bin/env python3
"""
XNEO Bot + Convertidor — todo en un solo proceso (GitHub Actions).
El bot recibe videos/URLs y, tras elegir categoría, convierte a HLS en background.
"""
import os
import re
import json
import uuid
import asyncio
import logging
import shutil
import subprocess
from pathlib import Path

import aiohttp
import aiofiles
import aioboto3
import asyncpg
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes,
)


# ============================================================
# CONFIG desde variables de entorno (GitHub Secrets)
# ============================================================
BOT_TOKEN    = os.environ["BOT_TOKEN"]
GH_TOKEN     = os.environ.get("GH_TOKEN", "")
GH_REPO      = os.environ.get("GH_REPO", "")
API_BASE     = os.environ["API_BASE"]
API_SECRET   = os.environ["API_SECRET"]
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_IDS    = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}

S3_ENDPOINT    = "https://s3.todus.cu"
S3_BUCKET      = "stream"
S3_REGION      = "us-east-1"
S3_PUBLIC_BASE = "https://s3.todus.cu/stream"

SEG_DURATION = 4
MAX_INPUT_SIZE = 2 * 1024 * 1024 * 1024
DOWNLOAD_CHUNK = 1024 * 1024
UPLOAD_CONCURRENCY = 2

QUALITIES = [
    ("240p", 426, 240, "300k", "64k"),
    ("360p", 640, 360, "700k", "96k"),
    ("480p", 854, 480, "1200k", "128k"),
    ("720p", 1280, 720, "2500k", "128k"),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("xneo-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

pool: asyncpg.Pool = None
_s3_session = aioboto3.Session()
_s3_cfg = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 5, "mode": "adaptive"},
    max_pool_connections=10,
    connect_timeout=90,
    read_timeout=600,
)


def _content_type(name: str) -> str:
    if name.endswith(".m3u8"):
        return "application/vnd.apple.mpegurl"
    if name.endswith(".m4s"):
        return "video/iso.segment"
    if name.endswith(".mp4"):
        return "video/mp4"
    if name.endswith(".jpg") or name.endswith(".jpeg"):
        return "image/jpeg"
    return "application/octet-stream"


def _cache_control(name: str) -> str:
    if name.endswith(".m3u8"):
        return "public, max-age=300"
    return "public, max-age=31536000, immutable"


async def _s3_client():
    return _s3_session.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public", aws_secret_access_key="public",
        region_name=S3_REGION, config=_s3_cfg,
    )


async def upload_file(local: Path, key: str, ctype: str, cache: str = "public, max-age=31536000, immutable"):
    async with await _s3_client() as s3:
        for attempt in range(3):
            try:
                with open(local, "rb") as f:
                    await s3.put_object(
                        Bucket=S3_BUCKET, Key=key, Body=f,
                        ContentType=ctype, CacheControl=cache,
                        ACL="public-read",
                    )
                return f"{S3_PUBLIC_BASE}/{key}"
            except Exception as e:
                if attempt == 2:
                    raise
                log.warning(f"Retry {attempt+1}/3 para {key}: {e}")
                await asyncio.sleep(2 ** attempt)


async def upload_dir(out_dir: Path, job_id: str):
    """Sube todos los archivos de out_dir a hls/<job_id>/ con concurrencia controlada."""
    sem = asyncio.Semaphore(UPLOAD_CONCURRENCY)
    files = sorted(f for f in out_dir.rglob("*") if f.is_file())

    async with await _s3_client() as s3:
        async def up(f: Path):
            rel = f.relative_to(out_dir).as_posix()
            key = f"hls/{job_id}/{rel}"
            async with sem:
                for attempt in range(3):
                    try:
                        with open(f, "rb") as body:
                            await s3.put_object(
                                Bucket=S3_BUCKET, Key=key, Body=body,
                                ContentType=_content_type(str(f)),
                                CacheControl=_cache_control(str(f)),
                                ACL="public-read",
                            )
                        return
                    except Exception as e:
                        if attempt == 2:
                            raise
                        log.warning(f"Retry {attempt+1}/3 subiendo {rel}: {e}")
                        await asyncio.sleep(2 ** attempt)
        await asyncio.gather(*(up(f) for f in files))
    return files


async def download(url: str, dest: Path):
    timeout = aiohttp.ClientTimeout(total=3600, connect=30, sock_read=300)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        async with s.get(url, headers={"User-Agent": "Mozilla/5.0"}) as r:
            if r.status >= 400:
                raise RuntimeError(f"HTTP {r.status}")
            total = int(r.headers.get("Content-Length", 0))
            if total and total > MAX_INPUT_SIZE:
                raise RuntimeError("Video demasiado grande")
            async with aiofiles.open(dest, "wb") as f:
                async for chunk in r.content.iter_chunked(DOWNLOAD_CHUNK):
                    await f.write(chunk)


async def download_tg_file(bot, file_id: str, dest: Path, max_size: int):
    tg_file = await bot.get_file(file_id)
    if tg_file.file_size and tg_file.file_size > max_size:
        raise RuntimeError(f"Archivo muy grande ({tg_file.file_size/1e6:.0f} MB)")
    async with aiohttp.ClientSession() as s:
        async with s.get(tg_file.file_path) as r:
            async with aiofiles.open(dest, "wb") as f:
                async for chunk in r.content.iter_chunked(1024 * 1024):
                    await f.write(chunk)


async def extract_metadata(path: Path) -> dict:
    cmd = ["ffprobe", "-v", "error",
           "-show_entries", "format=duration,size,bit_rate:stream=codec_type,codec_name,width,height",
           "-of", "json", str(path)]
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
        "width": video.get("width"),
        "height": video.get("height"),
    }


async def detect_fps(input_path: Path) -> int:
    def _run():
        return subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=r_frame_rate", "-of", "csv=p=0", str(input_path)],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
    raw = await asyncio.to_thread(_run)
    if "/" not in raw:
        return 30
    n, d = raw.split("/")
    return max(1, round(int(n) / int(d)))


async def make_thumbnail(path: Path, out: Path) -> bool:
    cmd = ["ffmpeg", "-y", "-ss", "3", "-i", str(path),
           "-frames:v", "1", "-vf", "scale=640:-2", "-q:v", "3", str(out)]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()
    return proc.returncode == 0 and out.exists()


async def transcode(input_path: Path, out_dir: Path):
    fps = await detect_fps(input_path)
    gop = fps * SEG_DURATION
    out_dir.mkdir(exist_ok=True)
    for name, *_ in QUALITIES:
        (out_dir / name).mkdir(exist_ok=True)

    n = len(QUALITIES)
    split = "".join(f"[v{i}]" for i in range(n))
    scale = ";".join(
        f"[v{i}]scale=w={w}:h={h}:force_original_aspect_ratio=decrease,"
        f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2[v{i}out]"
        for i, (_, w, h, _, _) in enumerate(QUALITIES)
    )
    fc = f"[0:v]split={n}{split};{scale}"

    cmd = ["ffmpeg", "-y", "-i", str(input_path), "-filter_complex", fc]
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
        "-hls_segment_filename", f"{out_dir}/%v/segment_%05d.m4s",
        "-master_pl_name", "master.m3u8",
        "-var_stream_map", var_map,
        f"{out_dir}/%v/playlist.m3u8",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg: {err[-500:].decode(errors='ignore')}")


async def notify_api(job_id: str, status: str, master_url: str = None, error: str = None, stats: dict = None):
    payload = {"job_id": job_id, "status": status, "master_url": master_url, "error": error, "stats": stats or {}}
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"{API_BASE}/api/admin/videos/{job_id}/finish",
            json=payload,
            headers={"X-Admin-Token": API_SECRET},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            log.info(f"callback {job_id}: {r.status}")


async def convert_job(job_id: str, source: str, title: str, category: str, chat_id: int):
    """
    Descarga, extrae metadata, miniatura, transcodifica a HLS y sube a Todus.
    Se ejecuta como tarea en background mientras el bot sigue escuchando Telegram.
    """
    work = Path("/tmp/xneo_jobs") / job_id
    work.mkdir(parents=True, exist_ok=True)
    input_path = work / "input.mp4"
    out_dir = work / "out"

    try:
        log.info(f"[{job_id}] descargando {source}")
        await download(source, input_path)

        log.info(f"[{job_id}] metadata")
        meta = await extract_metadata(input_path)

        log.info(f"[{job_id}] miniatura")
        thumb_local = work / "thumb.jpg"
        thumb_url = None
        if await make_thumbnail(input_path, thumb_local):
            thumb_url = await upload_file(thumb_local, f"thumbs/{job_id}.jpg", "image/jpeg")

        log.info(f"[{job_id}] transcodificando")
        await transcode(input_path, out_dir)

        log.info(f"[{job_id}] subiendo HLS")
        files = await upload_dir(out_dir, job_id)
        master_url = f"{S3_PUBLIC_BASE}/hls/{job_id}/master.m3u8"

        await notify_api(
            job_id, "ready",
            master_url=master_url,
            stats={
                "files": len(files),
                "bytes": sum(f.stat().st_size for f in files),
                "meta": meta,
                "thumb_url": thumb_url,
            },
        )

        # Borrar el tmp si vino de Todus (URL https://s3.todus.cu/stream/tmp/...)
        if "/stream/tmp/" in source:
            try:
                async with await _s3_client() as s3:
                    await s3.delete_object(Bucket=S3_BUCKET, Key=f"tmp/{job_id}.mp4")
            except Exception as e:
                log.warning(f"no pude borrar tmp: {e}")

        log.info(f"[{job_id}] OK -> {master_url}")

        # Borrar el mensaje de estado
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=f"✅ *Publicado*\n\n📺 {title}\n🏷️ {category}\n🆔 `{job_id}`\n\n[Ver en la web]({API_BASE}/play.html?id={job_id})",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        except Exception as e:
            log.warning(f"no pude avisar: {e}")

    except Exception as e:
        log.exception(f"[{job_id}] fallo")
        await notify_api(job_id, "error", error=str(e)[:500])
        try:
            await bot_app.bot.send_message(
                chat_id=chat_id,
                text=f"❌ Error en `{job_id}`:\n`{str(e)[:300]}`",
                parse_mode="Markdown",
            )
        except Exception:
            pass
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ============================================================
# HANDLERS DEL BOT
# ============================================================
def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in ADMIN_IDS:
            if update.message:
                await update.message.reply_text("No autorizado.")
            elif update.callback_query:
                await update.callback_query.answer("No autorizado.", show_alert=True)
            return
        return await func(update, ctx)
    return wrapper


@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text(
        "🎬 *XNEO Admin Bot*\n\nEnviame un *video* o una *URL directa*.\n"
        "El título se toma automáticamente del nombre del archivo (o de la URL).\n\n"
        "/cancel — cancelar\n/status `<job_id>` — estado\n/list — últimos videos",
        parse_mode="Markdown",
    )


@admin_only
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("Cancelado.")


async def on_video_or_url(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or update.effective_user.id not in ADMIN_IDS:
        return
    ctx.user_data.clear()

    if msg.video or msg.document:
        file_obj = msg.video or msg.document
        mime = getattr(file_obj, "mime_type", "") or ""
        if msg.document and not mime.startswith("video/"):
            await msg.reply_text("Eso no es un video.")
            return
        file_name = getattr(file_obj, "file_name", "") or "video.mp4"
        title = Path(file_name).stem
        ctx.user_data["source_type"] = "telegram"
        ctx.user_data["file_id"] = file_obj.file_id
        ctx.user_data["file_name"] = file_name
        ctx.user_data["title"] = title[:120]
    elif msg.text and re.match(r"https?://\S+", msg.text.strip()):
        url = msg.text.strip()
        # Título = último segmento de la URL sin query
        path = url.split("?")[0].rstrip("/")
        title = Path(path).stem or "video"
        ctx.user_data["source_type"] = "url"
        ctx.user_data["url"] = url
        ctx.user_data["title"] = title[:120]
    else:
        await msg.reply_text("Mándame un video o una URL. /start para ayuda.")
        return

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Hetero", callback_data="cat:hetero"),
        InlineKeyboardButton("Bi", callback_data="cat:bi"),
        InlineKeyboardButton("Gay", callback_data="cat:gay"),
    ]])
    await msg.reply_text(
        f"📂 ¿Categoría?\n\n_Título:_ `{ctx.user_data['title']}`",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def on_category(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ADMIN_IDS:
        return

    if not ctx.user_data.get("source_type"):
        await q.edit_message_text("⚠️ La conversación se perdió. Manda el video de nuevo.")
        return

    cat = q.data.split(":", 1)[1]
    title = ctx.user_data.get("title", "video")
    source_type = ctx.user_data["source_type"]

    await q.edit_message_text(
        f"✅ Categoría: *{cat}*\n📺 Título: `{title}`\n\n⚙️ Procesando…",
        parse_mode="Markdown",
    )

    job_id = uuid.uuid4().hex[:12]

    try:
        if source_type == "telegram":
            await q.edit_message_text(
                f"✅ Categoría: *{cat}*\n📺 Título: `{title}`\n\n"
                f"📥 Descargando de Telegram…",
                parse_mode="Markdown",
            )
            work = Path("/tmp/xneo_jobs") / job_id
            work.mkdir(parents=True, exist_ok=True)
            local = work / ctx.user_data["file_name"]
            await download_tg_file(ctx.bot, ctx.user_data["file_id"], local, 2 * 1024**3)

            await q.edit_message_text(
                f"✅ Categoría: *{cat}*\n📺 Título: `{title}`\n\n"
                f"☁️ Subiendo a Todus…",
                parse_mode="Markdown",
            )
            # Sube a tmp/ y usa esa URL como source
            async with await _s3_client() as s3:
                key = f"tmp/{job_id}.mp4"
                with open(local, "rb") as f:
                    await s3.put_object(
                        Bucket=S3_BUCKET, Key=key, Body=f,
                        ContentType="video/mp4",
                        CacheControl="public, max-age=3600",
                        ACL="public-read",
                    )
            source = f"{S3_PUBLIC_BASE}/{key}"
            # Borrar el local para liberar disco
            local.unlink(missing_ok=True)
        else:
            source = ctx.user_data["url"]

        # Registrar en la DB como processing
        async with pool.acquire() as con:
            await con.execute(
                """INSERT INTO videos
                   (job_id, title, category, source_url, status, admin_chat_id)
                   VALUES ($1,$2,$3,$4,$5,$6)""",
                job_id, title, cat, source, "processing", update.effective_chat.id,
            )

        await q.edit_message_text(
            f"✅ Categoría: *{cat}*\n📺 Título: `{title}`\n🆔 `{job_id}`\n\n"
            f"🚀 Convirtiendo en background…\nTe aviso cuando esté listo.",
            parse_mode="Markdown",
        )

        # Lanzar la conversión en background
        asyncio.create_task(convert_job(
            job_id=job_id,
            source=source,
            title=title,
            category=cat,
            chat_id=update.effective_chat.id,
        ))

    except Exception as e:
        log.exception("Fallo preparando job")
        await q.edit_message_text(
            f"❌ Error preparando:\n`{str(e)[:300]}`",
            parse_mode="Markdown",
        )


@admin_only
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Uso: /status <job_id>")
        return
    try:
        async with pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM videos WHERE job_id=$1", ctx.args[0])
        if not row:
            await update.message.reply_text("No existe.")
            return
        s = row["status"]
        emoji = {"processing": "⚙️", "ready": "✅", "error": "❌"}.get(s, "❔")
        text = f"{emoji} *{s.upper()}*\n\n📺 {row['title']}\n🆔 `{ctx.args[0]}`"
        if row["hls_url"]:
            text += f"\n\n🔗 [master.m3u8]({row['hls_url']})"
        if row["error"]:
            text += f"\n\n❌ `{row['error'][:200]}`"
        await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


@admin_only
async def cmd_list(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        async with pool.acquire() as con:
            rows = await con.fetch("SELECT * FROM videos ORDER BY created_at DESC LIMIT 10")
        if not rows:
            await update.message.reply_text("Sin videos.")
            return
        lines = []
        for v in rows:
            emoji = {"ready": "✅", "processing": "⚙️", "error": "❌"}.get(v["status"], "❔")
            lines.append(f"{emoji} `{v['job_id'][:8]}…` *{v['title']}* — {v['category']}")
        await update.message.reply_text("📼 *Últimos:*\n\n" + "\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")


bot_app: Application = None


async def post_init(app: Application):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    await app.bot.set_my_commands([
        BotCommand("start", "Iniciar"),
        BotCommand("cancel", "Cancelar"),
        BotCommand("status", "Estado de un job"),
        BotCommand("list", "Últimos videos"),
    ])
    log.info(f"Bot listo. Admins: {ADMIN_IDS}")


def main():
    global bot_app
    bot_app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CommandHandler("cancel", cmd_cancel))
    bot_app.add_handler(CommandHandler("status", cmd_status))
    bot_app.add_handler(CommandHandler("list", cmd_list))
    bot_app.add_handler(CallbackQueryHandler(on_category, pattern=r"^cat:"))
    bot_app.add_handler(MessageHandler((filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND, on_video_or_url))
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_video_or_url))
    log.info("Polling…")
    bot_app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
