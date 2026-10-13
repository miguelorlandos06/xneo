#!/usr/bin/env python3
"""
XNEO Bot — aislado, corre en GitHub Actions.
Lee toda la config desde variables de entorno (GitHub Secrets).
"""
import os
import re
import json
import uuid
import asyncio
import logging
import shutil
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
# CONFIG desde variables de entorno
# ============================================================
BOT_TOKEN    = os.environ["BOT_TOKEN"]
GH_TOKEN     = os.environ["GH_TOKEN"]
GH_REPO      = os.environ["GH_REPO"]
GH_WORKFLOW  = os.environ.get("GH_WORKFLOW", "convert.yml")
GH_REF       = os.environ.get("GH_REF", "main")
API_BASE     = os.environ["API_BASE"]
API_SECRET   = os.environ["API_SECRET"]
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_IDS    = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()}

S3_ENDPOINT    = "https://s3.todus.cu"
S3_BUCKET      = "stream"
S3_REGION      = "us-east-1"
S3_PUBLIC_BASE = "https://s3.todus.cu/stream"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("xneo-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

pool: asyncpg.Pool = None
_s3_session = aioboto3.Session()
_s3_cfg = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=20,
)


async def _s3_client():
    return _s3_session.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public", aws_secret_access_key="public",
        region_name=S3_REGION, config=_s3_cfg,
    )


async def upload_to_todus(local: Path, job_id: str) -> str:
    key = f"tmp/{job_id}.mp4"
    async with await _s3_client() as s3:
        with open(local, "rb") as f:
            await s3.put_object(
                Bucket=S3_BUCKET, Key=key, Body=f,
                ContentType="video/mp4", CacheControl="public, max-age=3600",
                ACL="public-read",
            )
    return f"{S3_PUBLIC_BASE}/{key}"


async def download_tg_file(bot, file_id: str, dest: Path, max_size: int):
    tg_file = await bot.get_file(file_id)
    if tg_file.file_size and tg_file.file_size > max_size:
        raise RuntimeError(f"Archivo muy grande ({tg_file.file_size/1e6:.0f} MB)")
    async with aiohttp.ClientSession() as s:
        async with s.get(tg_file.file_path) as r:
            async with aiofiles.open(dest, "wb") as f:
                async for chunk in r.content.iter_chunked(1024 * 1024):
                    await f.write(chunk)


async def dispatch_workflow(job_id: str, source: str, meta: dict):
    url = f"https://api.github.com/repos/{GH_REPO}/actions/workflows/{GH_WORKFLOW}/dispatches"
    payload = {"ref": GH_REF, "inputs": {"job_id": job_id, "source": source, "meta": json.dumps(meta)}}
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json=payload, headers={
            "Authorization": f"Bearer {GH_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status not in (200, 204):
                raise RuntimeError(f"GitHub {r.status}: {await r.text()}")


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
        "🎬 *XNEO Admin Bot*\n\nEnviame un *video* (máx 20 MB) o una *URL directa*.\n\n"
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
        ctx.user_data["source_type"] = "telegram"
        ctx.user_data["file_id"] = file_obj.file_id
        ctx.user_data["file_name"] = getattr(file_obj, "file_name", "video.mp4")
    elif msg.text and re.match(r"https?://\S+", msg.text.strip()):
        ctx.user_data["source_type"] = "url"
        ctx.user_data["url"] = msg.text.strip()
    else:
        await msg.reply_text("Mándame un video o una URL. /start para ayuda.")
        return

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Hetero", callback_data="cat:hetero"),
        InlineKeyboardButton("Bi", callback_data="cat:bi"),
        InlineKeyboardButton("Gay", callback_data="cat:gay"),
    ]])
    await msg.reply_text("📂 ¿Categoría?", reply_markup=kb)


async def on_category(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.user_data.get("source_type"):
        await q.edit_message_text("⚠️ La conversación se perdió. Manda el video de nuevo.")
        return
    cat = q.data.split(":", 1)[1]
    ctx.user_data["category"] = cat
    default_title = Path(ctx.user_data["file_name"]).stem if ctx.user_data.get("source_type") == "telegram" else ""
    hint = f"\n\n_Actual: `{default_title}`_" if default_title else ""
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Usar nombre del archivo", callback_data="title:auto")]]) if default_title else None
    await q.edit_message_text(
        f"✅ Categoría: *{cat}*\n\n✏️ ¿Título?{hint}",
        parse_mode="Markdown", reply_markup=kb,
    )


async def on_title_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not ctx.user_data.get("category"):
        await q.edit_message_text("⚠️ La conversación se perdió. Manda el video de nuevo.")
        return
    title = Path(ctx.user_data.get("file_name", "video")).stem
    ctx.user_data["title"] = title[:120]
    await q.edit_message_text(f"✅ Título: *{title}*", parse_mode="Markdown")
    await process_video(update, ctx)


async def on_text_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or update.effective_user.id not in ADMIN_IDS:
        return
    if ctx.user_data.get("category"):
        title = msg.text.strip()
        if not title:
            await msg.reply_text("Título vacío.")
            return
        ctx.user_data["title"] = title[:120]
        await msg.reply_text(f"✅ Título: *{title}*", parse_mode="Markdown")
        await process_video(update, ctx)
        return
    if re.match(r"https?://\S+", msg.text.strip()):
        await on_video_or_url(update, ctx)
        return
    await msg.reply_text("Mándame un video o una URL. /start para ayuda.")


async def process_video(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    status = await msg.reply_text("⚙️ Preparando…")

    async def edit(t):
        try:
            await status.edit_text(t, parse_mode="Markdown")
        except Exception:
            pass

    job_id = uuid.uuid4().hex[:12]
    work = Path("/tmp/xneo_jobs") / job_id
    work.mkdir(parents=True, exist_ok=True)

    try:
        meta = {"title": ctx.user_data["title"], "category": ctx.user_data["category"]}

        if ctx.user_data["source_type"] == "telegram":
            await edit("📥 Descargando de Telegram…")
            local = work / ctx.user_data["file_name"]
            await download_tg_file(ctx.bot, ctx.user_data["file_id"], local, 2 * 1024**3)
            await edit("☁️ Subiendo video a Todus…")
            source = await upload_to_todus(local, job_id)
        else:
            source = ctx.user_data["url"]

        await edit("📝 Registrando en la web…")
        async with pool.acquire() as con:
            await con.execute(
                """INSERT INTO videos
                   (job_id, title, category, source_url, status, admin_chat_id)
                   VALUES ($1,$2,$3,$4,$5,$6)""",
                job_id, meta["title"], meta["category"],
                source, "processing", update.effective_chat.id,
            )

        await edit("🚀 Enviando a transcodificar…")
        await dispatch_workflow(job_id, source, meta)

        await edit(
            f"✅ *En cola*\n\n🆔 `{job_id}`\n📺 {meta['title']}\n🏷️ {meta['category']}\n\nTe aviso cuando esté listo."
        )
    except Exception as e:
        log.exception("Fallo")
        await edit(f"❌ Error: `{str(e)[:300]}`")
    finally:
        asyncio.create_task(cleanup_later(work))


async def cleanup_later(path: Path, delay: int = 120):
    await asyncio.sleep(delay)
    shutil.rmtree(path, ignore_errors=True)


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


async def poll_finished(app: Application):
    await asyncio.sleep(30)
    while True:
        try:
            async with pool.acquire() as con:
                rows = await con.fetch(
                    """SELECT job_id, title, category, hls_url, admin_chat_id
                       FROM videos
                       WHERE status='ready' AND notified_at IS NULL AND admin_chat_id IS NOT NULL
                       ORDER BY created_at DESC LIMIT 20"""
                )
            for v in rows:
                text = (
                    f"✅ *Publicado*\n\n📺 {v['title']}\n🏷️ {v['category']}\n"
                    f"🆔 `{v['job_id']}`\n\n`{v.get('hls_url') or ''}`"
                )
                try:
                    await app.bot.send_message(
                        chat_id=int(v["admin_chat_id"]), text=text,
                        parse_mode="Markdown", disable_web_page_preview=True,
                    )
                    async with pool.acquire() as con:
                        await con.execute(
                            "UPDATE videos SET notified_at=NOW() WHERE job_id=$1", v["job_id"],
                        )
                except Exception as e:
                    log.warning("No pude notificar: %s", e)
        except Exception as e:
            log.warning("poll: %s", e)
        await asyncio.sleep(60)


async def post_init(app: Application):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    await app.bot.set_my_commands([
        BotCommand("start", "Iniciar"),
        BotCommand("cancel", "Cancelar"),
        BotCommand("status", "Estado de un job"),
        BotCommand("list", "Últimos videos"),
    ])
    asyncio.create_task(poll_finished(app))
    log.info("Bot listo. Admins: %s", ADMIN_IDS)


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CallbackQueryHandler(on_category, pattern=r"^cat:"))
    app.add_handler(CallbackQueryHandler(on_title_auto, pattern=r"^title:"))
    app.add_handler(MessageHandler((filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND, on_video_or_url))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))
    log.info("Polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
