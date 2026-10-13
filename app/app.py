#!/usr/bin/env python3
"""
XNEO — Servicio único: FastAPI + Bot de Telegram en el mismo proceso.
Sin ffmpeg: toda la extracción de metadatos y miniaturas la hace GitHub Actions.
"""
import re
import json
import uuid
import shutil
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import aiofiles
import aioboto3
import asyncpg
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

import os
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from jose import jwt, JWTError
from passlib.context import CryptContext

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes,
)


# ============================================================
# CONFIGURACIÓN
# ============================================================
DATABASE_URL = "postgresql://xneo_cyu8_user:ykP9NBKifhFsulos6RtIEFldt2DC2eOx@dpg-db5p862jnfac73b942ag-a.virginia-postgres.render.com/xneo_cyu8"
API_SECRET   = "7f3a9b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"
JWT_SECRET   = "b4e0c3d2f1a9e8d7c6b5a4938271605f4e3d2c1b0a9f8e7d6c5b4a3928170615f"

BOT_TOKEN   = "8821834032:AAEWhoBJt8T-OM_n_3WwpjjmXaVgRqQv5AY"
ADMIN_IDS   = {8282703640}
GH_TOKEN    = os.environ.get("GH_TOKEN", "")
GH_REPO     = "miguelorlandos06/xneo"
GH_WORKFLOW = "convert.yml"
GH_REF      = "main"

S3_ENDPOINT    = "https://s3.todus.cu"
S3_BUCKET      = "stream"
S3_REGION      = "us-east-1"
S3_PUBLIC_BASE = "https://s3.todus.cu/stream"

JWT_ALG = "HS256"
JWT_EXP_HOURS = 24 * 7
# ============================================================


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("xneo")
if not os.environ.get("GH_TOKEN"):
    print("! GH_TOKEN no configurado. El bot no podra disparar workflows.")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer = HTTPBearer(auto_error=False)

pool: asyncpg.Pool = None
_s3_session = aioboto3.Session()
_s3_cfg = BotoConfig(
    signature_version=UNSIGNED,
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=20,
)


async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with pool.acquire() as con:
        await con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id SERIAL PRIMARY KEY,
                job_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL,
                category TEXT NOT NULL,
                thumb_url TEXT,
                hls_url TEXT,
                source_url TEXT,
                duration REAL,
                size BIGINT,
                width INTEGER,
                height INTEGER,
                views BIGINT NOT NULL DEFAULT 0,
                likes BIGINT NOT NULL DEFAULT 0,
                dislikes BIGINT NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                admin_chat_id BIGINT,
                notified_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id SERIAL PRIMARY KEY,
                video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                username TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        await con.execute("""
            CREATE TABLE IF NOT EXISTS reactions (
                id SERIAL PRIMARY KEY,
                video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                kind TEXT NOT NULL CHECK (kind IN ('like','dislike')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(video_id, user_id)
            );
        """)
        await con.execute("CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category);")
        await con.execute("CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);")
        await con.execute("CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id);")


app = FastAPI(title="XNEO", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def make_token(user_id: int, username: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(hours=JWT_EXP_HOURS)
    return jwt.encode({"sub": str(user_id), "u": username, "exp": exp}, JWT_SECRET, algorithm=JWT_ALG)


async def current_user(cred: HTTPAuthorizationCredentials = Depends(bearer)):
    if not cred:
        raise HTTPException(401, "No autenticado")
    try:
        payload = jwt.decode(cred.credentials, JWT_SECRET, algorithms=[JWT_ALG])
        return {"id": int(payload["sub"]), "username": payload["u"]}
    except JWTError:
        raise HTTPException(401, "Token inválido")


def admin_required(x_admin_token: str = Header(None)):
    if x_admin_token != API_SECRET:
        raise HTTPException(401, "No autorizado")


class RegisterBody(BaseModel):
    username: str
    email: EmailStr
    password: str


class LoginBody(BaseModel):
    identifier: str
    password: str


class ReactionBody(BaseModel):
    kind: str


class CommentBody(BaseModel):
    text: str


class AdminCreateVideo(BaseModel):
    job_id: str
    title: str
    category: str
    thumb_url: Optional[str] = None
    duration: Optional[float] = None
    size: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    source_url: Optional[str] = None
    status: str = "processing"
    admin_chat_id: Optional[int] = None


class FinishBody(BaseModel):
    job_id: str
    status: str
    master_url: Optional[str] = None
    error: Optional[str] = None
    stats: Optional[dict] = None


@app.on_event("startup")
async def on_startup():
    await init_db()
    asyncio.create_task(start_bot())
    asyncio.create_task(poll_finished())
    log.info("Servicio listo (API + bot, sin ffmpeg)")


@app.on_event("shutdown")
async def on_shutdown():
    if pool:
        await pool.close()


@app.get("/health")
async def health():
    return {"ok": True, "service": "xneo"}


@app.post("/api/auth/register")
async def register(body: RegisterBody):
    if len(body.username) < 3:
        raise HTTPException(400, "Usuario muy corto")
    if len(body.password) < 6:
        raise HTTPException(400, "Contraseña muy corta")
    async with pool.acquire() as con:
        exists = await con.fetchrow(
            "SELECT id FROM users WHERE username=$1 OR email=$2",
            body.username, body.email,
        )
        if exists:
            raise HTTPException(409, "Usuario o email ya existe")
        row = await con.fetchrow(
            "INSERT INTO users (username, email, password) VALUES ($1,$2,$3) RETURNING id, username",
            body.username, body.email, pwd.hash(body.password),
        )
    return {"token": make_token(row["id"], row["username"]), "user": dict(row)}


@app.post("/api/auth/login")
async def login(body: LoginBody):
    async with pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT id, username, password FROM users WHERE username=$1 OR email=$1",
            body.identifier,
        )
    if not row or not pwd.verify(body.password, row["password"]):
        raise HTTPException(401, "Credenciales inválidas")
    return {"token": make_token(row["id"], row["username"]), "user": {"id": row["id"], "username": row["username"]}}


@app.get("/api/auth/me")
async def me(user=Depends(current_user)):
    return user


@app.get("/api/videos")
async def list_videos(cat: str = Query(None), limit: int = Query(50, le=100), offset: int = Query(0, ge=0)):
    async with pool.acquire() as con:
        if cat:
            rows = await con.fetch(
                """SELECT id, job_id, title, category, thumb_url, duration, views, likes, dislikes, created_at
                   FROM videos WHERE status='ready' AND category=$1
                   ORDER BY created_at DESC LIMIT $2 OFFSET $3""",
                cat, limit, offset,
            )
        else:
            rows = await con.fetch(
                """SELECT id, job_id, title, category, thumb_url, duration, views, likes, dislikes, created_at
                   FROM videos WHERE status='ready'
                   ORDER BY created_at DESC LIMIT $1 OFFSET $2""",
                limit, offset,
            )
    return {"items": [dict(r) for r in rows]}


@app.get("/api/video/{video_id}")
async def get_video(video_id: int):
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """SELECT id, job_id, title, category, thumb_url, hls_url, duration,
                      views, likes, dislikes, created_at
               FROM videos WHERE id=$1 AND status='ready'""",
            video_id,
        )
        if not row:
            raise HTTPException(404, "Video no encontrado")
        await con.execute("UPDATE videos SET views = views + 1 WHERE id=$1", video_id)
    data = dict(row)
    data["views"] += 1
    return data


@app.post("/api/video/{video_id}/react")
async def react(video_id: int, body: ReactionBody, user=Depends(current_user)):
    if body.kind not in ("like", "dislike"):
        raise HTTPException(400, "kind inválido")
    async with pool.acquire() as con:
        async with con.transaction():
            prev = await con.fetchrow(
                "SELECT kind FROM reactions WHERE video_id=$1 AND user_id=$2",
                video_id, user["id"],
            )
            if prev and prev["kind"] == body.kind:
                await con.execute(
                    "DELETE FROM reactions WHERE video_id=$1 AND user_id=$2",
                    video_id, user["id"],
                )
                col = "likes" if body.kind == "like" else "dislikes"
                await con.execute(f"UPDATE videos SET {col} = GREATEST({col} - 1, 0) WHERE id=$1", video_id)
                action = "removed"
            else:
                if prev:
                    old_col = "likes" if prev["kind"] == "like" else "dislikes"
                    await con.execute(f"UPDATE videos SET {old_col} = GREATEST({old_col} - 1, 0) WHERE id=$1", video_id)
                await con.execute(
                    """INSERT INTO reactions (video_id, user_id, kind) VALUES ($1,$2,$3)
                       ON CONFLICT (video_id, user_id) DO UPDATE SET kind=EXCLUDED.kind""",
                    video_id, user["id"], body.kind,
                )
                col = "likes" if body.kind == "like" else "dislikes"
                await con.execute(f"UPDATE videos SET {col} = {col} + 1 WHERE id=$1", video_id)
                action = "set"
            row = await con.fetchrow("SELECT likes, dislikes FROM videos WHERE id=$1", video_id)
    return {"action": action, "likes": row["likes"], "dislikes": row["dislikes"]}


@app.get("/api/video/{video_id}/comments")
async def list_comments(video_id: int, limit: int = Query(100, le=200)):
    async with pool.acquire() as con:
        rows = await con.fetch(
            """SELECT id, user_id, username, text, created_at FROM comments
               WHERE video_id=$1 ORDER BY created_at DESC LIMIT $2""",
            video_id, limit,
        )
    return {"items": [dict(r) for r in rows]}


@app.post("/api/video/{video_id}/comments")
async def add_comment(video_id: int, body: CommentBody, user=Depends(current_user)):
    text = body.text.strip()
    if not text or len(text) > 500:
        raise HTTPException(400, "Comentario inválido")
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """INSERT INTO comments (video_id, user_id, username, text)
               VALUES ($1,$2,$3,$4) RETURNING id, user_id, username, text, created_at""",
            video_id, user["id"], user["username"], text,
        )
    return dict(row)


@app.delete("/api/video/{video_id}/comments/{comment_id}")
async def delete_comment(video_id: int, comment_id: int, user=Depends(current_user)):
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT user_id FROM comments WHERE id=$1 AND video_id=$2", comment_id, video_id)
        if not row:
            raise HTTPException(404, "No existe")
        if row["user_id"] != user["id"]:
            raise HTTPException(403, "No es tuyo")
        await con.execute("DELETE FROM comments WHERE id=$1", comment_id)
    return {"ok": True}


@app.post("/api/admin/videos", dependencies=[Depends(admin_required)])
async def admin_create(body: AdminCreateVideo):
    async with pool.acquire() as con:
        row = await con.fetchrow(
            """INSERT INTO videos
               (job_id, title, category, thumb_url, duration, size, width, height,
                source_url, status, admin_chat_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               RETURNING id, job_id""",
            body.job_id, body.title, body.category, body.thumb_url,
            body.duration, body.size, body.width, body.height,
            body.source_url, body.status, body.admin_chat_id,
        )
    return dict(row)


@app.get("/api/admin/videos", dependencies=[Depends(admin_required)])
async def admin_list(status: Optional[str] = None, limit: int = Query(20, le=100), unnotified: bool = Query(False)):
    async with pool.acquire() as con:
        q = "SELECT * FROM videos WHERE 1=1"
        args = []
        if status:
            args.append(status)
            q += f" AND status=${len(args)}"
        if unnotified:
            q += " AND notified_at IS NULL AND status='ready'"
        args.append(limit)
        q += f" ORDER BY created_at DESC LIMIT ${len(args)}"
        rows = await con.fetch(q, *args)
    return {"items": [dict(r) for r in rows]}


@app.get("/api/admin/videos/{job_id}", dependencies=[Depends(admin_required)])
async def admin_get(job_id: str):
    async with pool.acquire() as con:
        row = await con.fetchrow("SELECT * FROM videos WHERE job_id=$1", job_id)
    if not row:
        raise HTTPException(404, "No existe")
    return dict(row)


@app.post("/api/admin/videos/{job_id}/finish", dependencies=[Depends(admin_required)])
async def admin_finish(job_id: str, body: FinishBody):
    async with pool.acquire() as con:
        if body.status == "ready":
            meta = (body.stats or {}).get("meta") or {}
            thumb = (body.stats or {}).get("thumb_url")
            await con.execute(
                """UPDATE videos
                   SET status='ready',
                       hls_url=$1,
                       thumb_url=COALESCE($2, thumb_url),
                       duration=COALESCE($3, duration),
                       size=COALESCE($4, size),
                       width=COALESCE($5, width),
                       height=COALESCE($6, height)
                   WHERE job_id=$7""",
                body.master_url,
                thumb,
                meta.get("duration"),
                meta.get("size"),
                meta.get("width"),
                meta.get("height"),
                job_id,
            )
        else:
            await con.execute(
                "UPDATE videos SET status='error', error=$1 WHERE job_id=$2",
                body.error, job_id,
            )
    return {"ok": True}


@app.post("/api/admin/videos/{job_id}/notified", dependencies=[Depends(admin_required)])
async def admin_notified(job_id: str):
    async with pool.acquire() as con:
        await con.execute("UPDATE videos SET notified_at=NOW() WHERE job_id=$1", job_id)
    return {"ok": True}


# ============================================================
# FRONTEND ESTÁTICO (web/)
# ============================================================
WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "web"))
if os.path.isdir(WEB_DIR):
    _assets = os.path.join(WEB_DIR, "assets")
    if os.path.isdir(_assets):
        app.mount("/assets", StaticFiles(directory=_assets), name="assets")

    @app.get("/auth.html", include_in_schema=False)
    async def _auth():
        return FileResponse(os.path.join(WEB_DIR, "auth.html"))

    @app.get("/web.html", include_in_schema=False)
    async def _web():
        return FileResponse(os.path.join(WEB_DIR, "web.html"))

    @app.get("/play.html", include_in_schema=False)
    async def _play():
        return FileResponse(os.path.join(WEB_DIR, "play.html"))

    @app.get("/favicon.svg", include_in_schema=False)
    async def _fav():
        return FileResponse(os.path.join(WEB_DIR, "favicon.svg"))

    @app.get("/", include_in_schema=False)
    async def _index():
        return FileResponse(os.path.join(WEB_DIR, "auth.html"))


WAIT_CATEGORY, WAIT_TITLE = range(2)


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
            return ConversationHandler.END
        return await func(update, ctx)
    return wrapper


@admin_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🎬 *XNEO Admin Bot*\n\nEnviame un *video* (máx 20 MB) o una *URL directa*.\n\n"
        "/cancel — cancelar\n/status `<job_id>` — estado\n/list — últimos videos",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


@admin_only
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("Cancelado.")
    return ConversationHandler.END


@admin_only
async def start_source(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg.video or msg.document:
        file_obj = msg.video or msg.document
        mime = getattr(file_obj, "mime_type", "") or ""
        if msg.document and not mime.startswith("video/"):
            await msg.reply_text("Eso no es un video.")
            return ConversationHandler.END
        ctx.user_data["source_type"] = "telegram"
        ctx.user_data["file_id"] = file_obj.file_id
        ctx.user_data["file_name"] = getattr(file_obj, "file_name", "video.mp4")
    elif msg.text and re.match(r"https?://\S+", msg.text.strip()):
        ctx.user_data["source_type"] = "url"
        ctx.user_data["url"] = msg.text.strip()
    else:
        await msg.reply_text("Mándame un video o una URL.")
        return ConversationHandler.END

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Hetero", callback_data="cat:hetero"),
        InlineKeyboardButton("Bi", callback_data="cat:bi"),
        InlineKeyboardButton("Gay", callback_data="cat:gay"),
    ]])
    await msg.reply_text("📂 ¿Categoría?", reply_markup=kb)
    return WAIT_CATEGORY


@admin_only
async def choose_category(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    cat = q.data.split(":", 1)[1]
    ctx.user_data["category"] = cat
    default_title = Path(ctx.user_data["file_name"]).stem if ctx.user_data["source_type"] == "telegram" else ""
    hint = f"\n\n_Actual: `{default_title}`_" if default_title else ""
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Usar nombre del archivo", callback_data="title:auto")]]) if default_title else None
    await q.edit_message_text(f"✅ Categoría: *{cat}*\n\n✏️ ¿Título?{hint}", parse_mode="Markdown", reply_markup=kb)
    return WAIT_TITLE


@admin_only
async def choose_title(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        if q.data == "title:auto":
            title = Path(ctx.user_data["file_name"]).stem
            await q.edit_message_text(f"✅ Título: *{title}*", parse_mode="Markdown")
        else:
            return WAIT_TITLE
    else:
        title = update.message.text.strip()
        if not title:
            await update.message.reply_text("Título vacío.")
            return WAIT_TITLE
        await update.message.reply_text(f"✅ Título: *{title}*", parse_mode="Markdown")
    ctx.user_data["title"] = title[:120]
    await process_video(update, ctx)
    return ConversationHandler.END


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
            f"✅ *En cola*\n\n"
            f"🆔 `{job_id}`\n"
            f"📺 {meta['title']}\n"
            f"🏷️ {meta['category']}\n\n"
            f"Te aviso cuando esté listo."
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


bot_app: Application = None


# ============================================================
# HANDLERS SIN ConversationHandler (más robustos)
# ============================================================
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
        await q.edit_message_text(
            "⚠️ La conversación se perdió. Manda el video de nuevo."
        )
        return

    cat = q.data.split(":", 1)[1]
    ctx.user_data["category"] = cat

    default_title = ""
    if ctx.user_data.get("source_type") == "telegram":
        default_title = Path(ctx.user_data["file_name"]).stem

    hint = f"\n\n_Actual: `{default_title}`_" if default_title else ""
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Usar nombre del archivo", callback_data="title:auto")
    ]]) if default_title else None

    await q.edit_message_text(
        f"✅ Categoría: *{cat}*\n\n✏️ ¿Título?{hint}",
        parse_mode="Markdown",
        reply_markup=kb,
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


async def on_title_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or update.effective_user.id not in ADMIN_IDS:
        return

    if not ctx.user_data.get("category"):
        return  # no está en flujo de título, ignorar

    title = msg.text.strip()
    if not title:
        await msg.reply_text("Título vacío.")
        return

    ctx.user_data["title"] = title[:120]
    await msg.reply_text(f"✅ Título: *{title}*", parse_mode="Markdown")
    await process_video(update, ctx)


async def on_cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    ctx.user_data.clear()
    await update.message.reply_text(
        "🎬 *XNEO Admin Bot*\n\nEnviame un *video* (máx 20 MB) o una *URL directa*.\n\n"
        "/cancel — cancelar\n/status `<job_id>` — estado\n/list — últimos videos",
        parse_mode="Markdown",
    )


async def start_bot():
    global bot_app
    bot_app = Application.builder().token(BOT_TOKEN).build()

    # Comandos
    bot_app.add_handler(CommandHandler("start", on_cmd_start))
    bot_app.add_handler(CommandHandler("cancel", cmd_cancel))
    bot_app.add_handler(CommandHandler("status", cmd_status))
    bot_app.add_handler(CommandHandler("list", cmd_list))

    # Callbacks (botones) — todos juntos y sin ConversationHandler
    bot_app.add_handler(CallbackQueryHandler(on_category, pattern=r"^cat:"))
    bot_app.add_handler(CallbackQueryHandler(on_title_auto, pattern=r"^title:"))

    # Mensajes de video / URL
    bot_app.add_handler(MessageHandler(
        (filters.VIDEO | filters.Document.VIDEO) & ~filters.COMMAND,
        on_video_or_url,
    ))

    # Texto: si viene URL lo procesa como fuente, si no y hay categoría pendiente lo trata como título
    bot_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))

    await bot_app.initialize()
    await bot_app.bot.set_my_commands([
        BotCommand("start", "Iniciar"),
        BotCommand("cancel", "Cancelar"),
        BotCommand("status", "Estado de un job"),
        BotCommand("list", "Últimos videos"),
    ])
    await bot_app.start()
    await bot_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    log.info("Bot de Telegram arrancado. Admins: %s", ADMIN_IDS)


async def on_text_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Decide si el texto es una URL (nueva fuente) o un título (flujo pendiente)."""
    msg = update.message
    if not msg or update.effective_user.id not in ADMIN_IDS:
        return

    # Si hay categoría pendiente, es un título
    if ctx.user_data.get("category"):
        await on_title_text(update, ctx)
        return

    # Si el texto parece URL, es una fuente nueva
    if re.match(r"https?://\S+", msg.text.strip()):
        await on_video_or_url(update, ctx)
        return

    await msg.reply_text("Mándame un video o una URL. /start para ayuda.")
