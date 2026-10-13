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
    log.info("Servicio listo (solo API)")


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


