#!/usr/bin/env python3
import asyncio
import aiohttp
import aioboto3
from botocore import UNSIGNED
from botocore.config import Config as BotoConfig

API_BASE   = "https://xneo-3znw.onrender.com"
API_SECRET = "7f3a9b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1"

S3_ENDPOINT = "https://s3.todus.cu"
S3_BUCKET = "stream"
S3_REGION = "us-east-1"

_cfg = BotoConfig(signature_version=UNSIGNED, retries={"max_attempts": 3})


async def get_existing_jobs():
    async with aiohttp.ClientSession() as s:
        async with s.get(
            f"{API_BASE}/api/admin/videos?limit=100",
            headers={"X-Admin-Token": API_SECRET},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            items = (await r.json()).get("items", [])
    return {v["job_id"] for v in items}


async def list_hls_jobs(s3):
    jobs = set()
    paginator = s3.get_paginator("list_objects_v2")
    async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="hls/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            prefix = cp["Prefix"]
            jobs.add(prefix.split("/")[1])
    return jobs


async def delete_prefix(s3, job_id):
    prefix = f"hls/{job_id}/"
    paginator = s3.get_paginator("list_objects_v2")
    to_delete = []
    async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})
    if not to_delete:
        return 0
    for i in range(0, len(to_delete), 1000):
        chunk = to_delete[i:i+1000]
        await s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": chunk})
    return len(to_delete)


async def main():
    existing = await get_existing_jobs()
    print("Jobs vivos:", len(existing))

    session = aioboto3.Session()
    async with session.client(
        "s3", endpoint_url=S3_ENDPOINT,
        aws_access_key_id="public", aws_secret_access_key="public",
        region_name=S3_REGION, config=_cfg,
    ) as s3:
        s3_jobs = await list_hls_jobs(s3)
        orphans = s3_jobs - existing
        print("HLS huérfanos:", len(orphans))
        total = 0
        for jid in orphans:
            n = await delete_prefix(s3, jid)
            total += n
        print("Total borrado:", total)


if __name__ == "__main__":
    asyncio.run(main())
