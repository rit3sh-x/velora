"""MongoDB async client for bronze layer (raw tweets).

§V cites: V36 (medallion bronze), V41 (consumer fan-out), V52 (HF cache N/A here).

Usage:
    from consumer.db_mongo import init_mongo, get_tweets_collection, close_mongo
"""
from __future__ import annotations

import logging

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from consumer.config import settings

log = logging.getLogger("velora.db_mongo")

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def init_mongo() -> AsyncIOMotorDatabase:
    """Open Motor client. Idempotent. Pings server before returning."""
    global _client, _db
    if _db is not None:
        return _db

    log.info("connecting mongo: %s db=%s", settings.mongodb_uri, settings.mongodb_db)
    _client = AsyncIOMotorClient(
        settings.mongodb_uri,
        serverSelectionTimeoutMS=10_000,
        uuidRepresentation="standard",
    )
    await _client.admin.command("ping")
    _db = _client[settings.mongodb_db]
    log.info("mongo connected")
    return _db


def get_mongo_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("mongo not initialized; call init_mongo() first")
    return _db


def get_tweets_collection() -> AsyncIOMotorCollection:
    """`raw_tweets` collection (bronze). Indexes provisioned by schema/01_indexes.js."""
    return get_mongo_db()[settings.mongodb_tweets_collection]


async def close_mongo() -> None:
    global _client, _db
    if _client is not None:
        _client.close()
        log.info("mongo client closed")
    _client = None
    _db = None
