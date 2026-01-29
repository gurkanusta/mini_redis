from __future__ import annotations
import time
import asyncio
from dataclasses import dataclass
from typing import Optional
from collections import OrderedDict
from config import MAX_KEYS

@dataclass
class Entry:
    value: str
    expire_at: Optional[float] = None

class Store:
    def __init__(self):
        self._data: dict[str, Entry] = {}
        self._lru: OrderedDict[str, None] = OrderedDict()
        self._lock = asyncio.Lock()

    def _is_expired(self, e: Entry) -> bool:
        return e.expire_at is not None and time.time() >= e.expire_at

    def _touch(self, key: str) -> None:
        if key in self._lru:
            self._lru.move_to_end(key, last=True)
        else:
            self._lru[key] = None

    def _remove_key(self, key: str) -> None:
        if key in self._data:
            del self._data[key]
        if key in self._lru:
            del self._lru[key]

    async def _purge_if_expired(self, key: str) -> bool:
        e = self._data.get(key)
        if e is None:
            return False
        if self._is_expired(e):
            self._remove_key(key)
            return True
        return False

    def _evict_if_needed(self) -> int:
        if MAX_KEYS is None or MAX_KEYS <= 0:
            return 0
        evicted = 0
        while len(self._data) > MAX_KEYS and self._lru:
            old_key, _ = self._lru.popitem(last=False)
            if old_key in self._data:
                del self._data[old_key]
                evicted += 1
        return evicted

    async def set(self, key: str, value: str) -> list[str]:
        async with self._lock:
            self._data[key] = Entry(value=value, expire_at=None)
            self._touch(key)
            evicted = []
            if MAX_KEYS is not None and MAX_KEYS > 0:
                while len(self._data) > MAX_KEYS and self._lru:
                    old_key, _ = self._lru.popitem(last=False)
                    if old_key in self._data:
                        del self._data[old_key]
                        evicted.append(old_key)
            return evicted

    async def get(self, key: str) -> Optional[str]:
        async with self._lock:
            await self._purge_if_expired(key)
            e = self._data.get(key)
            if e is None:
                return None
            self._touch(key)
            return e.value

    async def delete(self, key: str) -> int:
        async with self._lock:
            await self._purge_if_expired(key)
            if key in self._data:
                self._remove_key(key)
                return 1
            return 0

    async def exists(self, key: str) -> int:
        async with self._lock:
            await self._purge_if_expired(key)
            if key in self._data:
                self._touch(key)
                return 1
            return 0

    async def clear(self) -> int:
        async with self._lock:
            n = len(self._data)
            self._data.clear()
            self._lru.clear()
            return n

    async def incr(self, key: str) -> int:
        async with self._lock:
            await self._purge_if_expired(key)
            e = self._data.get(key)
            if e is None:
                self._data[key] = Entry("1")
                self._touch(key)
                self._evict_if_needed()
                return 1
            try:
                n = int(e.value)
            except ValueError:
                raise ValueError("value is not an integer")
            n += 1
            e.value = str(n)
            self._touch(key)
            return n

    async def expire(self, key: str, seconds: int) -> int:
        if seconds < 0:
            seconds = 0
        async with self._lock:
            await self._purge_if_expired(key)
            e = self._data.get(key)
            if e is None:
                return 0
            e.expire_at = time.time() + seconds
            self._touch(key)
            return 1

    async def ttl(self, key: str) -> int:
        async with self._lock:
            await self._purge_if_expired(key)
            e = self._data.get(key)
            if e is None:
                return -2
            if e.expire_at is None:
                self._touch(key)
                return -1
            remaining = int(e.expire_at - time.time())
            if remaining < 0:
                self._remove_key(key)
                return -2
            self._touch(key)
            return remaining

    async def keys(self) -> list[str]:
        async with self._lock:
            expired = [k for k, e in self._data.items() if self._is_expired(e)]
            for k in expired:
                self._remove_key(k)
            return sorted(self._data.keys())

    async def cleanup(self) -> int:
        async with self._lock:
            expired = [k for k, e in self._data.items() if self._is_expired(e)]
            for k in expired:
                self._remove_key(k)
            return len(expired)
