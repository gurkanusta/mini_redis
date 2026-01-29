from pathlib import Path

HOST= "127.0.0.1"
PORT= 6380
DATA_DIR= Path("data")
AOF_PATH= DATA_DIR / "appendonly.aof"


CLEANUP_INTERVAL= 1.0
MAX_KEYS = 5