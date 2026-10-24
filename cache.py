"""Disk-backed cache for Bright Data fetches and System One answers, so repeated demo runs are instant."""
import hashlib
import json
import os
import threading

DIR = os.path.join(os.path.dirname(__file__), ".cache")
os.makedirs(DIR, exist_ok=True)
_lock = threading.Lock()
_mem: dict[str, dict] = {}


def _path(ns: str) -> str:
    return os.path.join(DIR, f"{ns}.jsonl")


def _load(ns: str) -> dict:
    if ns not in _mem:
        _mem[ns] = {}
        if os.path.exists(_path(ns)):
            with open(_path(ns)) as f:
                for line in f:
                    try:
                        k, v = json.loads(line)
                        _mem[ns][k] = v
                    except ValueError:
                        pass
    return _mem[ns]


def key(*parts) -> str:
    return hashlib.sha1(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()


def get(ns: str, k: str):
    return _load(ns).get(k)


def put(ns: str, k: str, v) -> None:
    with _lock:
        _load(ns)[k] = v
        with open(_path(ns), "a") as f:
            f.write(json.dumps([k, v]) + "\n")
