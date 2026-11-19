"""Write live structured events and keep a private copy for each cloud run."""

import json
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

_log_path: ContextVar[Path | None] = ContextVar("firms_log_path", default=None)


@contextmanager
def run_logging(path: Path):
    token = _log_path.set(path)
    try:
        yield
    finally:
        _log_path.reset(token)


def event(name: str, *, level: str = "INFO", **fields: object) -> None:
    path = _log_path.get()
    if path is None:
        return
    line = json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level, "event": name, **fields,
    })
    print(line, file=sys.stderr, flush=True)
    with path.open("a", encoding="utf-8") as stream:
        print(line, file=stream)
