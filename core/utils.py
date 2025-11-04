# core/utils.py
import logging
import sys
from collections import Counter
from contextlib import contextmanager
import time

# ---------- Logging ----------
def get_logger(name: str = "taskflow"):
    """
    Returns a process-wide logger (idempotent: won't re-add handlers).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
            datefmt="%H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger

log = get_logger("taskflow")

# ---------- Metrics (very lightweight) ----------
metrics = Counter()

def inc(metric_name: str, by: int = 1):
    metrics[metric_name] += by

def get_metrics_snapshot() -> dict[str, int]:
    return dict(metrics)

# ---------- Timing helper (optional) ----------
@contextmanager
def stopwatch(metric_prefix: str | None = None):
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        if metric_prefix:
            inc(f"{metric_prefix}_count")
            # store rounded milliseconds bucket if you want
            inc(f"{metric_prefix}_ms_{int(duration*1000)//50*50}")
