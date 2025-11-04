from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass, field
import asyncio
import time
import traceback
from typing import Any, Optional
from core.utils import log, inc


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class TaskResult:
    ok: bool
    output: Any = None
    error: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    retries: int = 0

    @property
    def duration(self) -> Optional[float]:
        if self.finished_at is None:
            return None
        return self.finished_at - self.started_at


class Task(ABC):
    def __init__(self, name: str, max_retries: int = 3, retry_backoff_seconds: float = 1.0):
        self.name = name
        self.status: TaskStatus = TaskStatus.PENDING
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
    
    @abstractmethod
    async def run(self, **kwargs):
        pass

    async def execute(self, **kwargs):
        attempt = 0
        result = TaskResult(ok=False)
        self.status = TaskStatus.RUNNING
        inc("tasks_started")

        while True:
            try:
                log.info(f"[Task {self.name}] attempt {attempt+1} starting")
                output = await self._ensure_awaitable(self.run)(**kwargs)
                self.status = TaskStatus.SUCCEEDED
                result.ok = True
                result.output = output
                result.finished_at = time.time()
                inc("tasks_succeeded")
                dur = result.duration
                log.info(f"[Task {self.name}] SUCCEEDED in {dur:.3f}s (retries={result.retries})")
                return result

            except Exception:
                attempt += 1
                result.retries = attempt
                tb = traceback.format_exc()

                if attempt >= self.max_retries:
                    self.status = TaskStatus.FAILED
                    result.error = tb
                    result.finished_at = time.time()
                    inc("tasks_failed")
                    # last line of traceback is usually the message
                    last = tb.strip().splitlines()[-1] if tb else "unknown error"
                    dur = result.duration
                    log.error(f"[Task {self.name}] FAILED after {attempt} attempt(s) in {dur:.3f}s: {last}")
                    return result

                # will retry
                backoff = self.retry_backoff_seconds * attempt
                log.warning(f"[Task {self.name}] attempt {attempt} failed, retrying in {backoff:.3f}s")
                await asyncio.sleep(backoff)
    
    @staticmethod
    def _ensure_awaitable(func):
        if asyncio.iscoroutinefunction(func):
            return func
        else:
            async def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

class EchoTask(Task):
    async def run(self, message: str, delay: float = 0.0) -> dict:
        if delay:
            await asyncio.sleep(delay)
        return {"message": message, "at": time.time()}

class SumSquaresTask(Task):
    # Note: this is sync on purpose
    def run(self, n: int) -> int:
        # heavy-ish CPU loop
        return sum(i*i for i in range(n))

class PrintTask(Task):
    def run(self, **kwargs):
        message = kwargs.get("message", "Hello, World!")
        return f"Printed: {message}"

if __name__ == "__main__":
    task = PrintTask(name="PrintHello", max_retries=2)
    result = task.execute(message="Hello, OpenAI!")
    print(result)