from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass, field
import asyncio
import time
import traceback
from typing import Any, Optional

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


class Task(ABC):
    def __init__(self, name: str, max_replies: int = 3, retry_backoff_seconds: float = 1.0):
        self.name = name
        self.status: TaskStatus = TaskStatus.PENDING
        self.max_replies = max_replies
        self.retry_backoff_seconds = retry_backoff_seconds
    
    @abstractmethod
    async def run(self, **kwargs):
        pass

    async def execute(self, **kwargs):
        attempt = 0
        result = TaskResult(ok=False)
        self.status = TaskStatus.RUNNING

        while True:
            try:
                output = await self._ensure_awaitable(self.run)(**kwargs)
                self.status = TaskStatus.SUCCEEDED
                result.ok = True
                result.output = output
                result.finished_at = time.time()
                return result
            except Exception:
                attempt += 1
                result.retries = attempt
                tb = traceback.format_exc()

                if attempt >= self.max_replies:
                    self.status = TaskStatus.FAILED
                    result.error = tb
                    result.finished_at = time.time()
                    return result
                
                await asyncio.sleep(self.retry_backoff_seconds * attempt)
    
    @staticmethod
    def _ensure_awaitable(func):
        if asyncio.iscoroutinefunction(func):
            return func
        else:
            async def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper


class PrintTask(Task):
    def run(self, **kwargs):
        message = kwargs.get("message", "Hello, World!")
        return f"Printed: {message}"

if __name__ == "__main__":
    task = PrintTask(name="PrintHello", max_replies=2)
    result = task.execute(message="Hello, OpenAI!")
    print(result)