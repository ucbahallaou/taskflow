from pydantic import BaseModel

class EchoRequest(BaseModel):
    message: str = "hi"
    delay: float = 0.0

class TaskPublicResult(BaseModel):
    status: str
    ok: bool
    duration: float | None = None
    retries: int = 0
    error: str | None = None
