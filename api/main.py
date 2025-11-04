# taskflow/api/main.py
from fastapi import FastAPI
from core.task import EchoTask
from core.workflow import Workflow
from .schemas import EchoRequest, TaskPublicResult

app = FastAPI(title="TaskFlow API", version="0.1.0")

@app.get("/")
def root():
    return {"ok": True, "docs": "/docs"}

@app.post("/workflows/demo-run")
async def run_demo_workflow(payload: EchoRequest) -> dict[str, TaskPublicResult]:
    # Build A → (B, C) → D
    wf = Workflow("demo")
    A = EchoTask("A")
    B = EchoTask("B")
    C = EchoTask("C")
    D = EchoTask("D")

    wf.add_task(A)
    wf.add_task(B, depends_on=["A"])
    wf.add_task(C, depends_on=["A"])
    wf.add_task(D, depends_on=["B", "C"])

    results = await wf.run(message=payload.message, delay=payload.delay)

    # Normalize into public schema
    out: dict[str, TaskPublicResult] = {}
    for name, res in results.items():
        out[name] = TaskPublicResult(
            status=str(A.status.__class__(A.status)) if name == "A" else str(B.status.__class__(B.status)),  # will be overridden below
            ok=res.ok,
            duration=res.duration,
            retries=res.retries,
            error=res.error
        )

    # Fix statuses per task instance
    out["A"].status = wf.tasks["A"].status.value
    out["B"].status = wf.tasks["B"].status.value
    out["C"].status = wf.tasks["C"].status.value
    out["D"].status = wf.tasks["D"].status.value

    return out
