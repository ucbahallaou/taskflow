# storage/repository.py
from pathlib import Path
import json
import time
from typing import Dict
from core.task import TaskResult

RUNS_DIR = Path("runs")
RUNS_DIR.mkdir(exist_ok=True)

def write_run(run_id: str, results: Dict[str, TaskResult]) -> str:
    payload = {
        "run_id": run_id,
        "created_at": time.time(),
        "tasks": {
            name: {
                "ok": r.ok,
                "error": r.error,
                "retries": r.retries,
                "started_at": r.started_at,
                "finished_at": r.finished_at,
                "duration": r.duration,
            }
            for name, r in results.items()
        },
    }
    path = RUNS_DIR / f"{run_id}.json"
    path.write_text(json.dumps(payload, indent=2))
    return str(path)
