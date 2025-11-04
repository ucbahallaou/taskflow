from __future__ import annotations
from collections import defaultdict
from collections import deque
from typing import Dict, Set, List
import asyncio
import time

from core.task import Task, TaskStatus, TaskResult
from uuid import uuid4
from core.utils import log, inc
from storage.repository import write_run



class Workflow:
    def __init__(self, name: str):
        self.name = name
        self.tasks: Dict[str, Task] = {}      # "A" -> Task(...)
        self.deps: Dict[str, Set[str]] = {}
    
    def add_task(self, task: Task, depends_on: List[str] | None = None):
        if task.name in self.deps:
            raise ValueError(f"Task with name {task.name} already exists in the workflow.")
        
        self.tasks[task.name] = task
        self.deps[task.name] = set(depends_on or [])
    
    def _validate_all_deps_exist(self) -> None:
        missing: list[tuple[str, str]] = []
        for t, prereqs in self.deps.items():
            for p in prereqs:
                if p not in self.tasks:
                    missing.append((t, p))
        if missing:
            details = ", ".join(f"{t}→{p}" for t, p in missing)
            raise ValueError(f"Unknown dependency reference(s): {details}")
    
    def toposort(self) -> List[str]:
        """
        Return a valid topological ordering of task names (Kahn's algorithm).
        Raises ValueError if the graph has a cycle.
        """
        # Validate all deps point to real tasks first
        self._validate_all_deps_exist()

        # Compute in-degree (number of unmet prerequisites) for each task
        in_degree: Dict[str, int] = {name: 0 for name in self.tasks}
        # Build adjacency list: parent -> set(children that depend on parent)
        children: Dict[str, Set[str]] = {name: set() for name in self.tasks}

        for task_name, prereqs in self.deps.items():
            in_degree[task_name] = len(prereqs)
            for p in prereqs:
                children[p].add(task_name)

        # Start with all tasks that have no prerequisites
        ready = deque([name for name, deg in in_degree.items() if deg == 0])
        order: List[str] = []

        while ready:
            node = ready.popleft()
            order.append(node)
            # "Remove" this node: decrement in-degree of its dependents
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    ready.append(child)

        if len(order) != len(self.tasks):
            # Some nodes couldn't reach in-degree 0 => cycle exists
            # Optionally compute which are stuck:
            stuck = [n for n, deg in in_degree.items() if deg > 0]
            raise ValueError(f"Cycle detected in workflow '{self.name}'. Nodes involved: {stuck}")

        return order
    
    def validate_acyclic(self) -> bool:
        """
        True if DAG is acyclic. Raises descriptive error if not.
        """
        _ = self.toposort()
        return True

    async def run(self, **inputs) -> Dict[str, TaskResult]:
        """
        Execute tasks respecting dependencies, running all 'ready' tasks concurrently.
        If any prerequisite fails, dependents are SKIPPED.

        Returns: {task_name: TaskResult}
        """
        self._validate_all_deps_exist()
        log.info(f"[Workflow {self.name}] START inputs={list(inputs.keys())}")

        # Build children adjacency for quick fan-out updates.
        children: Dict[str, Set[str]] = {name: set() for name in self.tasks}
        for t, prereqs in self.deps.items():
            for p in prereqs:
                children[p].add(t)

        # Track remaining prereqs for each task.
        remaining_deps: Dict[str, Set[str]] = {t: set(prs) for t, prs in self.deps.items()}

        # Track whether any parent failed along the path.
        has_failed_parent: Dict[str, bool] = {t: False for t in self.tasks}

        # Results sink + in-flight bookkeeping.
        results: Dict[str, TaskResult] = {}
        running: Dict[str, asyncio.Task] = {}

        # Initial ready set: tasks with no prereqs.
        ready: Set[str] = {t for t, deps in remaining_deps.items() if not deps}

        if ready:
            log.info(f"[Workflow {self.name}] initial ready={sorted(ready)}")
        else:
            log.warning(f"[Workflow {self.name}] no initial ready tasks (graph may be invalid)")
        
        # Helper to mark a task (and transitively its descendants) as skipped
        # when it becomes unblocked but has a failed ancestor.
        def mark_skipped(task_name: str, reason: str = "SKIPPED: prerequisite failed"):
            if task_name in results:
                return
            skip_res = TaskResult(ok=False, error=reason, finished_at=time.time())
            self.tasks[task_name].status = TaskStatus.SKIPPED
            results[task_name] = skip_res
            inc("tasks_skipped")
            log.info(f"[Workflow {self.name}] SKIP {task_name} (reason: {reason})")
            # Propagate failure to descendants so they won't run either.
            for ch in children[task_name]:
                has_failed_parent[ch] = True
                # Remove the dependency edge so that descendants can be considered for skip propagation
                if task_name in remaining_deps[ch]:
                    remaining_deps[ch].remove(task_name)
                if not remaining_deps[ch]:
                    # When child becomes unblocked, immediately mark skipped (do not schedule)
                    mark_skipped(ch, reason)

        # Main loop: launch all currently-ready tasks, then react to completions.
        while ready or running:
            # Launch everything currently ready.
            for name in list(ready):
                if has_failed_parent[name]:
                    # Do not execute; mark skipped now.
                    mark_skipped(name)
                elif name not in results and name not in running:
                    log.info(f"[Workflow {self.name}] LAUNCH {name}")
                    running[name] = asyncio.create_task(self.tasks[name].execute(**inputs))
                ready.discard(name)

            if not running:
                # Nothing to run; remaining nodes must ultimately be skipped (e.g., due to failure cascade).
                break

            done, _pending = await asyncio.wait(
                set(running.values()),
                return_when=asyncio.FIRST_COMPLETED
            )

            # Map futures back to task names
            future_to_name = {fut: n for n, fut in running.items()}

            for fut in done:
                name = future_to_name[fut]
                # Remove from running
                del running[name]

                # Get execution result
                res: TaskResult = await fut
                results[name] = res  # Task.sets its own status inside execute()
                log.info(f"[Workflow {self.name}] DONE {name} -> ok={res.ok} retries={res.retries} dur={res.duration:.3f}s")

                # Update dependents
                for ch in children[name]:
                    if name in remaining_deps[ch]:
                        remaining_deps[ch].remove(name)
                    if not res.ok:
                        has_failed_parent[ch] = True

                    # If child is now unblocked:
                    if not remaining_deps[ch]:
                        if has_failed_parent[ch]:
                            mark_skipped(ch)
                        elif ch not in results and ch not in running:
                            # Schedule it in next iteration
                            ready.add(ch)
                            log.info(f"[Workflow {self.name}] UNLOCK {ch}")

        # After the loop, any tasks without results are not runnable (cycle or failure cascade).
        # We already detect cycles earlier; treat leftovers as skipped to be safe.
        for t in self.tasks:
            if t not in results:
                mark_skipped(t, "SKIPPED: unrunnable after failure or unresolved deps")


        run_id = f"{self.name}-{uuid4().hex[:8]}"
        path = write_run(run_id, results)
        log.info(f"[Workflow {self.name}] COMPLETE run_id={run_id} saved={path}")
        return results
