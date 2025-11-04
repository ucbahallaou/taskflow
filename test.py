from core.workflow import Workflow
from core.task import Task

class Dummy(Task):
    async def run(self, **kwargs):
        return "ok"

wf = Workflow("bad")
wf.add_task(Dummy("A"), depends_on=["B"])
wf.add_task(Dummy("B"), depends_on=["A"])
wf.toposort()  # raises ValueError with the stuck nodes
