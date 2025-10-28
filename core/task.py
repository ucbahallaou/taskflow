from abc import ABC, abstractmethod
import time

class Task(ABC):
    def __init__(self, name: str, max_replies: int = 3):
        self.name = name
        self.status = "PENDING"
        self.max_replies = max_replies
    
    @abstractmethod
    def run(self, **kwargs):
        pass

    def execute(self, **kwargs):
        self.status = "RUNNING"
        attempt = 0
        while attempt <= self.max_replies:
            try:
                attempt += 1
                start = time.time()
                result = self.run(**kwargs)
                end = time.time()
                self.status = "COMPLETED"
                return {
                    "ok" : True,
                    "result": result,
                    "time_taken": end - start,
                    "tries": attempt,
                    "error": None
                }
            except Exception as e:
                print(f"Attempt {attempt} Failed: {e}")
                if attempt > self.max_replies:
                    self.status = "FAILED"
                    raise
                time.sleep(1)


class PrintTask(Task):
    def run(self, **kwargs):
        message = kwargs.get("message", "Hello, World!")
        return f"Printed: {message}"

if __name__ == "__main__":
    task = PrintTask(name="PrintHello", max_replies=2)
    result = task.execute(message="Hello, OpenAI!")
    print(result)