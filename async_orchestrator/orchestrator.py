"""Async agent-task orchestration with backpressure and graceful failure.

A `while True` loop blocks the main thread; real agents need event-driven
concurrency. The Orchestrator runs many agent tasks concurrently with a
hard limit (backpressure), per-task timeouts, bounded retries with
exponential backoff, and ordered, exception-safe result collection. One
task's failure never kills the run.
"""

import asyncio
from dataclasses import dataclass, field


@dataclass
class TaskResult:
    name: str
    ok: bool
    value: object = None
    error: str = None
    attempts: int = 0


class Orchestrator:
    def __init__(self, concurrency=10, timeout=30.0, retries=0, backoff=0.05):
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.concurrency = concurrency
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff

    async def _run_one(self, sem, name, fn):
        async with sem:
            attempts = 0
            while True:
                attempts += 1
                try:
                    value = await asyncio.wait_for(fn(), timeout=self.timeout)
                    return TaskResult(name, True, value, None, attempts)
                except Exception as exc:  # timeout included
                    if attempts > self.retries:
                        return TaskResult(name, False, None, "%s: %s" % (type(exc).__name__, exc), attempts)
                    await asyncio.sleep(self.backoff * (2 ** (attempts - 1)))

    async def run(self, tasks):
        """Run {name: coroutine_fn} with bounded concurrency.

        Returns results in submission order. Never raises on task failure.
        """
        sem = asyncio.Semaphore(self.concurrency)
        names = list(tasks)
        results = await asyncio.gather(
            *(self._run_one(sem, name, tasks[name]) for name in names))
        return list(results)


async def run_sequential(orchestrator, tasks):
    """Deterministic helper for tests: same API, concurrency of 1."""
    orchestrator.concurrency = 1
    return await orchestrator.run(tasks)
