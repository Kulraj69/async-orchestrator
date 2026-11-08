# async-orchestrator

Async agent-task orchestration with backpressure and graceful failure.
Offline, standard library only (asyncio), no model calls.

Covers the "Async Agent Orchestrator" item: an event-driven runner for many
concurrent agent tasks with non-blocking tool calls and graceful error
recovery. `while True` loops block; this does not.

```python
import asyncio
from async_orchestrator import Orchestrator

orch = Orchestrator(concurrency=10, timeout=30.0, retries=2, backoff=0.5)
results = asyncio.run(orch.run({
    "fetch-docs": lambda: fetch_docs(),
    "call-tool":  lambda: call_tool(),
}))
# TaskResult(name, ok, value, error, attempts) in submission order
```

Guarantees: hard concurrency limit (backpressure), per-task timeouts,
bounded retries with exponential backoff, one task's failure never kills
the run, results always returned in submission order.

Honest scope: this orchestrates coroutines you give it - it is not a
durable workflow engine (no crash-surviving checkpoints; that is
stateful-agent's job) and not a multi-agent supervisor.

## Tests

```
python3 -m unittest discover -s tests
```

## Demo

`demo.py` reproduces the recorded demo run end to end, offline (standard
library only, no model calls):

    python demo.py

It executes the library's real code path and prints the same output shown in
the recorded demo video.
