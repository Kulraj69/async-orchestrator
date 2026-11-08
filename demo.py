"""Reproduces the demo video's real output: 24 tasks, concurrency 8, one flaky."""
import asyncio, time
from async_orchestrator import Orchestrator

flaky = {"calls": 0}
async def fetch(i):
    await asyncio.sleep(0.02 + (i % 5) * 0.01)
    if i == 7 and flaky["calls"] < 2:
        flaky["calls"] += 1
        raise TimeoutError("api timeout")
    return {"offer": f"IX-{i:03d}", "price": 4200 + i * 37}

async def main():
    o = Orchestrator(concurrency=8, timeout=5.0, retries=2, backoff=0.05)
    t0 = time.perf_counter()
    res = await o.run({f"fetch-offer-{i}": (lambda i=i: fetch(i)) for i in range(24)})
    dt = time.perf_counter() - t0
    print(f"done: {sum(r.ok for r in res)}/{len(res)} ok in {dt:.3f}s")
    for r in res:
        if r.attempts > 1:
            print(f"{r.name}: ok after {r.attempts} attempts")
if __name__ == "__main__":
    asyncio.run(main())
