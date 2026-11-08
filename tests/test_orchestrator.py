import asyncio
import time
import unittest

from async_orchestrator import Orchestrator


class TestOrchestrator(unittest.IsolatedAsyncioTestCase):
    async def test_results_in_submission_order(self):
        orch = Orchestrator(concurrency=4)
        async def job(v):
            await asyncio.sleep(0.001 * (5 - v))
            return v
        tasks = {"t%d" % i: (lambda v=i: job(v)) for i in range(5)}
        results = await orch.run(tasks)
        self.assertEqual([r.name for r in results], list(tasks))
        self.assertTrue(all(r.ok for r in results))

    async def test_concurrency_limit_respected(self):
        running = {"cur": 0, "peak": 0}
        async def job():
            running["cur"] += 1
            running["peak"] = max(running["peak"], running["cur"])
            await asyncio.sleep(0.01)
            running["cur"] -= 1
        orch = Orchestrator(concurrency=2)
        await orch.run({"t%d" % i: job for i in range(6)})
        self.assertLessEqual(running["peak"], 2)

    async def test_timeout_marks_failure(self):
        async def slow():
            await asyncio.sleep(5)
        orch = Orchestrator(concurrency=1, timeout=0.01)
        results = await orch.run({"slow": slow})
        self.assertFalse(results[0].ok)
        self.assertIn("TimeoutError", results[0].error)

    async def test_retry_then_success(self):
        calls = {"n": 0}
        async def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise ValueError("boom")
            return "ok"
        orch = Orchestrator(concurrency=1, retries=3, backoff=0.001)
        results = await orch.run({"flaky": flaky})
        self.assertTrue(results[0].ok)
        self.assertEqual(results[0].attempts, 3)

    async def test_retry_exhausted(self):
        async def bad():
            raise ValueError("always")
        orch = Orchestrator(concurrency=1, retries=2, backoff=0.001)
        results = await orch.run({"bad": bad})
        self.assertFalse(results[0].ok)
        self.assertEqual(results[0].attempts, 3)  # 1 initial + 2 retries

    async def test_one_failure_does_not_kill_others(self):
        async def bad():
            raise RuntimeError("x")
        async def good():
            return 1
        orch = Orchestrator(concurrency=2, retries=0)
        results = await orch.run({"a": bad, "b": good})
        self.assertFalse(results[0].ok)
        self.assertTrue(results[1].ok)

    async def test_invalid_concurrency(self):
        with self.assertRaises(ValueError):
            Orchestrator(concurrency=0)

    async def test_empty_task_set(self):
        orch = Orchestrator()
        self.assertEqual(await orch.run({}), [])


if __name__ == "__main__":
    unittest.main()
