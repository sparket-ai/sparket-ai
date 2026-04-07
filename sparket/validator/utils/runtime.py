

# assorted utils for validator runtime

import asyncio


async def concurrent_forward(self):
        coroutines = [
            self.forward()
            for _ in range(self.config.neuron.num_concurrent_forwards)
        ]
        await asyncio.gather(*coroutines)  # - run all the forward calls in parallel and wait for them to complete here 


def next_backoff_delay(current: float, *, factor: float, max_delay: float) -> float:
        """Compute next backoff delay with a cap."""
        if current <= 0:
            return max_delay
        return min(max_delay, current * factor)


def resolve_loop_timeouts(step_target_seconds: int) -> dict[str, int]:
        """Resolve per-task timeouts from the step target."""
        base = max(5, int(step_target_seconds))
        return {
            "forward": base,
            "scoring": max(300, base * 2),
            "provider": max(120, base * 2),
            "outcome": max(120, base * 2),
            "cleanup": max(60, base * 2),
            "worker_heartbeat": max(10, base),
            "ledger_export": max(60, base),
        }