from sparket.validator.utils.runtime import next_backoff_delay, resolve_loop_timeouts


def test_next_backoff_delay_doubles_with_cap():
    assert next_backoff_delay(5.0, factor=2.0, max_delay=30.0) == 10.0
    assert next_backoff_delay(20.0, factor=2.0, max_delay=30.0) == 30.0


def test_next_backoff_delay_handles_non_positive():
    assert next_backoff_delay(0.0, factor=2.0, max_delay=30.0) == 30.0
    assert next_backoff_delay(-1.0, factor=2.0, max_delay=30.0) == 30.0


def test_resolve_loop_timeouts():
    timeouts = resolve_loop_timeouts(12)
    assert timeouts["forward"] == 12
    assert timeouts["scoring"] == 300           # max(300, 24)
    assert timeouts["provider"] == 120          # max(120, 24)
    assert timeouts["cleanup"] == 60            # max(60, 24)
    assert timeouts["outcome"] == 120           # max(120, 24)
    assert timeouts["worker_heartbeat"] == 12   # max(10, 12)
