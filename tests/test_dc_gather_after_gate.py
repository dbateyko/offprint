from scripts.pipeline.dc_gather_after_gate import evaluate_gate


def test_gate_requires_complete_clean_bounded_run():
    attempts = [
        {"event": "dc_gather_attempt", "http_status": 200, "status": "downloaded"}
        for _ in range(100)
    ]
    assert evaluate_gate(attempts, min_attempts=100, max_failure_rate=0.01) == (
        False,
        "incomplete",
    )

    clean = attempts + [
        {
            "event": "dc_gather_complete",
            "attempted": 100,
            "downloaded": 100,
            "deferred": 0,
            "stop_reason": "",
        }
    ]
    passed, reason = evaluate_gate(clean, min_attempts=100, max_failure_rate=0.01)
    assert passed
    assert reason.startswith("passed")

    pressured = clean[:-1]
    pressured[0] = {
        "event": "dc_gather_attempt",
        "http_status": 429,
        "status": "deferred",
    }
    pressured.append(
        {
            "event": "dc_gather_complete",
            "attempted": 100,
            "downloaded": 99,
            "deferred": 1,
            "stop_reason": "",
        }
    )
    assert evaluate_gate(pressured, min_attempts=100, max_failure_rate=0.01) == (
        False,
        "pressure_responses=1",
    )
