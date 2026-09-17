from app.engine import SimEngine


def test_sim_engine_deterministic() -> None:
    first = list(SimEngine(ttft_ms=0, token_ms=0).complete("hello", 8))
    second = list(SimEngine(ttft_ms=0, token_ms=0).complete("hello", 8))
    assert first == second
    assert len(first) == 8


def test_sim_engine_prompt_sensitive() -> None:
    a = list(SimEngine(ttft_ms=0, token_ms=0).complete("alpha", 8))
    b = list(SimEngine(ttft_ms=0, token_ms=0).complete("beta", 8))
    assert a != b
