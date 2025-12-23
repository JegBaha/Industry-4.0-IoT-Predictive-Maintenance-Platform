from data_generator import generate_stream


def test_generate_stream_yields_readings():
    gen = generate_stream([("M1", "L1")], seed=42)
    first = next(gen)
    assert first.machine_code == "M1"
    assert first.line == "L1"
