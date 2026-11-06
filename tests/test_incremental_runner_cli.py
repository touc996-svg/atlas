from atlas.processing import pipeline_runner


def test_runner_cli_passes_incremental_mode_and_overlap(monkeypatch):
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return "run-id"

    monkeypatch.setattr(pipeline_runner, "run_pipeline", fake_run_pipeline)
    result = pipeline_runner.main([
        "--mode", "incremental",
        "--lookback-hours", "36",
        "--skip-s3",
        "--skip-dbt",
    ])

    assert result == 0
    assert captured["mode"] == "incremental"
    assert captured["lookback_hours"] == 36.0
    assert captured["skip_s3"] is True
    assert captured["skip_dbt"] is True


def test_runner_rejects_negative_lookback(monkeypatch):
    try:
        pipeline_runner.main(["--lookback-hours", "-1"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("Expected argparse to reject a negative lookback")
