import os

os.environ.setdefault("POSTGRES_USER", "atlas_ci")
os.environ.setdefault("POSTGRES_PASSWORD", "ci-only-password")

from atlas.api import main as api


class FakeResponse:
    status = 200

    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size=-1):
        return self.body


def test_redpanda_health_check_accepts_ready_response(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["timeout"] = timeout
        captured["url"] = request.full_url
        return FakeResponse(b'{"status":"ready"}')

    monkeypatch.setitem(api.check_http_service.__globals__, "urlopen", fake_urlopen)

    result = api.check_http_service(
        "http://redpanda:9644/v1/status/ready",
        expected_status="ready",
    )

    assert result["status"] == "online"
    assert captured["timeout"] == 5
    assert captured["url"].startswith("http://redpanda:")


def test_object_storage_health_check_accepts_empty_http_200(monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["timeout"] = timeout
        return FakeResponse(b"")

    monkeypatch.setitem(api.check_http_service.__globals__, "urlopen", fake_urlopen)

    result = api.check_http_service(
        "http://minio:9000/minio/health/live"
    )

    assert result["status"] == "online"
    assert captured["timeout"] == 5
