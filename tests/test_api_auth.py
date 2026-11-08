import asyncio
import os

os.environ.setdefault("POSTGRES_USER", "atlas_ci")
os.environ.setdefault("POSTGRES_PASSWORD", "ci-only-password")

from starlette.requests import Request
from starlette.responses import Response
from atlas.api import main as api


def invoke_middleware(path, headers=None):
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [
            (k.lower().encode(), v.encode())
            for k, v in (headers or {}).items()
        ],
        "server": ("testserver", 80),
        "client": ("testclient", 12345),
    }

    async def next_handler(_request):
        return Response(status_code=204)

    return asyncio.run(
        api.api_key_authentication(Request(scope), next_handler)
    )


def test_development_without_key_is_allowed(monkeypatch):
    monkeypatch.setenv("ATLAS_ENV", "development")
    monkeypatch.delenv("ATLAS_API_KEY", raising=False)
    assert invoke_middleware("/api/v1/events").status_code == 204


def test_private_api_requires_configured_key(monkeypatch):
    key = "a" * 64
    monkeypatch.setenv("ATLAS_ENV", "development")
    monkeypatch.setenv("ATLAS_API_KEY", key)

    assert invoke_middleware("/api/v1/events").status_code == 401
    assert invoke_middleware(
        "/api/v1/events", {"X-API-Key": "wrong"}
    ).status_code == 401
    assert invoke_middleware(
        "/api/v1/events", {"X-API-Key": key}
    ).status_code == 204


def test_health_endpoint_remains_public(monkeypatch):
    monkeypatch.setenv("ATLAS_ENV", "production")
    monkeypatch.delenv("ATLAS_API_KEY", raising=False)
    assert invoke_middleware("/health").status_code == 204


def test_production_requires_key(monkeypatch):
    monkeypatch.setenv("ATLAS_ENV", "production")
    monkeypatch.delenv("ATLAS_API_KEY", raising=False)
    try:
        api.validate_api_security_config()
    except RuntimeError as exc:
        assert "required in production" in str(exc)
    else:
        raise AssertionError("Production accepted a missing key.")


def test_weak_key_is_rejected(monkeypatch):
    monkeypatch.setenv("ATLAS_ENV", "production")
    monkeypatch.setenv("ATLAS_API_KEY", "weak")
    try:
        api.validate_api_security_config()
    except RuntimeError as exc:
        assert "at least 32 characters" in str(exc)
    else:
        raise AssertionError("Production accepted a weak key.")


def test_strong_production_key_is_accepted(monkeypatch):
    monkeypatch.setenv("ATLAS_ENV", "production")
    monkeypatch.setenv("ATLAS_API_KEY", "b" * 64)
    api.validate_api_security_config()
