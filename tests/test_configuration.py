import os

import pytest
from psycopg.conninfo import conninfo_to_dict

# Safe defaults let modules load in a clean CI environment.
os.environ.setdefault("POSTGRES_USER", "atlas_ci")
os.environ.setdefault("POSTGRES_PASSWORD", "ci-only-password")

from atlas.api import main as api
from atlas.ingestion import postgres_sink
from atlas.processing import spark_job


DATABASE_MODULES = [api, postgres_sink, spark_job]


@pytest.mark.parametrize("module", DATABASE_MODULES)
def test_explicit_database_url_takes_precedence(monkeypatch, module):
    expected = "postgresql://ci_user@localhost:5432/ci_db"
    monkeypatch.setenv("DATABASE_URL", expected)

    assert module.get_database_url() == expected


@pytest.mark.parametrize("module", DATABASE_MODULES)
def test_database_url_is_built_from_environment(monkeypatch, module):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "atlas_ci")
    monkeypatch.setenv("POSTGRES_PASSWORD", "ci password@with/specials")
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "55432")
    monkeypatch.setenv("POSTGRES_DB", "atlas_ci")

    parsed = conninfo_to_dict(module.get_database_url())

    assert parsed["user"] == "atlas_ci"
    assert parsed["password"] == "ci password@with/specials"
    assert parsed["host"] == "127.0.0.1"
    assert parsed["port"] == "55432"
    assert parsed["dbname"] == "atlas_ci"


@pytest.mark.parametrize("module", DATABASE_MODULES)
def test_missing_database_password_fails_clearly(monkeypatch, module):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.setenv("POSTGRES_USER", "atlas_ci")

    with pytest.raises(RuntimeError):
        module.get_database_url()


def test_api_root_contract():
    response = api.root()

    assert response["platform"] == "ATLAS"
    assert response["service"] == "Analytics API"
    assert response["status"] == "online"


def test_database_errors_are_returned_as_sanitized_503(monkeypatch):
    def fail_connection(*args, **kwargs):
        raise RuntimeError("private database connection detail")

    monkeypatch.setattr(api.psycopg, "connect", fail_connection)

    with pytest.raises(api.HTTPException) as error:
        api.query_db("SELECT 1")

    assert error.value.status_code == 503
    assert error.value.detail == "Analytics database unavailable"
    assert "private database connection detail" not in error.value.detail
