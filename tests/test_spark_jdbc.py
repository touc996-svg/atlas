from datetime import datetime, timezone

import pytest

from atlas.processing.spark_job import (
    DEFAULT_JDBC_PACKAGE,
    SOURCE_QUERY,
    build_jdbc_options,
    evaluate_quality,
)


def test_jdbc_url_and_credentials_are_separate_options():
    options = build_jdbc_options(
        "host=127.0.0.1 port=5432 dbname=atlas user=atlas "
        "password='a password with @ and / characters' sslmode=disable"
    )
    assert options["url"] == "jdbc:postgresql://127.0.0.1:5432/atlas"
    assert options["user"] == "atlas"
    assert options["password"] == "a password with @ and / characters"
    assert options["sslmode"] == "disable"
    assert "password" not in options["url"]


def test_jdbc_url_encodes_database_name_and_brackets_ipv6():
    options = build_jdbc_options(
        "host=2001:db8::1 port=55432 dbname='atlas warehouse' "
        "user=atlas password=secret"
    )
    assert options["url"] == "jdbc:postgresql://[2001:db8::1]:55432/atlas%20warehouse"


def test_jdbc_configuration_requires_credentials():
    with pytest.raises(RuntimeError, match="password"):
        build_jdbc_options("host=localhost dbname=atlas user=atlas")


def test_driver_package_is_pinned():
    assert DEFAULT_JDBC_PACKAGE == "org.postgresql:postgresql:42.7.14"


def test_source_query_is_read_only_and_selects_required_columns():
    normalized = " ".join(SOURCE_QUERY.lower().split())
    assert normalized.startswith("( select ")
    assert "from public.atlas_events" in normalized
    assert "event_timestamp" in normalized
    assert "payload::text as payload_json" in normalized
    assert "insert " not in normalized
    assert "delete " not in normalized


def test_quality_contract_passes_for_valid_input():
    checks = evaluate_quality(
        input_rows=110,
        output_rows=110,
        unique_ids=110,
        null_required_fields=0,
        invalid_payload_rows=0,
    )
    assert len(checks) == 5
    assert all(item["status"] == "PASS" for item in checks)


def test_quality_contract_fails_for_deduplicated_or_invalid_rows():
    checks = evaluate_quality(
        input_rows=110,
        output_rows=109,
        unique_ids=108,
        null_required_fields=1,
        invalid_payload_rows=2,
    )
    failures = {item["check_name"] for item in checks if item["status"] == "FAIL"}
    assert failures == {
        "row_count_preserved",
        "unique_event_ids",
        "required_fields_not_null",
        "payload_json_valid",
    }
