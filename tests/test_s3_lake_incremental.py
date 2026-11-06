from pathlib import Path

import pytest

from atlas.storage.s3_lake import list_existing_keys, sync_parquet_dataset


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.pages


class FakeS3:
    def __init__(self, pages=()):
        self.paginator = FakePaginator(pages)
        self.uploads = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator

    def upload_file(self, filename, bucket, key):
        self.uploads.append((Path(filename), bucket, key))


def make_files(root: Path):
    first = root / "event_date=2026-11-06" / "part-existing.parquet"
    second = root / "event_date=2026-11-06" / "part-new-a.parquet"
    third = root / "event_date=2026-11-06" / "part-new-b.parquet"
    for item in (first, second, third):
        item.parent.mkdir(parents=True, exist_ok=True)
        item.write_bytes(b"parquet-test")
    return first, second, third


def test_lists_all_pages_and_scopes_prefix():
    s3 = FakeS3([
        {"Contents": [{"Key": "silver/events/a.parquet"}]},
        {"Contents": [{"Key": "silver/events/b.parquet"}]},
    ])
    keys = list_existing_keys(s3, "atlas-lake", "/silver/events/")
    assert keys == {"silver/events/a.parquet", "silver/events/b.parquet"}
    assert s3.paginator.calls == [
        {"Bucket": "atlas-lake", "Prefix": "silver/events/"}
    ]


def test_uploads_only_new_parquet_keys(tmp_path):
    _, new_a, new_b = make_files(tmp_path)
    s3 = FakeS3([
        {"Contents": [{"Key": "silver/events/event_date=2026-11-06/part-existing.parquet"}]}
    ])

    result = sync_parquet_dataset(
        s3, source=tmp_path, bucket="atlas-lake", prefix="silver/events"
    )

    assert result["files_discovered"] == 3
    assert result["files_uploaded"] == 2
    assert result["files_skipped_existing"] == 1
    assert {upload[0] for upload in s3.uploads} == {new_a, new_b}
    assert all(upload[1] == "atlas-lake" for upload in s3.uploads)


def test_second_sync_is_idempotent(tmp_path):
    make_files(tmp_path)
    s3 = FakeS3([{"Contents": [
        {"Key": "silver/events/event_date=2026-11-06/part-existing.parquet"},
        {"Key": "silver/events/event_date=2026-11-06/part-new-a.parquet"},
        {"Key": "silver/events/event_date=2026-11-06/part-new-b.parquet"},
    ]}])

    result = sync_parquet_dataset(
        s3, source=tmp_path, bucket="atlas-lake", prefix="silver/events"
    )

    assert result["files_uploaded"] == 0
    assert result["files_skipped_existing"] == 3
    assert s3.uploads == []


def test_empty_source_fails_clearly(tmp_path):
    s3 = FakeS3([])
    with pytest.raises(FileNotFoundError, match="No Parquet files"):
        sync_parquet_dataset(s3, source=tmp_path, bucket="atlas-lake")
    assert s3.uploads == []


def test_prefix_is_normalized(tmp_path):
    _, new_a, _ = make_files(tmp_path)
    s3 = FakeS3([{"Contents": [
        {"Key": "silver/events/event_date=2026-11-06/part-existing.parquet"},
        {"Key": "silver/events/event_date=2026-11-06/part-new-b.parquet"},
    ]}])
    result = sync_parquet_dataset(
        s3, source=tmp_path, bucket="atlas-lake", prefix="/silver/events/"
    )
    assert result["prefix"] == "silver/events"
    assert s3.uploads[0][0] == new_a
    assert s3.uploads[0][2].startswith("silver/events/")


def test_destination_listing_failure_prevents_upload(tmp_path):
    make_files(tmp_path)

    class BrokenPaginator:
        def paginate(self, **kwargs):
            raise RuntimeError("list failed")
            yield  # pragma: no cover

    class BrokenS3(FakeS3):
        def get_paginator(self, name):
            return BrokenPaginator()

    s3 = BrokenS3([])
    with pytest.raises(RuntimeError, match="list failed"):
        sync_parquet_dataset(s3, source=tmp_path, bucket="atlas-lake")
    assert s3.uploads == []
