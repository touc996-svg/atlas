"""Read PostgreSQL data through Spark's native JDBC reader.

Credentials are read from ATLAS_JDBC_URL, ATLAS_JDBC_USER, and
ATLAS_JDBC_PASSWORD. The PostgreSQL JDBC driver must be available to Spark.
"""
from __future__ import annotations

import os
from typing import Optional


def read_postgres_table(
    spark,
    table: str,
    *,
    url: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    fetchsize: int = 10_000,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: Optional[int] = None,
):
    """Return a Spark DataFrame loaded from a PostgreSQL table via JDBC.

    For parallel reads, supply all four partition arguments. Spark uses the
    bounds to calculate partition strides; they do not filter table rows.
    Only pass trusted table/column identifiers from application configuration.
    """
    jdbc_url = url or os.getenv("ATLAS_JDBC_URL")
    jdbc_user = user or os.getenv("ATLAS_JDBC_USER")
    jdbc_password = password or os.getenv("ATLAS_JDBC_PASSWORD")
    if not all((jdbc_url, jdbc_user, jdbc_password)):
        raise RuntimeError(
            "Set ATLAS_JDBC_URL, ATLAS_JDBC_USER, and ATLAS_JDBC_PASSWORD "
            "before calling read_postgres_table()."
        )
    if fetchsize < 1:
        raise ValueError("fetchsize must be a positive integer")

    partition_values = (partition_column, lower_bound, upper_bound, num_partitions)
    if any(value is not None for value in partition_values) and not all(
        value is not None for value in partition_values
    ):
        raise ValueError(
            "Parallel JDBC reads require partition_column, lower_bound, "
            "upper_bound, and num_partitions together."
        )
    if num_partitions is not None and num_partitions < 1:
        raise ValueError("num_partitions must be a positive integer")

    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", str(fetchsize))
    )
    if partition_column is not None:
        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(num_partitions))
        )
    return reader.load()
