"""Create a local SparkSession with the Delta extensions registered."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Spark 3.5 / Hadoop 3.3.4 call Subject.getSubject(), which Java 22+ rejects.
_MAX_JAVA = 21
_JDK17_HOMES = (
    Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
)


def _java_major(java_bin: Path) -> int | None:
    proc = subprocess.run(
        [str(java_bin), "-version"],
        capture_output=True,
        text=True,
        check=False,
    )
    match = re.search(r'version "(\d+)', proc.stderr or proc.stdout)
    return int(match.group(1)) if match else None


def _use_java_home(home: Path) -> None:
    os.environ["JAVA_HOME"] = str(home)
    os.environ["PATH"] = f"{home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"


def ensure_spark_java() -> None:
    home = os.environ.get("JAVA_HOME")
    java_bin = Path(home, "bin", "java") if home else Path(shutil.which("java") or "")
    major = _java_major(java_bin) if java_bin.is_file() else None
    if major is not None and major <= _MAX_JAVA:
        return
    for candidate in _JDK17_HOMES:
        if (candidate / "bin" / "java").is_file():
            _use_java_home(candidate)
            return
    raise RuntimeError(
        "Spark 3.5 needs JDK 17 or 21. Java 22+ fails with "
        "`Subject.getSubject is not supported`. Install JDK 17: "
        "brew install openjdk@17"
    )


def spark_session(app_name: str = "delta-internals") -> SparkSession:
    ensure_spark_java()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
