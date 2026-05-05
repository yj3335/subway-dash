from __future__ import annotations

import os
import re
import subprocess
from typing import Iterable


def get_spark(app_name: str, extra_packages: Iterable[str] | None = None):
    """Create a SparkSession with repo defaults and optional connector packages."""
    require_java_17()
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)

    master = os.environ.get("SPARK_MASTER_URL")
    if master:
        builder = builder.master(master)

    packages = []
    env_packages = os.environ.get("PYSPARK_PACKAGES")
    if env_packages:
        packages.extend(pkg.strip() for pkg in env_packages.split(",") if pkg.strip())
    if extra_packages:
        packages.extend(extra_packages)
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(dict.fromkeys(packages)))

    builder = (
        builder.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    for key, value in os.environ.items():
        if key.startswith("SPARK_CONF_"):
            spark_key = key.removeprefix("SPARK_CONF_").replace("__", ".")
            builder = builder.config(spark_key, value)

    return builder.getOrCreate()


def require_java_17() -> None:
    try:
        completed = subprocess.run(["java", "-version"], check=False, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise RuntimeError(
            "Java is required for PySpark. Install Java 17, then set "
            "JAVA_HOME=$(/usr/libexec/java_home -v 17)."
        ) from exc

    output = f"{completed.stderr}\n{completed.stdout}"
    match = re.search(r'version "(\d+)(?:\.|")', output)
    major = int(match.group(1)) if match else 0
    if major < 17:
        raise RuntimeError(
            "PySpark requires Java 17+ for the installed Spark build, but the active Java runtime is older.\n"
            f"Detected java -version output:\n{output.strip()}\n\n"
            "On macOS with Homebrew:\n"
            "  brew install openjdk@17\n"
            "  export JAVA_HOME=$(/usr/libexec/java_home -v 17)\n"
            "  export PATH=\"$JAVA_HOME/bin:$PATH\"\n"
        )


def require_columns(df, columns: list[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}. Found: {df.columns}")
