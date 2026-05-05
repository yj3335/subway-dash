from __future__ import annotations

import argparse
from pathlib import Path

from processing.config import STAGING_DIR
from processing.spark_utils import get_spark
from processing.transforms import SERVICE_GRACE_SECS, SERVICE_SATURATION_SECS


TRIP_DELAYS_DIR = STAGING_DIR / "trip_delays"


def summarize_delay_distribution(df):
    from pyspark.sql import functions as F

    row = df.select(F.col("arrival_delay_secs").cast("double").alias("arrival_delay_secs")).agg(
        F.expr("percentile_approx(arrival_delay_secs, 0.50)").alias("p50"),
        F.expr("percentile_approx(arrival_delay_secs, 0.95)").alias("p95"),
        F.count("*").alias("row_count"),
    ).collect()[0]
    p50 = float(row["p50"] or 0.0)
    p95 = float(row["p95"] or 0.0)
    return {
        "row_count": int(row["row_count"]),
        "p50": p50,
        "p95": p95,
        "suggested_grace": max(30.0, p50),
        "suggested_saturation": max(240.0, p95),
    }


def render_markdown(summary: dict[str, float]) -> str:
    grace_delta = abs(summary["suggested_grace"] - SERVICE_GRACE_SECS) / SERVICE_GRACE_SECS
    sat_delta = abs(summary["suggested_saturation"] - SERVICE_SATURATION_SECS) / SERVICE_SATURATION_SECS
    status = "CONFIRMED" if grace_delta < 0.15 and sat_delta < 0.15 else "REVISED_CANDIDATE"
    return f"""# Phase 3 Validation

## Service Deficit Calibration

| Metric | Value |
|---|---:|
| Rows analyzed | {summary['row_count']} |
| Delay p50 seconds | {summary['p50']:.2f} |
| Delay p95 seconds | {summary['p95']:.2f} |
| Current grace seconds | {SERVICE_GRACE_SECS:.2f} |
| Current saturation seconds | {SERVICE_SATURATION_SECS:.2f} |
| Suggested grace seconds | {summary['suggested_grace']:.2f} |
| Suggested saturation seconds | {summary['suggested_saturation']:.2f} |
| Assumption A-07 status | {status} |

If status is `REVISED_CANDIDATE`, update the constants in `processing/transforms.py` after rechecking the two historical events from Phase 2.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calibrate service_deficit grace/saturation endpoints from observed trip delays.")
    parser.add_argument("--input", default=str(TRIP_DELAYS_DIR))
    parser.add_argument("--output", default="docs/phase3_validation.md")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("subway-dash-calibrate-service-deficit")
    df = spark.read.parquet(args.input)
    summary = summarize_delay_distribution(df)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_markdown(summary), encoding="utf-8")
    print(f"wrote {output}")


if __name__ == "__main__":
    main()

