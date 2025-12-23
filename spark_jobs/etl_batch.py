"""
Batch ETL for MetroPulse:
- Reads raw Parquet from MinIO (bucket raw, partition dt=YYYY-MM-DD)
- Loads staging tables in Postgres
- Builds dimensions (with SCD2 for dim_route) and facts in schema dwh

Usage (inside spark container or with spark-submit):
  spark-submit \
    --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    /opt/spark_jobs/etl_batch.py --date 2024-05-01
"""

import os
import argparse
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, Window


def get_spark():
    access = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret = os.getenv("MINIO_SECRET_KEY", "admin123")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    spark = (
        SparkSession.builder.appName("metropulse-etl")
        .config("spark.hadoop.fs.s3a.access.key", access)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    return spark


def read_raw(spark, date_str, name):
    path = f"s3a://raw/dt={date_str}/{name}.parquet"
    return spark.read.parquet(path)


def write_jdbc(df, table, mode="append"):
    url = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/dwh")
    props = {
        "user": os.getenv("PG_USER", "dwh"),
        "password": os.getenv("PG_PASSWORD", "dwhpass"),
        "driver": "org.postgresql.Driver",
    }
    df.write.jdbc(url=url, table=table, mode=mode, properties=props)


def load_staging(spark, date_str):
    staging_tables = ["routes", "users", "vehicles", "trips", "payments", "vehicle_positions"]
    for name in staging_tables:
        df = read_raw(spark, date_str, name)
        df = df.withColumn("load_id", F.lit(f"batch_{date_str}")).withColumn("ingestion_ts", F.current_timestamp())
        write_jdbc(df, f"staging.{name}_raw")
        print(f"Loaded staging.{name}_raw: {df.count()} rows")


def scd2_dim_route(spark, routes_df):
    url = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/dwh")
    props = {
        "user": os.getenv("PG_USER", "dwh"),
        "password": os.getenv("PG_PASSWORD", "dwhpass"),
        "driver": "org.postgresql.Driver",
    }

    try:
        current = spark.read.jdbc(url=url, table="dwh.dim_route", properties=props)
    except Exception:
        current = spark.createDataFrame([], routes_df.schema)

    incoming = (
        routes_df.select(
            "route_id",
            "name",
            "transport_type",
            "base_fare",
            "schema_json",
            F.lit(True).alias("active_flag"),
            F.current_timestamp().alias("valid_from"),
        )
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if current.rdd.isEmpty():
        write_jdbc(incoming, "dwh.dim_route")
        return

    # Latest versions per route
    curr_latest = (
        current.filter(F.col("is_current") == True)
        .select(
            "route_sk",
            "route_id",
            "name",
            "transport_type",
            "base_fare",
            "schema_json",
            "valid_from",
            "valid_to",
            "is_current",
        )
    )

    join_cond = [incoming.route_id == curr_latest.route_id]
    comp_cols = ["name", "transport_type", "base_fare", "schema_json"]

    changed = (
        incoming.alias("inc")
        .join(curr_latest.alias("cur"), join_cond, "left")
        .withColumn(
            "is_changed",
            F.when(
                F.col("cur.route_id").isNull()
                | F.array([F.col(f"inc.{c}") != F.col(f"cur.{c}") for c in comp_cols]).contains(True),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .filter("is_changed")
        .select("inc.*", F.col("cur.route_sk"))
    )

    # Close old versions for changed routes
    changed_ids = [row.route_id for row in changed.select("route_id").distinct().collect()]
    if changed_ids:
        closed = (
            curr_latest.filter(F.col("route_id").isin(changed_ids))
            .withColumn("valid_to", F.current_timestamp())
            .withColumn("is_current", F.lit(False))
        )
        write_jdbc(closed, "dwh.dim_route", mode="append")
        write_jdbc(changed.drop("route_sk"), "dwh.dim_route", mode="append")


def load_dimensions(spark, date_str):
    url = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/dwh")
    props = {
        "user": os.getenv("PG_USER", "dwh"),
        "password": os.getenv("PG_PASSWORD", "dwhpass"),
        "driver": "org.postgresql.Driver",
    }

    routes = read_raw(spark, date_str, "routes")
    scd2_dim_route(spark, routes)

    users = read_raw(spark, date_str, "users").select(
        "user_id", "city", "lang", "birthdate", "gender", "created_at", "is_active"
    )
    users = users.withColumn("effective_from", F.current_timestamp()).withColumn("effective_to", F.lit(None).cast("timestamp")).withColumn("is_current", F.lit(True))
    write_jdbc(users, "dwh.dim_user")

    vehicles = read_raw(spark, date_str, "vehicles").select(
        "vehicle_id",
        "route_id",
        "transport_type",
        "capacity",
        "vendor",
        "commissioned_at",
        "decommissioned_at",
    )
    write_jdbc(vehicles, "dwh.dim_vehicle")

    payments_methods = spark.createDataFrame(
        [
            ("card", "Bank Card", "mpay"),
            ("wallet", "Wallet", "mpay"),
            ("promo", "Promo", "mpay"),
        ],
        ["code", "name", "provider"],
    )
    write_jdbc(payments_methods, "dwh.dim_payment_method", mode="append")


def lookup_dimension(df, dim_table, business_key, sk_name, spark):
    url = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/dwh")
    props = {
        "user": os.getenv("PG_USER", "dwh"),
        "password": os.getenv("PG_PASSWORD", "dwhpass"),
        "driver": "org.postgresql.Driver",
    }
    dim = spark.read.jdbc(url=url, table=dim_table, properties=props)
    dim_current = dim.filter(F.col("is_current") == True) if "is_current" in dim.columns else dim
    return df.join(dim_current, business_key, "left")


def load_facts(spark, date_str):
    trips = read_raw(spark, date_str, "trips")
    payments = read_raw(spark, date_str, "payments")
    positions = read_raw(spark, date_str, "vehicle_positions")

    trips_enriched = lookup_dimension(trips, "dwh.dim_user", "user_id", "user_sk", spark)
    trips_enriched = lookup_dimension(trips_enriched, "dwh.dim_route", "route_id", "route_sk", spark)
    trips_enriched = lookup_dimension(trips_enriched, "dwh.dim_vehicle", "vehicle_id", "vehicle_sk", spark)

    trips_fact = trips_enriched.selectExpr(
        "trip_id",
        "user_sk",
        "route_sk",
        "vehicle_sk",
        "start_stop_id as start_stop_sk",
        "end_stop_id as end_stop_sk",
        "start_ts",
        "end_ts",
        "cast((unix_timestamp(end_ts) - unix_timestamp(start_ts)) as int) as duration_sec",
        "distance_m",
        "fare as fare_amount",
        "null as delay_start_sec",
        "null as delay_end_sec",
        "status",
        f"'batch_{date_str}' as load_id",
        "current_timestamp() as inserted_at",
    )
    write_jdbc(trips_fact, "dwh.fact_trip")

    payments_enriched = lookup_dimension(payments, "dwh.dim_user", "user_id", "user_sk", spark)
    payments_enriched = lookup_dimension(payments_enriched, "dwh.dim_route", "route_id", "route_sk", spark)
    payments_enriched = payments_enriched.join(
        spark.read.jdbc(
            url=os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/dwh"),
            table="dwh.dim_payment_method",
            properties={
                "user": os.getenv("PG_USER", "dwh"),
                "password": os.getenv("PG_PASSWORD", "dwhpass"),
                "driver": "org.postgresql.Driver",
            },
        ),
        payments_enriched.payment_method == F.col("code"),
        "left",
    )

    payments_fact = payments_enriched.selectExpr(
        "payment_id",
        "user_sk",
        "route_sk",
        "null as trip_sk",
        "payment_method_sk",
        "amount",
        "currency",
        "status",
        "paid_at",
        f"'batch_{date_str}' as load_id",
        "current_timestamp() as inserted_at",
    )
    write_jdbc(payments_fact, "dwh.fact_payment")

    positions_enriched = lookup_dimension(positions, "dwh.dim_route", "route_id", "route_sk", spark)
    positions_enriched = lookup_dimension(positions_enriched, "dwh.dim_vehicle", "vehicle_id", "vehicle_sk", spark)

    positions_fact = positions_enriched.withColumn("event_date_sk", F.date_format("event_ts", "yyyyMMdd").cast("int")).withColumn(
        "event_time_sk", (F.hour("event_ts") * 3600 + F.minute("event_ts") * 60 + F.second("event_ts")).cast("int")
    )
    positions_fact = positions_fact.selectExpr(
        "event_ts",
        "event_date_sk",
        "event_time_sk",
        "vehicle_sk",
        "route_sk",
        "lat",
        "lon",
        "speed",
        "heading",
        "provider_ts",
        f"'batch_{date_str}' as load_id",
        "current_timestamp() as inserted_at",
    )
    write_jdbc(positions_fact, "dwh.fact_vehicle_position")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Partition date dt=YYYY-MM-DD in raw bucket")
    args = parser.parse_args()

    spark = get_spark()
    load_staging(spark, args.date)
    load_dimensions(spark, args.date)
    load_facts(spark, args.date)
    spark.stop()


if __name__ == "__main__":
    main()

