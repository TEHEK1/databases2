"""
Builds hourly route performance mart and writes to ClickHouse.

Usage:
  spark-submit \
    --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.4.6 \
    /opt/spark_jobs/mart_route_hourly.py --date-from 2024-04-01 --date-to 2024-04-03
"""

import argparse
import os
from pyspark.sql import SparkSession, functions as F


def get_spark():
    return SparkSession.builder.appName("metropulse-mart-hourly").getOrCreate()


def read_fact_trip(spark, url, props, date_from, date_to):
    query = f"(select * from dwh.fact_trip where start_ts >= '{date_from}' and start_ts < '{date_to}') as fact_trip_sub"
    return spark.read.jdbc(url=url, table=query, properties=props)


def read_vehicle_pos(spark, url, props, date_from, date_to):
    query = f"(select * from dwh.fact_vehicle_position where event_ts >= '{date_from}' and event_ts < '{date_to}') as fvp_sub"
    return spark.read.jdbc(url=url, table=query, properties=props)


def read_dim_route(spark, url, props):
    return spark.read.jdbc(url=url, table="dwh.dim_route", properties=props).filter("is_current = true")


def write_clickhouse(df):
    ch_url = os.getenv("CH_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/default")
    props = {
        "user": os.getenv("CH_USER", "default"),
        "password": os.getenv("CH_PASSWORD", ""),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }
    df.write.jdbc(url=ch_url, table="mart_route_hourly", mode="append", properties=props)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    args = parser.parse_args()

    spark = get_spark()

    pg_url = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/dwh")
    pg_props = {
        "user": os.getenv("PG_USER", "dwh"),
        "password": os.getenv("PG_PASSWORD", "dwhpass"),
        "driver": "org.postgresql.Driver",
    }

    trips = read_fact_trip(spark, pg_url, pg_props, args.date_from, args.date_to)
    fvp = read_vehicle_pos(spark, pg_url, pg_props, args.date_from, args.date_to)
    dim_route = read_dim_route(spark, pg_url, pg_props)

    trips = trips.join(dim_route.select("route_sk", "route_id", "name", "transport_type"), "route_sk", "left")
    fvp = fvp.join(dim_route.select("route_sk", "route_id"), "route_sk", "left")

    trips_hourly = (
        trips.withColumn("hour_bucket", F.date_trunc("hour", "start_ts"))
        .groupBy("route_id", "name", "transport_type", "hour_bucket")
        .agg(
            F.countDistinct("trip_id").alias("trip_cnt"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("duration_sec").alias("avg_duration_sec"),
            F.sum("distance_m").alias("sum_distance_m"),
        )
    )

    speed_hourly = (
        fvp.withColumn("hour_bucket", F.date_trunc("hour", "event_ts"))
        .groupBy("route_id", "hour_bucket")
        .agg(F.avg("speed").alias("avg_speed"))
    )

    mart = (
        trips_hourly.join(speed_hourly, ["route_id", "hour_bucket"], "left")
        .withColumn("avg_speed", F.coalesce("avg_speed", F.lit(0.0)))
        .withColumn("snapshot_ts", F.current_timestamp())
    )

    write_clickhouse(mart)
    spark.stop()


if __name__ == "__main__":
    main()

