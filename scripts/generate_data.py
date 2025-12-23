"""
Synthetic data generator for MetroPulse.
Generates OLTP-like tables and telemetry, writes Parquet to local ./data/raw
and optionally uploads to MinIO bucket `raw`.

Dependencies: pandas, numpy, pyarrow, minio, faker
Usage:
  python scripts/generate_data.py --days 3 --upload
"""

import argparse
import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker
from minio import Minio


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def init_minio():
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def upload_dir(client: Minio, local_dir: str, bucket: str = "raw", prefix: str = ""):
    for root, _, files in os.walk(local_dir):
        for f in files:
            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, local_dir)
            object_name = f"{prefix}/{rel_path}".lstrip("/")
            client.fput_object(bucket, object_name, full_path)
            print(f"Uploaded {object_name}")


def gen_routes(fake: Faker, n=20):
    transport_types = ["bus", "tram", "metro"]
    data = []
    for i in range(n):
        rid = f"R{i:03d}"
        rtype = random.choice(transport_types)
        base_fare = round(random.uniform(20, 80), 2)
        data.append(
            {
                "route_id": rid,
                "name": f"Route {rid}",
                "transport_type": rtype,
                "base_fare": base_fare,
                "schema_json": {"stops": [fake.street_name() for _ in range(8)]},
                "active_from": datetime.utcnow() - timedelta(days=90),
                "active_to": None,
            }
        )
    return pd.DataFrame(data)


def gen_users(fake: Faker, n=2000):
    data = []
    for i in range(n):
        uid = f"U{i:06d}"
        created_at = fake.date_time_between(start_date="-180d", end_date="now")
        data.append(
            {
                "user_id": uid,
                "created_at": created_at,
                "city": fake.city(),
                "lang": random.choice(["ru", "en"]),
                "gender": random.choice(["m", "f"]),
                "birthdate": fake.date_of_birth(minimum_age=18, maximum_age=65),
                "is_active": random.random() > 0.05,
            }
        )
    return pd.DataFrame(data)


def gen_vehicles(routes_df: pd.DataFrame, per_route=5):
    data = []
    for _, r in routes_df.iterrows():
        for i in range(per_route):
            vid = f"V{r.route_id}{i:02d}"
            data.append(
                {
                    "vehicle_id": vid,
                    "route_id": r.route_id,
                    "transport_type": r.transport_type,
                    "capacity": random.choice([50, 70, 120]),
                    "vendor": random.choice(["MAN", "Volvo", "Scania", "CAF"]),
                    "commissioned_at": datetime.utcnow() - timedelta(days=random.randint(200, 2000)),
                    "decommissioned_at": None,
                }
            )
    return pd.DataFrame(data)


def gen_trips(fake: Faker, users_df, routes_df, vehicles_df, days=3, avg_trips_per_user=2):
    data = []
    now = datetime.utcnow()
    start_date = now - timedelta(days=days)
    trip_id = 0
    for _, user in users_df.iterrows():
        trips = np.random.poisson(avg_trips_per_user * days / 3)
        for _ in range(trips):
            route = routes_df.sample(1).iloc[0]
            vehicle = vehicles_df[vehicles_df.route_id == route.route_id].sample(1).iloc[0]
            start = fake.date_time_between(start_date=start_date, end_date=now)
            duration_min = random.randint(5, 60)
            end = start + timedelta(minutes=duration_min)
            fare = round(route.base_fare * random.uniform(0.8, 1.4), 2)
            distance = round(random.uniform(1.5, 25.0), 3)
            trip_id += 1
            data.append(
                {
                    "trip_id": f"T{trip_id:09d}",
                    "user_id": user.user_id,
                    "route_id": route.route_id,
                    "vehicle_id": vehicle.vehicle_id,
                    "start_ts": start,
                    "end_ts": end,
                    "start_stop_id": f"S{random.randint(1, 200)}",
                    "end_stop_id": f"S{random.randint(1, 200)}",
                    "distance_m": distance * 1000,
                    "fare": fare,
                    "status": "completed",
                }
            )
    return pd.DataFrame(data)


def gen_payments(trips_df):
    data = []
    pay_methods = ["card", "wallet", "promo"]
    for _, t in trips_df.iterrows():
        data.append(
            {
                "payment_id": f"P{t.trip_id}",
                "user_id": t.user_id,
                "route_id": t.route_id,
                "trip_id": t.trip_id,
                "amount": t.fare,
                "currency": "RUB",
                "status": "captured",
                "payment_method": random.choice(pay_methods),
                "paid_at": t.end_ts,
            }
        )
    return pd.DataFrame(data)


def gen_vehicle_positions(trips_df, vehicles_df, interval_sec=10):
    data = []
    for _, trip in trips_df.iterrows():
        duration = (trip.end_ts - trip.start_ts).total_seconds()
        steps = max(1, int(duration // interval_sec))
        lat0, lon0 = 55.75 + random.uniform(-0.2, 0.2), 37.61 + random.uniform(-0.2, 0.2)
        for i in range(steps):
            ts = trip.start_ts + timedelta(seconds=i * interval_sec)
            data.append(
                {
                    "event_ts": ts,
                    "vehicle_id": trip.vehicle_id,
                    "route_id": trip.route_id,
                    "lat": lat0 + random.uniform(-0.01, 0.01),
                    "lon": lon0 + random.uniform(-0.01, 0.01),
                    "speed": random.uniform(5, 60),
                    "heading": random.uniform(0, 360),
                    "provider_ts": ts,
                }
            )
    df = pd.DataFrame(data)
    if df.empty:
        return df
    return df.sample(frac=1)  # shuffle


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=3, help="Days of history to generate")
    parser.add_argument("--output", default="data/raw", help="Local output dir")
    parser.add_argument("--upload", action="store_true", help="Upload to MinIO bucket raw")
    args = parser.parse_args()

    fake = Faker()
    ensure_dir(args.output)

    print("Generating routes/users/vehicles...")
    routes = gen_routes(fake)
    users = gen_users(fake)
    vehicles = gen_vehicles(routes)

    print("Generating trips/payments...")
    trips = gen_trips(fake, users, routes, vehicles, days=args.days)
    payments = gen_payments(trips)

    print("Generating vehicle positions...")
    positions = gen_vehicle_positions(trips, vehicles)

    date_part = datetime.utcnow().strftime("%Y-%m-%d")
    base = os.path.join(args.output, f"dt={date_part}")
    ensure_dir(base)

    datasets = {
        "routes": routes,
        "users": users,
        "vehicles": vehicles,
        "trips": trips,
        "payments": payments,
        "vehicle_positions": positions,
    }

    for name, df in datasets.items():
        out_path = os.path.join(base, f"{name}.parquet")
        if df.empty:
            print(f"Skipping {name}: no data")
            continue
        df.to_parquet(out_path, index=False)
        print(f"Wrote {out_path} ({len(df)} rows)")

    if args.upload:
        print("Uploading to MinIO...")
        client = init_minio()
        upload_dir(client, args.output, bucket="raw", prefix="")
        print("Upload complete.")


if __name__ == "__main__":
    main()

