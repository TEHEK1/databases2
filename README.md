# MetroPulse Analytics Prototype

Полный прототип DWH + batch-пайплайн + витрина (вариант A, ClickHouse).

## Состав
- `docs/architecture_stage1.md` — архитектура и модель DWH (Kimball, SCD2 по маршрутам).
- `ddl/dwh_schema.sql` — DDL PostgreSQL/Greenplum (staging + dwh).
- `ddl/clickhouse_mart.sql` — DDL витрины в ClickHouse.
- `scripts/generate_data.py` — генерация синтетики в Parquet и загрузка в MinIO.
- `scripts/requirements.txt` — зависимости генератора.
- `spark_jobs/etl_batch.py` — Spark ETL: MinIO → staging → dwh (SCD2).
- `spark_jobs/mart_route_hourly.py` — Spark job для витрины часовыми агрегатами → ClickHouse.
- `docker-compose.yml` — Postgres, MinIO, Spark master/worker, ClickHouse, mc.

## Быстрый старт
1. Запуск окружения:
   ```bash
   docker-compose up -d
   ```
   Postgres: `localhost:5432` (dwh/dwhpass), MinIO: `localhost:9000` (admin/admin123), ClickHouse: `localhost:8123`.

2. Создать схемы DWH:
   ```bash
   PGPASSWORD=dwhpass psql -h localhost -U dwh -d dwh -f ddl/dwh_schema.sql
   ```

3. Генерировать данные (локально) и загрузить в MinIO:
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r scripts/requirements.txt
   python scripts/generate_data.py --days 3 --output data/raw --upload
   ```
   Файлы лягут в `data/raw/dt=YYYY-MM-DD/*.parquet` и в MinIO bucket `raw`.

4. Запустить batch ETL (в контейнере spark-master):
   ```bash
   docker exec -it metropulse_spark_master \
     spark-submit \
     --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4 \
     /opt/spark_jobs/etl_batch.py --date YYYY-MM-DD
   ```
   Заполнит `staging.*`, `dwh.dim_*`, `dwh.fact_*` с SCD2 по `dim_route`.

5. Создать витрину в ClickHouse:
   ```bash
   clickhouse-client --host localhost --port 9000 --user default --query "CREATE DATABASE IF NOT EXISTS default;"
   clickhouse-client --host localhost --query "DROP TABLE IF EXISTS mart_route_hourly;"
   clickhouse-client --host localhost --query "$(cat ddl/clickhouse_mart.sql)"
   ```

6. Построить витрину Spark'ом:
   ```bash
   docker exec -it metropulse_spark_master \
     spark-submit \
     --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.4.6 \
     /opt/spark_jobs/mart_route_hourly.py --date-from 2024-04-01 --date-to 2024-05-01
   ```
   Проверка:
   ```bash
   clickhouse-client --host localhost --query "SELECT * FROM mart_route_hourly LIMIT 10;"
   ```

## Настройки окружения
- PG: `PG_URL=jdbc:postgresql://postgres:5432/dwh`, `PG_USER=dwh`, `PG_PASSWORD=dwhpass`
- MinIO: `MINIO_ENDPOINT=http://minio:9000`, `MINIO_ACCESS_KEY=admin`, `MINIO_SECRET_KEY=admin123`
- ClickHouse: `CH_JDBC_URL=jdbc:clickhouse://clickhouse:8123/default`

## Оркестрация (эскиз Airflow)
- DAG ежедневный:
  1) Sensor наличия партиции `raw/dt={{ds}}`.
  2) Task `spark_submit` генерации календаря (необязательно).
  3) Task `spark_submit` etl_batch.py `--date {{ds}}`.
  4) Task `spark_submit` mart_route_hourly.py `--date-from {{ds}} --date-to {{ds}} + 1`.
  5) Data quality checks (COUNT > 0, уникальность business key).
- Параметры соединений Airflow: Conn MinIO (S3), Postgres (JDBC), ClickHouse (HTTP/JDBC).

## Реальное время (вариант B, если понадобится)
Для оперативного мониторинга можно добавить Flink job (Kafka -> Flink -> Redis) по той же схеме, но текущий репозиторий реализует вариант A (batch-витрина в ClickHouse).

## Что дальше
- Добавить тесты качества данных (Great Expectations/dbt tests).
- Добавить BRIN-партиционирование фактов в Postgres или перейти на Greenplum для горизонтального масштабирования.
- Для больших потоков — вынести телеметрию в columnar/TSDB (ClickHouse) сразу после Staging.

