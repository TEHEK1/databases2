# Архитектура аналитической платформы MetroPulse — Этап 1 (DWH)

## 1. Контекст и цели
- Бизнес: отчётность по популярности маршрутов, оперативные дашборды по трафику, подготовка датасетов для ML (ETA).
- Источники:  
  - Kafka `vehicle_positions`: каждые 10 с от ТС — `event_ts`, `vehicle_id`, `route_id`, `lat`, `lon`, `speed`, `heading`, `provider_ts`.  
  - OLTP (PostgreSQL): пользователи, профили, транзакции, поездки, построенные маршруты (структура недоступна, ниже — рабочие предположения для прототипа).
- Цель этапа: спроектировать DWH c выделенными слоями Staging и Core, обеспечить историю изменений (SCD2) для маршрутов.

## 2. Методология моделирования
- Рассмотрены:
  - **Kimball (Dimensional Model)** — проста для BI/дашбордов, быстрая выдача витрин, понятные факты/измерения, SCD встроен.
  - **Data Vault 2.0** — гибкая интеграция множества источников, лучше для долгосрочной эволюции, но сложнее для аналитиков, требует витрин поверх.
- **Выбор: Kimball**. Аргументы: 1) ключевой потребитель — BI/оперативные дашборды и ML-фичи; 2) один основной OLTP источник + один стрим, ограниченная гетерогенность; 3) нужен быстрый time-to-value; 4) SCD2 легко реализуем в измерениях.

## 3. Слои и потоки
- **Landing/Raw (в MinIO/S3)**: складируем партиционированные Parquet/JSON выгрузки из Kafka и OLTP.
- **Staging (PostgreSQL/Greenplum, схема `staging`)**: плоские таблицы близкие к источникам, минимальные приведения типов, раздельные инкременты по партиям загрузки.
- **Core DWH (схема `dwh`)**: размерности с суррогатными ключами и фактами на атомарном зерне. История маршрутов — SCD2.
- **(Дальше, этап 3)**: витрины `mart` (ClickHouse или Redis + Flink) строятся поверх `dwh`.

## 4. Логическая модель (звезда)
**Измерения**
- `dim_date`, `dim_time` — календарь и время суток.
- `dim_user` — пользовательские атрибуты (город, тип аккаунта).
- `dim_route` (SCD2) — идентификатор маршрута, тип ТС, название, тариф, схема движения.
- `dim_vehicle` — ТС и их тип/вместимость.
- `dim_stop` — остановки/станции (для планов поездок).
- `dim_payment_method` — типы оплаты (карта, кошелёк, промо).

**Факты**
- `fact_trip` — одна запись = завершённая поездка пользователя. Меры: длительность, расстояние, стоимость, задержка старта/финиша.
- `fact_payment` — финтранзакции, связанные с поездками или пополнениями.
- `fact_vehicle_position` — телеметрия каждые 10 с по ТС (гранулярный исторический след).
- (Опционально для ML/BI) `fact_route_snapshot_hourly` — часовые агрегации движения/скорости по маршруту (строится во время витрин).

## 5. Ключевые атрибуты и зерно
- `fact_trip` зерно: одна поездка пользователя; ссылки: `user_sk`, `route_sk`, `vehicle_sk`, `start_stop_sk`, `end_stop_sk`, `payment_sk?`.
- `fact_payment` зерно: одна транзакция; ссылки: `user_sk`, `route_sk`, `trip_sk?`, `payment_method_sk`.
- `fact_vehicle_position` зерно: одно событие телеметрии (ТС, маршрут, момент времени).

## 6. SCD Type 2 для `dim_route`
- Поля истории: `valid_from`, `valid_to`, `is_current`.  
- При изменении бизнес-атрибутов (тариф, трасса, тип ТС) закрываем текущую запись (`valid_to=ts-1`, `is_current=false`), вставляем новую с `is_current=true`.  
- Суррогатный ключ `route_sk` используется во всех фактах, что сохраняет историческую связность.

## 7. Физическая схема (PostgreSQL/Greenplum)
- Схемы: `staging`, `dwh`.
- Типы: `timestamp with time zone` для времени, `numeric` для денег, `geometry`/`jsonb` допустимо, но для переносимости — `jsonb` под схемы маршрутов.
- Партиционирование:
  - `fact_vehicle_position` — по дате события (LIST/RANGE по `event_date`).
  - `fact_trip`, `fact_payment` — RANGE по `trip_date` / `payment_date`.
- Индексы: суррогатные PK, уникальные на бизнес-ключи в `dim_*`, B-tree на внешние ключи, BRIN на больших фактах по дате.

## 8. Таблицы Staging (минимально нормализованные)
- `staging.vehicle_positions_raw(event_ts, vehicle_id, route_id, lat, lon, speed, heading, payload, load_id, ingestion_ts)`
- `staging.users_raw(user_id, created_at, city, lang, gender, birthdate, load_id, ingestion_ts, payload)`
- `staging.routes_raw(route_id, name, transport_type, base_fare, schema_json, active_from, active_to, load_id, ingestion_ts)`
- `staging.trips_raw(trip_id, user_id, route_id, vehicle_id, start_ts, end_ts, start_stop_id, end_stop_id, distance_m, fare, status, load_id, ingestion_ts)`
- `staging.payments_raw(payment_id, user_id, route_id, trip_id, amount, currency, status, payment_method, paid_at, load_id, ingestion_ts)`

## 9. Таблицы Core DWH (измерения)
- `dwh.dim_date(date_sk, full_date, day, week, month, quarter, year, is_weekend)`
- `dwh.dim_time(time_sk, hh24mi, hour, minute, second)`
- `dwh.dim_user(user_sk, user_id, city, lang, birthdate, gender, created_at, is_active, effective_from, effective_to, is_current)`
- `dwh.dim_route` (SCD2) — см. пункт 6.
- `dwh.dim_vehicle(vehicle_sk, vehicle_id, route_id, transport_type, capacity, vendor, commissioned_at, decommissioned_at)`
- `dwh.dim_stop(stop_sk, stop_id, name, lat, lon, city)`
- `dwh.dim_payment_method(payment_method_sk, code, name, provider)`

## 10. Таблицы Core DWH (факты)
- `dwh.fact_trip(trip_sk, trip_id, user_sk, route_sk, vehicle_sk, start_stop_sk, end_stop_sk, start_ts, end_ts, duration_sec, distance_m, fare_amount, delay_start_sec, delay_end_sec, status, load_id, inserted_at)`
- `dwh.fact_payment(payment_sk, payment_id, user_sk, route_sk, trip_sk, payment_method_sk, amount, currency, status, paid_at, load_id, inserted_at)`
- `dwh.fact_vehicle_position(position_sk, event_ts, event_date_sk, event_time_sk, vehicle_sk, route_sk, lat, lon, speed, heading, provider_ts, load_id, inserted_at)`

## 11. Потоки загрузки (высокоуровнево)
1) **Staging**: инкрементальная загрузка из Kafka/MinIO и OLTP снапшотов → таблицы `*_raw` с `load_id`.  
2) **Dedupe/typing**: чистка дубликатов по бизнес-ключам, приведение типов, отбраковка аномалий (некорректные координаты/скорость).  
3) **SCD2 для `dim_route`**: сравнение входящего набора по `route_id` с текущими версиями; применение upsert с закрытием старых версий.  
4) **Факты**: обогащение суррогатными ключами, расчет производных полей (`duration_sec`, `delay_*`, `distance_m` если доступно), вставка в партиции.  
5) **Календарные измерения**: предзагрузка/регулярное наполнение `dim_date`, `dim_time`.

## 12. Качество данных и аудит
- `load_id`, `ingestion_ts`, `inserted_at` в каждой таблице.
- Контроль уникальности бизнес-ключей в измерениях (`dim_*`).
- BRIN индексы на фактах по датам для ускорения сканов.
- Базовые проверки качества (NOT NULL на ключах, диапазоны координат, speed ≥ 0).

## 13. Почему подходит для аналитиков и DS
- Чёткое зерно фактов → быстрые агрегаты для BI.
- История изменений маршрутов через SCD2 → корректные исторические отчёты и фичи.
- Наличие `fact_vehicle_position` с высокой гранулярностью → фичи для ETA и мониторинг.

## 14. Дальнейшие шаги (выход за этап 1)
- Подготовить Docker-compose (Spark + PostgreSQL/Greenplum + MinIO).
- Генерация синтетических данных в Parquet; загрузка в Staging.
- Spark ETL с реализацией SCD2 и загрузкой фактов.
- Оркестрация (Airflow) и построение витрин (ClickHouse или Flink+Redis).

