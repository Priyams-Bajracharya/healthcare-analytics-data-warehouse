-- One-time seed step: populates dim_date for the full date range.
-- This is a load-only step (not real ETL) — no source table, generated via generate_series.
-- Run once after 06_dim_date.sql creates the table structure.

INSERT INTO dim_date (
    date_key,
    year,
    quarter,
    month,
    month_name,
    week,
    day,
    day_of_week,
    day_of_week_name,
    is_weekend
)
SELECT
    d::date AS date_key,
    EXTRACT(YEAR FROM d)::int AS year,
    EXTRACT(quarter FROM d)::int AS quarter,
    EXTRACT(MONTH FROM d)::int AS month,
    trim(to_char(d, 'Month')) AS month_name,
    EXTRACT(week FROM d)::int AS week,
    EXTRACT(DAY FROM d)::int AS day,
    EXTRACT(ISODOW FROM d)::int AS day_of_week,
    trim(to_char(d, 'Day')) AS day_of_week_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2000-01-01'::date, '2021-11-19'::date, '1 day') AS d;