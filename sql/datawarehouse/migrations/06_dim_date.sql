-- dim_date: generated calendar table (not sourced from a staging table).
-- Populated separately via generate_series() — see 03_populate_dim_date.sql.
-- Supports Power BI drill-down (year -> quarter -> month -> day) and trend analysis.
CREATE TABLE dim_date (
    date_key          date PRIMARY KEY,
    year              int NOT NULL,
    quarter           int NOT NULL,
    month             int NOT NULL,
    month_name        varchar(10) NOT NULL,
    week              int NOT NULL,
    day               int NOT NULL,
    day_of_week       int NOT NULL,
    day_of_week_name  varchar(10) NOT NULL,
    is_weekend        boolean NOT NULL
);
