-- ============================================================
-- openmeteo_forecast : tables + procédures stockées
-- À exécuter une seule fois pour initialiser le pipeline
-- ============================================================

-- BRONZE
CREATE TABLE IF NOT EXISTS raw.openmeteo_forecast (
    date                       DATE PRIMARY KEY,
    temperature_2m_mean        DOUBLE PRECISION,
    temperature_2m_min         DOUBLE PRECISION,
    temperature_2m_max         DOUBLE PRECISION,
    precipitation_sum          DOUBLE PRECISION,
    wind_speed_10m_max         DOUBLE PRECISION,
    wind_gusts_10m_max         DOUBLE PRECISION,
    relative_humidity_2m_mean  DOUBLE PRECISION,
    cloud_cover_mean           DOUBLE PRECISION,
    shortwave_radiation_sum    DOUBLE PRECISION,
    fetched_at                 TIMESTAMPTZ DEFAULT NOW()
);

-- SILVER
CREATE TABLE IF NOT EXISTS silver.openmeteo_forecast_daily (
    date                    DATE PRIMARY KEY,
    temp_mean               DOUBLE PRECISION NOT NULL,
    temp_min                DOUBLE PRECISION NOT NULL,
    temp_max                DOUBLE PRECISION NOT NULL,
    precipitation_sum       DOUBLE PRECISION NOT NULL DEFAULT 0,
    wind_speed_max          DOUBLE PRECISION NOT NULL DEFAULT 0,
    wind_gusts_max          DOUBLE PRECISION NOT NULL DEFAULT 0,
    humidity_mean           DOUBLE PRECISION NOT NULL DEFAULT 0,
    cloudcover_mean         DOUBLE PRECISION NOT NULL DEFAULT 0,
    shortwave_radiation_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    complete                BOOLEAN DEFAULT TRUE,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

-- GOLD
CREATE TABLE IF NOT EXISTS gold.openmeteo_forecast (
    date                    DATE PRIMARY KEY,
    temp_mean               DOUBLE PRECISION NOT NULL,
    temp_min                DOUBLE PRECISION NOT NULL,
    temp_max                DOUBLE PRECISION NOT NULL,
    precipitation_sum       DOUBLE PRECISION NOT NULL,
    wind_speed_max          DOUBLE PRECISION NOT NULL,
    wind_gusts_max          DOUBLE PRECISION NOT NULL,
    humidity_mean           DOUBLE PRECISION NOT NULL,
    cloudcover_mean         DOUBLE PRECISION NOT NULL,
    shortwave_radiation_sum DOUBLE PRECISION NOT NULL,
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

-- GOLD forecast tempo (sortie du modèle)
CREATE TABLE IF NOT EXISTS gold.tempo_forecast (
    date                  DATE PRIMARY KEY,
    temp_mean             DOUBLE PRECISION,
    conso_pred_mwh        DOUBLE PRECISION,
    conso_lower_mwh       DOUBLE PRECISION,
    conso_upper_mwh       DOUBLE PRECISION,
    conso_nette_std_pred  DOUBLE PRECISION,
    seuil_blanc_rouge     DOUBLE PRECISION,
    seuil_rouge           DOUBLE PRECISION,
    p_blue                DOUBLE PRECISION,
    p_white               DOUBLE PRECISION,
    p_red                 DOUBLE PRECISION,
    couleur_predite       TEXT,
    confiance             DOUBLE PRECISION,
    stock_rouge           INT,
    stock_blanc           INT,
    jour_tempo            INT,
    jours_restants        INT,
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- SILVER : nettoyage raw → silver
-- ============================================================
CREATE OR REPLACE FUNCTION silver.refresh_openmeteo_forecast()
RETURNS TABLE(rows_upserted BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    INSERT INTO silver.openmeteo_forecast_daily
        (date, temp_mean, temp_min, temp_max,
         precipitation_sum, wind_speed_max, wind_gusts_max,
         humidity_mean, cloudcover_mean, shortwave_radiation_sum,
         complete, updated_at)
    SELECT
        date,
        COALESCE(temperature_2m_mean, 0),
        COALESCE(temperature_2m_min, 0),
        COALESCE(temperature_2m_max, 0),
        COALESCE(precipitation_sum, 0),
        COALESCE(wind_speed_10m_max, 0),
        COALESCE(wind_gusts_10m_max, 0),
        COALESCE(relative_humidity_2m_mean, 0),
        COALESCE(cloud_cover_mean, 0),
        COALESCE(shortwave_radiation_sum, 0),
        (temperature_2m_mean IS NOT NULL
         AND temperature_2m_min IS NOT NULL
         AND temperature_2m_max IS NOT NULL),
        NOW()
    FROM raw.openmeteo_forecast
    WHERE date >= CURRENT_DATE
    ON CONFLICT (date) DO UPDATE SET
        temp_mean               = EXCLUDED.temp_mean,
        temp_min                = EXCLUDED.temp_min,
        temp_max                = EXCLUDED.temp_max,
        precipitation_sum       = EXCLUDED.precipitation_sum,
        wind_speed_max          = EXCLUDED.wind_speed_max,
        wind_gusts_max          = EXCLUDED.wind_gusts_max,
        humidity_mean           = EXCLUDED.humidity_mean,
        cloudcover_mean         = EXCLUDED.cloudcover_mean,
        shortwave_radiation_sum = EXCLUDED.shortwave_radiation_sum,
        complete                = EXCLUDED.complete,
        updated_at              = NOW();

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN QUERY SELECT v_count;
END;
$$;

-- ============================================================
-- GOLD : silver → gold (uniquement les lignes complètes)
-- ============================================================
CREATE OR REPLACE FUNCTION gold.refresh_openmeteo_forecast()
RETURNS TABLE(rows_upserted BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count BIGINT;
BEGIN
    INSERT INTO gold.openmeteo_forecast
        (date, temp_mean, temp_min, temp_max,
         precipitation_sum, wind_speed_max, wind_gusts_max,
         humidity_mean, cloudcover_mean, shortwave_radiation_sum,
         updated_at)
    SELECT
        date, temp_mean, temp_min, temp_max,
        precipitation_sum, wind_speed_max, wind_gusts_max,
        humidity_mean, cloudcover_mean, shortwave_radiation_sum,
        NOW()
    FROM silver.openmeteo_forecast_daily
    WHERE date >= CURRENT_DATE AND complete = TRUE
    ON CONFLICT (date) DO UPDATE SET
        temp_mean               = EXCLUDED.temp_mean,
        temp_min                = EXCLUDED.temp_min,
        temp_max                = EXCLUDED.temp_max,
        precipitation_sum       = EXCLUDED.precipitation_sum,
        wind_speed_max          = EXCLUDED.wind_speed_max,
        wind_gusts_max          = EXCLUDED.wind_gusts_max,
        humidity_mean           = EXCLUDED.humidity_mean,
        cloudcover_mean         = EXCLUDED.cloudcover_mean,
        shortwave_radiation_sum = EXCLUDED.shortwave_radiation_sum,
        updated_at              = NOW();

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN QUERY SELECT v_count;
END;
$$;
