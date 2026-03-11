CREATE OR REPLACE FUNCTION gold.refresh_tempo_analysis()
RETURNS TABLE(total_jours BIGINT, correct BIGINT, incorrect BIGINT, pct_match NUMERIC) AS $$
DECLARE
    p_nc DOUBLE PRECISION;
    p_ne DOUBLE PRECISION;
    p_sbr_i DOUBLE PRECISION;
    p_sbr_j DOUBLE PRECISION;
    p_sbr_s DOUBLE PRECISION;
    p_sr_i DOUBLE PRECISION;
    p_sr_j DOUBLE PRECISION;
    p_sr_s DOUBLE PRECISION;
BEGIN
    -- Lire les derniers paramètres optimisés
    SELECT norm_centre, norm_echelle,
           sbr_intercept, sbr_coeff_jour, sbr_coeff_stock,
           sr_intercept, sr_coeff_jour, sr_coeff_stock
    INTO p_nc, p_ne, p_sbr_i, p_sbr_j, p_sbr_s, p_sr_i, p_sr_j, p_sr_s
    FROM gold.tempo_params
    ORDER BY updated_at DESC
    LIMIT 1;

    -- Fallback paramètres RTE originaux si table vide
    IF p_nc IS NULL THEN
        p_nc := 46050; p_ne := 2160;
        p_sbr_i := 4.00; p_sbr_j := 0.015; p_sbr_s := 0.026;
        p_sr_i := 3.15; p_sr_j := 0.010; p_sr_s := 0.031;
    END IF;

    WITH
    conso_journaliere AS (
        SELECT d.date, avg(c.avg_value_mw) AS conso_brute_mw
        FROM raw.rte_tempo_calendar d
        JOIN silver.rte_consumption_hourly c
            ON c.ts_hour >= (d.date::timestamp + interval '6 hours') AT TIME ZONE 'Europe/Paris'
           AND c.ts_hour <  (d.date::timestamp + interval '30 hours') AT TIME ZONE 'Europe/Paris'
        GROUP BY d.date
    ),
    prod_renouvelable AS (
        SELECT d.date,
            coalesce(avg(CASE WHEN g.production_type = 'WIND' THEN g.avg_value END), 0) AS eolien_mw,
            coalesce(avg(CASE WHEN g.production_type = 'SOLAR' THEN g.avg_value END), 0) AS pv_mw
        FROM raw.rte_tempo_calendar d
        JOIN gold.rte_hourly g
            ON g.ts_hour >= (d.date::timestamp + interval '6 hours') AT TIME ZONE 'Europe/Paris'
           AND g.ts_hour <  (d.date::timestamp + interval '30 hours') AT TIME ZONE 'Europe/Paris'
        WHERE g.production_type IN ('WIND', 'SOLAR')
        GROUP BY d.date
    ),
    conso_nette AS (
        SELECT c.date, c.conso_brute_mw,
            coalesce(p.eolien_mw, 0) AS eolien_mw,
            coalesce(p.pv_mw, 0) AS pv_mw,
            c.conso_brute_mw - coalesce(p.eolien_mw, 0) - coalesce(p.pv_mw, 0) AS conso_nette_mw
        FROM conso_journaliere c
        LEFT JOIN prod_renouvelable p ON p.date = c.date
    ),
    normalise AS (
        SELECT *, (conso_nette_mw - p_nc) / nullif(p_ne, 0) AS conso_nette_std
        FROM conso_nette
    ),
    avec_stock AS (
        SELECT n.*,
            (n.date - '2025-09-01'::date + 1) AS jour_tempo,
            22 - coalesce((SELECT count(*) FROM raw.rte_tempo_calendar t2 WHERE t2.color = 'RED' AND t2.date < n.date AND t2.date >= '2025-09-01'), 0) AS stock_rouge,
            43 - coalesce((SELECT count(*) FROM raw.rte_tempo_calendar t2 WHERE t2.color = 'WHITE' AND t2.date < n.date AND t2.date >= '2025-09-01'), 0) AS stock_blanc
        FROM normalise n
    ),
    avec_seuils AS (
        SELECT *,
            p_sbr_i - p_sbr_j * jour_tempo - p_sbr_s * (stock_rouge + stock_blanc) AS seuil_blanc_rouge,
            p_sr_i  - p_sr_j  * jour_tempo - p_sr_s  * stock_rouge AS seuil_rouge
        FROM avec_stock
    ),
    avec_couleur AS (
        SELECT *,
            CASE
                WHEN conso_nette_std > seuil_rouge AND extract(month FROM date) IN (11,12,1,2,3) AND extract(dow FROM date) NOT IN (0,6) THEN 'RED'
                WHEN conso_nette_std > seuil_blanc_rouge AND extract(dow FROM date) != 0 THEN 'WHITE'
                ELSE 'BLUE'
            END AS couleur_calculee
        FROM avec_seuils
    )
    INSERT INTO gold.tempo_analysis
        (date, conso_nette_mwh, conso_nette_std, seuil_blanc_rouge, seuil_rouge,
         couleur_calculee, couleur_officielle, match, jour_tempo, stock_rouge_restant, stock_blanc_restant, type)
    SELECT a.date, round(a.conso_nette_mw::numeric), round(a.conso_nette_std::numeric, 4),
        round(a.seuil_blanc_rouge::numeric, 4), round(a.seuil_rouge::numeric, 4),
        a.couleur_calculee, t.color, a.couleur_calculee = t.color,
        a.jour_tempo, a.stock_rouge, a.stock_blanc, 'RETROSPECTIVE'
    FROM avec_couleur a
    JOIN raw.rte_tempo_calendar t ON t.date = a.date
    ON CONFLICT (date) DO UPDATE SET
        conso_nette_mwh = EXCLUDED.conso_nette_mwh, conso_nette_std = EXCLUDED.conso_nette_std,
        seuil_blanc_rouge = EXCLUDED.seuil_blanc_rouge, seuil_rouge = EXCLUDED.seuil_rouge,
        couleur_calculee = EXCLUDED.couleur_calculee, couleur_officielle = EXCLUDED.couleur_officielle,
        match = EXCLUDED.match, jour_tempo = EXCLUDED.jour_tempo,
        stock_rouge_restant = EXCLUDED.stock_rouge_restant, stock_blanc_restant = EXCLUDED.stock_blanc_restant;

    SELECT count(*), count(*) FILTER (WHERE g.match), count(*) FILTER (WHERE NOT g.match),
        round(100.0 * count(*) FILTER (WHERE g.match) / count(*), 1)
    INTO total_jours, correct, incorrect, pct_match
    FROM gold.tempo_analysis g WHERE g.conso_nette_mwh IS NOT NULL;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
