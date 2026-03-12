DELETE FROM dbt_gold.linky_hourly WHERE hour >= NOW() - INTERVAL '3 hours';

INSERT INTO dbt_gold.linky_hourly (hour, tarif_couleur, periode, tier_num, consommation_kwh, cumul_kwh)
WITH hourly_data AS (
    SELECT
        date_trunc('hour', hour) AS hour,
        tarif_couleur,
        periode,
        tier_num,
        MIN(energie_kwh) AS energie_start,
        MAX(energie_kwh) AS energie_end
    FROM dbt_silver.linky_energy
    WHERE hour >= NOW() - INTERVAL '3 hours'
    GROUP BY 1, 2, 3, 4
)
SELECT
    hour,
    tarif_couleur,
    periode,
    tier_num,
    energie_end - energie_start AS consommation_kwh,
    energie_end AS cumul_kwh
FROM hourly_data;
