INSERT INTO
    STV2025021848__DWH.h_countries(hk_country_id, country_name, load_dt, load_src)
SELECT
    HASH(country) AS hk_country_id,
    country AS country_name,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.transactions 
WHERE
    transaction_dt::DATE = '{{ ds }}' AND (HASH(country) NOT IN (SELECT hk_country_id FROM STV2025021848__DWH.h_countries))
LIMIT
    1 OVER (PARTITION BY country ORDER BY id DESC);
