INSERT INTO
    STV2025021848__DWH.h_currencies(hk_currency_id, currency_code, load_dt, load_src)
SELECT
    HASH(currency_code) AS hk_currency_id,
    currency_code,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.currencies 
WHERE
    date_update = '{{ ds }}' AND (HASH(currency_code) NOT IN (SELECT hk_currency_id FROM STV2025021848__DWH.h_currencies))
LIMIT
    1 OVER (PARTITION BY currency_code ORDER BY id DESC);
