INSERT INTO
    STV2025021848__DWH.l_currency_pair(hk_l_currency_pair, hk_currency_id, hk_currency_with_id, load_dt, load_src)
SELECT
    HASH(dc.hk_currency_id, dcw.hk_currency_id) AS hk_l_currency_pair,
    dc.hk_currency_id AS hk_currency_id,
    dcw.hk_currency_id AS hk_currency_with_id,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.currencies sc
LEFT JOIN
    STV2025021848__DWH.h_currencies dc ON dc.currency_code = sc.currency_code
LEFT JOIN
    STV2025021848__DWH.h_currencies dcw ON dcw.currency_code = sc.currency_code_with
WHERE
    sc.date_update = '{{ ds }}'
    AND (HASH(dc.hk_currency_id, dcw.hk_currency_id) NOT IN (SELECT hk_l_currency_pair FROM STV2025021848__DWH.l_currency_pair))
LIMIT
    1 OVER (PARTITION BY dc.hk_currency_id, dcw.hk_currency_id ORDER BY sc.id DESC);
