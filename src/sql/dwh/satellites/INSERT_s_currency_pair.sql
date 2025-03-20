INSERT INTO 
	STV2025021848__DWH.s_currency_pair(hk_l_currency_pair, currency_code_div, date_update, load_dt, load_src)
SELECT
    dcp.hk_l_currency_pair AS hk_l_currency_pair,
    sc.currency_with_div AS currency_code_div,
    sc.date_update AS date_update,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.currencies sc
LEFT JOIN
    STV2025021848__DWH.h_currencies dc ON dc.currency_code = sc.currency_code
LEFT JOIN
    STV2025021848__DWH.h_currencies dcw ON dcw.currency_code = sc.currency_code_with
LEFT JOIN
    STV2025021848__DWH.l_currency_pair dcp ON dcp.hk_currency_id = dc.hk_currency_id AND dcp.hk_currency_with_id = dcw.hk_currency_id
WHERE
    sc.date_update = '{{ ds }}'
LIMIT
    1 OVER (PARTITION BY hk_l_currency_pair, date_update, currency_code_div ORDER BY sc.id DESC);
