INSERT INTO
    STV2025021848__DWH.l_transaction_country(hk_l_transaction_country, hk_transaction_id, hk_country_id, load_dt, load_src)
SELECT
    HASH(dt.hk_transaction_id, dc.hk_country_id) AS hk_l_transaction_country,
    dt.hk_transaction_id AS hk_transaction_id,
    dc.hk_country_id AS hk_country_id,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.transactions st
LEFT JOIN
    STV2025021848__DWH.h_transactions dt ON dt.operation_id = st.operation_id
LEFT JOIN
    STV2025021848__DWH.h_countries dc ON dc.country_name = st.country
WHERE
    st.transaction_dt::DATE = '{{ ds }}'
    AND (HASH(dt.hk_transaction_id, dc.hk_country_id) NOT IN (SELECT hk_l_transaction_country FROM STV2025021848__DWH.l_transaction_country))
LIMIT
    1 OVER (PARTITION BY hk_transaction_id, hk_country_id ORDER BY st.id DESC);
