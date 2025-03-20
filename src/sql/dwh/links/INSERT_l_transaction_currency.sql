INSERT INTO
    STV2025021848__DWH.l_transaction_currency(hk_l_transaction_currency, hk_transaction_id, hk_currency_id, load_dt, load_src)
SELECT
    HASH(dt.hk_transaction_id, dc.hk_currency_id) AS hk_l_transaction_currency,
    dt.hk_transaction_id AS hk_transaction_id,
    dc.hk_currency_id AS hk_currency_id,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.transactions st
LEFT JOIN
    STV2025021848__DWH.h_transactions dt ON dt.operation_id = st.operation_id
LEFT JOIN
    STV2025021848__DWH.h_currencies dc ON dc.currency_code = st.currency_code
WHERE
    st.transaction_dt::DATE = '{{ ds }}'
    AND (HASH(dt.hk_transaction_id, dc.hk_currency_id) NOT IN (SELECT hk_l_transaction_currency FROM STV2025021848__DWH.l_transaction_currency))
LIMIT
    1 OVER (PARTITION BY hk_transaction_id, hk_currency_id ORDER BY st.id DESC);
