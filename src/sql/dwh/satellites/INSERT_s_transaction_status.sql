INSERT INTO
    STV2025021848__DWH.s_transaction_status(hk_transaction_id, status, status_dt, load_dt, load_src)
SELECT
    dt.hk_transaction_id AS hk_transaction_id,
    st.status AS status,
    dt.transaction_dt AS status_dt,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.transactions st
LEFT JOIN
    STV2025021848__DWH.h_transactions dt ON dt.operation_id = st.operation_id
WHERE
    st.transaction_dt::DATE = '{{ ds }}'
LIMIT
    1 OVER (PARTITION BY hk_transaction_id, status_dt, status ORDER BY st.id DESC);
