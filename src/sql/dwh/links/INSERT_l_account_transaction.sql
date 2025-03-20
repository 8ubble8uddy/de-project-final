INSERT INTO
    STV2025021848__DWH.l_account_transaction(hk_l_account_transaction, hk_transaction_id, hk_account_from_id, hk_account_to_id, load_dt, load_src)
SELECT
    HASH(daf.hk_account_id, dat.hk_account_id, dt.hk_transaction_id) AS hk_l_account_transaction,
    dt.hk_transaction_id AS hk_transaction_id,
    daf.hk_account_id AS hk_account_from_id,
    dat.hk_account_id AS hk_account_to_id,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.transactions st
LEFT JOIN
    STV2025021848__DWH.h_accounts daf ON daf.account_number = st.account_number_from
LEFT JOIN
    STV2025021848__DWH.h_accounts dat ON dat.account_number = st.account_number_to
LEFT JOIN
    STV2025021848__DWH.h_transactions dt ON dt.operation_id = st.operation_id
WHERE
    st.transaction_dt::DATE = '{{ ds }}'
    AND (HASH(daf.hk_account_id, dat.hk_account_id, dt.hk_transaction_id) NOT IN (SELECT hk_l_account_transaction FROM STV2025021848__DWH.l_account_transaction))
LIMIT
    1 OVER (PARTITION BY dt.hk_transaction_id, daf.hk_account_id, dat.hk_account_id ORDER BY st.id DESC);
