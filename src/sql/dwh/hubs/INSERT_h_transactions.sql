INSERT INTO
    STV2025021848__DWH.h_transactions(hk_transaction_id, operation_id, transaction_type, amount, transaction_dt, load_dt, load_src)
SELECT
    HASH(operation_id) AS hk_operation_id,
    operation_id,
    transaction_type,
    amount,
    transaction_dt,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM
    STV2025021848__STAGING.transactions 
WHERE
    transaction_dt::date = '{{ ds }}' AND (HASH(operation_id) NOT IN (SELECT hk_transaction_id FROM STV2025021848__DWH.h_transactions))
LIMIT
    1 OVER (PARTITION BY operation_id ORDER BY id DESC);
