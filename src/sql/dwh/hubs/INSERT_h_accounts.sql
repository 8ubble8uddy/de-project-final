INSERT INTO
    STV2025021848__DWH.h_accounts(hk_account_id, account_number, load_dt, load_src)
SELECT
    HASH(t3.account_number) AS hk_account_id,
    t3.account_number AS account_number,
    '{{ ds }}' AS load_dt,
    's3' AS load_src
FROM (
    (SELECT t1.account_number_from AS account_number
    FROM STV2025021848__STAGING.transactions t1
    WHERE t1.transaction_dt::DATE = '{{ ds }}'
    LIMIT 1 OVER (PARTITION BY t1.account_number_from ORDER BY t1.id DESC))
    UNION
    (SELECT t2.account_number_to
    FROM STV2025021848__STAGING.transactions t2
    WHERE t2.transaction_dt::DATE = '{{ ds }}'
    LIMIT 1 OVER (PARTITION BY t2.account_number_to ORDER BY t2.id DESC))
) AS t3
WHERE
    HASH(t3.account_number) NOT IN (SELECT hk_account_id FROM STV2025021848__DWH.h_accounts);
