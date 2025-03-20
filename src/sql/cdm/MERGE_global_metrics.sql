MERGE INTO STV2025021848__DWH.global_metrics gm
USING (
    WITH currencies AS (
        SELECT 
            hc.currency_code,
            hcw.currency_code AS currecncy_code_with,
            scp.currency_code_div,
            scp.date_update
        FROM
            STV2025021848__DWH.h_currencies hc
        LEFT JOIN
            STV2025021848__DWH.l_currency_pair lcp ON lcp.hk_currency_id = hc.hk_currency_id
        LEFT JOIN
            STV2025021848__DWH.h_currencies hcw ON hcw.hk_currency_id = lcp.hk_currency_with_id
        LEFT JOIN
            STV2025021848__DWH.s_currency_pair scp ON scp.hk_l_currency_pair = lcp.hk_l_currency_pair
        WHERE
            scp.date_update = '{{ ds }}' AND hcw.currency_code = 420
    ),
    transactions AS (
        SELECT
            ht.operation_id,
            ha.account_number,
            hc.currency_code,
            ht.amount,
            ht.transaction_dt
        FROM
            STV2025021848__DWH.h_transactions ht
        LEFT JOIN 
            STV2025021848__DWH.l_account_transaction lat ON lat.hk_transaction_id = ht.hk_transaction_id
        LEFT JOIN
            STV2025021848__DWH.h_accounts ha ON ha.hk_account_id = lat.hk_account_from_id
        LEFT JOIN
            STV2025021848__DWH.l_transaction_currency ltc ON ltc.hk_transaction_id = ht.hk_transaction_id
        LEFT JOIN
            STV2025021848__DWH.h_currencies hc ON hc.hk_currency_id = ltc.hk_currency_id
        LEFT JOIN
            STV2025021848__DWH.s_transaction_status sts ON sts.hk_transaction_id = ht.hk_transaction_id
        WHERE
            (ht.transaction_dt::DATE = '{{ ds }}' AND sts.status = 'done')
            AND (ht.transaction_type IN ('c2b_partner_incoming','sbp_incoming','transfer_outgoing','sbp_outgoing','transfer_incoming','c2a_incoming'))
    )
    SELECT 
        c.date_update,
        t.currency_code AS currency_from,
        SUM(t.amount * c.currency_code_div) AS amount_total,
        COUNT(t.operation_id) as cnt_transactions,
        COUNT(t.operation_id) / COUNT(DISTINCT t.account_number) AS avg_transactions_per_account,
        COUNT(DISTINCT t.account_number) as cnt_accounts_make_transactions
    FROM
        transactions t
    INNER JOIN 
        currencies c ON c.date_update = t.transaction_dt::DATE AND c.currency_code = t.currency_code
    GROUP BY
        c.date_update, t.currency_code
    )  AS cte
ON cte.date_update = gm.date_update AND cte.currency_from = gm.currency_from
WHEN MATCHED THEN UPDATE SET 
    amount_total = cte.amount_total, 
    cnt_transactions = cte.cnt_transactions, 
    avg_transactions_per_account = cte.avg_transactions_per_account
WHEN NOT MATCHED THEN INSERT (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
    VALUES (
        cte.date_update,
        cte.currency_from,
        cte.amount_total,
        cte.cnt_transactions,
        cte.avg_transactions_per_account,
        cte.cnt_accounts_make_transactions
    )
