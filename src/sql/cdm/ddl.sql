CREATE TABLE IF NOT EXISTS STV2025021848__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from NUMERIC(3) NOT NULL,
    amount_total NUMERIC(14,2) NOT NULL,
    cnt_transactions INTEGER NOT NULL,
    avg_transactions_per_account NUMERIC(14,2) NOT NULL,
    cnt_accounts_make_transactions INTEGER NOT NULL,
    PRIMARY KEY (date_update, currency_from) ENABLED
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update
GROUP BY CALENDAR_HIERARCHY_DAY(date_update, 3, 2);
