CREATE TABLE IF NOT EXISTS STV2025021848__DWH.s_transaction_status (
    hk_transaction_id INTEGER NOT NULL CONSTRAINT fk_s_transaction_status_transaction REFERENCES STV2025021848__DWH.h_transactions(hk_transaction_id),
    status VARCHAR(30) NOT NULL,
    status_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY status_dt
SEGMENTED BY hk_transaction_id ALL nodes
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.s_currency_pair (
    hk_l_currency_pair INTEGER NOT NULL CONSTRAINT fk_s_currency_pair_currency REFERENCES STV2025021848__DWH.l_currency_pair(hk_l_currency_pair),
    currency_code_div NUMERIC(5, 2) NOT NULL,
    date_update DATE NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY date_update
SEGMENTED BY hk_l_currency_pair ALL nodes
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);
