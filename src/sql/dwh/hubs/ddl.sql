CREATE TABLE IF NOT EXISTS STV2025021848__DWH.h_transactions (
    hk_transaction_id INTEGER PRIMARY KEY ENABLED,
    operation_id UUID NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount INTEGER NOT NULL,
    transaction_dt TIMESTAMP(3) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY transaction_dt
SEGMENTED BY hk_transaction_id ALL nodes
PARTITION BY transaction_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.h_accounts (
    hk_account_id INTEGER PRIMARY KEY ENABLED,
    account_number INTEGER NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_account_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.h_currencies (
    hk_currency_id INTEGER PRIMARY KEY ENABLED,
    currency_code NUMERIC(3) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_currency_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.h_countries (
    hk_country_id INTEGER PRIMARY KEY ENABLED,
    country_name VARCHAR(30) NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_country_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);
