CREATE TABLE IF NOT EXISTS STV2025021848__DWH.l_account_transaction (
    hk_l_account_transaction INTEGER PRIMARY KEY ENABLED,
    hk_transaction_id INTEGER NOT NULL CONSTRAINT fk_l_account_transaction_transaction REFERENCES STV2025021848__DWH.h_transactions(hk_transaction_id),
    hk_account_from_id INTEGER NOT NULL CONSTRAINT fk_l_account_transaction_account_from REFERENCES STV2025021848__DWH.h_accounts(hk_account_id),
    hk_account_to_id INTEGER NOT NULL CONSTRAINT fk_l_account_transaction_account_to REFERENCES STV2025021848__DWH.h_accounts(hk_account_id),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_l_account_transaction ALL nodes
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.l_currency_pair (
    hk_l_currency_pair INTEGER PRIMARY KEY ENABLED,
    hk_currency_id INTEGER NOT NULL CONSTRAINT fk_l_currency_pair_currency REFERENCES STV2025021848__DWH.h_currencies(hk_currency_id),
    hk_currency_with_id INTEGER NOT NULL CONSTRAINT fk_l_currency_pair_currency_with REFERENCES STV2025021848__DWH.h_currencies(hk_currency_id),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_l_currency_pair ALL nodes
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.l_transaction_currency (
    hk_l_transaction_currency INTEGER PRIMARY KEY ENABLED,
    hk_transaction_id INTEGER NOT NULL CONSTRAINT fk_l_transaction_currency_transaction REFERENCES STV2025021848__DWH.h_transactions(hk_transaction_id),
    hk_currency_id INTEGER NOT NULL CONSTRAINT fk_l_transaction_currency_currency REFERENCES STV2025021848__DWH.h_currencies(hk_currency_id),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_l_transaction_currency ALL nodes
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__DWH.l_transaction_country (
    hk_l_transaction_country INTEGER PRIMARY KEY ENABLED,
    hk_transaction_id INTEGER NOT NULL CONSTRAINT fk_l_transaction_country_transaction REFERENCES STV2025021848__DWH.h_transactions(hk_transaction_id),
    hk_country_id INTEGER NOT NULL CONSTRAINT fk_l_transaction_country_country REFERENCES STV2025021848__DWH.h_countries(hk_country_id),
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(30) NOT NULL
)
ORDER BY load_dt
SEGMENTED BY hk_l_transaction_country ALL nodes
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);
