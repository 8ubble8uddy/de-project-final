CREATE TABLE IF NOT EXISTS STV2025021848__STAGING.currencies (
    id AUTO_INCREMENT PRIMARY KEY ENABLED,
	date_update DATE NOT NULL,
	currency_code NUMERIC(3) NOT NULL,
	currency_code_with NUMERIC(3) NOT NULL,
	currency_with_div NUMERIC(5, 2) NOT NULL,
)
ORDER BY date_update, id
SEGMENTED BY HASH(currency_code, currency_code_with) ALL NODES
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2);


CREATE TABLE IF NOT EXISTS STV2025021848__STAGING.transactions (
    id AUTO_INCREMENT PRIMARY KEY ENABLED,
    operation_id UUID NOT NULL,
    account_number_from INTEGER NOT NULL,
    account_number_to INTEGER NOT NULL,
    currency_code NUMERIC(3) NOT NULL,
    country VARCHAR(30) NOT NULL,
    status VARCHAR(30) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    amount INTEGER NOT NULL,
    transaction_dt TIMESTAMP(3) NOT NULL
)
ORDER BY transaction_dt, id
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);
