CREATE TABLE "BankData".transaction (
    transaction_date DATE NOT NULL,
    transaction_type VARCHAR(50),
    sort_code VARCHAR(8) NOT NULL,
    account_number VARCHAR(10) NOT NULL,
    transaction_description VARCHAR(255),
    debit_amount DECIMAL(10, 2),
    credit_amount DECIMAL(10, 2),
    balance DECIMAL(10, 2) NOT NULL,
	bank_name VARCHAR(10)
);