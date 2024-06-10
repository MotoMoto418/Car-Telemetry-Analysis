CREATE SCHEMS IF NOT EXISTS "dbt";

USE "dbt";

CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Transactions (
    transaction_id INT PRIMARY KEY,
    customer_id INT,
    payment_method VARCHAR(50),
    transaction_date DATETIME,
    order_total DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE TABLE IF NOT EXISTS Refunds (
    refund_id INT PRIMARY KEY,
    transaction_id INT,
    reason VARCHAR(255),
    amount_refunded DECIMAL(10, 2),
    refund_date DATETIME,
    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id)
);

CREATE TABLE IF NOT EXISTS Items (
    item_id INT PRIMARY KEY,
    transaction_id INT,
    item_name VARCHAR(255),
    quantity INT,
    unit_price DECIMAL(10, 2),
    FOREIGN KEY (transaction_id) REFERENCES Transactions(transaction_id)
);
