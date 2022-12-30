CREATE TABLE IF NOT EXISTS datalake.fact_order_accumulating (
    orders_date_id INTEGER,
    invoices_date_id INTEGER,
    payments_date_id INTEGER,
    customer_id INTEGER,
    order_number  VARCHAR,
    invoice_number TEXT,
    payment_number TEXT,
    total_order_quantity BIGINT,
    total_order_usd_amount NUMERIC,
    order_to_invoice_lag_days INTEGER,
    invoice_to_payment_lag_days INTEGER,
    data_updated_at timestamp
);

CREATE INDEX IF NOT EXISTS fact_order_accumulating_customer_id
    ON datalake.fact_order_accumulating USING btree
    (customer_id ASC NULLS LAST)
    TABLESPACE pg_default;