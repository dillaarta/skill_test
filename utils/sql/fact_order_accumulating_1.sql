SELECT 
	to_char(orders.date, 'YYYYMMDD')::INT as orders_date_id,
	to_char(invoices.date, 'YYYYMMDD')::INT as invoices_date_id,
	to_char(payments.date, 'YYYYMMDD')::INT as payments_date_id,
	orders.customer_id,
	orders.order_number as order_number,
	invoices.invoice_number as invoice_number,
	payments.payment_number,
	sum(ol.quantity) as total_order_quantity,
	sum(ol.usd_amount*ol.quantity) as total_order_usd_amount,
	(invoices.date-orders.date) as order_to_invoice_lag_days,
	(payments.date-invoices.date) as invoice_to_payment_lag_days
FROM 
	orders orders
LEFT JOIN order_lines ol
ON orders.order_number = ol.order_number
LEFT JOIN invoices invoices
ON orders.order_number = invoices.order_number
LEFT JOIN payments payments
ON invoices.invoice_number = payments.invoice_number
GROUP BY 
	orders.order_number,
	orders.customer_id,
	orders.date,
	invoices.date,
	payments.date,
	invoices.invoice_number,
	ol.order_number,
	payments.payment_number