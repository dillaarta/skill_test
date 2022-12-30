-- 1
-- Aim to know which product sold the most each month
SELECT 
    product_name,
    month,
	year,
    total_order_quantity,
    total_order_usd_amount
FROM (
    SELECT 
        pd.name as product_name
        , dd.month
		, dd.year	
        , sum(fao.total_order_quantity) as total_order_quantity
		, sum(fao.total_order_usd_amount) as total_order_usd_amount
        , rank()OVER(PARTITION BY dd.month ORDER BY sum(fao.total_order_usd_amount) DESC) as ranks
    FROM public.fact_order_accumulating fao
	INNER JOIN public.order_lines ol
    ON fao.order_number = ol.order_number
    LEFT JOIN public.products pd
    ON ol.product_id = pd.id 
	LEFT JOIN public.dim_date dd
    ON fao.orders_date_id = dd.id 
    GROUP BY pd.name
        , dd.month, dd.year) a
WHERE ranks = 1 ;

-- 2
-- Aim to know customer who spent the most each quarter
SELECT 
    customer_name,
    quarter_of_year,
    total_order_quantity,
    total_order_usd_amount
FROM (
    SELECT 
        cs.name as customer_name
        , dd.quarter_of_year
        , sum(fao.total_order_quantity) as total_order_quantity
		, sum(fao.total_order_usd_amount) as total_order_usd_amount
        , rank()OVER(PARTITION BY dd.quarter_of_year ORDER BY sum(fao.total_order_usd_amount) DESC) as ranks
    FROM public.fact_order_accumulating fao
    LEFT JOIN public.dim_customer cs
    ON fao.customer_id = cs.id 
	LEFT JOIN public.dim_date dd
	ON dd.id = fao.orders_date_id
    GROUP BY cs.name
        , dd.quarter_of_year) a
WHERE ranks = 1;

-- 3 
-- Aim to get total lag days, both order_to_invoices and invoices_to_payment each month
SELECT 
	dd.month,
	dd.year,
	dd.quarter_of_year,
	sum(fao.order_to_invoice_lag_days) total_lag_days_order_to_invoices,
	sum(fao.invoice_to_payment_lag_days) total_lag_days_invoices_to_payment
FROM public.fact_order_accumulating fao
LEFT JOIN public.dim_date dd
ON dd.id = fao.orders_date_id
GROUP BY dd.month,
	dd.year,
	dd.quarter_of_year
ORDER BY dd.month, dd.year;
	
