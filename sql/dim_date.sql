WITH all_day as (
	SELECT 
		to_char(date, 'YYYYMMDD')::INT as id,
		date,
		extract(month from date)::INT as month,
		extract(quarter from date)::INT as quarter_of_year,
		extract(year from date)::INT as year,
		extract(isodow from date)::INT as week,
		to_char(date, 'Day') as days
	FROM orders)	
SELECT 
	id,
	date, 
	month, 
	quarter_of_year, 
	year, 
	CASE WHEN days = 'Saturday' AND days = 'Sunday' THEN True
	ELSE False END as is_weekend
FROM all_day