-- Run these on output/warehouse.db using a SQLite client.
-- 1. Sales by category (amount displayed in major currency units).
SELECT p.category, SUM(f.quantity) AS units_sold,
       ROUND(SUM(f.net_amount_cents) / 100.0, 2) AS revenue
FROM fact_sales_order_line f
JOIN dim_product p ON p.product_key = f.product_key
GROUP BY p.category ORDER BY revenue DESC;

-- 2. Monthly sales and orders.
SELECT d.year, d.month, COUNT(DISTINCT f.order_id) AS order_count,
       ROUND(SUM(f.net_amount_cents) / 100.0, 2) AS revenue
FROM fact_sales_order_line f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month ORDER BY d.year, d.month;

-- 3. Top 5 customers by net sales.
SELECT c.customer_id, c.customer_name,
       ROUND(SUM(f.net_amount_cents) / 100.0, 2) AS net_spend
FROM fact_sales_order_line f
JOIN dim_customer c ON c.customer_key = f.customer_key
GROUP BY c.customer_id, c.customer_name
ORDER BY net_spend DESC LIMIT 5;
