-- name: revenue_by_category
-- Integer cents preserve exact financial totals. Empty categories are not shown.
SELECT p.category,
       SUM(f.quantity) AS units_sold,
       SUM(f.gross_amount_cents) AS gross_cents,
       SUM(f.discount_cents) AS discount_cents,
       SUM(f.net_amount_cents) AS net_revenue_cents
FROM fact_sales_order_line AS f
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY net_revenue_cents DESC, p.category;

-- name: monthly_revenue_and_completed_orders
SELECT d.year, d.month,
       COUNT(DISTINCT f.order_id) AS completed_orders,
       SUM(f.net_amount_cents) AS net_revenue_cents
FROM fact_sales_order_line AS f
JOIN dim_date AS d ON d.date_key = f.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- name: top_10_customers_by_revenue
SELECT c.customer_id, c.customer_name, c.city,
       COUNT(DISTINCT f.order_id) AS completed_orders,
       SUM(f.net_amount_cents) AS net_revenue_cents
FROM fact_sales_order_line AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.customer_id, c.customer_name, c.city
ORDER BY net_revenue_cents DESC, c.customer_id
LIMIT 10;

-- name: top_10_products_by_quantity_and_revenue
SELECT p.product_id, p.product_name, p.category,
       SUM(f.quantity) AS units_sold,
       SUM(f.net_amount_cents) AS net_revenue_cents
FROM fact_sales_order_line AS f
JOIN dim_product AS p ON p.product_key = f.product_key
GROUP BY p.product_key, p.product_id, p.product_name, p.category
ORDER BY units_sold DESC, net_revenue_cents DESC, p.product_id
LIMIT 10;

-- name: daily_sales_trend
SELECT d.full_date AS sale_date,
       COUNT(DISTINCT f.order_id) AS completed_orders,
       SUM(f.quantity) AS units_sold,
       SUM(f.net_amount_cents) AS net_revenue_cents
FROM fact_sales_order_line AS f
JOIN dim_date AS d ON d.date_key = f.date_key
GROUP BY d.date_key, d.full_date
ORDER BY d.date_key;
