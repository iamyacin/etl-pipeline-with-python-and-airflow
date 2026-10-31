-- Example analytics over the warehouse.
-- Run with: docker compose exec postgres psql -U etl -d warehouse

-- 1. Monthly gross and net revenue (net excludes cancelled and returned orders).
SELECT
    order_month,
    round(sum(line_total), 2) AS gross_revenue,
    round(
        sum(line_total) FILTER (WHERE status NOT IN ('cancelled', 'returned')),
        2
    ) AS net_revenue,
    count(DISTINCT order_id) AS orders
FROM fact_order_items
GROUP BY order_month
ORDER BY order_month;

-- 2. Average order value by month.
WITH order_totals AS (
    SELECT
        order_month,
        order_id,
        sum(line_total) AS order_value
    FROM fact_order_items
    WHERE status NOT IN ('cancelled', 'returned')
    GROUP BY order_month, order_id
)
SELECT
    order_month,
    count(*) AS orders,
    round(avg(order_value), 2) AS average_order_value
FROM order_totals
GROUP BY order_month
ORDER BY order_month;

-- 3. Top product categories by net revenue.
SELECT
    p.category,
    round(sum(f.line_total), 2) AS net_revenue,
    sum(f.quantity) AS units
FROM fact_order_items f
JOIN dim_products p ON p.product_id = f.product_id
WHERE f.status NOT IN ('cancelled', 'returned')
GROUP BY p.category
ORDER BY net_revenue DESC;

-- 4. Customer lifetime value, top 10.
SELECT
    f.customer_id,
    c.country,
    count(DISTINCT f.order_id) AS orders,
    round(sum(f.line_total), 2) AS lifetime_value
FROM fact_order_items f
JOIN dim_customers c ON c.customer_id = f.customer_id
WHERE f.status NOT IN ('cancelled', 'returned')
GROUP BY f.customer_id, c.country
ORDER BY lifetime_value DESC
LIMIT 10;

-- 5. Why rows were rejected, and how often.
SELECT
    reject_reason,
    count(*) AS rejected_rows
FROM rejected_order_items
GROUP BY reject_reason
ORDER BY rejected_rows DESC;

-- 6. Operational history of the pipeline.
SELECT
    run_id,
    source_file,
    status,
    rows_read,
    rows_valid,
    rows_rejected,
    rows_upserted,
    revenue_loaded,
    started_at,
    finished_at
FROM etl_run_log
ORDER BY started_at DESC;
