CREATE OR REPLACE FUNCTION get_mature_customers_metrics(
    p_business_id integer,
    p_start_date date,
    p_end_date date
)
RETURNS TABLE (
    period_start date,
    period_end date,
    customer_count bigint,
    gmv numeric,
    orders bigint,
    unique_customers bigint,
    aov numeric,
    avg_bill_per_user numeric,
    arpu numeric,
    orders_per_day numeric,
    orders_per_day_per_store numeric,
    purchase_frequency numeric,
    avg_basket_size numeric,
    monthly_spend numeric
) 
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_customers bigint;
BEGIN
    -- Get total customer base for ARPU calculation
    SELECT COUNT(DISTINCT customer_id) INTO v_total_customers
    FROM customers
    WHERE business_id = p_business_id;

    RETURN QUERY
    WITH RECURSIVE months AS (
        SELECT 
            date_trunc('month', p_start_date)::date as month_start,
            (date_trunc('month', p_start_date) + interval '1 month' - interval '1 day')::date as month_end
        UNION ALL
        SELECT 
            (month_start + interval '1 month')::date,
            (month_start + interval '2 months' - interval '1 day')::date
        FROM months
        WHERE month_start < date_trunc('month', p_end_date)::date
    ),
    customer_purchases AS (
        SELECT 
            t.customer_id,
            t.transaction_date,
            t.total_amount,
            COUNT(*) OVER (PARTITION BY t.customer_id) as purchase_count,
            MIN(t.transaction_date) OVER (PARTITION BY t.customer_id) as first_purchase_date,
            MAX(t.transaction_date) OVER (PARTITION BY t.customer_id) as last_purchase_date
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    mature_customers AS (
        SELECT 
            cp.customer_id,
            cp.purchase_count,
            cp.first_purchase_date,
            cp.last_purchase_date,
            cp.last_purchase_date - cp.first_purchase_date as days_between_purchases,
            AVG(cp.total_amount) as avg_purchase_amount,
            SUM(cp.total_amount) as total_spent
        FROM customer_purchases cp
        WHERE cp.purchase_count >= 4
        AND cp.last_purchase_date - cp.first_purchase_date > INTERVAL '90 days'
        AND cp.last_purchase_date - cp.first_purchase_date <= INTERVAL '180 days'
        GROUP BY 
            cp.customer_id, 
            cp.purchase_count,
            cp.first_purchase_date,
            cp.last_purchase_date
    ),
    monthly_metrics AS (
        SELECT 
            m.month_start,
            m.month_end,
            COUNT(DISTINCT mc.customer_id) as customer_count,
            COALESCE(SUM(mc.total_spent), 0)::numeric as monthly_gmv,
            COALESCE(SUM(mc.purchase_count), 0)::bigint as monthly_orders,
            COUNT(DISTINCT mc.customer_id) as monthly_unique_customers,
            COALESCE(COUNT(DISTINCT s.store_id), 0) as store_count,
            GREATEST((EXTRACT(EPOCH FROM (m.month_end::timestamp - m.month_start::timestamp))/86400 + 1), 1) as days_in_period,
            COALESCE(
                SUM(mc.purchase_count)::float / 
                NULLIF(EXTRACT(EPOCH FROM (MAX(mc.last_purchase_date) - MIN(mc.first_purchase_date)))/86400, 0),
                0
            )::numeric as purchase_frequency,
            COALESCE(AVG(mc.avg_purchase_amount), 0)::numeric as avg_basket_size,
            COALESCE(
                SUM(mc.total_spent) / 
                NULLIF(EXTRACT(EPOCH FROM (MAX(mc.last_purchase_date) - MIN(mc.first_purchase_date)))/2592000, 0),
                0
            )::numeric as monthly_spend
        FROM months m
        LEFT JOIN mature_customers mc ON mc.first_purchase_date BETWEEN m.month_start AND m.month_end
        LEFT JOIN transactions t ON t.customer_id = mc.customer_id 
            AND t.transaction_date BETWEEN m.month_start AND m.month_end
        LEFT JOIN stores s ON t.store_id = s.store_id
        GROUP BY m.month_start, m.month_end
    )
    SELECT 
        mm.month_start as period_start,
        mm.month_end as period_end,
        mm.customer_count as customer_count,
        mm.monthly_gmv as gmv,
        mm.monthly_orders as orders,
        mm.monthly_unique_customers as unique_customers,
        CASE 
            WHEN mm.monthly_orders = 0 THEN 0::numeric
            ELSE (mm.monthly_gmv / mm.monthly_orders)::numeric
        END as aov,
        CASE 
            WHEN mm.monthly_unique_customers = 0 THEN 0::numeric
            ELSE (mm.monthly_gmv / mm.monthly_unique_customers)::numeric
        END as avg_bill_per_user,
        CASE 
            WHEN v_total_customers = 0 THEN 0::numeric
            ELSE (mm.monthly_gmv / v_total_customers)::numeric
        END as arpu,
        CASE 
            WHEN mm.days_in_period = 0 THEN 0::numeric
            ELSE (mm.monthly_orders::numeric / mm.days_in_period)::numeric
        END as orders_per_day,
        CASE 
            WHEN mm.days_in_period = 0 OR mm.store_count = 0 THEN 0::numeric
            ELSE (mm.monthly_orders::numeric / (mm.days_in_period * mm.store_count))::numeric
        END as orders_per_day_per_store,
        mm.purchase_frequency,
        mm.avg_basket_size,
        mm.monthly_spend
    FROM monthly_metrics mm
    ORDER BY mm.month_start;
END;
$$;

-- Example of how to use the function:
-- SELECT * FROM get_mature_customers_metrics(1, '2023-01-01', '2023-12-31'); 