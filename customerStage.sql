---- new customer--------
CREATE OR REPLACE FUNCTION get_new_customers_metrics(
    p_business_id integer,
    p_start_date DATE,
    p_end_date DATE
)
RETURNS TABLE (
    period_start DATE,
    period_end DATE,
    customer_count BIGINT,           -- New customers who made their first (and only) purchase in last 30 days of period
    gmv NUMERIC,                     -- Total value of all transactions by these new customers
    orders BIGINT,                   -- Total count of transactions by these new customers
    unique_customers BIGINT,         -- Distinct new customers who transacted in the period
    aov NUMERIC,                     -- GMV / Orders
    avg_bill_per_user NUMERIC,       -- GMV / Unique Customers
    arpu NUMERIC,                    -- GMV / Total Customer Base
    orders_per_day NUMERIC,          -- Orders / Days in period
    orders_per_day_per_store NUMERIC, -- Orders / Days / Store count
    first_purchase_gmv NUMERIC,      -- Value of only first purchases
    avg_first_purchase_value NUMERIC, -- Average value of first purchases
    conversion_to_second_purchase_rate NUMERIC -- % of new customers who made a second purchase within 30 days
) 
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_customers BIGINT;
    v_days_between INTEGER;
    v_need_breakdown BOOLEAN;
BEGIN
    -- Calculate time range in days
    v_days_between := (p_end_date - p_start_date);
    
    -- Determine if we need monthly breakdown - break down if period is > 31 days
    v_need_breakdown := v_days_between > 31;

    -- Get total customer base for ARPU calculation
    SELECT COUNT(DISTINCT customer_id) INTO v_total_customers
    FROM customers
    WHERE business_id = p_business_id;

    RETURN QUERY
    WITH monthly_dates AS (
        -- Generate all month start dates in range
        SELECT generate_series(
            DATE_TRUNC('month', p_start_date)::DATE,
            DATE_TRUNC('month', p_end_date)::DATE,
            INTERVAL '1 month'
        )::DATE as month_start
    ),
    date_ranges AS (
        -- Create date ranges based on whether we need breakdown
        SELECT 
            CASE 
                WHEN v_need_breakdown THEN 
                    -- For first month, use actual start date
                    CASE WHEN md.month_start = DATE_TRUNC('month', p_start_date)::DATE 
                        THEN p_start_date
                        ELSE md.month_start
                    END
                ELSE p_start_date
            END as period_start,
            CASE 
                WHEN v_need_breakdown THEN 
                    -- For last month, use actual end date
                    CASE WHEN md.month_start = DATE_TRUNC('month', p_end_date)::DATE
                        THEN p_end_date
                        ELSE LEAST((md.month_start + INTERVAL '1 month' - INTERVAL '1 day')::DATE, p_end_date)
                    END
                ELSE p_end_date
            END as period_end
        FROM monthly_dates md
        WHERE 
            -- For non-breakdown case, only include the first row to avoid duplicates
            NOT v_need_breakdown AND md.month_start = (SELECT MIN(month_start) FROM monthly_dates)
            OR 
            -- For breakdown case, include all months
            (v_need_breakdown AND md.month_start <= DATE_TRUNC('month', p_end_date)::DATE)
    ),
    -- First identify the first transaction for each customer across all history
    first_transactions AS (
        SELECT 
            customer_id,
            MIN(transaction_date) AS first_transaction_date,
            (SELECT t2.total_amount 
             FROM transactions t2 
             WHERE t2.customer_id = t1.customer_id 
             AND t2.transaction_date = MIN(t1.transaction_date)
             AND t2.business_id = p_business_id
             LIMIT 1) AS first_purchase_amount
        FROM transactions t1
        WHERE business_id = p_business_id
        GROUP BY customer_id
    ),
    -- Then identify new customers for each period with modified conditions
    new_customers AS (
        SELECT 
            dr.period_start,
            dr.period_end,
            ft.customer_id,
            ft.first_transaction_date AS transaction_date,
            ft.first_purchase_amount AS total_amount
        FROM date_ranges dr
        LEFT JOIN first_transactions ft ON 
            ft.first_transaction_date::DATE BETWEEN dr.period_start AND dr.period_end AND
            ft.first_transaction_date::DATE BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE AND
            (SELECT COUNT(*) FROM transactions t 
             WHERE t.customer_id = ft.customer_id 
             AND t.business_id = p_business_id) = 1
    ),
    -- Calculate period metrics
    period_metrics AS (
        SELECT 
            nc.period_start,
            nc.period_end,
            COUNT(DISTINCT CASE WHEN nc.customer_id IS NOT NULL THEN nc.customer_id END) as new_customer_count,
            COALESCE(SUM(nc.total_amount), 0)::NUMERIC as first_purchase_gmv,
            COALESCE(SUM(t.total_amount), 0)::NUMERIC as period_gmv,
            COUNT(DISTINCT t.transaction_id) as period_orders,
            COUNT(DISTINCT t.customer_id) as period_unique_customers,
            COALESCE(COUNT(DISTINCT s.store_id), 0) as store_count,
            EXTRACT(EPOCH FROM (nc.period_end::TIMESTAMP - nc.period_start::TIMESTAMP + INTERVAL '1 day')) / 86400 as days_in_period
        FROM date_ranges dr
        LEFT JOIN new_customers nc ON nc.period_start = dr.period_start AND nc.period_end = dr.period_end
        LEFT JOIN transactions t ON t.customer_id = nc.customer_id
            AND t.transaction_date::DATE BETWEEN nc.period_start AND nc.period_end
            AND t.business_id = p_business_id
        LEFT JOIN stores s ON t.store_id = s.store_id AND s.business_id = p_business_id
        GROUP BY nc.period_start, nc.period_end
    ),
    -- Calculate second purchase conversion (within 30 days of first purchase)
    second_purchase_metrics AS (
        SELECT 
            nc.period_start,
            nc.period_end,
            COUNT(DISTINCT CASE WHEN EXISTS (
                SELECT 1 
                FROM transactions t2 
                WHERE t2.customer_id = nc.customer_id 
                AND t2.transaction_date > nc.transaction_date
                AND t2.transaction_date <= (nc.transaction_date + INTERVAL '30 days')
                AND t2.business_id = p_business_id
            ) THEN nc.customer_id END) as second_purchase_count,
            COUNT(DISTINCT nc.customer_id) as total_new_customers
        FROM new_customers nc
        GROUP BY nc.period_start, nc.period_end
    ),
    -- This CTE ensures we have complete records for all date ranges, even with no data
    complete_periods AS (
        SELECT 
            dr.period_start,
            dr.period_end,
            COALESCE(pm.new_customer_count, 0) as customer_count,
            COALESCE(pm.period_gmv, 0) as gmv,
            COALESCE(pm.period_orders, 0) as orders,
            COALESCE(pm.period_unique_customers, 0) as unique_customers,
            COALESCE(pm.store_count, 0) as store_count,
            COALESCE(pm.days_in_period, 
                EXTRACT(EPOCH FROM (dr.period_end::TIMESTAMP - dr.period_start::TIMESTAMP + INTERVAL '1 day')) / 86400
            ) as days_in_period,
            COALESCE(pm.first_purchase_gmv, 0) as first_purchase_gmv,
            COALESCE(spm.second_purchase_count, 0) as second_purchase_count,
            COALESCE(spm.total_new_customers, 0) as total_new_customers
        FROM date_ranges dr
        LEFT JOIN period_metrics pm ON pm.period_start = dr.period_start AND pm.period_end = dr.period_end
        LEFT JOIN second_purchase_metrics spm ON spm.period_start = dr.period_start AND spm.period_end = dr.period_end
    )
    SELECT 
        cp.period_start,
        cp.period_end,
        cp.customer_count,
        cp.gmv,
        cp.orders,
        cp.unique_customers,
        CASE 
            WHEN cp.orders = 0 THEN 0::NUMERIC
            ELSE (cp.gmv / cp.orders)::NUMERIC
        END as aov,
        CASE 
            WHEN cp.unique_customers = 0 THEN 0::NUMERIC
            ELSE (cp.gmv / cp.unique_customers)::NUMERIC
        END as avg_bill_per_user,
        CASE 
            WHEN v_total_customers = 0 THEN 0::NUMERIC
            ELSE (cp.gmv / v_total_customers)::NUMERIC
        END as arpu,
        CASE 
            WHEN cp.days_in_period = 0 THEN 0::NUMERIC
            ELSE (cp.orders::NUMERIC / cp.days_in_period)::NUMERIC
        END as orders_per_day,
        CASE 
            WHEN cp.days_in_period = 0 OR cp.store_count = 0 THEN 0::NUMERIC
            ELSE (cp.orders::NUMERIC / (cp.days_in_period * cp.store_count))::NUMERIC
        END as orders_per_day_per_store,
        cp.first_purchase_gmv,
        CASE 
            WHEN cp.customer_count = 0 THEN 0::NUMERIC
            ELSE (cp.first_purchase_gmv / cp.customer_count)::NUMERIC
        END as avg_first_purchase_value,
        CASE 
            WHEN cp.total_new_customers = 0 THEN 0::NUMERIC
            ELSE (cp.second_purchase_count::NUMERIC / cp.total_new_customers)::NUMERIC
        END as conversion_to_second_purchase_rate
    FROM complete_periods cp
    ORDER BY cp.period_start;
END;
$$;


-------early life customer-----------


CREATE OR REPLACE FUNCTION get_early_life_customers_metrics(
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
    repeat_purchase_rate numeric,
    avg_time_between_purchases numeric,
    avg_order_value numeric
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
    WITH RECURSIVE periods AS (
        SELECT 
            CASE 
                WHEN p_end_date - p_start_date <= 31 THEN p_start_date
                ELSE p_start_date
            END AS p_start,
            CASE 
                WHEN p_end_date - p_start_date <= 31 THEN p_end_date
                ELSE LEAST((date_trunc('month', p_start_date) + interval '1 month - 1 day')::date, p_end_date)
            END AS p_end
        UNION ALL
        SELECT
            GREATEST(date_trunc('month', p_start + interval '1 month')::date, p_start_date),
            LEAST((date_trunc('month', p_start + interval '1 month') + interval '1 month - 1 day')::date, p_end_date)
        FROM periods
        WHERE p_end < p_end_date AND p_end_date - p_start_date > 31
    ),
    transaction_history AS (
        -- Get all transactions in order
        SELECT 
            t.customer_id,
            t.transaction_id,
            t.transaction_date,
            t.total_amount,
            ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.transaction_date) as transaction_num
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    qualified_early_life_customers AS (
        -- Find the transaction where customer becomes an early life customer (transaction #2 or #3)
        SELECT 
            th.customer_id,
            MIN(th.transaction_date) as first_purchase_date,
            MAX(th.transaction_date) as qualification_date, -- Date when they become early life customer
            COUNT(*) as purchase_count,
            AVG(th.total_amount) as avg_purchase_amount,
            SUM(th.total_amount) as total_spent
        FROM transaction_history th
        GROUP BY th.customer_id
        HAVING 
            COUNT(*) BETWEEN 2 AND 3
            AND MAX(th.transaction_date) - MIN(th.transaction_date) <= INTERVAL '90 days'
            AND MAX(th.transaction_date) BETWEEN p_start_date AND p_end_date
    ),
    early_life_customers AS (
        -- Add period information based on qualification date, not first purchase date
        SELECT 
            qec.customer_id,
            qec.purchase_count,
            qec.first_purchase_date,
            qec.qualification_date as last_purchase_date,
            qec.qualification_date - qec.first_purchase_date as days_between_purchases,
            qec.avg_purchase_amount,
            qec.total_spent,
            p.p_start as period_start,
            p.p_end as period_end
        FROM qualified_early_life_customers qec
        JOIN periods p ON qec.qualification_date BETWEEN p.p_start AND p.p_end
    ),
    monthly_metrics AS (
        SELECT 
            p.p_start as period_start,
            p.p_end as period_end,
            COUNT(DISTINCT elc.customer_id) AS customer_count,
            COALESCE(SUM(elc.total_spent), 0)::numeric AS monthly_gmv,
            COALESCE(SUM(elc.purchase_count), 0)::bigint AS monthly_orders,
            COUNT(DISTINCT elc.customer_id) as monthly_unique_customers,
            COALESCE(COUNT(DISTINCT s.store_id), 0) AS store_count,
            GREATEST((EXTRACT(EPOCH FROM (p.p_end::timestamp - p.p_start::timestamp))/86400 + 1), 1) AS days_in_period,
            COALESCE(
                COUNT(DISTINCT CASE WHEN elc.purchase_count > 1 THEN elc.customer_id END)::float / 
                NULLIF(COUNT(DISTINCT elc.customer_id), 0),
                0
            )::numeric AS repeat_purchase_rate,
            COALESCE(
                AVG(EXTRACT(EPOCH FROM elc.days_between_purchases)/86400),
                0
            )::numeric AS avg_time_between_purchases,
            COALESCE(AVG(elc.avg_purchase_amount), 0)::numeric AS avg_order_value
        FROM periods p
        LEFT JOIN early_life_customers elc ON elc.period_start = p.p_start AND elc.period_end = p.p_end
        LEFT JOIN transactions t ON t.customer_id = elc.customer_id 
            AND t.transaction_date BETWEEN p.p_start AND p.p_end
        LEFT JOIN stores s ON t.store_id = s.store_id
        GROUP BY p.p_start, p.p_end
    )
    SELECT 
        mm.period_start,
        mm.period_end,
        mm.customer_count,
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
        mm.repeat_purchase_rate,
        mm.avg_time_between_purchases,
        mm.avg_order_value
    FROM monthly_metrics mm
    ORDER BY mm.period_start;
END;
$$;

--------mature customer---------

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
    WITH RECURSIVE periods AS (
        SELECT 
            CASE 
                WHEN p_end_date - p_start_date <= 31 THEN p_start_date
                ELSE p_start_date
            END AS p_start,
            CASE 
                WHEN p_end_date - p_start_date <= 31 THEN p_end_date
                ELSE LEAST((date_trunc('month', p_start_date) + interval '1 month - 1 day')::date, p_end_date)
            END AS p_end
        UNION ALL
        SELECT
            GREATEST(date_trunc('month', p_start + interval '1 month')::date, p_start_date),
            LEAST((date_trunc('month', p_start + interval '1 month') + interval '1 month - 1 day')::date, p_end_date)
        FROM periods
        WHERE p_end < p_end_date AND p_end_date - p_start_date > 31
    ),
    transaction_history AS (
        -- Get all transactions in order
        SELECT 
            t.customer_id,
            t.transaction_date,
            t.total_amount,
            ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.transaction_date) as transaction_num
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    customer_purchase_history AS (
        -- Aggregate purchase history for each customer
        SELECT 
            th.customer_id,
            COUNT(*) as purchase_count,
            MIN(th.transaction_date) as first_purchase_date,
            MAX(th.transaction_date) as last_purchase_date,
            MAX(th.transaction_date) - MIN(th.transaction_date) as tenure,
            AVG(th.total_amount) as avg_purchase_amount,
            SUM(th.total_amount) as total_spent
        FROM transaction_history th
        GROUP BY th.customer_id
    ),
    qualified_mature_customers AS (
        -- Identify customers who meet the "mature" criteria - qualification happens at their last purchase
        SELECT 
            cph.customer_id,
            cph.purchase_count,
            cph.first_purchase_date,
            cph.last_purchase_date,
            cph.tenure as days_between_purchases,
            cph.avg_purchase_amount,
            cph.total_spent,
            -- Calculate metrics specific to mature customers
            (cph.total_spent / 
             NULLIF(EXTRACT(EPOCH FROM cph.tenure)/2592000, 0)) as monthly_spend,
            (cph.purchase_count::float / 
             NULLIF(EXTRACT(EPOCH FROM cph.tenure)/86400, 0)) as purchase_frequency
        FROM customer_purchase_history cph
        WHERE cph.purchase_count >= 4
        AND cph.tenure > INTERVAL '90 days'
        AND cph.tenure <= INTERVAL '180 days'
        AND cph.last_purchase_date BETWEEN p_start_date AND p_end_date
    ),
    mature_customers_by_period AS (
        -- Assign mature customers to periods based on qualification date (last purchase date)
        SELECT 
            qmc.*,
            p.p_start as period_start,
            p.p_end as period_end
        FROM qualified_mature_customers qmc
        JOIN periods p ON qmc.last_purchase_date BETWEEN p.p_start AND p.p_end
    ),
    monthly_metrics AS (
        SELECT 
            p.p_start as period_start,
            p.p_end as period_end,
            COUNT(DISTINCT mc.customer_id) as customer_count,
            COALESCE(SUM(mc.total_spent), 0)::numeric as monthly_gmv,
            COALESCE(SUM(mc.purchase_count), 0)::bigint as monthly_orders,
            COUNT(DISTINCT mc.customer_id) as monthly_unique_customers,
            COALESCE(COUNT(DISTINCT s.store_id), 0) as store_count,
            GREATEST((EXTRACT(EPOCH FROM (p.p_end::timestamp - p.p_start::timestamp))/86400 + 1), 1) as days_in_period,
            COALESCE(AVG(mc.purchase_frequency), 0)::numeric as purchase_frequency,
            COALESCE(AVG(mc.avg_purchase_amount), 0)::numeric as avg_basket_size,
            COALESCE(AVG(mc.monthly_spend), 0)::numeric as monthly_spend
        FROM periods p
        LEFT JOIN mature_customers_by_period mc ON mc.period_start = p.p_start AND mc.period_end = p.p_end
        LEFT JOIN transactions t ON t.customer_id = mc.customer_id 
            AND t.transaction_date BETWEEN p.p_start AND p.p_end
        LEFT JOIN stores s ON t.store_id = s.store_id
        GROUP BY p.p_start, p.p_end
    )
    SELECT 
        mm.period_start,
        mm.period_end,
        mm.customer_count,
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
    ORDER BY mm.period_start;
END;
$$;

------loyal customer---------

CREATE OR REPLACE FUNCTION get_loyal_customers_metrics(
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
    annual_customer_value numeric,
    purchase_frequency numeric,
    category_penetration numeric
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
    WITH RECURSIVE periods AS (
        SELECT 
            CASE 
                WHEN p_end_date - p_start_date <= 31 THEN p_start_date
                ELSE p_start_date
            END AS p_start,
            CASE 
                WHEN p_end_date - p_start_date <= 31 THEN p_end_date
                ELSE LEAST((date_trunc('month', p_start_date) + interval '1 month - 1 day')::date, p_end_date)
            END AS p_end
        UNION ALL
        SELECT
            GREATEST(date_trunc('month', p_start + interval '1 month')::date, p_start_date),
            LEAST((date_trunc('month', p_start + interval '1 month') + interval '1 month - 1 day')::date, p_end_date)
        FROM periods
        WHERE p_end < p_end_date AND p_end_date - p_start_date > 31
    ),
    transaction_history AS (
        -- Get all transactions in order
        SELECT 
            t.customer_id,
            t.transaction_id,
            t.transaction_date,
            t.total_amount,
            t.product_line_id,
            ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.transaction_date) as transaction_num
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    customer_purchase_history AS (
        -- Aggregate purchase history for each customer
        SELECT 
            th.customer_id,
            COUNT(DISTINCT th.transaction_id) as purchase_count,
            MIN(th.transaction_date) as first_purchase_date,
            MAX(th.transaction_date) as last_purchase_date,
            MAX(th.transaction_date) - MIN(th.transaction_date) as tenure,
            SUM(th.total_amount) as total_spend,
            -- Count unique categories per customer
            COUNT(DISTINCT pl.category) as unique_categories
        FROM transaction_history th
        JOIN product_lines pl ON th.product_line_id = pl.product_line_id
        GROUP BY th.customer_id
    ),
    qualified_loyal_customers AS (
        -- Identify customers who meet loyal criteria at their last transaction
        SELECT 
            cph.customer_id,
            cph.purchase_count,
            cph.first_purchase_date,
            cph.last_purchase_date,
            cph.tenure as days_between_purchases,
            cph.total_spend,
            cph.unique_categories,
            -- Calculate derived metrics
            (cph.total_spend * 365.0 / NULLIF(EXTRACT(EPOCH FROM cph.tenure)/86400, 0)) as annual_customer_value,
            (cph.purchase_count::float / NULLIF(EXTRACT(EPOCH FROM cph.tenure)/86400, 0)) as purchase_frequency
        FROM customer_purchase_history cph
        WHERE cph.purchase_count >= 4
        AND cph.tenure > INTERVAL '180 days'
        AND cph.last_purchase_date >= (CURRENT_DATE - INTERVAL '60 days')
        AND cph.last_purchase_date BETWEEN p_start_date AND p_end_date
    ),
    loyal_customers_by_period AS (
        -- Assign loyal customers to periods based on qualification date (last purchase)
        SELECT 
            qlc.*,
            p.period_start,
            p.period_end
        FROM qualified_loyal_customers qlc
        JOIN periods p ON qlc.last_purchase_date BETWEEN p.period_start AND p.period_end
    ),
    total_categories AS (
        SELECT COUNT(DISTINCT category) as total
        FROM product_lines
        WHERE business_id = p_business_id
    ),
    customer_details AS (
        SELECT 
            c.customer_id::uuid,
            c.first_name::text,
            c.last_name::text,
            c.email::text,
            c.phone::text,
            c.gender::text,
            c.birth_date::date,
            c.registration_date::timestamp,
            c.address::text,
            c.city::text,
            lc.purchase_count::bigint,
            lc.first_purchase_date::timestamp,
            lc.last_purchase_date::timestamp,
            EXTRACT(EPOCH FROM lc.days_between_purchases)/86400::numeric as days_between_purchases,
            lc.total_spend::numeric,
            lc.annual_customer_value::numeric,
            lc.purchase_frequency::numeric,
            lc.unique_categories::bigint,
            (lc.unique_categories::FLOAT / NULLIF(tc.total, 0))::numeric as category_penetration,
            COALESCE(STRING_AGG(DISTINCT pl.category, ', '), '')::text as purchase_categories,
            COUNT(DISTINCT pl.brand)::bigint as brands_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.brand, ', '), '')::text as brand_names,
            COUNT(DISTINCT s.store_id)::bigint as stores_visited,
            COALESCE(STRING_AGG(DISTINCT s.store_name, ', '), '')::text as store_names,
            COALESCE(STRING_AGG(DISTINCT t.payment_method, ', '), '')::text as payment_methods,
            EXTRACT(EPOCH FROM (CURRENT_DATE - lc.first_purchase_date))/86400::numeric as days_since_first_purchase,
            EXTRACT(EPOCH FROM (CURRENT_DATE - lc.last_purchase_date))/86400::numeric as days_since_last_purchase,
            EXTRACT(EPOCH FROM (lc.last_purchase_date - lc.first_purchase_date))/86400::numeric as customer_tenure,
            EXTRACT(EPOCH FROM (CURRENT_DATE - lc.last_purchase_date))/86400::numeric as last_purchase_days_ago
        FROM loyal_customers_by_period lc
        JOIN customers c ON lc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
            AND t.transaction_date BETWEEN lc.period_start AND lc.period_end
        LEFT JOIN product_lines pl ON t.product_line_id = pl.product_line_id
        LEFT JOIN stores s ON t.store_id = s.store_id
        CROSS JOIN total_categories tc
        WHERE c.business_id = p_business_id
        GROUP BY 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.gender,
            c.birth_date,
            c.registration_date,
            c.address,
            c.city,
            lc.purchase_count,
            lc.first_purchase_date,
            lc.last_purchase_date,
            lc.days_between_purchases,
            lc.total_spend,
            lc.annual_customer_value,
            lc.purchase_frequency,
            lc.unique_categories,
            tc.total
    )
    SELECT * FROM customer_details
    ORDER BY total_spend DESC;
END;
$$ LANGUAGE plpgsql;

---------------------------

CREATE OR REPLACE FUNCTION get_customer_stage_monthly_breakdown(
    p_business_id integer,
    p_start_date date,
    p_end_date date
)
RETURNS TABLE (
    month_start date,
    month_end date,
    stage text,
    customer_count bigint,
    metrics jsonb
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH periods AS (
        -- Get periods based on the range
        WITH RECURSIVE date_periods AS (
            SELECT 
                CASE 
                    WHEN p_end_date - p_start_date <= 31 THEN p_start_date
                    ELSE p_start_date
                END AS p_start,
                CASE 
                    WHEN p_end_date - p_start_date <= 31 THEN p_end_date
                    ELSE LEAST((date_trunc('month', p_start_date) + interval '1 month - 1 day')::date, p_end_date)
                END AS p_end
            UNION ALL
            SELECT
                GREATEST(date_trunc('month', p_start + interval '1 month')::date, p_start_date),
                LEAST((date_trunc('month', p_start + interval '1 month') + interval '1 month - 1 day')::date, p_end_date)
            FROM date_periods
            WHERE p_end < p_end_date AND p_end_date - p_start_date > 31
        )
        SELECT p_start as period_start, p_end as period_end FROM date_periods
    ),
    
    -- Call each of the individual metrics functions and combine results
    new_customers AS (
        -- Get new customer metrics for each period
        SELECT 
            nc.period_start,
            nc.period_end,
            'New Customers'::text as stage,
            COALESCE(nc.customer_count, 0) as cust_count,
            jsonb_build_object(
                'gmv', COALESCE(nc.gmv, 0),
                'orders', COALESCE(nc.orders, 0),
                'unique_customers', COALESCE(nc.unique_customers, 0),
                'aov', COALESCE(nc.aov, 0),
                'avg_bill_per_user', COALESCE(nc.avg_bill_per_user, 0), 
                'arpu', COALESCE(nc.arpu, 0),
                'orders_per_day', COALESCE(nc.orders_per_day, 0),
                'orders_per_day_per_store', COALESCE(nc.orders_per_day_per_store, 0),
                'first_purchase_gmv', COALESCE(nc.first_purchase_gmv, 0),
                'avg_first_purchase_value', COALESCE(nc.avg_first_purchase_value, 0),
                'conversion_to_second_purchase_rate', COALESCE(nc.conversion_to_second_purchase_rate, 0)
            ) as metrics
        FROM get_new_customers_metrics(p_business_id, p_start_date, p_end_date) nc
    ),
    
    early_life_customers AS (
        -- Get early life customer metrics for each period
        SELECT 
            elc.period_start,
            elc.period_end,
            'Early Life Customers'::text as stage,
            COALESCE(elc.customer_count, 0) as cust_count,
            jsonb_build_object(
                'gmv', COALESCE(elc.gmv, 0),
                'orders', COALESCE(elc.orders, 0),
                'unique_customers', COALESCE(elc.unique_customers, 0),
                'aov', COALESCE(elc.aov, 0),
                'avg_bill_per_user', COALESCE(elc.avg_bill_per_user, 0), 
                'arpu', COALESCE(elc.arpu, 0),
                'orders_per_day', COALESCE(elc.orders_per_day, 0),
                'orders_per_day_per_store', COALESCE(elc.orders_per_day_per_store, 0),
                'repeat_purchase_rate', COALESCE(elc.repeat_purchase_rate, 0),
                'avg_time_between_purchases', COALESCE(elc.avg_time_between_purchases, 0),
                'avg_order_value', COALESCE(elc.avg_order_value, 0)
            ) as metrics
        FROM get_early_life_customers_metrics(p_business_id, p_start_date, p_end_date) elc
    ),
    
    mature_customers AS (
        -- Get mature customer metrics for each period
        SELECT 
            mc.period_start,
            mc.period_end,
            'Mature Customers'::text as stage,
            COALESCE(mc.customer_count, 0) as cust_count,
            jsonb_build_object(
                'gmv', COALESCE(mc.gmv, 0),
                'orders', COALESCE(mc.orders, 0),
                'unique_customers', COALESCE(mc.unique_customers, 0),
                'aov', COALESCE(mc.aov, 0),
                'avg_bill_per_user', COALESCE(mc.avg_bill_per_user, 0), 
                'arpu', COALESCE(mc.arpu, 0),
                'orders_per_day', COALESCE(mc.orders_per_day, 0),
                'orders_per_day_per_store', COALESCE(mc.orders_per_day_per_store, 0),
                'purchase_frequency', COALESCE(mc.purchase_frequency, 0),
                'avg_basket_size', COALESCE(mc.avg_basket_size, 0),
                'monthly_spend', COALESCE(mc.monthly_spend, 0)
            ) as metrics
        FROM get_mature_customers_metrics(p_business_id, p_start_date, p_end_date) mc
    ),
    
    loyal_customers AS (
        -- Get loyal customer metrics for each period
        SELECT 
            lc.period_start,
            lc.period_end,
            'Loyal Customers'::text as stage,
            COALESCE(lc.customer_count, 0) as cust_count,
            jsonb_build_object(
                'gmv', COALESCE(lc.gmv, 0),
                'orders', COALESCE(lc.orders, 0),
                'unique_customers', COALESCE(lc.unique_customers, 0),
                'aov', COALESCE(lc.aov, 0),
                'avg_bill_per_user', COALESCE(lc.avg_bill_per_user, 0), 
                'arpu', COALESCE(lc.arpu, 0),
                'orders_per_day', COALESCE(lc.orders_per_day, 0),
                'orders_per_day_per_store', COALESCE(lc.orders_per_day_per_store, 0),
                'annual_customer_value', COALESCE(lc.annual_customer_value, 0),
                'purchase_frequency', COALESCE(lc.purchase_frequency, 0),
                'category_penetration', COALESCE(lc.category_penetration, 0)
            ) as metrics
        FROM get_loyal_customers_metrics(p_business_id, p_start_date, p_end_date) lc
    ),
    
    -- Combine all customer stages
    combined_stages AS (
        SELECT * FROM new_customers
        UNION ALL
        SELECT * FROM early_life_customers
        UNION ALL
        SELECT * FROM mature_customers
        UNION ALL
        SELECT * FROM loyal_customers
    )
    
    -- Return final result
    SELECT
        cs.period_start as month_start,
        cs.period_end as month_end,
        cs.stage,
        cs.cust_count as customer_count,
        cs.metrics
    FROM combined_stages cs
    ORDER BY cs.period_start, cs.stage;
END;
$$;

------------------------------------detail new customers--------------------

CREATE OR REPLACE FUNCTION get_detailed_new_customers_info(
    p_business_id integer,
    p_start_date DATE,
    p_end_date DATE
)
RETURNS TABLE (
    -- Customer Profile
    customer_id UUID,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    phone VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    registration_date TIMESTAMP,
    address VARCHAR,
    city VARCHAR,
    
    -- First Purchase Information
    first_purchase_date TIMESTAMP,
    first_purchase_amount NUMERIC,
    has_second_purchase BOOLEAN,
    
    -- Additional Transaction Details
    total_purchases BIGINT,
    total_spent NUMERIC,
    avg_order_value NUMERIC,
    
    -- Product Information
    categories_purchased BIGINT,
    purchase_categories VARCHAR,
    brands_purchased BIGINT,
    brand_names VARCHAR,
    
    -- Store Information
    stores_visited BIGINT,
    store_names VARCHAR,
    
    -- Payment Information
    payment_methods VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    WITH periods AS (
        -- Get periods from the get_new_customers_metrics function
        SELECT period_start, period_end
        FROM get_new_customers_metrics(p_business_id, p_start_date, p_end_date)
    ),
    -- First identify the first transaction for each customer across all history
    first_transactions AS (
        SELECT 
            t1.customer_id,
            MIN(t1.transaction_date)::TIMESTAMP AS first_transaction_date,
            (SELECT t2.total_amount::NUMERIC 
             FROM transactions t2 
             WHERE t2.customer_id = t1.customer_id 
             AND t2.transaction_date = MIN(t1.transaction_date)
             AND t2.business_id = p_business_id
             LIMIT 1) AS first_purchase_amount
        FROM transactions t1
        WHERE t1.business_id = p_business_id
        GROUP BY t1.customer_id
    ),
    -- Then identify new customers using the same criteria as in get_new_customers_metrics
    new_customers AS (
        SELECT 
            p.period_start,
            p.period_end,
            ft.customer_id,
            ft.first_transaction_date,
            ft.first_purchase_amount
        FROM periods p
        JOIN first_transactions ft ON 
            ft.first_transaction_date::DATE BETWEEN p.period_start AND p.period_end AND
            ft.first_transaction_date::DATE BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE AND
            (SELECT COUNT(*) FROM transactions t 
             WHERE t.customer_id = ft.customer_id 
             AND t.business_id = p_business_id) = 1
    ),
    -- Check if customers made a second purchase within 30 days of first purchase
    second_purchase_check AS (
        SELECT 
            nc.period_start,
            nc.period_end,
            nc.customer_id,
            nc.first_transaction_date as first_purchase_date,
            nc.first_purchase_amount,
            EXISTS (
                SELECT 1 
                FROM transactions t2 
                WHERE t2.customer_id = nc.customer_id 
                AND t2.transaction_date > nc.first_transaction_date
                AND t2.transaction_date <= (nc.first_transaction_date + INTERVAL '30 days')
                AND t2.business_id = p_business_id
            ) as has_second_purchase
        FROM new_customers nc
    ),
    -- Gather all detailed customer information with aggregated transaction data
    customer_details AS (
        SELECT 
            spc.period_start,
            spc.period_end,
            c.customer_id,
            c.first_name::VARCHAR,
            c.last_name::VARCHAR,
            c.email::VARCHAR,
            c.phone::VARCHAR,
            c.gender::VARCHAR,
            c.birth_date,
            c.registration_date::TIMESTAMP,
            c.address::VARCHAR,
            c.city::VARCHAR,
            spc.first_purchase_date::TIMESTAMP,
            spc.first_purchase_amount::NUMERIC,
            spc.has_second_purchase,
            COUNT(DISTINCT t.transaction_id) as total_purchases,
            COALESCE(SUM(t.total_amount), 0)::NUMERIC as total_spent,
            CASE 
                WHEN COUNT(DISTINCT t.transaction_id) = 0 THEN 0::NUMERIC
                ELSE (COALESCE(SUM(t.total_amount), 0)::NUMERIC / COUNT(DISTINCT t.transaction_id))::NUMERIC
            END as avg_order_value,
            COUNT(DISTINCT pl.category) as categories_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.category, ', '), '')::VARCHAR as purchase_categories,
            COUNT(DISTINCT pl.brand) as brands_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.brand, ', '), '')::VARCHAR as brand_names,
            COUNT(DISTINCT s.store_id) as stores_visited,
            COALESCE(STRING_AGG(DISTINCT s.store_name, ', '), '')::VARCHAR as store_names,
            COALESCE(STRING_AGG(DISTINCT t.payment_method, ', '), '')::VARCHAR as payment_methods
        FROM second_purchase_check spc
        JOIN customers c ON spc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
            AND t.business_id = p_business_id
            AND t.transaction_date BETWEEN 
                spc.period_start::TIMESTAMP AND 
                (spc.period_end + INTERVAL '1 day')::TIMESTAMP - INTERVAL '1 second'
        LEFT JOIN product_lines pl ON t.product_line_id = pl.product_line_id
        LEFT JOIN stores s ON t.store_id = s.store_id
        WHERE c.business_id = p_business_id
        GROUP BY 
            spc.period_start,
            spc.period_end,
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.gender,
            c.birth_date,
            c.registration_date,
            c.address,
            c.city,
            spc.first_purchase_date,
            spc.first_purchase_amount,
            spc.has_second_purchase
    )
    SELECT 
        cd.customer_id,
        cd.first_name::VARCHAR,
        cd.last_name::VARCHAR,
        cd.email::VARCHAR,
        cd.phone::VARCHAR,
        cd.gender::VARCHAR,
        cd.birth_date,
        cd.registration_date::TIMESTAMP,
        cd.address::VARCHAR,
        cd.city::VARCHAR,
        cd.first_purchase_date::TIMESTAMP,
        cd.first_purchase_amount::NUMERIC,
        cd.has_second_purchase,
        cd.total_purchases,
        cd.total_spent::NUMERIC,
        cd.avg_order_value::NUMERIC,
        cd.categories_purchased,
        cd.purchase_categories::VARCHAR,
        cd.brands_purchased,
        cd.brand_names::VARCHAR,
        cd.stores_visited,
        cd.store_names::VARCHAR,
        cd.payment_methods::VARCHAR
    FROM customer_details cd
    ORDER BY cd.first_purchase_date DESC;
END;
$$ LANGUAGE plpgsql;

------------------------------------detail early life customers--------------------

CREATE OR REPLACE FUNCTION get_detailed_early_life_customers_info(
    p_business_id integer,
    p_start_date date,
    p_end_date date
)
RETURNS TABLE (
    -- Customer Profile
    customer_id uuid,
    first_name text,
    last_name text,
    email text,
    phone text,
    gender text,
    birth_date date,
    registration_date timestamp,
    address text,
    city text,
    
    -- Purchase Information
    purchase_count bigint,
    first_purchase_date timestamp,
    last_purchase_date timestamp,
    days_between_purchases numeric,
    avg_purchase_amount numeric,
    total_spent numeric,
    
    -- Product Information
    categories_purchased bigint,
    purchase_categories text,
    brands_purchased bigint,
    brand_names text,
    
    -- Store Information
    stores_visited bigint,
    store_names text,
    
    -- Payment Information
    payment_methods text,
    
    -- Time-based Metrics
    days_since_first_purchase numeric,
    days_since_last_purchase numeric
) AS $$
BEGIN
    RETURN QUERY
    WITH periods AS (
        -- Get periods from the get_early_life_customers_metrics function
        SELECT period_start, period_end
        FROM get_early_life_customers_metrics(p_business_id, p_start_date, p_end_date)
    ),
    transaction_history AS (
        -- Get all transactions in order
        SELECT 
            t.customer_id,
            t.transaction_id,
            t.transaction_date,
            t.total_amount,
            ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.transaction_date) as transaction_num
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    qualified_early_life_customers AS (
        -- Find the transaction where customer becomes an early life customer (transaction #2 or #3)
        SELECT 
            th.customer_id,
            MIN(th.transaction_date) as first_purchase_date,
            MAX(th.transaction_date) as qualification_date, -- Date when they become early life customer
            COUNT(*) as purchase_count,
            AVG(th.total_amount) as avg_purchase_amount,
            SUM(th.total_amount) as total_spent
        FROM transaction_history th
        GROUP BY th.customer_id
        HAVING 
            COUNT(*) BETWEEN 2 AND 3
            AND MAX(th.transaction_date) - MIN(th.transaction_date) <= INTERVAL '90 days'
            AND MAX(th.transaction_date) BETWEEN p_start_date AND p_end_date
    ),
    early_life_customers AS (
        -- Add period information based on qualification date, not first purchase date
        SELECT 
            qec.customer_id,
            qec.purchase_count,
            qec.first_purchase_date,
            qec.qualification_date as last_purchase_date,
            qec.qualification_date - qec.first_purchase_date as days_between_purchases,
            qec.avg_purchase_amount,
            qec.total_spent,
            p.period_start,
            p.period_end
        FROM qualified_early_life_customers qec
        JOIN periods p ON qec.qualification_date BETWEEN p.period_start AND p.period_end
    ),
    customer_details AS (
        SELECT 
            c.customer_id::uuid,
            c.first_name::text,
            c.last_name::text,
            c.email::text,
            c.phone::text,
            c.gender::text,
            c.birth_date::date,
            c.registration_date::timestamp,
            c.address::text,
            c.city::text,
            elc.purchase_count::bigint,
            elc.first_purchase_date::timestamp,
            elc.last_purchase_date::timestamp,
            EXTRACT(EPOCH FROM elc.days_between_purchases)/86400::numeric as days_between_purchases,
            elc.avg_purchase_amount::numeric,
            elc.total_spent::numeric,
            COUNT(DISTINCT pl.category)::bigint as categories_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.category, ', '), '')::text as purchase_categories,
            COUNT(DISTINCT pl.brand)::bigint as brands_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.brand, ', '), '')::text as brand_names,
            COUNT(DISTINCT s.store_id)::bigint as stores_visited,
            COALESCE(STRING_AGG(DISTINCT s.store_name, ', '), '')::text as store_names,
            COALESCE(STRING_AGG(DISTINCT t.payment_method, ', '), '')::text as payment_methods,
            EXTRACT(EPOCH FROM (CURRENT_DATE - elc.first_purchase_date))/86400::numeric as days_since_first_purchase,
            EXTRACT(EPOCH FROM (CURRENT_DATE - elc.last_purchase_date))/86400::numeric as days_since_last_purchase
        FROM early_life_customers elc
        JOIN customers c ON elc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
            AND t.transaction_date BETWEEN elc.period_start AND elc.period_end
        LEFT JOIN product_lines pl ON t.product_line_id = pl.product_line_id
        LEFT JOIN stores s ON t.store_id = s.store_id
        WHERE c.business_id = p_business_id
        GROUP BY 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.gender,
            c.birth_date,
            c.registration_date,
            c.address,
            c.city,
            elc.purchase_count,
            elc.first_purchase_date,
            elc.last_purchase_date,
            elc.days_between_purchases,
            elc.avg_purchase_amount,
            elc.total_spent
    )
    SELECT 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        cd.phone,
        cd.gender,
        cd.birth_date,
        cd.registration_date,
        cd.address,
        cd.city,
        cd.purchase_count,
        cd.first_purchase_date,
        cd.last_purchase_date,
        cd.days_between_purchases,
        cd.avg_purchase_amount,
        cd.total_spent,
        cd.categories_purchased,
        cd.purchase_categories,
        cd.brands_purchased,
        cd.brand_names,
        cd.stores_visited,
        cd.store_names,
        cd.payment_methods,
        cd.days_since_first_purchase,
        cd.days_since_last_purchase
    FROM customer_details cd
    ORDER BY cd.total_spent DESC;
END;
$$ LANGUAGE plpgsql;

------------------------------------detail mature customers--------------------

CREATE OR REPLACE FUNCTION get_detailed_mature_customers_info(
    p_business_id integer,
    p_start_date date,
    p_end_date date
)
RETURNS TABLE (
    -- Customer Profile
    customer_id uuid,
    first_name text,
    last_name text,
    email text,
    phone text,
    gender text,
    birth_date date,
    registration_date timestamp,
    address text,
    city text,
    
    -- Purchase Information
    purchase_count bigint,
    first_purchase_date timestamp,
    last_purchase_date timestamp,
    days_between_purchases numeric,
    avg_purchase_amount numeric,
    total_spent numeric,
    monthly_spend numeric,
    purchase_frequency numeric,
    
    -- Product Information
    categories_purchased bigint,
    purchase_categories text,
    brands_purchased bigint,
    brand_names text,
    
    -- Store Information
    stores_visited bigint,
    store_names text,
    
    -- Payment Information
    payment_methods text,
    
    -- Time-based Metrics
    days_since_first_purchase numeric,
    days_since_last_purchase numeric,
    customer_tenure numeric
) AS $$
BEGIN
    RETURN QUERY
    WITH periods AS (
        -- Get periods from the get_mature_customers_metrics function
        SELECT period_start, period_end
        FROM get_mature_customers_metrics(p_business_id, p_start_date, p_end_date)
    ),
    transaction_history AS (
        -- Get all transactions in order
        SELECT 
            t.customer_id,
            t.transaction_date,
            t.total_amount,
            ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.transaction_date) as transaction_num
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    customer_purchase_history AS (
        -- Aggregate purchase history for each customer
        SELECT 
            th.customer_id,
            COUNT(*) as purchase_count,
            MIN(th.transaction_date) as first_purchase_date,
            MAX(th.transaction_date) as last_purchase_date,
            MAX(th.transaction_date) - MIN(th.transaction_date) as tenure,
            AVG(th.total_amount) as avg_purchase_amount,
            SUM(th.total_amount) as total_spent
        FROM transaction_history th
        GROUP BY th.customer_id
    ),
    qualified_mature_customers AS (
        -- Identify customers who meet the "mature" criteria - qualification happens at their last purchase
        SELECT 
            cph.customer_id,
            cph.purchase_count,
            cph.first_purchase_date,
            cph.last_purchase_date,
            cph.tenure as days_between_purchases,
            cph.avg_purchase_amount,
            cph.total_spent,
            -- Calculate metrics specific to mature customers
            (cph.total_spent / 
             NULLIF(EXTRACT(EPOCH FROM cph.tenure)/2592000, 0)) as monthly_spend,
            (cph.purchase_count::float / 
             NULLIF(EXTRACT(EPOCH FROM cph.tenure)/86400, 0)) as purchase_frequency
        FROM customer_purchase_history cph
        WHERE cph.purchase_count >= 4
        AND cph.tenure > INTERVAL '90 days'
        AND cph.tenure <= INTERVAL '180 days'
        AND cph.last_purchase_date BETWEEN p_start_date AND p_end_date
    ),
    mature_customers_by_period AS (
        -- Assign mature customers to periods based on qualification date (last purchase date)
        SELECT 
            qmc.*,
            p.period_start,
            p.period_end
        FROM qualified_mature_customers qmc
        JOIN periods p ON qmc.last_purchase_date BETWEEN p.period_start AND p.period_end
    ),
    customer_details AS (
        SELECT 
            c.customer_id::uuid,
            c.first_name::text,
            c.last_name::text,
            c.email::text,
            c.phone::text,
            c.gender::text,
            c.birth_date::date,
            c.registration_date::timestamp,
            c.address::text,
            c.city::text,
            mc.purchase_count::bigint,
            mc.first_purchase_date::timestamp,
            mc.last_purchase_date::timestamp,
            EXTRACT(EPOCH FROM mc.days_between_purchases)/86400::numeric as days_between_purchases,
            mc.avg_purchase_amount::numeric,
            mc.total_spent::numeric,
            mc.monthly_spend::numeric,
            mc.purchase_frequency::numeric,
            COUNT(DISTINCT pl.category)::bigint as categories_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.category, ', '), '')::text as purchase_categories,
            COUNT(DISTINCT pl.brand)::bigint as brands_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.brand, ', '), '')::text as brand_names,
            COUNT(DISTINCT s.store_id)::bigint as stores_visited,
            COALESCE(STRING_AGG(DISTINCT s.store_name, ', '), '')::text as store_names,
            COALESCE(STRING_AGG(DISTINCT t.payment_method, ', '), '')::text as payment_methods,
            EXTRACT(EPOCH FROM (CURRENT_DATE - mc.first_purchase_date))/86400::numeric as days_since_first_purchase,
            EXTRACT(EPOCH FROM (CURRENT_DATE - mc.last_purchase_date))/86400::numeric as days_since_last_purchase,
            EXTRACT(EPOCH FROM (mc.last_purchase_date - mc.first_purchase_date))/86400::numeric as customer_tenure
        FROM mature_customers_by_period mc
        JOIN customers c ON mc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
            AND t.transaction_date BETWEEN mc.period_start AND mc.period_end
        LEFT JOIN product_lines pl ON t.product_line_id = pl.product_line_id
        LEFT JOIN stores s ON t.store_id = s.store_id
        WHERE c.business_id = p_business_id
        GROUP BY 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.gender,
            c.birth_date,
            c.registration_date,
            c.address,
            c.city,
            mc.purchase_count,
            mc.first_purchase_date,
            mc.last_purchase_date,
            mc.days_between_purchases,
            mc.avg_purchase_amount,
            mc.total_spent,
            mc.monthly_spend,
            mc.purchase_frequency
    )
    SELECT * FROM customer_details
    ORDER BY total_spent DESC;
END;
$$ LANGUAGE plpgsql;

------------------------------------detail loyal customers--------------------

CREATE OR REPLACE FUNCTION get_detailed_loyal_customers_info(
    p_business_id integer,
    p_start_date date,
    p_end_date date
)
RETURNS TABLE (
    -- Customer Profile
    customer_id uuid,
    first_name text,
    last_name text,
    email text,
    phone text,
    gender text,
    birth_date date,
    registration_date timestamp,
    address text,
    city text,
    
    -- Purchase Information
    purchase_count bigint,
    first_purchase_date timestamp,
    last_purchase_date timestamp,
    days_between_purchases numeric,
    total_spend numeric,
    annual_customer_value numeric,
    purchase_frequency numeric,
    
    -- Product Information
    unique_categories bigint,
    category_penetration numeric,
    purchase_categories text,
    brands_purchased bigint,
    brand_names text,
    
    -- Store Information
    stores_visited bigint,
    store_names text,
    
    -- Payment Information
    payment_methods text,
    
    -- Time-based Metrics
    days_since_first_purchase numeric,
    days_since_last_purchase numeric,
    customer_tenure numeric,
    last_purchase_days_ago numeric
) AS $$
BEGIN
    RETURN QUERY
    WITH periods AS (
        -- Get periods from the get_loyal_customers_metrics function
        SELECT period_start, period_end
        FROM get_loyal_customers_metrics(p_business_id, p_start_date, p_end_date)
    ),
    transaction_history AS (
        -- Get all transactions in order
        SELECT 
            t.customer_id,
            t.transaction_id,
            t.transaction_date,
            t.total_amount,
            t.product_line_id,
            ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.transaction_date) as transaction_num
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
    ),
    customer_purchase_history AS (
        -- Aggregate purchase history for each customer
        SELECT 
            th.customer_id,
            COUNT(DISTINCT th.transaction_id) as purchase_count,
            MIN(th.transaction_date) as first_purchase_date,
            MAX(th.transaction_date) as last_purchase_date,
            MAX(th.transaction_date) - MIN(th.transaction_date) as tenure,
            SUM(th.total_amount) as total_spend,
            -- Count unique categories per customer
            COUNT(DISTINCT pl.category) as unique_categories
        FROM transaction_history th
        JOIN product_lines pl ON th.product_line_id = pl.product_line_id
        GROUP BY th.customer_id
    ),
    qualified_loyal_customers AS (
        -- Identify customers who meet loyal criteria at their last transaction
        SELECT 
            cph.customer_id,
            cph.purchase_count,
            cph.first_purchase_date,
            cph.last_purchase_date,
            cph.tenure as days_between_purchases,
            cph.total_spend,
            cph.unique_categories,
            -- Calculate derived metrics
            (cph.total_spend * 365.0 / NULLIF(EXTRACT(EPOCH FROM cph.tenure)/86400, 0)) as annual_customer_value,
            (cph.purchase_count::float / NULLIF(EXTRACT(EPOCH FROM cph.tenure)/86400, 0)) as purchase_frequency
        FROM customer_purchase_history cph
        WHERE cph.purchase_count >= 4
        AND cph.tenure > INTERVAL '180 days'
        AND cph.last_purchase_date >= (CURRENT_DATE - INTERVAL '60 days')
        AND cph.last_purchase_date BETWEEN p_start_date AND p_end_date
    ),
    loyal_customers_by_period AS (
        -- Assign loyal customers to periods based on qualification date (last purchase)
        SELECT 
            qlc.*,
            p.period_start,
            p.period_end
        FROM qualified_loyal_customers qlc
        JOIN periods p ON qlc.last_purchase_date BETWEEN p.period_start AND p.period_end
    ),
    total_categories AS (
        SELECT COUNT(DISTINCT category) as total
        FROM product_lines
        WHERE business_id = p_business_id
    ),
    customer_details AS (
        SELECT 
            c.customer_id::uuid,
            c.first_name::text,
            c.last_name::text,
            c.email::text,
            c.phone::text,
            c.gender::text,
            c.birth_date::date,
            c.registration_date::timestamp,
            c.address::text,
            c.city::text,
            lc.purchase_count::bigint,
            lc.first_purchase_date::timestamp,
            lc.last_purchase_date::timestamp,
            EXTRACT(EPOCH FROM lc.days_between_purchases)/86400::numeric as days_between_purchases,
            lc.total_spend::numeric,
            lc.annual_customer_value::numeric,
            lc.purchase_frequency::numeric,
            lc.unique_categories::bigint,
            (lc.unique_categories::FLOAT / NULLIF(tc.total, 0))::numeric as category_penetration,
            COALESCE(STRING_AGG(DISTINCT pl.category, ', '), '')::text as purchase_categories,
            COUNT(DISTINCT pl.brand)::bigint as brands_purchased,
            COALESCE(STRING_AGG(DISTINCT pl.brand, ', '), '')::text as brand_names,
            COUNT(DISTINCT s.store_id)::bigint as stores_visited,
            COALESCE(STRING_AGG(DISTINCT s.store_name, ', '), '')::text as store_names,
            COALESCE(STRING_AGG(DISTINCT t.payment_method, ', '), '')::text as payment_methods,
            EXTRACT(EPOCH FROM (CURRENT_DATE - lc.first_purchase_date))/86400::numeric as days_since_first_purchase,
            EXTRACT(EPOCH FROM (CURRENT_DATE - lc.last_purchase_date))/86400::numeric as days_since_last_purchase,
            EXTRACT(EPOCH FROM (lc.last_purchase_date - lc.first_purchase_date))/86400::numeric as customer_tenure,
            EXTRACT(EPOCH FROM (CURRENT_DATE - lc.last_purchase_date))/86400::numeric as last_purchase_days_ago
        FROM loyal_customers_by_period lc
        JOIN customers c ON lc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
            AND t.transaction_date BETWEEN lc.period_start AND lc.period_end
        LEFT JOIN product_lines pl ON t.product_line_id = pl.product_line_id
        LEFT JOIN stores s ON t.store_id = s.store_id
        CROSS JOIN total_categories tc
        WHERE c.business_id = p_business_id
        GROUP BY 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email,
            c.phone,
            c.gender,
            c.birth_date,
            c.registration_date,
            c.address,
            c.city,
            lc.purchase_count,
            lc.first_purchase_date,
            lc.last_purchase_date,
            lc.days_between_purchases,
            lc.total_spend,
            lc.annual_customer_value,
            lc.purchase_frequency,
            lc.unique_categories,
            tc.total
    )
    SELECT * FROM customer_details
    ORDER BY total_spend DESC;
END;
$$ LANGUAGE plpgsql;
