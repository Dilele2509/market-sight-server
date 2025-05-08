CREATE OR REPLACE FUNCTION get_detailed_mature_customers_info(
    p_business_id integer,
    p_start_date date,
    p_end_date date
)
RETURNS TABLE (
    -- Same return columns as before
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
    purchase_count bigint,
    first_purchase_date timestamp,
    last_purchase_date timestamp,
    days_between_purchases numeric,
    avg_purchase_amount numeric,
    total_spent numeric,
    monthly_spend numeric,
    purchase_frequency numeric,
    categories_purchased bigint,
    purchase_categories text,
    brands_purchased bigint,
    brand_names text,
    stores_visited bigint,
    store_names text,
    payment_methods text,
    days_since_first_purchase numeric,
    days_since_last_purchase numeric,
    customer_tenure numeric
) AS $$
BEGIN
    RETURN QUERY
    WITH customer_purchases AS (
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
            SUM(cp.total_amount) as total_spent,
            SUM(cp.total_amount) / 
            NULLIF(EXTRACT(EPOCH FROM (cp.last_purchase_date - cp.first_purchase_date))/2592000, 0) as monthly_spend,
            cp.purchase_count::float / 
            NULLIF(EXTRACT(EPOCH FROM (cp.last_purchase_date - cp.first_purchase_date))/86400, 0) as purchase_frequency
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
    customer_details AS (
        -- Rest of the function remains the same
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
            EXTRACT(EPOCH FROM (p_end_date - mc.first_purchase_date))/86400::numeric as days_since_first_purchase,
            EXTRACT(EPOCH FROM (p_end_date - mc.last_purchase_date))/86400::numeric as days_since_last_purchase,
            EXTRACT(EPOCH FROM (mc.last_purchase_date - mc.first_purchase_date))/86400::numeric as customer_tenure
        FROM mature_customers mc
        JOIN customers c ON mc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
        AND t.transaction_date BETWEEN p_start_date AND p_end_date
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

-- Example of how to use the function:
-- SELECT * FROM get_detailed_mature_customers_info(1, '2023-01-01', '2023-12-31'); 