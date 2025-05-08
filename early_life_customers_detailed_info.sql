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
    WITH customer_purchases AS (
        -- Get all purchases for each customer with their purchase sequence
        SELECT 
            t.customer_id,
            t.transaction_date,
            t.total_amount,
            COUNT(*) OVER (PARTITION BY t.customer_id) as purchase_count,
            MIN(t.transaction_date) OVER (PARTITION BY t.customer_id) as first_purchase_date,
            MAX(t.transaction_date) OVER (PARTITION BY t.customer_id) as last_purchase_date
        FROM transactions t
        WHERE t.business_id = p_business_id
        AND t.transaction_date >= p_start_date
        AND t.transaction_date <= p_end_date
    ),
    early_life_customers AS (
        -- Filter for customers with 2-3 purchases within 90 days
        SELECT 
            cp.customer_id,
            cp.purchase_count,
            cp.first_purchase_date,
            cp.last_purchase_date,
            cp.last_purchase_date - cp.first_purchase_date as days_between_purchases,
            AVG(cp.total_amount) as avg_purchase_amount,
            SUM(cp.total_amount) as total_spent
        FROM customer_purchases cp
        WHERE cp.purchase_count BETWEEN 2 AND 3
        AND cp.last_purchase_date - cp.first_purchase_date <= INTERVAL '90 days'
        GROUP BY 
            cp.customer_id, 
            cp.purchase_count,
            cp.first_purchase_date,
            cp.last_purchase_date
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
            EXTRACT(EPOCH FROM (p_end_date - elc.first_purchase_date))/86400::numeric as days_since_first_purchase,
            EXTRACT(EPOCH FROM (p_end_date - elc.last_purchase_date))/86400::numeric as days_since_last_purchase
        FROM early_life_customers elc
        JOIN customers c ON elc.customer_id = c.customer_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
        AND t.transaction_date >= p_start_date
        AND t.transaction_date <= p_end_date
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

-- Example of how to use the function:
-- SELECT * FROM get_detailed_early_life_customers_info(1, '2023-01-01', '2023-12-31'); 