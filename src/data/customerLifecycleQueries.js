export const getNewCustomersMetricsQuery = `
  SELECT * FROM get_new_customers_metrics($1);
`;

export const getEarlyLifeCustomersMetricsQuery = `
  WITH purchase_intervals AS (
    SELECT 
      t.customer_id,
      t.transaction_id,
      EXTRACT(EPOCH FROM (
        t.transaction_date - LAG(t.transaction_date) OVER (
          PARTITION BY t.customer_id ORDER BY t.transaction_date
        )
      ))/86400 as days_between_purchases
    FROM transactions t
    WHERE t.business_id = $1
  )
  SELECT 
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(DISTINCT t.transaction_id)::float / COUNT(DISTINCT c.customer_id) as repeat_purchase_rate,
    AVG(pi.days_between_purchases) as avg_time_between_purchases,
    AVG(t.total_amount) as avg_order_value,
    COUNT(DISTINCT t.transaction_id) as orders,
    AVG(t.total_amount) as aov,
    SUM(t.total_amount) / COUNT(DISTINCT c.customer_id) as arpu,
    COUNT(DISTINCT t.transaction_id) / 
    (EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/86400) as orders_per_day
  FROM customers c
  JOIN transactions t ON c.customer_id = t.customer_id
  JOIN purchase_intervals pi ON t.transaction_id = pi.transaction_id
  WHERE c.business_id = $1
    AND t.transaction_date >= $2
    AND t.transaction_date <= $3
    AND t.transaction_date >= (
      SELECT MIN(t2.transaction_date)
      FROM transactions t2
      WHERE t2.customer_id = c.customer_id
    )
    AND t.transaction_date <= (
      SELECT MIN(t2.transaction_date) + INTERVAL '90 days'
      FROM transactions t2
      WHERE t2.customer_id = c.customer_id
    )
`;

export const getMatureCustomersMetricsQuery = `
  SELECT 
    COUNT(DISTINCT c.customer_id) as customer_count,
    COUNT(DISTINCT t.transaction_id)::float / 
    (EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/86400) as purchase_frequency,
    AVG(t.total_amount) as avg_basket_size,
    SUM(t.total_amount) / 
    (EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/2592000) as monthly_spend,
    COUNT(DISTINCT t.transaction_id) as orders,
    AVG(t.total_amount) as aov,
    SUM(t.total_amount) / COUNT(DISTINCT c.customer_id) as arpu,
    COUNT(DISTINCT t.transaction_id) / 
    (EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/86400) as orders_per_day
  FROM customers c
  JOIN transactions t ON c.customer_id = t.customer_id
  WHERE c.business_id = $1
    AND t.transaction_date >= $2
    AND t.transaction_date <= $3
    AND (
      SELECT COUNT(*)
      FROM transactions t2
      WHERE t2.customer_id = c.customer_id
    ) >= 4
    AND t.transaction_date >= (
      SELECT MIN(t2.transaction_date) + INTERVAL '90 days'
      FROM transactions t2
      WHERE t2.customer_id = c.customer_id
    )
`;

export const getLoyalCustomersMetricsQuery = `
  SELECT 
    COUNT(DISTINCT c.customer_id) as customer_count,
    SUM(t.total_amount) * 12 / COUNT(DISTINCT c.customer_id) as annual_customer_value,
    COUNT(DISTINCT t.transaction_id)::float / 
    (EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/86400) as purchase_frequency,
    COUNT(DISTINCT pl.category)::float / (
      SELECT COUNT(DISTINCT category) 
      FROM product_lines 
      WHERE business_id = $1
    ) as category_penetration,
    COUNT(DISTINCT t.transaction_id) as orders,
    AVG(t.total_amount) as aov,
    SUM(t.total_amount) / COUNT(DISTINCT c.customer_id) as arpu,
    COUNT(DISTINCT t.transaction_id) / 
    (EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/86400) as orders_per_day
  FROM customers c
  JOIN transactions t ON c.customer_id = t.customer_id
  JOIN product_lines pl ON t.product_line_id = pl.product_line_id
  WHERE c.business_id = $1
    AND t.transaction_date >= $2
    AND t.transaction_date <= $3
    AND t.transaction_date >= (
      SELECT MIN(t2.transaction_date) + INTERVAL '180 days'
      FROM transactions t2
      WHERE t2.customer_id = c.customer_id
    )
    AND t.transaction_date >= CURRENT_DATE - INTERVAL '60 days'
`;

export const updateCustomerSegmentsQuery = `
  WITH customer_metrics AS (
    SELECT 
      c.customer_id,
      c.business_id,
      MIN(t.transaction_date) as first_purchase_date,
      MAX(t.transaction_date) as last_purchase_date,
      COUNT(DISTINCT t.transaction_id) as total_purchases,
      EXTRACT(EPOCH FROM (MAX(t.transaction_date) - MIN(t.transaction_date)))/86400 as days_between_first_last_purchase,
      AVG(EXTRACT(EPOCH FROM (
        t.transaction_date - LAG(t.transaction_date) OVER (
          PARTITION BY c.customer_id ORDER BY t.transaction_date
        )
      ))/86400) as avg_days_between_purchases,
      MAX(EXTRACT(EPOCH FROM (
        t.transaction_date - LAG(t.transaction_date) OVER (
          PARTITION BY c.customer_id ORDER BY t.transaction_date
        )
      ))/86400) as max_gap_between_purchases,
      AVG(t.total_amount) as avg_order_value,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.total_amount) as median_order_value
    FROM customers c
    JOIN transactions t ON c.customer_id = t.customer_id
    CROSS JOIN transactions t2
    WHERE c.business_id = $1
    GROUP BY c.customer_id, c.business_id
  )
  UPDATE customers
  SET 
    customer_segment = CASE
      WHEN cm.total_purchases >= 8 
        AND cm.days_between_first_last_purchase >= 180
        AND cm.avg_days_between_purchases <= 45
        AND cm.max_gap_between_purchases <= 60
        AND cm.avg_order_value >= cm.median_order_value
        AND cm.last_purchase_date >= CURRENT_DATE - INTERVAL '60 days'
        THEN 'Loyal Customer'
      WHEN cm.total_purchases >= 4
        AND cm.days_between_first_last_purchase >= 90
        AND cm.avg_days_between_purchases <= 45
        THEN 'Mature Customer'
      WHEN cm.total_purchases BETWEEN 2 AND 3
        AND cm.first_purchase_date >= CURRENT_DATE - INTERVAL '90 days'
        AND cm.days_between_first_last_purchase <= 90
        THEN 'Early-life Customer'
      WHEN cm.total_purchases = 1
        AND cm.first_purchase_date >= CURRENT_DATE - INTERVAL '30 days'
        THEN 'New Customer'
      ELSE 'Inactive'
    END,
    segment_updated_at = NOW()
  FROM customer_metrics cm
  WHERE customers.customer_id = cm.customer_id
    AND customers.business_id = cm.business_id
`;

export const getCustomerJourneyQuery = `
  WITH customer_segment_history AS (
    SELECT 
      c.customer_id,
      c.business_id,
      c.customer_segment,
      c.segment_updated_at,
      LAG(c.customer_segment) OVER (
        PARTITION BY c.customer_id 
        ORDER BY c.segment_updated_at
      ) as previous_segment,
      EXTRACT(EPOCH FROM (
        c.segment_updated_at - LAG(c.segment_updated_at) OVER (
          PARTITION BY c.customer_id 
          ORDER BY c.segment_updated_at
        )
      ))/86400 as days_in_previous_segment
    FROM customers c
    WHERE c.business_id = $1
      AND c.segment_updated_at >= $2
      AND c.segment_updated_at <= $3
  )
  SELECT 
    customer_segment,
    previous_segment,
    COUNT(DISTINCT customer_id) as customer_count,
    AVG(days_in_previous_segment) as avg_days_in_previous_segment,
    COUNT(DISTINCT CASE 
      WHEN previous_segment IS NOT NULL 
      THEN customer_id 
    END) as transitioned_customers
  FROM customer_segment_history
  GROUP BY customer_segment, previous_segment
  ORDER BY customer_segment, previous_segment;
`;

export const updateBusinessIdsQuery = `
  -- First, get all users and their business_ids
  WITH user_businesses AS (
    SELECT user_id, business_id 
    FROM users 
    WHERE business_id IS NOT NULL
  ),
  
  -- Update customers table
  updated_customers AS (
    UPDATE customers c
    SET business_id = ub.business_id
    FROM user_businesses ub
    WHERE c.user_id = ub.user_id
    RETURNING c.customer_id, c.business_id
  ),
  
  -- Update transactions table
  updated_transactions AS (
    UPDATE transactions t
    SET business_id = c.business_id
    FROM customers c
    WHERE t.customer_id = c.customer_id
    AND t.business_id IS NULL
    RETURNING t.transaction_id, t.business_id
  ),
  
  -- Update product_lines table
  updated_product_lines AS (
    UPDATE product_lines pl
    SET business_id = t.business_id
    FROM transactions t
    WHERE pl.product_line_id = t.product_line_id
    AND pl.business_id IS NULL
    RETURNING pl.product_line_id, pl.business_id
  )   
  
  -- Return summary of updates
  SELECT 
    (SELECT COUNT(*) FROM updated_customers) as customers_updated,
    (SELECT COUNT(*) FROM updated_transactions) as transactions_updated,
    (SELECT COUNT(*) FROM updated_product_lines) as product_lines_updated;
`;