

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
