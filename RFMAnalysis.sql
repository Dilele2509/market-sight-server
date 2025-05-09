CREATE OR REPLACE FUNCTION analyze_rfm_for_period(
  target_business_id INTEGER,
  start_date TIMESTAMP WITH TIME ZONE,
  end_date TIMESTAMP WITH TIME ZONE
) RETURNS VOID AS $$
DECLARE
  months_in_period NUMERIC;
BEGIN
  -- 1. Calculate number of months in the analysis period
  months_in_period = GREATEST(1, EXTRACT(EPOCH FROM (end_date - start_date)) / (30 * 24 * 60 * 60));
  
  -- 2. Create temporary table with initial RFM data
  DROP TABLE IF EXISTS temp_rfm_data;
  CREATE TEMP TABLE temp_rfm_data AS
  SELECT
    customer_id,
    target_business_id AS business_id,
    EXTRACT(DAY FROM (end_date - MAX(transaction_date)))::INTEGER AS recency_value,
    COUNT(DISTINCT transaction_id)::INTEGER AS frequency_value,
    SUM(total_amount)::NUMERIC AS monetary_value
  FROM transactions
  WHERE 
    business_id = target_business_id AND
    transaction_date BETWEEN start_date AND end_date
  GROUP BY customer_id;
  
  -- 3. Calculate average RFM values for percentile determination
  DECLARE
    avg_recency NUMERIC;
    avg_frequency NUMERIC;
    avg_monetary NUMERIC;
    
    -- 4. Define percentile thresholds
    r_threshold_20 NUMERIC;
    r_threshold_40 NUMERIC;
    r_threshold_60 NUMERIC;
    r_threshold_80 NUMERIC;
    
    f_threshold_20 NUMERIC;
    f_threshold_40 NUMERIC;
    f_threshold_60 NUMERIC;
    f_threshold_80 NUMERIC;
    
    m_threshold_20 NUMERIC;
    m_threshold_40 NUMERIC;
    m_threshold_60 NUMERIC;
    m_threshold_80 NUMERIC;
  BEGIN
    -- Calculate averages
    SELECT 
      AVG(recency_value), 
      AVG(frequency_value), 
      AVG(monetary_value)
    INTO 
      avg_recency, 
      avg_frequency, 
      avg_monetary
    FROM temp_rfm_data;
    
    -- Calculate thresholds for recency (lower is better)
    r_threshold_20 = avg_recency * 1.6;  -- 20% worse than average (higher recency)
    r_threshold_40 = avg_recency * 1.2;  -- 40% worse than average
    r_threshold_60 = avg_recency * 0.8;  -- 60% better than average
    r_threshold_80 = avg_recency * 0.4;  -- 80% better than average
    
    -- Calculate thresholds for frequency and monetary (higher is better)
    f_threshold_20 = avg_frequency * 0.2;  -- 20% of average
    f_threshold_40 = avg_frequency * 0.4;  -- 40% of average
    f_threshold_60 = avg_frequency * 0.6;  -- 60% of average
    f_threshold_80 = avg_frequency * 0.8;  -- 80% of average
    
    m_threshold_20 = avg_monetary * 0.2;  -- 20% of average
    m_threshold_40 = avg_monetary * 0.4;  -- 40% of average
    m_threshold_60 = avg_monetary * 0.6;  -- 60% of average
    m_threshold_80 = avg_monetary * 0.8;  -- 80% of average
    
    -- 5. Calculate RFM scores for each customer
    DROP TABLE IF EXISTS temp_rfm_scores;
    CREATE TEMP TABLE temp_rfm_scores AS
    SELECT
      customer_id,
      business_id,
      recency_value,
      frequency_value,
      monetary_value,
      CASE
        WHEN recency_value <= r_threshold_80 THEN 5
        WHEN recency_value <= r_threshold_60 THEN 4
        WHEN recency_value <= r_threshold_40 THEN 3
        WHEN recency_value <= r_threshold_20 THEN 2
        ELSE 1
      END AS r_score,
      CASE
        WHEN frequency_value > avg_frequency * 1.6 THEN 5
        WHEN frequency_value > avg_frequency * 1.2 THEN 4
        WHEN frequency_value > avg_frequency * 0.8 THEN 3
        WHEN frequency_value > avg_frequency * 0.4 THEN 2
        ELSE 1
      END AS f_score,
      CASE
        WHEN monetary_value > avg_monetary * 1.6 THEN 5
        WHEN monetary_value > avg_monetary * 1.2 THEN 4
        WHEN monetary_value > avg_monetary * 0.8 THEN 3
        WHEN monetary_value > avg_monetary * 0.4 THEN 2
        ELSE 1
      END AS m_score
    FROM temp_rfm_data;
    
    -- 6. Segment customers based on RFM scores
    -- Clear existing RFM data for this business for the period
    DELETE FROM rfm_scores WHERE business_id = target_business_id;
    
    -- Insert new RFM scores with segments
    INSERT INTO rfm_scores (
      customer_id,
      business_id,
      recency_value,
      frequency_value,
      monetary_value,
      r_score,
      f_score,
      m_score,
      segment,
      last_updated
    )
    SELECT
      customer_id,
      business_id,
      recency_value,
      frequency_value,
      monetary_value,
      r_score,
      f_score,
      m_score,
      CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 2 AND r_score <= 4 AND f_score >= 3 AND f_score <= 4 AND m_score >= 4 AND m_score <= 5 THEN 'Loyal Customers'
        WHEN r_score >= 3 AND r_score <= 5 AND f_score >= 1 AND f_score <= 3 AND m_score >= 1 AND m_score <= 3 THEN 'Potential Loyalist'
        WHEN r_score >= 4 AND r_score <= 5 AND f_score < 2 AND m_score < 2 THEN 'New Customers'
        WHEN r_score >= 3 AND r_score <= 4 AND f_score < 2 AND m_score < 2 THEN 'Promising'
        WHEN r_score >= 3 AND r_score <= 4 AND f_score >= 3 AND f_score <= 4 AND m_score >= 3 AND m_score <= 4 THEN 'Need Attention'
        WHEN r_score >= 2 AND r_score <= 3 AND f_score < 3 AND m_score < 3 THEN 'About To Sleep'
        WHEN r_score < 3 AND f_score >= 2 AND f_score <= 5 AND m_score >= 2 AND m_score <= 5 THEN 'At Risk'
        WHEN r_score < 2 AND f_score >= 4 AND f_score <= 5 AND m_score >= 4 AND m_score <= 5 THEN 'Can''t Lose Them'
        WHEN r_score >= 2 AND r_score <= 3 AND f_score >= 2 AND f_score <= 3 AND m_score >= 2 AND m_score <= 3 THEN 'Hibernating'
        ELSE 'Lost'
      END AS segment,
      CURRENT_TIMESTAMP AS last_updated
    FROM temp_rfm_scores;
  END;
END;
$$ LANGUAGE plpgsql;

-- Function to analyze RFM for a specific customer
CREATE OR REPLACE FUNCTION analyze_rfm_for_customer(
  target_customer_id UUID,
  target_business_id INTEGER,
  start_date TIMESTAMP WITH TIME ZONE,
  end_date TIMESTAMP WITH TIME ZONE
) RETURNS VOID AS $$
DECLARE
  months_in_period NUMERIC;
  avg_recency NUMERIC;
  avg_frequency NUMERIC;
  avg_monetary NUMERIC;
  
  customer_recency INTEGER;
  customer_frequency INTEGER;
  customer_monetary NUMERIC;
  
  r_score SMALLINT;
  f_score SMALLINT;
  m_score SMALLINT;
  segment VARCHAR;
BEGIN
  -- 1. Calculate number of months in the analysis period
  months_in_period = GREATEST(1, EXTRACT(EPOCH FROM (end_date - start_date)) / (30 * 24 * 60 * 60));
  
  -- 2. Get RFM averages from all customers
  SELECT 
    AVG(EXTRACT(DAY FROM (end_date - MAX(transaction_date)))),
    AVG(COUNT(DISTINCT transaction_id)),
    AVG(SUM(total_amount))
  INTO 
    avg_recency,
    avg_frequency,
    avg_monetary
  FROM transactions
  WHERE 
    business_id = target_business_id AND
    transaction_date BETWEEN start_date AND end_date
  GROUP BY customer_id;
  
  -- 3. Calculate RFM values for the specific customer
  SELECT
    EXTRACT(DAY FROM (end_date - MAX(transaction_date)))::INTEGER,
    COUNT(DISTINCT transaction_id)::INTEGER,
    SUM(total_amount)::NUMERIC
  INTO
    customer_recency,
    customer_frequency,
    customer_monetary
  FROM transactions
  WHERE 
    business_id = target_business_id AND
    customer_id = target_customer_id AND
    transaction_date BETWEEN start_date AND end_date
  GROUP BY customer_id;
  
  -- 4. Calculate RFM scores for the customer
  -- Recency score (lower is better)
  IF customer_recency <= avg_recency * 0.4 THEN r_score = 5;
  ELSIF customer_recency <= avg_recency * 0.8 THEN r_score = 4;
  ELSIF customer_recency <= avg_recency * 1.2 THEN r_score = 3;
  ELSIF customer_recency <= avg_recency * 1.6 THEN r_score = 2;
  ELSE r_score = 1;
  END IF;
  
  -- Frequency score (higher is better)
  IF customer_frequency > avg_frequency * 1.6 THEN f_score = 5;
  ELSIF customer_frequency > avg_frequency * 1.2 THEN f_score = 4;
  ELSIF customer_frequency > avg_frequency * 0.8 THEN f_score = 3;
  ELSIF customer_frequency > avg_frequency * 0.4 THEN f_score = 2;
  ELSE f_score = 1;
  END IF;
  
  -- Monetary score (higher is better)
  IF customer_monetary > avg_monetary * 1.6 THEN m_score = 5;
  ELSIF customer_monetary > avg_monetary * 1.2 THEN m_score = 4;
  ELSIF customer_monetary > avg_monetary * 0.8 THEN m_score = 3;
  ELSIF customer_monetary > avg_monetary * 0.4 THEN m_score = 2;
  ELSE m_score = 1;
  END IF;
  
  -- 5. Determine customer segment
  IF r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN
    segment = 'Champions';
  ELSIF r_score >= 2 AND r_score <= 4 AND f_score >= 3 AND f_score <= 4 AND m_score >= 4 AND m_score <= 5 THEN
    segment = 'Loyal Customers';
  ELSIF r_score >= 3 AND r_score <= 5 AND f_score >= 1 AND f_score <= 3 AND m_score >= 1 AND m_score <= 3 THEN
    segment = 'Potential Loyalist';
  ELSIF r_score >= 4 AND r_score <= 5 AND f_score < 2 AND m_score < 2 THEN
    segment = 'New Customers';
  ELSIF r_score >= 3 AND r_score <= 4 AND f_score < 2 AND m_score < 2 THEN
    segment = 'Promising';
  ELSIF r_score >= 3 AND r_score <= 4 AND f_score >= 3 AND f_score <= 4 AND m_score >= 3 AND m_score <= 4 THEN
    segment = 'Need Attention';
  ELSIF r_score >= 2 AND r_score <= 3 AND f_score < 3 AND m_score < 3 THEN
    segment = 'About To Sleep';
  ELSIF r_score < 3 AND f_score >= 2 AND f_score <= 5 AND m_score >= 2 AND m_score <= 5 THEN
    segment = 'At Risk';
  ELSIF r_score < 2 AND f_score >= 4 AND f_score <= 5 AND m_score >= 4 AND m_score <= 5 THEN
    segment = 'Can''t Lose Them';
  ELSIF r_score >= 2 AND r_score <= 3 AND f_score >= 2 AND f_score <= 3 AND m_score >= 2 AND m_score <= 3 THEN
    segment = 'Hibernating';
  ELSE
    segment = 'Lost';
  END IF;
  
  -- 6. Update or insert RFM score for the customer
  DELETE FROM rfm_scores 
  WHERE customer_id = target_customer_id AND business_id = target_business_id;
  
  INSERT INTO rfm_scores (
    customer_id,
    business_id,
    recency_value,
    frequency_value,
    monetary_value,
    r_score,
    f_score,
    m_score,
    segment,
    last_updated
  ) VALUES (
    target_customer_id,
    target_business_id,
    customer_recency,
    customer_frequency,
    customer_monetary,
    r_score,
    f_score,
    m_score,
    segment,
    CURRENT_TIMESTAMP
  );
END;
$$ LANGUAGE plpgsql;

-- Function to get detailed customer information for each RFM segment
CREATE OR REPLACE FUNCTION get_rfm_segment_customers(
  target_business_id INTEGER,
  target_segment VARCHAR DEFAULT NULL,
  start_date TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  end_date TIMESTAMP WITH TIME ZONE DEFAULT NULL
) RETURNS TABLE (
  customer_id UUID,
  business_id INTEGER,
  first_name VARCHAR,
  last_name VARCHAR,
  email VARCHAR,
  phone VARCHAR,
  gender VARCHAR,
  birth_date DATE,
  registration_date TIMESTAMP WITH TIME ZONE,
  address TEXT,
  city VARCHAR,
  recency_value INTEGER,
  frequency_value INTEGER,
  monetary_value NUMERIC,
  r_score SMALLINT,
  f_score SMALLINT,
  m_score SMALLINT,
  segment VARCHAR,
  last_updated TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
  -- Check if dates are provided, if not, use results from the latest RFM analysis
  IF start_date IS NOT NULL AND end_date IS NOT NULL THEN
    -- Analyze RFM for the period if dates are provided
    PERFORM analyze_rfm_for_period(target_business_id, start_date, end_date);
  END IF;

  -- Now return the detailed customer data
  RETURN QUERY
  SELECT 
    c.customer_id,
    c.business_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.gender,
    c.birth_date,
    c.registration_date,
    c.address,
    c.city,
    r.recency_value,
    r.frequency_value,
    r.monetary_value,
    r.r_score,
    r.f_score,
    r.m_score,
    r.segment,
    r.last_updated
  FROM 
    rfm_scores r
    JOIN customers c ON r.customer_id = c.customer_id
  WHERE 
    r.business_id = target_business_id
    AND (target_segment IS NULL OR r.segment = target_segment)
  ORDER BY 
    r.segment ASC,
    c.last_name ASC,
    c.first_name ASC;
END;
$$ LANGUAGE plpgsql;
