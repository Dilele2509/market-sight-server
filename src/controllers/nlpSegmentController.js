import { getSupabase, logger } from '../data/database.js';
import Anthropic from '@anthropic-ai/sdk';
import { v4 as uuidv4 } from 'uuid';
import { valueStandardizationService } from '../services/valueStandardizationService.js';

const anthropic = new Anthropic({
  apiKey: process.env.CLAUDE_API_KEY,
});

// Function to generate SQL query from natural language using Claude
const generateSQLFromNLP = async (nlpQuery, user) => {
  try {
    const supabase = getSupabase();
    logger.info('Generating SQL from NLP query', { nlpQuery });

    const prompt = `Given the following natural language query about customer segmentation, generate a PostgreSQL query that would find matching customers. 
    The query should work with these tables and their columns:

    customers table (alias: c):
    - customer_id (uuid)
    - first_name (text)
    - last_name (text)
    - email (text)
    - phone (text)
    - gender (text) - Use 'F' for female, 'M' for male
    - birth_date (date)
    - registration_date (timestamp)
    - address (text)
    - city (text) - Use proper capitalization (e.g., 'Los Angeles', 'Ho Chi Minh')

    transactions table (alias: t):
    - transaction_id (uuid)
    - customer_id (uuid)
    - store_id (uuid)
    - transaction_date (timestamp with time zone)
    - total_amount (double precision)
    - product_line_id (uuid)
    - quantity (bigint)
    - unit_price (double precision)
    - business_id (integer)
    - payment_method (text) - Use 'CASH', 'CREDIT_CARD', 'BANK_TRANSFER'

    product_lines table (alias: p):
    - product_line_id (uuid)
    - unit_cost (numeric)
    - business_id (integer)
    - brand (varchar)
    - subcategory (varchar)
    - name (varchar)
    - category (varchar)

    stores table (alias: s):
    - store_id (uuid)
    - opening_date (date)
    - business_id (integer)
    - city (varchar) - Use proper capitalization
    - store_type (varchar) - Use 'STORE', 'SUPERMARKET'
    - region (varchar)
    - store_name (varchar)
    - address (text)

    Only return the SQL query without any explanation. The query should:
    1. Always start with "SELECT DISTINCT c.* FROM customers c"
    2. Use proper table aliases (c for customers, t for transactions, p for product_lines, s for stores)
    3. Join other tables as needed using proper JOIN syntax
    4. Include appropriate business_id filters for each table
    5. Use proper WHERE clause syntax
    6. Do not include a semicolon at the end
    7. Use standard values for:
       - gender: 'F' for female, 'M' for male
       - city: Proper capitalization (e.g., 'Los Angeles', 'Ho Chi Minh')
       - payment_method: 'CASH', 'CREDIT_CARD', 'BANK_TRANSFER'
       - store_type: 'STORE', 'SUPERMARKET'
    8. For date intervals, use proper PostgreSQL syntax:
       - Use INTERVAL '3 months' instead of INTERVAL 3 MONTH
       - Use CURRENT_DATE for current date
       - Use DATE_TRUNC('month', date) for month truncation
    9. Important: When filtering by city:
       - For customer location: use c.city
       - For store location: use s.city
       - Always specify which table's city you're filtering on
    
    Example query structure:
    SELECT DISTINCT c.* 
    FROM customers c
    JOIN transactions t ON c.customer_id = t.customer_id
    JOIN product_lines p ON t.product_line_id = p.product_line_id
    WHERE c.business_id = [business_id]
    AND t.business_id = [business_id]
    AND p.business_id = [business_id]
    
    Natural language query: ${nlpQuery}`;

    const message = await anthropic.messages.create({
      model: "claude-3-haiku-20240307",
      max_tokens: 1000,
      messages: [{ role: "user", content: prompt }],
    });

    // Clean the query by removing any trailing semicolon and whitespace
    let sqlQuery = message.content[0].text.trim().replace(/;$/, '');

    // Ensure the query starts with SELECT DISTINCT c.*
    if (!sqlQuery.toLowerCase().startsWith('select distinct c.*')) {
      sqlQuery = 'SELECT DISTINCT c.* ' + sqlQuery.substring(sqlQuery.toLowerCase().indexOf('from'));
    }

    // Replace [business_id] placeholders with actual business_id
    sqlQuery = sqlQuery.replace(/\[business_id\]/g, user.business_id);

    // Add business_id filter if not present
    if (!sqlQuery.toLowerCase().includes('where')) {
      sqlQuery += ' WHERE c.business_id = ' + user.business_id;
    } else if (!sqlQuery.toLowerCase().includes('business_id')) {
      sqlQuery = sqlQuery.replace(/where/i, 'WHERE c.business_id = ' + user.business_id + ' AND ');
    }

    // Fix interval syntax if present
    sqlQuery = sqlQuery.replace(/INTERVAL\s+(\d+)\s+MONTH/i, "INTERVAL '$1 months'");

    // Fix city conditions - ensure we're using c.city for customer location
    if (sqlQuery.toLowerCase().includes('s.city') && !sqlQuery.toLowerCase().includes('c.city')) {
      // If the query is about customer location but uses store city, fix it
      sqlQuery = sqlQuery.replace(/s\.city\s*=\s*['"]([^'"]+)['"]/i, "c.city = '$1'");
    }

    // Standardize values in the query
    const mappingTypes = ['city', 'gender', 'payment_method', 'store_type'];
    for (const type of mappingTypes) {
      const regex = new RegExp(`\\b${type}\\s*=\\s*['"]([^'"]+)['"]`, 'gi');
      let match;
      while ((match = regex.exec(sqlQuery)) !== null) {
        const inputValue = match[1];
        const standardValue = await valueStandardizationService.getStandardValue(type, inputValue);
        sqlQuery = sqlQuery.replace(match[0], `${type} = '${standardValue}'`);
      }
    }

    logger.info('Executing generated SQL query', { sqlQuery });

    // Execute the query
    const { data: result, error } = await supabase.rpc('execute_dynamic_query', {
      query_text: sqlQuery
    });

    if (error) {
      logger.error('SQL Query Error:', { error, sqlQuery });
      throw new Error('Failed to execute SQL query: ' + error.message);
    }

    return sqlQuery;
  } catch (error) {
    logger.error('Error generating SQL from NLP:', { error });
    throw new Error('Failed to generate SQL query from natural language');
  }
};

// Function to preview segmentation results
const previewSegmentation = async (req, res) => {
  try {
    const { nlpQuery } = req.body;
    const user = req.user;
    const supabase = getSupabase();

    logger.info('Preview segmentation request', { nlpQuery, userId: user?.user_id });

    if (!user || !user.user_id || !user.business_id) {
      return res.status(400).json({
        success: false,
        error: "User authentication required with business_id"
      });
    }

    // Generate SQL query from NLP
    const sqlQuery = await generateSQLFromNLP(nlpQuery, user);

    // Execute the query to get matching customers
    const { data: result, error: queryError } = await supabase.rpc('execute_dynamic_query', {
      query_text: sqlQuery
    });

    if (queryError) {
      logger.error('Query execution error:', { error: queryError, sqlQuery });
      throw queryError;
    }

    // Extract customers from the JSON array result
    const customers = result[0] || [];

    // Transform the results to ensure we have the expected structure
    const transformedCustomers = customers.map(customer => ({
      customer_id: customer.customer_id,
      first_name: customer.first_name,
      last_name: customer.last_name,
      email: customer.email,
      phone: customer.phone,
      gender: customer.gender,
      birth_date: customer.birth_date,
      registration_date: customer.registration_date,
      address: customer.address,
      city: customer.city
    }));

    logger.info('Preview segmentation successful', { 
      customerCount: transformedCustomers.length 
    });

    res.json({
      success: true,
      data: {
        customers: transformedCustomers,
        sqlQuery: sqlQuery,
        count: transformedCustomers.length
      }
    });
  } catch (error) {
    logger.error('Error in preview segmentation:', { error });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

// Function to create segmentation from NLP
const createSegmentationFromNLP = async (req, res) => {
  try {
    const { nlpQuery, segmentName, description } = req.body;
    const user = req.user;
    const supabase = getSupabase();

    logger.info('Create segmentation request', { 
      nlpQuery, 
      segmentName, 
      userId: user?.user_id 
    });

    if (!user || !user.user_id || !user.business_id) {
      return res.status(400).json({
        success: false,
        error: "User authentication required with business_id"
      });
    }

    // Generate SQL query from NLP
    const sqlQuery = await generateSQLFromNLP(nlpQuery, user);

    // Create new segment
    const segmentId = uuidv4();
    const now = new Date().toISOString();

    // Insert into segmentation table
    const { error: segmentError } = await supabase
      .from('segmentation')
      .insert({
        segment_id: segmentId,
        segment_name: segmentName,
        description: description,
        business_id: user.business_id,
        created_by_user_id: user.user_id,
        created_at: now,
        updated_at: now,
        status: 'active',
        filter_criteria: {
          nlp_query: nlpQuery,
          sql_query: sqlQuery
        },
        dataset: 'customers'
      });

    if (segmentError) {
      logger.error('Error creating segment:', { error: segmentError });
      throw segmentError;
    }

    // Execute the query to get matching customers
    const { data: result, error: queryError } = await supabase.rpc('execute_dynamic_query', {
      query_text: sqlQuery
    });

    if (queryError) {
      logger.error('Query execution error:', { error: queryError, sqlQuery });
      throw queryError;
    }

    // Extract customers from the JSON array result
    const customers = result[0] || [];

    // Insert matching customers into segment_customers table
    const segmentCustomers = customers.map(customer => ({
      customer_id: customer.customer_id,
      segment_id: segmentId,
      assigned_at: now
    }));

    const { error: customersError } = await supabase
      .from('segment_customers')
      .insert(segmentCustomers);

    if (customersError) {
      logger.error('Error inserting segment customers:', { error: customersError });
      throw customersError;
    }

    logger.info('Segmentation created successfully', { 
      segmentId, 
      customerCount: customers.length 
    });

    res.json({
      success: true,
      message: "Segmentation created successfully",
      data: {
        segment_id: segmentId,
        customer_count: customers.length
      }
    });
  } catch (error) {
    logger.error('Error in create segmentation:', { error });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

export {
  previewSegmentation,
  createSegmentationFromNLP
}; 