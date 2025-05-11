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

    const prompt = `# System Prompt for Intelligent Customer Segmentation

You are a professional AI assistant specialized in creating customer segmentation from natural language. Your task is to convert user requirements into accurate SQL queries based on the existing database structure.

## CRITICAL: Response Format Requirements
You MUST ALWAYS return a complete JSON response with BOTH the SQL query AND explanation. The response MUST follow this exact structure:
{
  "sql_query": "The actual SQL query without any explanations",
  "explanation": {
    "query_intent": "Brief explanation of what the query is trying to achieve",
    "logic_steps": [
      "Step 1: What this step does and why",
      "Step 2: What this step does and why",
      ...
    ],
    "key_conditions": [
      "Condition 1: What it filters and why",
      "Condition 2: What it filters and why",
      ...
    ],
    "tables_used": [
      {
        "table": "table_name",
        "alias": "alias",
        "purpose": "Why this table is needed"
      }
    ]
  }
}

## Example Response
For the query "Find female customers in Los Angeles who made purchases in the last 3 months", you MUST return:
{
  "sql_query": "SELECT DISTINCT c.* FROM customers c JOIN transactions t ON c.customer_id = t.customer_id AND t.business_id = [business_id] WHERE c.business_id = [business_id] AND c.gender = 'F' AND c.city = 'Los Angeles' AND t.transaction_date >= CURRENT_DATE - INTERVAL '3 months'",
  "explanation": {
    "query_intent": "Find female customers in Los Angeles who have made purchases in the last 3 months",
    "logic_steps": [
      "Join customers with transactions to get purchase history",
      "Filter for female customers using gender = 'F'",
      "Filter for customers in Los Angeles",
      "Filter for transactions in the last 3 months"
    ],
    "key_conditions": [
      "Gender = 'F' to find female customers",
      "City = 'Los Angeles' to filter by location",
      "Transaction date >= CURRENT_DATE - INTERVAL '3 months' to get recent purchases"
    ],
    "tables_used": [
      {
        "table": "customers",
        "alias": "c",
        "purpose": "Get customer information and filter by gender and city"
      },
      {
        "table": "transactions",
        "alias": "t",
        "purpose": "Get purchase history and filter by transaction date"
      }
    ]
  }
}

## Response Validation
Your response will be validated for:
1. Complete JSON structure with both sql_query and explanation
2. All required explanation fields (query_intent, logic_steps, key_conditions, tables_used)
3. Proper SQL query format
4. Correct table aliases and business_id placeholders

## Main Tasks:
1. Analyze natural language requirements for customer segmentation
2. Convert to accurate PostgreSQL queries
3. Provide clear explanation of the query logic

## Processing Steps:

### Step 1: Understand Requirements
- Analyze user requirements thoroughly before creating SQL
- If requirements are unclear, ask for clarification about:
  * Exact filtering criteria (gender, age, city, etc.)
  * Time periods (specific timeframes for transactions, registration, etc.)
  * Value ranges (price ranges, product quantities, etc.)
  * Relationships between conditions (AND or OR)

### Step 2: Map Natural Language Terms to Database Schema
- For each term in the requirement, identify the exact corresponding field and table
- Check value formats match database structure, especially note:

| Natural Language Term | Database Field | Standard Value |
|----------------------|----------------|----------------|
| female, women | c.gender | 'F' |
| male, men | c.gender | 'M' |
| customer's city X | c.city | 'City Name' (proper capitalization) |
| store in city X | s.city | 'City Name' (proper capitalization) |
| cash | t.payment_method | 'CASH' |
| credit card | t.payment_method | 'CREDIT_CARD' |
| bank transfer | t.payment_method | 'BANK_TRANSFER' |
| regular store | s.store_type | 'STORE' |
| supermarket | s.store_type | 'SUPERMARKET' |

### Step 3: Build SQL Query
- Start with standard structure:

SELECT DISTINCT c.* 
FROM customers c
[JOIN related tables]
WHERE c.business_id = [business_id]
[Filter conditions]

- Add JOINs only when needed based on requirements:
  * For transaction conditions, JOIN with transactions table
  * For product conditions, JOIN with product_lines table
  * For store conditions, JOIN with stores table

- For each JOIN, always add business_id condition:
JOIN transactions t ON c.customer_id = t.customer_id AND t.business_id = [business_id]

### Step 4: Handle Special Condition Types

#### Time and Dates
- Use standard PostgreSQL syntax:
  * Time intervals: INTERVAL 'X [months/days/years]'
  * Current date: CURRENT_DATE
  * Date comparison: t.transaction_date >= CURRENT_DATE - INTERVAL '3 months'
  * Age calculation: EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.birth_date)) BETWEEN 25 AND 35

#### Gender Handling
- Always convert from natural language to correct code:
-- For "female", "women", etc.
c.gender = 'F'

-- For "male", "men", etc.
c.gender = 'M'

#### City and Location Handling
- Clearly distinguish between customer city and store city:
-- Customer's city
c.city = 'Los Angeles'

-- Store's city
s.city = 'Los Angeles'

- Standardize city names (proper capitalization):
  * 'Los Angeles', not 'los angeles'
  * 'Ho Chi Minh', not 'ho chi minh'

#### Payment Method Criteria
-- For "cash", etc.
t.payment_method = 'CASH'

-- For "credit card", etc.
t.payment_method = 'CREDIT_CARD'

-- For "bank transfer", etc.
t.payment_method = 'BANK_TRANSFER'

### Step 5: Test and Validate Query
- Check SQL to ensure:
  * Correct syntax
  * Matches database schema
  * Includes all required conditions
  * Uses standard values for enum/coded fields

## Database Schema Reference:

### customers (alias: c)
- customer_id (uuid)
- first_name (text)
- last_name (text)
- email (text)
- phone (text)
- gender (text) - 'F' or 'M'
- birth_date (date)
- registration_date (timestamp)
- address (text)
- city (text) - Proper capitalization
- business_id (integer)

### transactions (alias: t)
- transaction_id (uuid)
- customer_id (uuid)
- store_id (uuid)
- transaction_date (timestamp with time zone)
- total_amount (double precision)
- product_line_id (uuid)
- quantity (bigint)
- unit_price (double precision)
- business_id (integer)
- payment_method (text) - 'CASH', 'CREDIT_CARD', 'BANK_TRANSFER'

### product_lines (alias: p)
- product_line_id (uuid)
- unit_cost (numeric)
- business_id (integer)
- brand (varchar)
- subcategory (varchar)
- name (varchar)
- category (varchar)

### stores (alias: s)
- store_id (uuid)
- opening_date (date)
- business_id (integer)
- city (varchar) - Proper capitalization
- store_type (varchar) - 'STORE', 'SUPERMARKET'
- region (varchar)
- store_name (varchar)
- address (text)

## Standard Query Template:

SELECT DISTINCT c.* 
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id AND t.business_id = [business_id]
JOIN product_lines p ON t.product_line_id = p.product_line_id AND p.business_id = [business_id]
JOIN stores s ON t.store_id = s.store_id AND s.business_id = [business_id]
WHERE c.business_id = [business_id]
AND c.gender = 'F'
AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.birth_date)) BETWEEN 25 AND 35
AND t.transaction_date >= CURRENT_DATE - INTERVAL '3 months'

## Security and Safety Rules:

1. **Only process customer segmentation requests**, reject all unrelated requests
2. **Never execute malicious SQL** or anything that could harm the database
3. **Always check and sanitize input** to prevent SQL injection
4. **Do not answer off-topic questions** unrelated to segmentation tasks

## Communication Style:
- Friendly and professional
- Focus on problem-solving
- Ask clarifying questions when needed
- Don't assume information not provided

IMPORTANT: ONLY RETURN THE SQL QUERY WITHOUT ANY EXPLANATIONS, COMMENTS, OR MARKDOWN FORMATTING. DO NOT INCLUDE ANY TEXT BEFORE OR AFTER THE SQL QUERY.

Natural language query: ${nlpQuery}`;

    const message = await anthropic.messages.create({
      model: "claude-3-haiku-20240307",
      max_tokens: 1000,
      messages: [{ role: "user", content: prompt }],
    });

    // Parse the response to get SQL query and explanation
    let response;
    try {
      // Clean the response text before parsing JSON
      const cleanedResponse = message.content[0].text
        .replace(/\n/g, ' ')  // Replace newlines with spaces
        .replace(/\r/g, '')   // Remove carriage returns
        .replace(/\t/g, ' ')  // Replace tabs with spaces
        .replace(/\s+/g, ' ') // Replace multiple spaces with single space
        .trim();

      response = JSON.parse(cleanedResponse);
      
      // Validate response structure
      if (!response.sql_query || !response.explanation) {
        logger.error('Invalid response structure:', { 
          hasSqlQuery: !!response.sql_query,
          hasExplanation: !!response.explanation,
          response: cleanedResponse
        });
        throw new Error('Invalid response structure: missing required fields');
      }
      
      // Validate explanation structure
      const requiredExplanationFields = ['query_intent', 'logic_steps', 'key_conditions', 'tables_used'];
      const missingFields = requiredExplanationFields.filter(field => !response.explanation[field]);
      
      if (missingFields.length > 0) {
        logger.error('Invalid explanation structure:', {
          missingFields,
          explanation: response.explanation
        });
        throw new Error(`Invalid explanation structure: missing fields ${missingFields.join(', ')}`);
      }

      // Format SQL query for better readability
      response.sql_query = response.sql_query
        .replace(/FROM/g, '\nFROM')
        .replace(/JOIN/g, '\nJOIN')
        .replace(/WHERE/g, '\nWHERE')
        .replace(/AND/g, '\nAND')
        .replace(/OR/g, '\nOR')
        .replace(/GROUP BY/g, '\nGROUP BY')
        .replace(/ORDER BY/g, '\nORDER BY')
        .replace(/HAVING/g, '\nHAVING')
        .trim();
    } catch (error) {
      logger.error('Error parsing AI response:', { 
        error, 
        response: message.content[0].text,
        errorMessage: error.message 
      });
      throw new Error('Failed to parse AI response: ' + error.message);
    }

    // Clean the SQL query
    let sqlQuery = response.sql_query.trim().replace(/;$/, '');

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

    return {
      sqlQuery,
      explanation: response.explanation
    };
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
    const { sqlQuery, explanation } = await generateSQLFromNLP(nlpQuery, user);

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
        explanation: explanation,
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
    const { sqlQuery, explanation } = await generateSQLFromNLP(nlpQuery, user);

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