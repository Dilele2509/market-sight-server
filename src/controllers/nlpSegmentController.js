import { getSupabase, logger } from '../data/database.js';
import Anthropic from '@anthropic-ai/sdk';
import { valueStandardizationService } from '../services/valueStandardizationService.js';
import { generateFilterCriteria } from '../services/filterCriteriaService.js';
import { OPERATORS, EVENT_CONDITION_TYPES, FREQUENCY_OPTIONS, TIME_PERIOD_OPTIONS } from '../constants/operators.js';

const anthropic = new Anthropic({
  apiKey: process.env.CLAUDE_API_KEY,
});

// Function to generate SQL query from natural language using Claude
const generateSQLFromNLP = async (nlpQuery, user) => {
  try {
    const supabase = getSupabase();
    logger.info('Generating SQL from NLP query', { nlpQuery });

    // Prepare operator references for the prompt
    const textOperators = OPERATORS.text.map(op => `${op.value}: "${op.label}"`).join(', ');
    const numberOperators = OPERATORS.number.map(op => `${op.value}: "${op.label}"`).join(', ');
    const datetimeOperators = OPERATORS.datetime.map(op => `${op.value}: "${op.label}"`).join(', ');
    const booleanOperators = OPERATORS.boolean.map(op => `${op.value}: "${op.label}"`).join(', ');
    const eventConditionTypes = EVENT_CONDITION_TYPES.map(op => `${op.value}: "${op.label}"`).join(', ');
    const frequencyOptions = FREQUENCY_OPTIONS.map(op => `${op.value}: "${op.label}"`).join(', ');
    const timePeriodOptions = TIME_PERIOD_OPTIONS.map(op => `${op.value}: "${op.label}"`).join(', ');

    // Add security context to the prompt
    const prompt = `# System Prompt for Intelligent Customer Segmentation

You are a professional AI assistant specialized in creating customer segmentation from natural language. Your task is to convert user requirements into accurate SQL queries based on the existing database structure.

## SECURITY RULES - STRICTLY ENFORCED
1. You MUST ONLY generate READ-ONLY queries (such as SELECT or WITH CTEs)
2. You MUST NEVER generate data-modifying queries:
   - DELETE queries
   - UPDATE queries
   - INSERT queries
   - UPSERT queries
   - DROP queries
   - ALTER queries
   - CREATE queries
   - TRUNCATE queries
3. If the user requests data modification, respond with:
   "I can only help you create customer segments by retrieving information. I cannot modify or delete any data. Please use the segmentation feature to analyze your customer data."

## CRITICAL: PostgreSQL Constraints
1. When using SELECT DISTINCT, any ORDER BY expressions must appear in the SELECT list
2. When using GROUP BY, any column in the SELECT list that is not in an aggregate function must be in the GROUP BY clause
3. If you need to ORDER BY with aggregates when using DISTINCT, use a subquery or CTE approach

## Examples of Correct Pattern for ORDER BY with DISTINCT:
INCORRECT:
\`\`\`sql
SELECT DISTINCT c.* 
FROM customers c
JOIN transactions t ON c.customer_id = t.customer_id
ORDER BY SUM(t.total_amount) DESC
\`\`\`

CORRECT:
\`\`\`sql
WITH customer_totals AS (
  SELECT c.customer_id, SUM(t.total_amount) as total_spent
  FROM customers c
  JOIN transactions t ON c.customer_id = t.customer_id
  GROUP BY c.customer_id
)
SELECT c.*
FROM customers c
JOIN customer_totals ct ON c.customer_id = ct.customer_id
ORDER BY ct.total_spent DESC
\`\`\`

## CRITICAL: Response Format Requirements
You MUST ALWAYS return a complete JSON response with the SQL query, explanation, AND filter operators. The response MUST follow this exact structure:
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
  },
  "filter_criteria": {
    "type": "group",
    "logic_operator": "AND",
    "conditions": [
      {
        "dataset": "customers",
        "field": "gender",
        "operator": "equals",
        "value": "F"
      },
      {
        "dataset": "customers",
        "field": "city",
        "operator": "equals",
        "value": "Los Angeles"
      },
      {
        "type": "event",
        "event_name": "purchase",
        "event_condition_type": "performed",
        "frequency": {
          "operator": "at_least",
          "value": 1
        },
        "time_period": {
          "unit": "months",
          "value": 3
        }
      }
      // Add all conditions detected in the query
    ]
  }
}

## Example Response
For the query "Find female customers in Los Angeles who made purchases in the last 3 months", you MUST return:
{
  "sql_query": "SELECT DISTINCT c.* FROM customers c JOIN transactions t ON c.customer_id = t.customer_id AND t.business_id = [business_id] WHERE c.business_id = [business_id] AND c.gender = 'F' AND c.city = 'Los Angeles' AND t.transaction_date >= CURRENT_DATE - INTERVAL '3 months'",
  "explanation": {
    "query_intent": "Find female customers in Los Angeles who made purchases in the last 3 months",
    "logic_steps": [
      "Select distinct customer records to avoid duplicates",
      "Join with transactions to check purchase history",
      "Filter customers by gender = F (female)",
      "Filter customers by city = Los Angeles",
      "Filter transactions from the last 3 months",
      "Ensure results are scoped to the current business"
    ],
    "key_conditions": [
      "Gender = F to find female customers",
      "City = Los Angeles to filter by location",
      "Transaction date >= CURRENT_DATE - INTERVAL '3 months' to find recent purchases",
      "Business ID filter to ensure data isolation"
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
        "purpose": "Check purchase history within timeframe"
      }
    ]
  },
  "filter_criteria": {
    "type": "group",
    "logic_operator": "AND",
    "conditions": [
      {
        "dataset": "customers",
        "field": "gender",
        "operator": "equals",
        "value": "F"
      },
      {
        "dataset": "customers",
        "field": "city",
        "operator": "equals",
        "value": "Los Angeles"
      },
      {
        "type": "event",
        "event_name": "purchase",
        "event_condition_type": "performed",
        "frequency": {
          "operator": "at_least",
          "value": 1
        },
        "time_period": {
          "unit": "months",
          "value": 3
        }
      }
    ]
  }
}

## Filter Criteria Format Specification
When generating the filter_criteria object, use the following formats and operators:

### Text Field Operators (Use exactly these operator values):
${textOperators}

### Number Field Operators (Use exactly these operator values):
${numberOperators}

### Date/Time Field Operators (Use exactly these operator values):
${datetimeOperators}

### Boolean Field Operators (Use exactly these operator values):
${booleanOperators}

### Event Condition Types (Use exactly these values):
${eventConditionTypes}

### Frequency Options (Use exactly these values):
${frequencyOptions}

### Time Period Options (Use exactly these values):
${timePeriodOptions}

### For simple attribute conditions:
{
  "dataset": "[table name: customers, transactions, product_lines, stores]",
  "field": "[field name]",
  "operator": "[use exact operator value from lists above]",
  "value": "[value]",
  "value2": "[second value, only for 'between' operator]"
}

### For purchase events:
{
  "type": "event",
  "event_name": "purchase",
  "event_condition_type": "[use exact value from event condition types list]",
  "frequency": {
    "operator": "[use exact value from frequency options list]",
    "value": "[number]"
  },
  "time_period": {
    "unit": "[use exact value from time period options list]",
    "value": "[number]"
  }
}

### For purchase amount conditions:
{
  "type": "event",
  "event_name": "purchase",
  "event_condition_type": "amount",
  "operator": "[use exact operator value from number operators list]",
  "value": "[amount]",
  "value2": "[second amount, only for 'between' operator]"
}

### For age calculations:
{
  "dataset": "customers",
  "field": "birth_date",
  "operator": "age_between",
  "value": "[min age]",
  "value2": "[max age]"
}

## Response Validation
Your response will be validated for:
1. Complete JSON structure with sql_query, explanation, and filter_criteria
2. All required explanation fields (query_intent, logic_steps, key_conditions, tables_used)
3. Proper SQL query format
4. Correct table aliases and business_id placeholders
5. Complete filter_criteria structure representing all conditions in the query
6. Use of exact operator values as specified in the lists above

## Main Tasks:
1. Analyze natural language requirements for customer segmentation
2. Convert to accurate PostgreSQL queries
3. Extract structured filter criteria that can be used programmatically
4. Provide clear explanation of the query logic

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
| female, women         | c.gender       | 'F'            |
| male, men             | c.gender       | 'M'            |
| customer's city X     | c.city         | 'City Name' (proper capitalization) |
| store in city X       | s.city         | 'City Name' (proper capitalization) |
| cash                  | t.payment_method | 'CASH'       |
| credit card           | t.payment_method | 'CREDIT_CARD'   |
| bank transfer         | t.payment_method | 'BANK_TRANSFER' |
| regular store         | s.store_type   | 'STORE'        |
| supermarket           | s.store_type   | 'SUPERMARKET'  |

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
      const responseText = message.content[0].text.trim();
      const cleanedResponse = responseText
        .replace(/\n/g, ' ')
        .replace(/\r/g, '')
        .replace(/\t/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

      // Check if the response starts with a curly brace (JSON) or is plain text
      const isJsonResponse = responseText.trim().startsWith('{');
      
      // Handle plain text responses
      if (!isJsonResponse) {
        logger.info('Received plain text response from AI', { response: responseText });
        return {
          isRejected: true,
          message: responseText
        };
      }

      // Check if the response is a rejection message
      if (cleanedResponse.includes("I can only help you create customer segments") ||
          cleanedResponse.includes("I cannot modify or delete any data")) {
        logger.info('AI rejected dangerous operation', { response: cleanedResponse });
        return {
          isRejected: true,
          message: cleanedResponse
        };
      }

      try {
        response = JSON.parse(cleanedResponse);
      } catch (parseError) {
        logger.error('Failed to parse AI response as JSON:', { 
          response: cleanedResponse,
          error: parseError.message 
        });
        
        // Return the original text response when JSON parsing fails
        return {
          isRejected: true,
          message: responseText || "The AI couldn't generate a proper query. Please try rephrasing your question to focus on customer segmentation."
        };
      }
      
      // Validate response structure
      if (!response.sql_query || !response.explanation || !response.filter_criteria) {
        logger.error('Invalid response structure:', { 
          hasSqlQuery: !!response.sql_query,
          hasExplanation: !!response.explanation,
          hasFilterCriteria: !!response.filter_criteria,
          response: cleanedResponse
        });
        throw new Error('Invalid response structure: missing required fields');
      }

      // Use filter criteria service to standardize values and handle operators
      // The service will use the filter_criteria from Claude as a base and enhance it
      const enhancedFilterCriteria = await valueStandardizationService.standardizeFilterCriteria(response.filter_criteria, user);

      // Security validation - ensure read-only queries
      const upperSqlQuery = response.sql_query.trim().toUpperCase();
      if (!upperSqlQuery.startsWith('SELECT') && !upperSqlQuery.startsWith('WITH')) {
        logger.error('Security violation: Non-read-only query detected', {
          query: response.sql_query,
          user: user.user_id
        });
        throw new Error('Only read-only queries (SELECT or WITH) are allowed for customer segmentation');
      }

      // Additional security checks for data modification operations
      const dangerousKeywords = [
        'DELETE', 'UPDATE', 'INSERT', 'UPSERT', 'DROP', 'ALTER', 'TRUNCATE',
        'CREATE', 'MODIFY', 'REMOVE', 'REPLACE'
      ];
      
      const containsDangerousKeyword = dangerousKeywords.some(keyword => {
        // Check for dangerous keywords as full words, not as parts of other words
        const regex = new RegExp(`\\b${keyword}\\b`, 'i');
        return regex.test(upperSqlQuery);
      });

      if (containsDangerousKeyword) {
        logger.error('Security violation: Dangerous keyword detected', {
          query: response.sql_query,
          user: user.user_id
        });
        throw new Error('Query contains dangerous operations. Only read-only queries are allowed.');
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
        .replace(/WITH/g, '\nWITH')
        .trim();

      // Clean the SQL query
      let finalSqlQuery = response.sql_query.trim().replace(/;$/, '');

      // Check if query is using DISTINCT with an ORDER BY or aggregate function
      const hasDistinct = finalSqlQuery.toUpperCase().includes("SELECT DISTINCT");
      const hasOrderBy = finalSqlQuery.toUpperCase().includes("ORDER BY");
      const hasAggregate = /SUM\(|AVG\(|MAX\(|MIN\(|COUNT\(/.test(finalSqlQuery.toUpperCase());
      
      // If it has DISTINCT and either ORDER BY or aggregates, verify if using CTE/subquery approach
      if (hasDistinct && (hasOrderBy || hasAggregate)) {
        const hasCTE = finalSqlQuery.toUpperCase().includes("WITH ");
        const hasSubquery = finalSqlQuery.toUpperCase().includes(") AS ");
        
        // If not using CTE or subquery, transform the query to use CTE
        if (!hasCTE && !hasSubquery) {
          logger.info('Transforming query to use CTE for DISTINCT with ORDER BY/aggregates');
          
          // Extract ORDER BY clause
          const orderByMatch = finalSqlQuery.match(/ORDER BY\s+(.+?)(?:LIMIT|$)/i);
          const orderByClause = orderByMatch ? orderByMatch[1].trim() : '';
          
          // Remove ORDER BY from original query
          finalSqlQuery = finalSqlQuery.replace(/ORDER BY\s+(.+?)(?:LIMIT|$)/i, '');
          
          // Extract LIMIT clause if present
          const limitMatch = finalSqlQuery.match(/LIMIT\s+(\d+)/i);
          const limitClause = limitMatch ? `LIMIT ${limitMatch[1]}` : '';
          
          // Remove LIMIT from original query if present
          finalSqlQuery = finalSqlQuery.replace(/LIMIT\s+\d+/i, '');
          
          // Transform into CTE approach
          if (orderByClause) {
            finalSqlQuery = `WITH customer_data AS (
              ${finalSqlQuery}
            )
            SELECT * FROM customer_data
            ORDER BY ${orderByClause} ${limitClause}`;
          }
        }
      }

      // Replace [business_id] placeholder with actual business_id
      if (!user || !user.business_id) {
        logger.error('Business ID missing from user object', { user });
        throw new Error('Business ID is required for query execution');
      }
      
      finalSqlQuery = finalSqlQuery.replace(/\[business_id\]/g, user.business_id);

      // Add business_id filter if not present
      if (!finalSqlQuery.toLowerCase().includes('where')) {
        finalSqlQuery += ' WHERE c.business_id = ' + user.business_id;
      } else if (!finalSqlQuery.toLowerCase().includes('business_id')) {
        finalSqlQuery = finalSqlQuery.replace(/where/i, 'WHERE c.business_id = ' + user.business_id + ' AND ');
      }

      logger.info('Executing generated SQL query', { sqlQuery: finalSqlQuery });

      // Execute the query
      const { data: result, error } = await supabase.rpc('execute_dynamic_query', {
        query_text: finalSqlQuery
      });

      if (error) {
        logger.error('SQL Query Error:', { error, sqlQuery: finalSqlQuery });
        throw new Error('Failed to execute SQL query: ' + error.message);
      }

      return {
        isRejected: false,
        sqlQuery: finalSqlQuery,
        explanation: response.explanation,
        filter_criteria: enhancedFilterCriteria
      };
    } catch (error) {
      logger.error('Error parsing AI response:', { 
        error, 
        response: message.content[0].text,
        errorMessage: error.message 
      });
      
      // Return a friendly error message
      return {
        isRejected: true,
        message: "I couldn't understand your request. Please try rephrasing it to focus on customer segmentation criteria."
      };
    }
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
    const result = await generateSQLFromNLP(nlpQuery, user);

    // If the query was rejected, return the rejection message
    if (result.isRejected) {
      return res.json({
        success: false,
        error: result.message,
        isAIResponse: true // Flag to indicate this is a direct AI response
      });
    }

    // Transform filter criteria to storage format
    const storageFilterCriteria = transformFilterCriteriaForStorage(result.filter_criteria);

    // Execute the query to get matching customers
    const { data: queryResult, error: queryError } = await supabase.rpc('execute_dynamic_query', {
      query_text: result.sqlQuery
    });

    if (queryError) {
      logger.error('Query execution error:', { error: queryError, sqlQuery: result.sqlQuery });
      throw queryError;
    }

    // Safely extract customers from the JSON array result
    let customers = [];
    
    if (queryResult && Array.isArray(queryResult) && queryResult.length > 0) {
      // If queryResult[0] is an array, use it directly
      if (Array.isArray(queryResult[0])) {
        customers = queryResult[0] || [];
      } else {
        // If queryResult is just an array of objects directly, use that
        customers = queryResult;
      }
    }
    
    // Ensure customers is an array before mapping
    if (!Array.isArray(customers)) {
      logger.warn('Expected customers array but got:', { 
        type: typeof customers, 
        value: customers 
      });
      customers = [];
    }

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
        sqlQuery: result.sqlQuery,
        explanation: result.explanation,
        filter_criteria: result.filter_criteria,
        storage_filter_criteria: storageFilterCriteria, // Include the transformed filter criteria
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

// Function to transform Claude's filter criteria into storage format
const transformFilterCriteriaForStorage = (filterCriteria) => {
  try {
    // Initialize the storage structure
    const storageFormat = {
      size: 0,
      conditions: [],
      conditionGroups: [],
      rootOperator: filterCriteria.logic_operator || "AND"
    };

    // Helper function to validate and normalize operators based on field type
    const normalizeOperator = (operator, fieldType) => {
      // Default to text if field type not provided
      const type = fieldType || 'text';
      
      // If not a valid operator type, default to text
      const operatorList = OPERATORS[type] || OPERATORS.text;
      
      // Find exact match by value
      const exactMatch = operatorList.find(op => op.value === operator);
      if (exactMatch) return operator;
      
      // Find match by label (case insensitive)
      const labelMatch = operatorList.find(op => 
        op.label.toLowerCase() === (operator || '').toLowerCase()
      );
      if (labelMatch) return labelMatch.value;
      
      // Default to first operator in the list for the type
      return operatorList[0].value;
    };
    
    // Helper function to validate and normalize event condition types
    const normalizeEventConditionType = (conditionType) => {
      // Find exact match by value
      const exactMatch = EVENT_CONDITION_TYPES.find(type => type.value === conditionType);
      if (exactMatch) return conditionType;
      
      // Find match by label (case insensitive)
      const labelMatch = EVENT_CONDITION_TYPES.find(type => 
        type.label.toLowerCase() === (conditionType || '').toLowerCase()
      );
      if (labelMatch) return labelMatch.value;
      
      // Default to 'performed'
      return 'performed';
    };
    
    // Helper function to validate and normalize frequency options
    const normalizeFrequency = (frequency) => {
      // Find exact match by value
      const exactMatch = FREQUENCY_OPTIONS.find(opt => opt.value === frequency);
      if (exactMatch) return frequency;
      
      // Find match by label (case insensitive)
      const labelMatch = FREQUENCY_OPTIONS.find(opt => 
        opt.label.toLowerCase() === (frequency || '').toLowerCase()
      );
      if (labelMatch) return labelMatch.value;
      
      // Default to 'at_least'
      return 'at_least';
    };
    
    // Helper function to validate and normalize time period options
    const normalizeTimePeriod = (timePeriod) => {
      // Find exact match by value
      const exactMatch = TIME_PERIOD_OPTIONS.find(opt => opt.value === timePeriod);
      if (exactMatch) return timePeriod;
      
      // Find match by label (case insensitive)
      const labelMatch = TIME_PERIOD_OPTIONS.find(opt => 
        opt.label.toLowerCase() === (timePeriod || '').toLowerCase()
      );
      if (labelMatch) return labelMatch.value;
      
      // Default to 'days'
      return 'days';
    };

    // Process conditions
    if (filterCriteria.conditions && Array.isArray(filterCriteria.conditions)) {
      let conditionId = 1;
      let attributeConditionId = 1;

      // Map each condition to the storage format
      filterCriteria.conditions.forEach(condition => {
        // Determine field type for appropriate operator validation
        let fieldType = 'text'; // Default type
        if (condition.dataset) {
          if (condition.field) {
            // Handle common field types
            if (condition.field.includes('date') || condition.field.includes('time')) {
              fieldType = 'datetime';
            } else if (condition.field.includes('amount') || condition.field.includes('quantity') || 
                     condition.field.includes('price') || condition.field.includes('cost')) {
              fieldType = 'number';
            } else if (condition.field.includes('is_') || condition.field.includes('has_')) {
              fieldType = 'boolean';
            }
          }
        }

        if (condition.type === 'event') {
          // Handle event conditions (purchase, etc.)
          const eventCondition = {
            id: conditionId++,
            columnKey: condition.dataset || "customer_id",
            relatedColKey: condition.relatedColKey || "customer_id",
            type: "event",
            eventType: normalizeEventConditionType(condition.event_condition_type),
            operator: filterCriteria.logic_operator || "AND",
            chosen: false,
            selected: false,
            attributeConditions: [],
            relatedConditions: [],
            relatedAttributeConditions: []
          };

          // Add frequency and time period if available
          if (condition.frequency) {
            eventCondition.frequency = normalizeFrequency(condition.frequency.operator);
            eventCondition.count = condition.frequency.value || 1;
          }

          if (condition.time_period) {
            eventCondition.timePeriod = normalizeTimePeriod(condition.time_period.unit);
            eventCondition.timeValue = condition.time_period.value || 30;
          }

          // Add attribute conditions if any
          if (condition.operator && (condition.value !== undefined || condition.event_condition_type === 'amount')) {
            const attrCondition = {
              id: attributeConditionId++,
              field: condition.field || "total_amount",
              operator: normalizeOperator(condition.operator, 'number'),
              value: String(condition.value || ""),
              value2: condition.value2 ? String(condition.value2) : "",
              chosen: false,
              selected: false
            };
            
            eventCondition.attributeConditions.push(attrCondition);
            eventCondition.relatedAttributeConditions.push(attrCondition);
          } else if (condition.attributeConditions && Array.isArray(condition.attributeConditions)) {
            // Handle explicitly provided attribute conditions
            condition.attributeConditions.forEach(attr => {
              // Determine attribute field type
              let attrFieldType = 'text';
              if (attr.field) {
                if (attr.field.includes('date') || attr.field.includes('time')) {
                  attrFieldType = 'datetime';
                } else if (attr.field.includes('amount') || attr.field.includes('quantity') || 
                         attr.field.includes('price') || attr.field.includes('cost')) {
                  attrFieldType = 'number';
                } else if (attr.field.includes('is_') || attr.field.includes('has_')) {
                  attrFieldType = 'boolean';
                }
              }
              
              const attrCondition = {
                id: attributeConditionId++,
                field: attr.field,
                operator: normalizeOperator(attr.operator, attrFieldType),
                value: String(attr.value || ""),
                value2: attr.value2 ? String(attr.value2) : "",
                chosen: false,
                selected: false
              };
              
              eventCondition.attributeConditions.push(attrCondition);
              eventCondition.relatedAttributeConditions.push(attrCondition);
            });
          }

          storageFormat.conditions.push(eventCondition);
        } else if (condition.dataset && condition.field) {
          // Handle regular attribute conditions
          const attributeCondition = {
            id: conditionId++,
            columnKey: condition.field,
            datasetKey: condition.dataset,
            type: "attribute",
            operator: normalizeOperator(condition.operator, fieldType),
            value: String(condition.value || ""),
            value2: condition.value2 ? String(condition.value2) : "",
            logicOperator: filterCriteria.logic_operator || "AND",
            chosen: false,
            selected: false
          };
          
          storageFormat.conditions.push(attributeCondition);
        }
      });

      // Handle condition groups if present
      if (filterCriteria.condition_groups && Array.isArray(filterCriteria.condition_groups)) {
        filterCriteria.condition_groups.forEach((group, index) => {
          const conditionGroup = {
            id: index + 1,
            operator: group.operator || "AND",
            conditions: []
          };
          
          if (group.conditions && Array.isArray(group.conditions)) {
            group.conditions.forEach(condition => {
              // Determine field type for operator validation
              let fieldType = 'text';
              if (condition.field) {
                if (condition.field.includes('date') || condition.field.includes('time')) {
                  fieldType = 'datetime';
                } else if (condition.field.includes('amount') || condition.field.includes('quantity') || 
                         condition.field.includes('price') || condition.field.includes('cost')) {
                  fieldType = 'number';
                } else if (condition.field.includes('is_') || condition.field.includes('has_')) {
                  fieldType = 'boolean';
                }
              }
              
              // Process each condition in the group
              conditionGroup.conditions.push({
                id: conditionId++,
                columnKey: condition.field,
                datasetKey: condition.dataset,
                type: condition.type || "attribute",
                operator: normalizeOperator(condition.operator, fieldType),
                value: String(condition.value || ""),
                value2: condition.value2 ? String(condition.value2) : "",
                logicOperator: group.operator || "AND",
                chosen: false,
                selected: false
              });
            });
          }
          
          storageFormat.conditionGroups.push(conditionGroup);
        });
      }
    }

    // Set size to total number of conditions across all groups
    storageFormat.size = storageFormat.conditions.length + 
      storageFormat.conditionGroups.reduce((sum, group) => sum + (group.conditions ? group.conditions.length : 0), 0);

    return storageFormat;
  } catch (error) {
    logger.error('Error transforming filter criteria:', { error });
    // Return a minimal valid structure if there's an error
    return {
      size: 0,
      conditions: [],
      conditionGroups: [],
      rootOperator: "AND"
    };
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
    const nlpResult = await generateSQLFromNLP(nlpQuery, user);

    // If the query was rejected, return the rejection message
    if (nlpResult.isRejected) {
      return res.json({
        success: false,
        error: nlpResult.message,
        isAIResponse: true // Flag to indicate this is a direct AI response
      });
    }

    // Create new segment
    const segmentNameSlug = segmentName
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-') // Replace any non-alphanumeric characters with hyphens
      .replace(/^-+|-+$/g, ''); // Remove leading and trailing hyphens
    const segmentId = `segment:${segmentNameSlug}`;
    const now = new Date().toISOString();

    // Transform filter criteria to storage format
    const storageFilterCriteria = transformFilterCriteriaForStorage(nlpResult.filter_criteria);

    // Insert into segmentation table
    const { error: segmentError } = await supabase
      .from('segmentation')
      .insert({
        segment_id: segmentId,
        segment_name: segmentName,
        description: description || '',
        business_id: user.business_id,
        created_by_user_id: user.user_id,
        created_at: now,
        updated_at: now,
        status: 'active',
          // nlp_query: nlpQuery,
          // sql_query: nlpResult.sqlQuery,
          filter_criteria: storageFilterCriteria,  // Add transformed filter criteria
        dataset: 'customers'
      });

    if (segmentError) {
      logger.error('Error creating segment:', { error: segmentError });
      throw segmentError;
    }

    // Execute the query to get matching customers
    const { data: queryResult, error: queryError } = await supabase.rpc('execute_dynamic_query', {
      query_text: nlpResult.sqlQuery
    });

    if (queryError) {
      logger.error('Query execution error:', { error: queryError, sqlQuery: nlpResult.sqlQuery });
      throw queryError;
    }

    // Safely extract customers from the JSON array result
    let customers = [];
    
    if (queryResult && Array.isArray(queryResult) && queryResult.length > 0) {
      // If queryResult[0] is an array, use it directly
      if (Array.isArray(queryResult[0])) {
        customers = queryResult[0] || [];
      } else {
        // If queryResult is just an array of objects directly, use that
        customers = queryResult;
      }
    }
    
    // Ensure customers is an array before proceeding
    if (!Array.isArray(customers)) {
      logger.warn('Expected customers array but got:', { 
        type: typeof customers, 
        value: customers 
      });
      customers = [];
    }

    // Only insert if we have customers
    if (customers.length > 0) {
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
        customer_count: customers.length,
        filter_criteria: storageFilterCriteria  // Return the transformed filter criteria in the response
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