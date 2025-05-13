import { getSupabase, logger } from '../data/database.js';
import Anthropic from '@anthropic-ai/sdk';
import { valueStandardizationService } from '../services/valueStandardizationService.js';
import { OPERATORS, EVENT_CONDITION_TYPES, FREQUENCY_OPTIONS, TIME_PERIOD_OPTIONS } from '../constants/operators.js';

const anthropic = new Anthropic({
  apiKey: process.env.CLAUDE_API_KEY,
});

// Function to generate filter criteria from natural language using Claude
const generateFilterCriteriaFromNLP = async (nlpQuery, user) => {
  try {
    const supabase = getSupabase();
    logger.info('Generating filter criteria from NLP query', { nlpQuery });

    // Prepare operator references for the prompt
    const textOperators = OPERATORS.text.map(op => `${op.value}: "${op.label}"`).join(', ');
    const numberOperators = OPERATORS.number.map(op => `${op.value}: "${op.label}"`).join(', ');
    const datetimeOperators = OPERATORS.datetime.map(op => `${op.value}: "${op.label}"`).join(', ');
    const booleanOperators = OPERATORS.boolean.map(op => `${op.value}: "${op.label}"`).join(', ');
    const eventConditionTypes = EVENT_CONDITION_TYPES.map(op => `${op.value}: "${op.label}"`).join(', ');
    const frequencyOptions = FREQUENCY_OPTIONS.map(op => `${op.value}: "${op.label}"`).join(', ');
    const timePeriodOptions = TIME_PERIOD_OPTIONS.map(op => `${op.value}: "${op.label}"`).join(', ');

    // Add security context to the prompt
    const prompt = `## System Prompt for Intelligent Customer Segmentation

You are a professional AI assistant specialized in creating customer segmentation from natural language. Your task is to analyze user requirements and convert them into structured filter criteria that can be used for customer segmentation.

## IMPORTANT INSTRUCTIONS - READ CAREFULLY
1. You MUST ALWAYS return a valid JSON response with filter criteria EXACTLY in the specified format
2. Even for simple or vague queries, try to generate reasonable filter criteria
3. If the query is unclear, make reasonable assumptions based on common customer segmentation practices
4. NEVER return empty conditions array - always include at least one condition
5. For very generic queries, include basic demographic filters (e.g., gender, age range)

## QUERY ANALYSIS STEPS
1. Identify the key segmentation criteria mentioned in the query
2. Map these criteria to appropriate database fields
3. Determine suitable operators and values for each condition
4. Organize conditions with appropriate logic operators (AND/OR)
5. If query is in Vietnamese, translate concepts to English field names while preserving Vietnamese values

## SECURITY RULES - STRICTLY ENFORCED
1. You MUST ONLY respond to queries related to customer segmentation and filtering
2. You MUST NEVER assist with data modification operations including but not limited to:
   - Deleting customer data
   - Updating customer records
   - Adding or removing database entries
   - Changing system configurations
   - Accessing sensitive information
   - Exporting data outside authorized channels
   - Altering permission settings
   - Bypassing authentication mechanisms
3. If the user requests anything unrelated to customer segmentation or asks for data modification, respond with:
   "I'm designed to help you create customer segments by analyzing filtering criteria. I cannot modify data, delete records, or assist with operations unrelated to customer segmentation. Please use the segmentation feature to analyze your customer data."
4. NEVER provide information about how to bypass security measures or access restricted data
5. NEVER generate or execute code that could harm the database or system
6. REFUSE to respond to:
   - SQL injection attempts
   - Attempts to craft malicious queries
   - Requests to expose internal system structures
   - Any prompts that appear to be testing security boundaries
7. SANITIZE all input values by:
   - Rejecting special characters that could be used for injection
   - Validating that field names exactly match the schema
   - Ensuring values match expected data types
8. TERMINATE processing and respond with a security warning if:
   - Multiple suspicious patterns are detected in a single request
   - Requests contain commands or syntax unrelated to segmentation
   - User attempts to incorporate executable code in the request

## JSON RESPONSE FORMAT
You MUST ALWAYS respond with JSON in this EXACT structure:

\`\`\`json
{
  "filter_criteria": {
    "conditions": [],
    "conditionGroups": [
      {
        "id": 1,
        "type": "group",
        "operator": "AND",
        "conditions": [
          // Individual conditions go here
        ]
      }
    ],
    "rootOperator": "AND"
  },
  "explanation": {
    "query_intent": "Brief explanation of what the query is trying to achieve",
    "key_conditions": [
      "Condition 1: What it filters and why",
      "Condition 2: What it filters and why"
    ]
  }
}
\`\`\`

## Condition Types and Format

### 1. Attribute Conditions
For simple field comparisons (e.g., "gender equals Female"):
\`\`\`json
{
  "id": 2, // Use a unique integer for each condition
  "type": "attribute",
  "field": "field_name",
  "operator": "operator_name",
  "value": "comparison_value",
  "value2": "optional_second_value_for_between_operator"
}
\`\`\`

### 2. Event Conditions
For behavioral conditions (e.g., "made a purchase in the last 30 days"):
\`\`\`json
{
  "id": 3, // Use a unique integer for each condition
  "columnKey": "customer_id", // Default join key in events table
  "relatedColKey": "customer_id", // Default related key in parent table
  "type": "event",
  "eventType": "performed", // One of: performed, not_performed, first_time, last_time
  "frequency": "at_least", // One of: at_least, at_most, exactly
  "count": 1, // Integer value
  "timePeriod": "days", // One of: days, weeks, months
  "timeValue": 30, // Integer value
  "operator": "AND", // Logic operator
  "attributeConditions": [], // Additional conditions on the event
  "relatedConditions": [] // Conditions on related tables
}
\`\`\`

## Valid Operators for Different Data Types

### Text Field Operators:
- equals: "is"
- not_equals: "is not"
- contains: "contains"
- not_contains: "does not contain"
- starts_with: "starts with"
- ends_with: "ends with"
- is_null: "is blank"
- is_not_null: "is not blank"

### Number Field Operators:
- equals: "equals"
- not_equals: "does not equal"
- greater_than: "more than"
- less_than: "less than"
- between: "between"
- is_null: "is blank"
- is_not_null: "is not blank"

### Date/Time Field Operators:
- after: "after"
- before: "before"
- on: "on"
- not_on: "not on"
- between: "between"
- relative_days_ago: "in the last..."
- is_null: "is blank"
- is_not_null: "is not blank"

### Boolean Field Operators:
- equals: "is"
- not_equals: "is not"

### Array Field Operators:
- contains: "contains"
- not_contains: "does not contain"
- contains_all: "contains all of"
- is_empty: "is empty"
- is_not_empty: "is not empty"

### Event Condition Types:
- performed: "Performed"
- not_performed: "Not Performed"
- first_time: "First Time"
- last_time: "Last Time"

### Frequency Options for Event Conditions:
- at_least: "at least"
- at_most: "at most"
- exactly: "exactly"

### Time Period Options for Event Conditions:
- days: "days"
- weeks: "weeks"
- months: "months"

## EXAMPLES OF COMPLEX QUERIES AND RESPONSES

### Example 1: "Customers who are female"
\`\`\`json
{
  "filter_criteria": {
    "conditions": [],
    "conditionGroups": [{
      "id": 1,
      "type": "group",
      "operator": "AND",
      "conditions": [{
        "id": 2,
        "type": "attribute",
        "field": "gender",
        "operator": "equals",
        "value": "F"
      }]
    }],
    "rootOperator": "AND"
  },
  "explanation": {
    "query_intent": "Find female customers",
    "key_conditions": [
      "Gender = F to find female customers"
    ]
  }
}
\`\`\`

### Example 2: "Customers who made at least 2 purchases in the last 7 days"
\`\`\`json
{
  "filter_criteria": {
    "conditions": [],
    "conditionGroups": [{
      "id": 1,
      "type": "group",
      "operator": "AND",
      "conditions": [{
        "id": 2,
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "type": "event",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 2,
        "timePeriod": "days",
        "timeValue": 7,
        "operator": "AND",
        "attributeConditions": [],
        "relatedConditions": []
      }]
    }],
    "rootOperator": "AND"
  },
  "explanation": {
    "query_intent": "Find customers with multiple recent purchases",
    "key_conditions": [
      "Made at least 2 purchases in the last 7 days"
    ]
  }
}
\`\`\`

### Example 3: "Female customers from New York who spent more than $100 in the last month"
\`\`\`json
{
  "filter_criteria": {
    "conditions": [],
    "conditionGroups": [{
      "id": 1,
      "type": "group",
      "operator": "AND",
      "conditions": [
        {
          "id": 2,
          "type": "attribute",
          "field": "gender",
          "operator": "equals",
          "value": "F"
        },
        {
          "id": 3,
          "type": "attribute",
          "field": "city",
          "operator": "equals",
          "value": "New York"
        },
        {
          "id": 4,
          "columnKey": "customer_id",
          "relatedColKey": "customer_id",
          "type": "event",
          "eventType": "performed",
          "frequency": "at_least",
          "count": 1,
          "timePeriod": "months",
          "timeValue": 1,
          "operator": "AND",
          "attributeConditions": [
            {
              "id": 5,
              "field": "total_amount",
              "operator": "greater_than",
              "value": "100"
            }
          ],
          "relatedConditions": []
        }
      ]
    }],
    "rootOperator": "AND"
  },
  "explanation": {
    "query_intent": "Find female customers in New York with significant spending in the last month",
    "key_conditions": [
      "Gender = F to find female customers",
      "City = New York to filter by location",
      "Purchases with total amount > $100 within the last month"
    ]
  }
}
\`\`\`

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

## Value Standardization Rules

### Gender Values
- Use 'F' for female, women, nữ, phụ nữ, chị, cô
- Use 'M' for male, men, nam, đàn ông, anh, ông

### Payment Methods
- Use 'CASH' for cash, tiền mặt, tiền, cash payment
- Use 'CREDIT_CARD' for credit card, thẻ tín dụng, card, thẻ, credit, visa, mastercard
- Use 'BANK_TRANSFER' for bank transfer, chuyển khoản, transfer, wire transfer, banking

### Store Types
- Use 'STORE' for regular store, cửa hàng, store, shop, retail store, outlet
- Use 'SUPERMARKET' for supermarket, siêu thị, hypermarket, mega store

### City Names
- Use proper capitalization: "Los Angeles", "New York", "San Francisco"
- For Vietnamese cities, maintain proper format: "Hà Nội", "Đà Nẵng", "Hồ Chí Minh", "Thành phố Hồ Chí Minh"
- For international cities, use local spelling conventions where appropriate

## MULTI-LANGUAGE CAPABILITIES
This system can interpret queries in multiple languages including:
- English
- Vietnamese
- Spanish
- French
- And other major languages

The system will normalize all inputs to standard field names and values as defined in the schema.

## SECURITY AND PRIVACY SAFEGUARDS

### PII Protection
1. NEVER include actual customer PII in examples or responses
2. Use placeholder values when discussing specific data patterns
3. Reference fields by name without exposing actual data

### Data Access Controls
1. All queries will be logged and monitored for security analysis
2. Suspicious patterns or potential abuse will trigger automatic alerts
3. Session-level access controls will be enforced based on user permissions

### Query Rate Limiting
1. Excessive query volume will trigger automated throttling
2. Abnormal query patterns will be flagged for review
3. Potential abuse scenarios will be blocked automatically

## REAL-TIME VALIDATION PROCESS
1. Every input undergoes real-time validation against security rules
2. Pattern matching algorithms identify potential SQL injection or command attempts
3. Multi-layered security checks prevent bypass attempts
4. Unusual query patterns trigger additional verification steps
5. All failed validation attempts are logged for security review

## SECURE OPERATIONAL GUIDELINES
1. NEVER execute raw input directly against the database
2. ALWAYS sanitize all parameters before processing
3. Use parameterized queries for all database operations
4. Implement least-privilege execution context
5. Apply context-aware security policies based on user role

IMPORTANT: ONLY RETURN THE JSON RESPONSE WITH FILTER CRITERIA AND EXPLANATION. DO NOT INCLUDE ANY SQL QUERIES OR EXECUTION LOGIC.


Natural language query: ${nlpQuery}`;

    // Declare message variable before using it
    let message;
      
    // Sử dụng Claude 3.7 Sonnet
    try {
      logger.info(`Sử dụng model claude-3-7-sonnet-20250219 cho query: "${nlpQuery}"`);
      
      // Thêm hướng dẫn cụ thể cho tiếng Việt vào prompt
      const enhancedPrompt = prompt + `

## VIETNAMESE LANGUAGE PROCESSING
Câu truy vấn của người dùng là tiếng Việt. Hãy phân tích và xử lý cẩn thận:
1. Nhận diện các từ khóa tiếng Việt liên quan đến phân khúc khách hàng
2. Chuyển đổi các khái niệm tiếng Việt sang các trường trong cơ sở dữ liệu
3. Giữ nguyên các giá trị tiếng Việt (tên thành phố, v.v.)
4. Đảm bảo trả về JSON hợp lệ với các điều kiện phù hợp với yêu cầu
5. Nếu không hiểu yêu cầu, KHÔNG trả về giá trị mặc định, hãy trả về lỗi

Ví dụ từ khóa tiếng Việt và ý nghĩa:
- "nữ", "phụ nữ", "chị", "cô" = gender: "F"
- "nam", "đàn ông", "anh", "ông" = gender: "M"
- "mua hàng", "giao dịch", "thanh toán" = event: "purchase"
- "năm nay", "năm hiện tại" = time_period: { unit: "months", value: 12 }
- "tháng trước", "tháng vừa qua" = time_period: { unit: "months", value: 1 }
- "tuần trước", "tuần vừa qua" = time_period: { unit: "weeks", value: 1 }

Nếu không hiểu được yêu cầu, hãy trả về thông báo lỗi bằng tiếng Việt:
{
  "error": "Tôi không hiểu yêu cầu này. Vui lòng cung cấp thêm thông tin về phân khúc khách hàng bạn muốn tạo."
}

Hãy xử lý câu truy vấn này: "${nlpQuery}"
`;
      
      message = await anthropic.messages.create({
        model: "claude-3-7-sonnet-20250219",
        max_tokens: 1000,
        messages: [{ role: "user", content: enhancedPrompt }],
        system: "You are a customer segmentation assistant. Always respond with valid JSON only, no additional text. Follow the format exactly as specified in the prompt."
      });
      logger.info('Đã nhận phản hồi từ Claude 3.7');
      
      // Ghi log phản hồi để debug
      logger.info('Raw response from Claude:', { 
        response: message.content[0].text.substring(0, 500) + (message.content[0].text.length > 500 ? '...' : '')
      });
    } catch (error) {
      logger.error('Lỗi khi gọi Claude API:', { 
        error: error.message, 
        errorType: error.type,
        errorStatus: error.status,
        query: nlpQuery
      });
      throw new Error(`Không thể tạo phản hồi từ Claude: ${error.message}`);
    }

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

      // Try to extract JSON from the response text
      let jsonText = null;
      try {
        // Look for JSON object in the response text
        const jsonRegex = /(\{(?:[^{}]|(?:\{(?:[^{}]|(?:\{[^{}]*\}))*\}))*\})/g;
        const matches = [...responseText.matchAll(jsonRegex)];
        
        // Find the largest match which is most likely the complete JSON
        if (matches.length > 0) {
          let largestMatch = matches[0][0];
          for (const match of matches) {
            if (match[0].length > largestMatch.length) {
              largestMatch = match[0];
            }
          }
          jsonText = largestMatch;
          
          // Try to parse it to validate it's actually JSON
          JSON.parse(jsonText);
          logger.info('Successfully extracted JSON from response text');
        } else {
          // Thử phương pháp khác: tìm kiếm từ dấu { đầu tiên đến dấu } cuối cùng
          const firstBrace = responseText.indexOf('{');
          const lastBrace = responseText.lastIndexOf('}');
          
          if (firstBrace !== -1 && lastBrace !== -1 && firstBrace < lastBrace) {
            jsonText = responseText.substring(firstBrace, lastBrace + 1);
            // Kiểm tra xem có phải JSON hợp lệ không
            try {
              JSON.parse(jsonText);
              logger.info('Successfully extracted JSON using brace matching method');
            } catch (e) {
              jsonText = null;
              logger.warn('Failed to extract valid JSON using brace matching method', { error: e.message });
            }
          }
        }
      } catch (error) {
        logger.warn('Failed to extract valid JSON from response text', { error: error.message });
        jsonText = null;
      }
      
      // Thử phương pháp cuối cùng: tìm kiếm chuỗi bắt đầu bằng {"filter_criteria"
      if (!jsonText) {
        try {
          const filterCriteriaStart = responseText.indexOf('{"filter_criteria"');
          if (filterCriteriaStart !== -1) {
            for (let endPos = responseText.length - 1; endPos > filterCriteriaStart; endPos--) {
              if (responseText[endPos] === '}') {
                const potentialJson = responseText.substring(filterCriteriaStart, endPos + 1);
                try {
                  JSON.parse(potentialJson);
                  jsonText = potentialJson;
                  logger.info('Successfully extracted JSON using filter_criteria marker');
                  break;
                } catch (e) {
                  // Tiếp tục thử với vị trí kết thúc khác
                }
              }
            }
          }
        } catch (error) {
          logger.warn('Failed to extract JSON using filter_criteria marker', { error: error.message });
        }
      }
      
      // Check if the response contains JSON or is plain text
      const isJsonResponse = responseText.trim().startsWith('{') || jsonText;
      
      // Handle plain text responses that don't contain JSON
      if (!isJsonResponse) {
        logger.info('Received plain text response from AI without JSON', { response: responseText });
        return {
          isRejected: true,
          message: responseText
        };
      }

      // Check if the response is a rejection message
      if (cleanedResponse.includes("I'm designed to help you create customer segments") ||
          cleanedResponse.includes("I cannot modify data, delete records") ||
          cleanedResponse.includes("I cannot process this request due to security constraints") ||
          cleanedResponse.includes("security warning") ||
          cleanedResponse.includes("security constraints") ||
          cleanedResponse.includes("suspicious patterns")) {
        logger.info('AI rejected dangerous operation', { response: cleanedResponse });
        return {
          isRejected: true,
          message: responseText
        };
      }

      try {
        // If we have extracted JSON from text, use that, otherwise use the whole response
        const textToParse = jsonText || cleanedResponse;
        
        try {
          response = JSON.parse(textToParse);
        } catch (jsonError) {
          logger.error('Failed to parse as JSON:', { error: jsonError.message, text: textToParse });
          return {
            isRejected: true,
            message: "The AI couldn't generate a proper JSON response. Please try rephrasing your question to focus on customer segmentation criteria."
          };
        }
        
        // Kiểm tra cấu trúc cơ bản trước khi xử lý chi tiết
        if (typeof response !== 'object' || response === null) {
          logger.error('Response is not a valid JSON object', { response: textToParse });
          return {
            isRejected: true,
            message: "The AI response format was invalid. Please try again with a clearer question about customer segmentation."
          };
        }

        // Chuẩn hóa dữ liệu JSON
        response = normalizeJsonResponse(response);

        // Validate response structure
        if (!response.filter_criteria || !response.explanation) {
          logger.error('Invalid response structure:', { 
            hasFilterCriteria: !!response.filter_criteria,
            hasExplanation: !!response.explanation,
            response: textToParse
          });
          return {
            isRejected: true,
            message: "The AI response was missing required fields. Please try again with a focus on customer segmentation criteria."
          };
        }

        // Validate filter_criteria structure
        if (!response.filter_criteria.type && 
            !response.filter_criteria.logic_operator && 
            !Array.isArray(response.filter_criteria.conditions) &&
            !Array.isArray(response.filter_criteria.conditionGroups)) {
          logger.error('Invalid filter_criteria structure:', {
            hasType: !!response.filter_criteria.type,
            hasLogicOperator: !!response.filter_criteria.logic_operator,
            hasConditionsArray: Array.isArray(response.filter_criteria.conditions),
            hasConditionGroups: Array.isArray(response.filter_criteria.conditionGroups),
            response: textToParse
          });
          return {
            isRejected: true,
            message: "The AI response had an invalid filter structure. Please try again with a clearer question."
          };
        }

        // Kiểm tra xem có ít nhất một điều kiện hợp lệ
        const hasValidConditions = 
          (response.filter_criteria.conditions && response.filter_criteria.conditions.length > 0) || 
          (response.filter_criteria.conditionGroups && 
           response.filter_criteria.conditionGroups.some(group => 
             group.conditions && group.conditions.length > 0));
             
        if (!hasValidConditions) {
          logger.error('No valid conditions found in filter criteria', { 
            response: textToParse
          });
          return {
            isRejected: true,
            message: "Không tìm thấy điều kiện lọc hợp lệ. Vui lòng thử lại với câu hỏi cụ thể hơn."
          };
        }
        
        // Validate explanation structure
        if (!response.explanation || !response.explanation.query_intent || 
            !Array.isArray(response.explanation.key_conditions)) {
          logger.info('Missing or invalid explanation structure, creating default explanation', {
            hasExplanation: !!response.explanation,
            hasQueryIntent: response.explanation && !!response.explanation.query_intent,
            hasKeyConditionsArray: response.explanation && Array.isArray(response.explanation.key_conditions)
          });
          
          // Tạo explanation mặc định thay vì báo lỗi
          response.explanation = {
            query_intent: nlpQuery,
            key_conditions: ["Generated from natural language query"]
          };
        }

        // Use filter criteria service to standardize values and handle operators
        // The service will use the filter_criteria from Claude as a base and enhance it
        const enhancedFilterCriteria = await valueStandardizationService.standardizeFilterCriteria(response.filter_criteria, user);

        // Return the filter criteria and explanation only
        return {
          isRejected: false,
          filter_criteria: enhancedFilterCriteria,
          explanation: response.explanation
        };
      } catch (parseError) {
        logger.error('Error processing AI response:', { 
          error: parseError,
          errorMessage: parseError.message,
          response: jsonText || cleanedResponse
        });
        
        return {
          isRejected: true,
          message: "There was an error processing the AI response. Please try again with a simpler question."
        };
      }
    } catch (error) {
      logger.error('Error generating filter criteria from NLP:', { 
        error: error.message, 
        stack: error.stack,
        nlpQuery
      });
      throw new Error(`Failed to generate filter criteria from natural language: ${error.message}`);
    }
  } catch (error) {
    logger.error('Error generating filter criteria from NLP:', { 
      error: error.message, 
      stack: error.stack,
      nlpQuery
    });
    throw new Error(`Failed to generate filter criteria from natural language: ${error.message}`);
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
      rootOperator: filterCriteria.rootOperator || filterCriteria.logic_operator || "AND"
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

    // Xử lý cấu trúc mới với conditionGroups
    if (filterCriteria.conditionGroups && Array.isArray(filterCriteria.conditionGroups)) {
      let conditionId = 1;
      let attributeConditionId = 1;
      
      // Helper function to process a condition
      const processCondition = (condition) => {
        // Handle event conditions
        if (condition.type === 'event') {
          // Handle event conditions (purchase, etc.)
          const eventCondition = {
            id: condition.id || conditionId++,
            columnKey: condition.columnKey || "customer_id",
            relatedColKey: condition.relatedColKey || "customer_id",
            type: "event",
            eventType: normalizeEventConditionType(condition.eventType),
            operator: condition.operator || "AND",
            chosen: false,
            selected: false,
            attributeConditions: [],
            relatedConditions: [],
            relatedAttributeConditions: []
          };

          // Add frequency and time period if available
          if (condition.frequency) {
            eventCondition.frequency = normalizeFrequency(condition.frequency);
            eventCondition.count = condition.count || 1;
          }

          if (condition.timePeriod) {
            eventCondition.timePeriod = normalizeTimePeriod(condition.timePeriod);
            eventCondition.timeValue = condition.timeValue || 30;
          }

          // Add attribute conditions if any
          if (condition.attributeConditions && Array.isArray(condition.attributeConditions)) {
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
                id: attr.id || attributeConditionId++,
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

          return eventCondition;
        }
        
        // Handle regular attribute conditions
        else if (condition.type === 'attribute') {
          // Determine field type for appropriate operator validation
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
          
          return {
            id: condition.id || conditionId++,
            columnKey: condition.field,
            datasetKey: condition.dataset || "customers",
            type: "attribute",
            operator: normalizeOperator(condition.operator, fieldType),
            value: String(condition.value || ""),
            value2: condition.value2 ? String(condition.value2) : "",
            logicOperator: condition.logicOperator || filterCriteria.rootOperator || "AND",
            chosen: false,
            selected: false
          };
        }
        
        return null;
      };

      // Xử lý mỗi conditionGroup
      filterCriteria.conditionGroups.forEach((group, index) => {
        const groupId = group.id || (index + 1);
        const groupConditions = [];
        
        // Xử lý từng condition trong group
        if (group.conditions && Array.isArray(group.conditions)) {
          group.conditions.forEach(condition => {
            const processedCondition = processCondition(condition);
            if (processedCondition) {
              storageFormat.conditions.push(processedCondition);
              groupConditions.push(processedCondition.id);
            }
          });
        }
        
        // Thêm group vào conditionGroups
        storageFormat.conditionGroups.push({
          id: groupId,
          operator: group.operator || "AND",
          conditions: groupConditions
        });
      });
    }
    // Xử lý cấu trúc cũ nếu không có conditionGroups
    else if (filterCriteria.conditions && Array.isArray(filterCriteria.conditions)) {
      let conditionId = 1;
      let attributeConditionId = 1;

      // Helper function to process conditions recursively
      const processCondition = (condition) => {
        // Handle nested group conditions
        if (condition.type === 'group' && condition.conditions && Array.isArray(condition.conditions)) {
          const nestedGroup = {
            id: conditionId++,
            operator: condition.logic_operator || "AND",
            conditions: []
          };
          
          // Process each condition in the nested group
          condition.conditions.forEach(nestedCondition => {
            const processed = processCondition(nestedCondition);
            if (processed) {
              if (Array.isArray(processed)) {
                nestedGroup.conditions.push(...processed);
              } else {
                nestedGroup.conditions.push(processed);
              }
            }
          });
          
          storageFormat.conditionGroups.push(nestedGroup);
          return null; // Nested groups are added directly to storageFormat.conditionGroups
        }
        
        // Handle event conditions
        else if (condition.type === 'event') {
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

          return eventCondition;
        }
        
        // Handle regular attribute conditions
        else if (condition.dataset && condition.field) {
          // Determine field type for appropriate operator validation
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
          
          return {
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
        }
        
        return null;
      };

      // Process each top-level condition
      filterCriteria.conditions.forEach(condition => {
        const processedCondition = processCondition(condition);
        if (processedCondition) {
          storageFormat.conditions.push(processedCondition);
        }
      });
    }

    // Set size to total number of conditions
    storageFormat.size = storageFormat.conditions.length;

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

    // Generate filter criteria from NLP
    const nlpResult = await generateFilterCriteriaFromNLP(nlpQuery, user);

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
        filter_criteria: storageFilterCriteria,
        dataset: 'customers'
      });

    if (segmentError) {
      logger.error('Error creating segment:', { error: segmentError });
      throw segmentError;
    }

    logger.info('Segmentation created successfully', { 
      segmentId
    });

    res.json({
      success: true,
      message: "Segmentation created successfully",
      data: {
        segment_id: segmentId,
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

// Function to normalize JSON response
const normalizeJsonResponse = (response) => {
  try {
    // Kiểm tra nếu response có trường error
    if (response && response.error) {
      logger.info('Claude trả về thông báo lỗi:', { error: response.error });
      return {
        isRejected: true,
        message: response.error
      };
    }
    
    // Tạo một bản sao để không làm thay đổi dữ liệu gốc
    const normalized = JSON.parse(JSON.stringify(response));
    
    // Kiểm tra và xử lý trường hợp response là chuỗi JSON
    if (typeof normalized === 'string') {
      try {
        const parsedJson = JSON.parse(normalized);
        return normalizeJsonResponse(parsedJson);
      } catch (e) {
        // Nếu không phải JSON hợp lệ, trả về lỗi
        logger.warn('Response is a string but not valid JSON', { response: normalized });
        return {
          isRejected: true,
          message: "Phản hồi không phải là JSON hợp lệ. Vui lòng thử lại."
        };
      }
    }
    
    // Kiểm tra nếu response không phải là object
    if (typeof normalized !== 'object' || normalized === null) {
      logger.warn('Response is not an object', { responseType: typeof normalized });
      return {
        isRejected: true,
        message: "Phản hồi không phải là đối tượng hợp lệ. Vui lòng thử lại."
      };
    }
    
    // Trường hợp 1: Response là một conditionGroup trực tiếp (có id, type="group", operator, conditions)
    if (normalized.id && 
        normalized.type === 'group' && 
        normalized.operator && 
        Array.isArray(normalized.conditions)) {
      
      logger.info('Found direct conditionGroup object, wrapping in proper structure');
      
      return {
        filter_criteria: {
          conditionGroups: [normalized],
          conditions: [],
          rootOperator: normalized.operator || "AND"
        },
        explanation: {
          query_intent: "Processed from natural language query",
          key_conditions: ["Generated from Claude AI"]
        }
      };
    }
    
    // Trường hợp 2: Response chứa trực tiếp conditionGroups (không có filter_criteria wrapper)
    if (!normalized.filter_criteria && 
        normalized.conditionGroups && 
        Array.isArray(normalized.conditionGroups) && 
        normalized.rootOperator) {
      
      logger.info('Found direct conditionGroups structure, adding filter_criteria wrapper');
      
      // Chỉ thêm wrapper filter_criteria mà không thay đổi cấu trúc bên trong
      return {
        filter_criteria: {
          conditionGroups: normalized.conditionGroups,
          conditions: normalized.conditions || [],
          rootOperator: normalized.rootOperator
        },
        explanation: normalized.explanation || {
          query_intent: "Processed from natural language query",
          key_conditions: ["Generated from Claude AI"]
        }
      };
    }
    
    // Chuẩn hóa tên trường
    if (!normalized.filter_criteria && normalized.filterCriteria) {
      normalized.filter_criteria = normalized.filterCriteria;
    }
    
    if (!normalized.explanation && normalized.queryExplanation) {
      normalized.explanation = normalized.queryExplanation;
    }
    
    // Kiểm tra nếu không có filter_criteria
    if (!normalized.filter_criteria) {
      logger.warn('Missing filter_criteria in response', { response: JSON.stringify(normalized) });
      return {
        isRejected: true,
        message: "Không tìm thấy tiêu chí lọc trong phản hồi. Vui lòng thử lại với yêu cầu cụ thể hơn."
      };
    }
    
    // Đảm bảo explanation tồn tại
    if (!normalized.explanation) {
      normalized.explanation = {
        query_intent: "Processed from natural language query",
        key_conditions: ["Generated from Claude AI"]
      };
    }
    
    return normalized;
  } catch (error) {
    logger.error('Error normalizing JSON response', { error: error.message });
    return {
      isRejected: true,
      message: "Lỗi khi xử lý phản hồi. Vui lòng thử lại."
    };
  }
};

// Function to process chatbot query and return filter criteria
const processChatbotQuery = async (req, res) => {
  try {
    const { nlpQuery } = req.body;
    const user = req.user;

    logger.info('Chatbot query request', { 
      nlpQuery, 
      userId: user?.user_id 
    });

    if (!user || !user.user_id || !user.business_id) {
      return res.status(400).json({
        success: false,
        error: "User authentication required with business_id"
      });
    }

    // Kiểm tra nếu nlpQuery quá ngắn hoặc không có ý nghĩa
    if (!nlpQuery || nlpQuery.trim().length < 3) {
      return res.status(400).json({
        success: false,
        error: "Câu truy vấn quá ngắn hoặc trống. Vui lòng cung cấp mô tả chi tiết hơn."
      });
    }

    // Generate filter criteria from NLP
    const nlpResult = await generateFilterCriteriaFromNLP(nlpQuery, user);

    // If the query was rejected, return the rejection message
    if (nlpResult.isRejected) {
      return res.json({
        success: false,
        error: nlpResult.message,
        isAIResponse: true
      });
    }

    // Transform filter criteria to storage format
    const storageFilterCriteria = transformFilterCriteriaForStorage(nlpResult.filter_criteria);

    logger.info('Chatbot query processed successfully', { 
      query: nlpQuery,
      hasConditions: storageFilterCriteria.conditions.length > 0
    });

    res.json({
      success: true,
      data: {
        query: nlpQuery,
        explanation: nlpResult.explanation,
        filter_criteria: nlpResult.filter_criteria,
        storage_filter_criteria: storageFilterCriteria
      }
    });
  } catch (error) {
    logger.error('Error processing chatbot query:', { error, query: req.body?.nlpQuery });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

export {
  createSegmentationFromNLP,
  processChatbotQuery
}; 