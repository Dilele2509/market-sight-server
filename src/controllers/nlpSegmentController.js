import { getSupabase, logger } from '../data/database.js';
import Anthropic from '@anthropic-ai/sdk';
import { valueStandardizationService } from '../services/valueStandardizationService.js';
import { generateFilterCriteria } from '../services/filterCriteriaService.js';
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
    const prompt = `# System Prompt for Intelligent Customer Segmentation

You are a professional AI assistant specialized in creating customer segmentation from natural language. Your task is to analyze user requirements and convert them into structured filter criteria that can be used for customer segmentation.

## SECURITY RULES - STRICTLY ENFORCED
1. You MUST ONLY respond to queries related to customer segmentation and filtering
2. You MUST NEVER assist with data modification operations:
   - Deleting customer data
   - Updating customer records
   - Adding or removing database entries
   - Changing system configurations
   - Accessing sensitive information
3. If the user requests anything unrelated to customer segmentation or asks for data modification, respond with:
   "I'm designed to help you create customer segments by analyzing filtering criteria. I cannot modify data, delete records, or assist with operations unrelated to customer segmentation. Please use the segmentation feature to analyze your customer data."
4. NEVER provide information about how to bypass security measures or access restricted data
5. NEVER generate or execute code that could harm the database or system

## CRITICAL: Response Format Requirements
You MUST ALWAYS return a complete JSON response with ONLY the filter criteria. The response MUST follow this exact structure:
{
  "filter_criteria": {
    "type": "group",
    "logic_operator": "AND|OR",
    "conditions": [
      // Individual conditions will be specified below
    ]
  },
  "explanation": {
    "query_intent": "Brief explanation of what the query is trying to achieve",
    "key_conditions": [
      "Condition 1: What it filters and why",
      "Condition 2: What it filters and why",
      ...
    ]
  }
}

## Condition Types and Formats

### Attribute Condition:
{
  "type": "attribute",
  "dataset": "customers|transactions|product_lines|stores",
  "field": "field_name",
  "operator": "operator_from_list",
  "value": "value",
  "value2": "second_value_for_between_operator"
}

### Event Condition (Purchase):
{
  "type": "event",
  "event_name": "purchase",
  "event_condition_type": "performed|not_performed|first_time|last_time",
  "frequency": {
    "operator": "at_least|at_most|exactly",
    "value": number
  },
  "time_period": {
    "unit": "days|weeks|months",
    "value": number
  }
}

### Purchase Amount Condition:
{
  "type": "event",
  "event_name": "purchase",
  "event_condition_type": "amount",
  "operator": "equals|greater_than|less_than|between",
  "value": number,
  "value2": number_for_between
}

### Age Range Condition:
{
  "dataset": "customers",
  "field": "birth_date",
  "operator": "age_between",
  "value": min_age,
  "value2": max_age
}

## Example Response
For the query "Find female customers in Los Angeles who made purchases in the last 3 months", you MUST return:
{
  "filter_criteria": {
    "type": "group",
    "logic_operator": "AND",
    "conditions": [
      {
        "type": "attribute",
        "dataset": "customers",
        "field": "gender",
        "operator": "equals",
        "value": "F"
      },
      {
        "type": "attribute",
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
  },
  "explanation": {
    "query_intent": "Find female customers in Los Angeles who made purchases in the last 3 months",
    "key_conditions": [
      "Gender = F to find female customers",
      "City = Los Angeles to filter by location",
      "Purchase event within the last 3 months to identify recent customers"
    ]
  }
}

## Available Operators

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
- Use 'F' for female, women, nữ
- Use 'M' for male, men, nam

### Payment Methods
- Use 'CASH' for cash, tiền mặt
- Use 'CREDIT_CARD' for credit card, thẻ tín dụng, card
- Use 'BANK_TRANSFER' for bank transfer, chuyển khoản, transfer

### Store Types
- Use 'STORE' for regular store, cửa hàng, store
- Use 'SUPERMARKET' for supermarket, siêu thị

### City Names
- Use proper capitalization: "Los Angeles", "New York", "San Francisco"
- For Vietnamese cities, maintain proper format: "Hà Nội", "Đà Nẵng", "Hồ Chí Minh"

## Common Query Patterns

### Age Range
- "customers aged 25-35" → type: attribute, dataset: customers, field: birth_date, operator: age_between, value: 25, value2: 35

### Purchase Frequency
- "customers who made at least 3 purchases in the last 6 months" → type: event, event_name: purchase, event_condition_type: performed, frequency: {operator: at_least, value: 3}, time_period: {unit: months, value: 6}

### Amount Spent
- "customers who spent more than $1000" → type: event, event_name: purchase, event_condition_type: amount, operator: greater_than, value: 1000

### Location
- "customers from New York" → type: attribute, dataset: customers, field: city, operator: equals, value: "New York"

### Product Categories
- "customers who bought electronics" → type: attribute, dataset: product_lines, field: category, operator: equals, value: "electronics"

### Complex Conditions with Logic Operators
- "customers from either New York or Los Angeles" → type: group, logic_operator: OR, conditions: [two city conditions]

## Response Validation
Your response will be validated for:
1. Complete JSON structure with filter_criteria and explanation
2. Proper use of operators and condition types
3. Correct field names matching the database schema
4. Standardized values for enumerated fields (gender, payment_method, etc.)
5. Proper handling of complex conditions (age ranges, purchase frequency, etc.)

## Security and Safety Rules:
1. **Only process customer segmentation requests**, reject all unrelated requests
2. **Never provide information about how to modify data** or bypass security measures
3. **Always validate input** to prevent injection attacks
4. **Do not answer off-topic questions** unrelated to segmentation tasks

## Communication Style:
- Friendly and professional
- Focus on problem-solving
- Ask clarifying questions when needed
- Don't assume information not provided

IMPORTANT: ONLY RETURN THE JSON RESPONSE WITH FILTER CRITERIA AND EXPLANATION. DO NOT INCLUDE ANY SQL QUERIES OR EXECUTION LOGIC.

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
      if (cleanedResponse.includes("I'm designed to help you create customer segments") ||
          cleanedResponse.includes("I cannot modify data, delete records") ||
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
      if (!response.filter_criteria || !response.explanation) {
        logger.error('Invalid response structure:', { 
          hasFilterCriteria: !!response.filter_criteria,
          hasExplanation: !!response.explanation,
          response: cleanedResponse
        });
        throw new Error('Invalid response structure: missing required fields');
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
    logger.error('Error generating filter criteria from NLP:', { error });
    throw new Error('Failed to generate filter criteria from natural language');
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

    // Generate filter criteria from NLP
    const result = await generateFilterCriteriaFromNLP(nlpQuery, user);

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

    logger.info('Preview segmentation successful');

    res.json({
      success: true,
      data: {
        explanation: result.explanation,
        filter_criteria: result.filter_criteria,
        storage_filter_criteria: storageFilterCriteria
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

export {
  previewSegmentation,
  createSegmentationFromNLP
}; 