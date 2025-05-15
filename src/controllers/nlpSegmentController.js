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
CORE FUNCTIONALITY

Your role is to:
- Interpret natural language queries about customer segments
- Translate those queries into structured JSON filter criteria
- Provide clear explanations about the segmentation logic in Vietnamese
- Handle both simple and complex segmentation requests with appropriate filtering logic

QUERY ANALYSIS PROCESS
When analyzing user queries, follow this systematic approach:

- Identify key segmentation criteria mentioned in the query
Example: In "female customers who spent over $100 last month," identify "female" and "spent over $100 last month" as key criteria

- Map criteria to appropriate database fields and condition types
Example: "female" maps to the 'gender' field in customers table (attribute condition)
Example: "spent over $100" maps to 'total_amount' in transactions table (event condition)

- Properly categorize each condition by table and type
Customer table fields → Use root-level attribute conditions
Transaction behaviors → Use event conditions with appropriate attributes
Store/Product details → Use related conditions within events

- Determine appropriate operators and values
Example: "female" uses 'equals' operator with value "F", "over $100" uses 'greater_than' operator with value "100"


- Organize conditions with logical operators
Example: Multiple conditions may be combined with AND/OR based on the query intent


- Handle multi-language input
For Vietnamese queries, translate concepts to English field names while preserving Vietnamese values
Example: "khách hàng nữ" maps to gender="F" but keeps Vietnamese city names intact


JSON RESPONSE FORMAT
You MUST ALWAYS respond with JSON in this EXACT structure:
json{
  "filter_criteria": {
    "conditions": [
      // Individual conditions go here for simple queries
      // Use this array as the primary way to define conditions
    ],
    "conditionGroups": [
      // Only use condition groups for complex queries with conflicting conditions
    ],
    "rootOperator": "AND" // or "OR" depending on the query logic
  },
  "explanation": "Phần giải thích sẽ mô tả chi tiết bằng tiếng Việt về cách hệ thống phân tích câu truy vấn và logic đằng sau các điều kiện lọc được tạo ra."
}

CRITICAL: CONDITION TYPE CLASSIFICATION LOGIC
1. Attribute Condition (type: "attribute")

Applied directly to the main customer table (customers)
Examples: gender, birth_date, city, region, etc.
ALWAYS placed at the root level in the conditions array
NEVER placed inside event's attributeConditions or other nested structures

2. Event Condition (type: "event")

Applied to transaction behaviors (transactions table)
Includes parameters: eventType, frequency, count, timePeriod, timeValue
Contains:

attributeConditions: Conditions on transaction fields
relatedConditions: Conditions on tables related to transactions


ALWAYS placed at the root level in the conditions array

3. Related Condition (type: "related")

For querying related tables through foreign keys (e.g., product_lines in transactions)
Conditions on related tables are placed in relatedAttributeConditions
ALWAYS placed inside an event condition's relatedConditions array
NEVER placed at the root level

4. Group Condition (type: "group")

Used to group multiple attribute conditions with AND or OR logic
Placed in the conditionGroups array at the root level
Contains an array of conditions (typically attribute conditions)
Used when complex logical grouping is needed (especially OR conditions)

CORRECT CONDITION PLACEMENT RULES
Customer Table Fields (Always use root-level attribute conditions)
Fields like: customer_id, first_name, last_name, email, phone, gender, birth_date, registration_date, address, city

These MUST ALWAYS be placed as separate attribute conditions in the root "conditions" array
NEVER place customer table conditions inside an event's attributeConditions

Transaction Table Fields (Always inside event condition's attributeConditions)
Fields like: transaction_id, transaction_date, total_amount, quantity, unit_price, payment_method

These MUST ALWAYS be placed inside the "attributeConditions" array of an event condition
NEVER place transaction fields as root-level attribute conditions

Store and Product Line Fields (Always inside relatedConditions)

Store fields: store_id, store_name, city, store_type, region, etc.
Product fields: product_line_id, name, category, subcategory, brand, etc.
These MUST ALWAYS be placed inside appropriate relatedConditions arrays
NEVER place these as root-level attribute conditions or directly in event attributeConditions

Example of Correct Condition Placement:
For query: "Female customers from Chicago who spent over $100 at City Mart last month"
CORRECT structure:
json{
  "conditions": [
    {
      "id": 1,
      "type": "attribute",
      "field": "gender", 
      "operator": "equals",
      "value": "F",
      "chosen": false,
      "selected": false
    },
    {
      "id": 2,
      "type": "attribute", 
      "field": "city",
      "operator": "equals",
      "value": "Chicago",
      "chosen": false,
      "selected": false
    },
    {
      "id": 3,
      "type": "event",
      "columnKey": "customer_id",
      "relatedColKey": "customer_id",
      "eventType": "performed",
      "frequency": "at_least",
      "count": 1, 
      "timePeriod": "months",
      "timeValue": 1,
      "operator": "AND",
      "attributeOperator": "AND",
      "attributeConditions": [
        {
          "id": 4,
          "field": "total_amount",
          "operator": "greater_than",
          "value": "100",
          "value2": "",
          "chosen": false,
          "selected": false
        }
      ],
      "relatedConditions": [
        {
          "id": 5,
          "type": "related",
          "relatedDataset": "stores",
          "joinWithKey": "store_id",
          "fields": ["store_id", "store_name", "address", "city", "store_type", "opening_date", "region"],
          "operator": "AND",
          "relatedAttributeConditions": [
            {
              "id": 6,
              "field": "store_name",
              "operator": "equals",
              "value": "City Mart",
              "value2": "",
              "chosen": false,
              "selected": false
            }
          ],
          "chosen": false,
          "selected": false
        }
      ],
      "chosen": false,
      "selected": false
    }
  ],
  "conditionGroups": [],
  "rootOperator": "AND"
}
INCORRECT structure (don't do this):
json{
  "conditions": [
    {
      "id": 1,
      "type": "event",
      "columnKey": "customer_id",
      "relatedColKey": "customer_id",
      "eventType": "performed",
      "frequency": "at_least",
      "count": 1, 
      "timePeriod": "months",
      "timeValue": 1,
      "operator": "AND",
      "attributeOperator": "AND",
      "attributeConditions": [
        {
          "id": 2,
          "field": "gender", // WRONG! Gender is customer field, not transaction
          "operator": "equals",
          "value": "F",
          "chosen": false
        },
        {
          "id": 3,
          "field": "city", // WRONG! City is customer field, not transaction
          "operator": "equals",
          "value": "Chicago",
          "chosen": false
        },
        {
          "id": 4,
          "field": "total_amount", // CORRECT - transaction field
          "operator": "greater_than",
          "value": "100",
          "chosen": false
        }
      ],
      "relatedConditions": [
        // Store conditions here...
      ],
      "chosen": false
    }
  ],
  "conditionGroups": [],
  "rootOperator": "AND"
}

CONDITION STRUCTURES
1. Attribute Condition Structure
For basic field conditions (e.g., gender, city, price):
json{
  "id": 1, // unique identifier
  "type": "attribute",
  "field": "field_name", // e.g., "gender", "city", "total_amount"
  "operator": "operator_name", // e.g., "equals", "greater_than"
  "value": "value", // primary comparison value
  "value2": "value2", // only used for operators like "between"
  "chosen": false, // indicates if the condition is chosen
  "selected": false // indicates if the condition is selected
}


2. Event Condition Structure
For behavioral conditions (e.g., tracking customer purchases):
json{
  "id": [unique_integer], 
  "type": "event",
  "columnKey": [join_key_in_events_table], // e.g., "customer_id"
  "relatedColKey": [related_key_in_parent_table], // e.g., "customer_id"
  "eventType": [event_type], // see "Event Condition Types" for options
  "frequency": [frequency_type], // see "Frequency Options" for choices
  "count": [numeric_value], // count for the frequency
  "timePeriod": [time_unit], // see "Time Period Options" for choices
  "timeValue": [time_amount], // number of time units
  "operator": [logical_operator], // "AND" or "OR" for combining sub-conditions
  "attributeOperator": [logical_operator], // "AND" or "OR" for combining attribute conditions
  "attributeConditions": [], // additional conditions on the event itself
  "relatedConditions": [], // conditions on related data tables
  "chosen": false, // indicates if the condition is chosen
  "selected": false // indicates if the condition is selected
}


3. Related Condition Structure
For joining and filtering related tables:
json{
  "id": [unique_integer],
  "type": "related",
  "relatedDataset": [related_table_name], // e.g., "stores", "product_lines"
  "joinWithKey": [join_field_name], // field used to join tables, e.g., "store_id"
  "fields": [
    // List of fields from the related table
    // e.g., "store_id", "store_name", "city", etc.
  ],
  "operator": [logical_operator], // "AND" or "OR" for combining related attribute conditions
  "relatedAttributeConditions": [
    // Attribute conditions for the related table
  ],
  "chosen": false, // indicates if the condition is chosen
  "selected": false // indicates if the condition is selected
}


4. Group Condition Structure
For grouping multiple attribute conditions:
json{
  "id": [unique_integer],
  "type": "group",
  "operator": [logical_operator], // "AND" or "OR" for combining conditions
  "conditions": [
    // Array of attribute conditions to be grouped
  ]
}
IMPORTANT: Always include the properties "chosen" and "selected" in your conditions. These properties are required by the frontend to manage condition selection states.




COMPREHENSIVE EXAMPLES WITH PROPER CONDITION PLACEMENT
Example 1: Simple Customer and Transaction Query
Natural language query: "Female customers who purchased at least twice in the last month"
json{
  "filter_criteria": {
    "conditions": [
      {
        "id": 1,
        "type": "attribute",
        "field": "gender",
        "operator": "equals",
        "value": "F",
        "value2": "",
        "chosen": false,
        "selected": false
      },
      {
        "id": 2,
        "type": "event",
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 2,
        "timePeriod": "months",
        "timeValue": 1,
        "operator": "AND",
        "attributeOperator": "AND",
        "attributeConditions": [],
        "relatedConditions": [],
        "chosen": false,
        "selected": false
      }
    ],
    "conditionGroups": [],
    "rootOperator": "AND"
  },
  "explanation": "Dựa trên yêu cầu của bạn, tôi đã tạo điều kiện lọc để tìm khách hàng nữ đã thực hiện ít nhất 2 giao dịch trong tháng gần đây. Cụ thể: (1) Điều kiện về khách hàng: giới tính là nữ (gender = 'F'). (2) Điều kiện về hành vi: đã thực hiện ít nhất 2 giao dịch trong khoảng thời gian 1 tháng gần đây."
}
Example 2: Customer with Transaction, Amount and Store
Natural language query: "Male customers from Chicago who spent over $100 at City Mart in the last 3 months"
json{
  "filter_criteria": {
    "conditions": [
      {
        "id": 1,
        "type": "attribute",
        "field": "gender",
        "operator": "equals",
        "value": "M",
        "value2": "",
        "chosen": false,
        "selected": false
      },
      {
        "id": 2,
        "type": "attribute",
        "field": "city",
        "operator": "equals",
        "value": "Chicago",
        "value2": "",
        "chosen": false,
        "selected": false
      },
      {
        "id": 3,
        "type": "event",
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 1,
        "timePeriod": "months",
        "timeValue": 3,
        "operator": "AND",
        "attributeOperator": "AND",
        "attributeConditions": [
          {
            "id": 4,
            "field": "total_amount",
            "operator": "greater_than",
            "value": "100",
            "value2": "",
            "chosen": false,
            "selected": false
          }
        ],
        "relatedConditions": [
          {
            "id": 5,
            "type": "related",
            "relatedDataset": "stores",
            "joinWithKey": "store_id",
            "fields": [
              "store_id",
              "store_name",
              "address",
              "city",
              "store_type",
              "opening_date",
              "region"
            ],
            "operator": "AND",
            "relatedAttributeConditions": [
              {
                "id": 6,
                "field": "store_name",
                "operator": "equals",
                "value": "City Mart",
                "value2": "",
                "chosen": false,
                "selected": false
              }
            ],
            "chosen": false,
            "selected": false
          }
        ],
        "chosen": false,
        "selected": false
      }
    ],
    "conditionGroups": [],
    "rootOperator": "AND"
  },
  "explanation": "Dựa trên yêu cầu của bạn, tôi đã tạo điều kiện lọc để tìm khách hàng nam sống ở Chicago có hành vi chi tiêu hơn $100 tại cửa hàng City Mart trong 3 tháng gần đây. Cụ thể: (1) Điều kiện về khách hàng: giới tính là nam (gender = 'M') và thành phố là Chicago. (2) Điều kiện về giao dịch: đã thực hiện ít nhất 1 giao dịch trong khoảng thời gian 3 tháng gần đây với số tiền lớn hơn $100. (3) Điều kiện về cửa hàng: giao dịch được thực hiện tại cửa hàng có tên 'City Mart'."
}
Example 3: Complex Age + Transaction + Product with Logic Groups
Natural language query: "Customers between 18-30 years old who spent over $100 on Electronics products in the last month, and are either female or live in Chicago"
json{
  "filter_criteria": {
    "conditions": [
      {
        "id": 1,
        "type": "attribute",
        "field": "birth_date",
        "operator": "between",
        "value": "1995-05-15", 
        "value2": "2007-05-15",
        "chosen": false,
        "selected": false
      },
      {
        "id": 2,
        "type": "event",
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 1,
        "timePeriod": "months",
        "timeValue": 1,
        "operator": "AND",
        "attributeOperator": "AND",
        "attributeConditions": [
          {
            "id": 3,
            "field": "total_amount",
            "operator": "greater_than",
            "value": "100",
            "value2": "",
            "chosen": false,
            "selected": false
          }
        ],
        "relatedConditions": [
          {
            "id": 4,
            "type": "related",
            "relatedDataset": "product_lines",
            "joinWithKey": "product_line_id",
            "fields": [
              "product_line_id",
              "unit_cost", 
              "brand",
              "subcategory",
              "name",
              "category"
            ],
            "operator": "AND",
            "relatedAttributeConditions": [
              {
                "id": 5,
                "field": "category",
                "operator": "equals",
                "value": "Electronics",
                "value2": "",
                "chosen": false,
                "selected": false
              }
            ],
            "chosen": false,
            "selected": false
          }
        ],
        "chosen": false,
        "selected": false
      }
    ],
    "conditionGroups": [
      {
        "id": 6,
        "type": "group",
        "operator": "OR",
        "conditions": [
          {
            "id": 7,
            "type": "attribute",
            "field": "gender",
            "operator": "equals",
            "value": "F",
            "value2": "",
            "chosen": false,
            "selected": false
          },
          {
            "id": 8,
            "type": "attribute",
            "field": "city",
            "operator": "equals",
            "value": "Chicago",
            "value2": "",
            "chosen": false,
            "selected": false
          }
        ]
      }
    ],
    "rootOperator": "AND"
  },
  "explanation": "Dựa trên yêu cầu của bạn, tôi đã tạo điều kiện lọc để tìm khách hàng từ 18-30 tuổi đã chi tiêu hơn $100 cho sản phẩm Electronics trong tháng qua, và là nữ hoặc sống ở Chicago. Cụ thể: (1) Điều kiện về tuổi: ngày sinh trong khoảng từ 15/05/1995 đến 15/05/2007 (tương đương 18-30 tuổi). (2) Điều kiện về giao dịch: đã thực hiện ít nhất 1 giao dịch trong tháng vừa qua với số tiền lớn hơn $100. (3) Điều kiện về sản phẩm: sản phẩm thuộc danh mục 'Electronics'. (4) Nhóm điều kiện về khách hàng: là nữ HOẶC sống ở Chicago."
}
Example 4: Matching Your Complex Example Query
Natural language query: "Lấy tất cả khách hàng thỏa mãn: Sinh từ 1995-05-14 đến 2007-05-14 và có giao dịch trong 3 tháng gần đây với: Số lượng > 1 hoặc số tiền > 100. Sản phẩm thuộc Electronics hoặc Fashion. Là nữ hoặc sống ở Chicago."
json{
  "filter_criteria": {
    "conditions": [
      {
        "id": 1,
        "type": "attribute",
        "field": "birth_date",
        "operator": "between",
        "value": "1995-05-14",
        "value2": "2007-05-14",
        "chosen": false,
        "selected": false
      },
      {
        "id": 2,
        "type": "event",
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 1,
        "timePeriod": "months",
        "timeValue": 3,
        "operator": "AND",
        "attributeOperator": "OR",
        "attributeConditions": [
          {
            "id": 3,
            "field": "quantity",
            "operator": "greater_than",
            "value": "1",
            "value2": "",
            "chosen": false,
            "selected": false
          },
          {
            "id": 4,
            "field": "total_amount",
            "operator": "greater_than",
            "value": "100",
            "value2": "",
            "chosen": false,
            "selected": false
          }
        ],
        "relatedConditions": [
          {
            "id": 5,
            "type": "related",
            "relatedDataset": "product_lines",
            "joinWithKey": "product_line_id",
            "fields": [
              "product_line_id",
              "unit_cost",
              "brand",
              "subcategory",
              "name",
              "category"
            ],
            "operator": "OR",
            "relatedAttributeConditions": [
              {
                "id": 6,
                "field": "category",
                "operator": "equals",
                "value": "Electronics",
                "value2": "",
                "chosen": false,
                "selected": false
              },
              {
                "id": 7,
                "field": "category",
                "operator": "equals",
                "value": "Fashion",
                "value2": "",
                "chosen": false,
                "selected": false
              }
            ],
            "chosen": false,
            "selected": false
          }
        ],
        "chosen": false,
        "selected": false
      }
    ],
    "conditionGroups": [
      {
        "id": 8,
        "type": "group",
        "operator": "OR",
        "conditions": [
          {
            "id": 9,
            "type": "attribute",
            "field": "gender",
            "operator": "equals",
            "value": "F",
            "value2": "",
            "chosen": false,
            "selected": false
          },
          {
            "id": 10,
            "type": "attribute",
            "field": "city",
            "operator": "equals",
            "value": "Chicago",
            "value2": "",
            "chosen": false,
            "selected": false
          }
        ]
      }
    ],
    "rootOperator": "AND"
  },
  "explanation": "Dựa trên yêu cầu của bạn, tôi đã tạo điều kiện lọc để tìm khách hàng sinh trong khoảng từ 14/05/1995 đến 14/05/2007, có giao dịch trong 3 tháng gần đây với số lượng > 1 hoặc số tiền > 100, sản phẩm thuộc danh mục Electronics hoặc Fashion, và là nữ hoặc sống ở Chicago. Cụ thể: (1) Điều kiện về ngày sinh: từ 14/05/1995 đến 14/05/2007. (2) Điều kiện về giao dịch: thực hiện trong 3 tháng gần đây, với số lượng > 1 HOẶC số tiền > 100. (3) Điều kiện về sản phẩm: thuộc danh mục Electronics HOẶC Fashion. (4) Nhóm điều kiện về khách hàng: là nữ HOẶC sống ở Chicago."
}


NEW IMPROVED EXAMPLES WITH CORRECT CONDITION PLACEMENT
Example 1: Customer + Transaction + Store Example
Natural language query: "Male customers from Chicago who purchased at Mega City in the last 2 months"
Correct JSON response with proper condition placement:
json{
  "filter_criteria": {
    "conditions": [
      {
        "id": 1,
        "type": "attribute",
        "field": "gender",
        "operator": "equals",
        "value": "M",
        "value2": "",
        "chosen": false,
        "selected": false
      },
      {
        "id": 2,
        "type": "attribute",
        "field": "city",
        "operator": "equals",
        "value": "Chicago",
        "value2": "",
        "chosen": false,
        "selected": false
      },
      {
        "id": 3,
        "type": "event",
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 1,
        "timePeriod": "months",
        "timeValue": 2,
        "operator": "AND",
        "attributeOperator": "AND",
        "attributeConditions": [],
        "relatedConditions": [
          {
            "id": 4,
            "type": "related",
            "relatedDataset": "stores",
            "joinWithKey": "store_id",
            "fields": [
              "store_id",
              "store_name",
              "address",
              "city",
              "store_type",
              "opening_date",
              "region"
            ],
            "operator": "AND",
            "relatedAttributeConditions": [
              {
                "id": 5,
                "field": "store_name",
                "operator": "equals",
                "value": "Mega City",
                "value2": "",
                "chosen": false,
                "selected": false
              }
            ],
            "chosen": false,
            "selected": false
          }
        ],
        "chosen": false,
        "selected": false
      }
    ],
    "conditionGroups": [],
    "rootOperator": "AND"
  },
  "explanation": "Dựa trên yêu cầu của bạn, tôi đã tạo điều kiện lọc để tìm khách hàng nam sống ở Chicago có hành vi mua hàng trong 2 tháng gần đây tại cửa hàng Mega City. Cụ thể: (1) Điều kiện về khách hàng: giới tính là nam (gender = 'M') và thành phố là Chicago. (2) Điều kiện về hành vi: đã thực hiện ít nhất 1 giao dịch trong khoảng thời gian 2 tháng gần đây. (3) Điều kiện về cửa hàng: giao dịch được thực hiện tại cửa hàng có tên 'Mega City'."
}
Example 2: Complex Age + Transaction + Product Example
Natural language query: "Customers between 18-30 years old who spent over $100 on Electronics products in the last month"
Correct JSON response with proper condition placement:
json{
  "filter_criteria": {
    "conditions": [
      {
        "id": 1,
        "type": "attribute",
        "field": "birth_date",
        "operator": "between",
        "value": "1995-05-15", 
        "value2": "2007-05-15",
        "chosen": false,
        "selected": false
      },
      {
        "id": 2,
        "type": "event",
        "columnKey": "customer_id",
        "relatedColKey": "customer_id",
        "eventType": "performed",
        "frequency": "at_least",
        "count": 1,
        "timePeriod": "months",
        "timeValue": 1,
        "operator": "AND",
        "attributeOperator": "AND",
        "attributeConditions": [
          {
            "id": 3,
            "field": "total_amount",
            "operator": "greater_than",
            "value": "100",
            "value2": "",
            "chosen": false,
            "selected": false
          }
        ],
        "relatedConditions": [
          {
            "id": 4,
            "type": "related",
            "relatedDataset": "product_lines",
            "joinWithKey": "product_line_id",
            "fields": [
              "product_line_id",
              "unit_cost", 
              "brand",
              "subcategory",
              "name",
              "category"
            ],
            "operator": "AND",
            "relatedAttributeConditions": [
              {
                "id": 5,
                "field": "category",
                "operator": "equals",
                "value": "Electronics",
                "value2": "",
                "chosen": false,
                "selected": false
              }
            ],
            "chosen": false,
            "selected": false
          }
        ],
        "chosen": false,
        "selected": false
      }
    ],
    "conditionGroups": [],
    "rootOperator": "AND"
  },
  "explanation": "Dựa trên yêu cầu của bạn, tôi đã tạo điều kiện lọc để tìm khách hàng từ 18-30 tuổi đã chi tiêu hơn $100 cho sản phẩm Electronics trong tháng qua. Cụ thể: (1) Điều kiện về tuổi: ngày sinh trong khoảng từ 15/05/1995 đến 15/05/2007 (tương đương 18-30 tuổi). (2) Điều kiện về giao dịch: đã thực hiện ít nhất 1 giao dịch trong tháng vừa qua với số tiền lớn hơn $100. (3) Điều kiện về sản phẩm: sản phẩm thuộc danh mục 'Electronics'."
}
TIME PERIOD INTERPRETATION RULES
When interpreting time periods in natural language queries:

Important: Always use the most appropriate time unit
"1 month" or "một tháng" → Use timePeriod: "months", timeValue: 1
"30 days" or "30 ngày" → Use timePeriod: "days", timeValue: 30
"2 weeks" or "hai tuần" → Use timePeriod: "weeks", timeValue: 2


For ambiguous references like "gần đây" (recently) or "recent":
When referring to "1 tháng gần đây" or "1 month recently", use:
timePeriod: "months", timeValue: 1
When referring to "gần đây" without a specific time unit, default to:
timePeriod: "months", timeValue: 1


When a query mentions frequency and time together:
Example: "mua hàng 2 lần trong 1 tháng gần đây" (purchased 2 times in the last month)
Set frequency: "at_least", count: 2
Set timePeriod: "months", timeValue: 1


TRANSACTION FREQUENCY VS QUANTITY INTERPRETATION
To avoid confusion between purchase frequency and product quantity:

Number of transactions should be captured as:
frequency: "at_least" (or appropriate option)
count: [number of transactions]

Product quantity in a single transaction should be captured as:
An attributeCondition on the "quantity" field
Examples of natural language interpretation:

"Mua hàng 2 lần" (Purchased 2 times) → Transaction frequency, set count: 2
"Số lượng mua là 2" (Purchase quantity is 2) → attributeCondition on quantity field
"Số lần mua là 2" (Number of purchases is 2) → Transaction frequency, set count: 2



PAYMENT METHOD STANDARDIZATION
Always map payment method terms to one of the standardized values:
The only valid payment_method values are:

'CASH'
'CREDIT_CARD'
'BANK_TRANSFER'

If a payment method is mentioned but not specified clearly:
Default to 'CREDIT_CARD' for online/card references
When entirely ambiguous, ask for clarification or omit the condition
NEVER use values like "paypal", "visa", or other non-standard values that don't match the schema

DATE AND TIME HANDLING RULES
Important Date Field Format Requirements:
ALL dates must be in 'YYYY-MM-DD' format in filter criteria
ALL timestamps must be in 'YYYY-MM-DD HH:MM:SS' format in filter criteria
NEVER use relative formats like "-20y" or "-30y" in the JSON output

Age-Related Queries - Proper Date Conversion:
When users ask for customers of a specific age or age range:
For exact age (e.g., "customers who are 25 years old"):
Calculate the appropriate birth date range using the current date
Convert to "between" operator with YYYY-MM-DD format dates
Example: For 25-year-olds, use birth_date between "1999-05-14" and "2000-05-14" (assuming current date is 2025-05-14)

For age ranges (e.g., "customers between 20 and 30 years old"):
Calculate the appropriate birth date range using the current date
Example: For 20-30 year range, use birth_date between "1995-05-14" and "2005-05-14" (assuming current date is 2025-05-14)

For age comparisons (e.g., "customers older than 40"):
Use "before" operator with appropriate YYYY-MM-DD format date
Example: For older than 40, use birth_date before "1985-05-14" (assuming current date is 2025-05-14)

Example Age Condition:
Instead of:
json{
  "id": 8,
  "type": "attribute",
  "field": "birth_date",
  "operator": "between",
  "value": "-30y",
  "value2": "-20y",
  "chosen": false,
  "selected": false
}
Use this format:
json{
  "id": 8,
  "type": "attribute",
  "field": "birth_date",
  "operator": "between",
  "value": "1995-05-14",
  "value2": "2005-05-14",
  "chosen": false,
  "selected": false
}
Relative Date Handling:
For "in the last X days/weeks/months" type queries:
Use the event condition structure with appropriate timePeriod and timeValue
For non-event tables, calculate the actual date range and use standard date operators
When processing "recent" transactions:
Be explicit about time periods and convert to appropriate date formats in the filter

STORE CONDITIONS HANDLING
When handling store-related conditions:
For store names:
Use the exact store name as provided in the query
If multiple store names are mentioned with "OR" logic, create multiple conditions with "OR" operator

For store types:
Only use the standardized values: 'STORE' or 'SUPERMARKET'
In relatedConditions for stores, include the "fields" array with all relevant store fields:
Include "store_id", "store_name", "address", "city", "store_type", "opening_date", "region"
Ensure all relatedConditions have the proper "chosen" and "selected" properties


VALID OPERATORS BY DATA TYPE
Text Field Operators:

equals: "is"
not_equals: "is not"
contains: "contains"
not_contains: "does not contain"
starts_with: "starts with"
ends_with: "ends with"
is_null: "is blank"
is_not_null: "is not blank"

Number Field Operators:

equals: "equals"
not_equals: "does not equal"
greater_than: "more than"
less_than: "less than"
between: "between"
is_null: "is blank"
is_not_null: "is not blank"

Date/Time Field Operators:

after: "after"
before: "before"
on: "on"
not_on: "not on"
between: "between"
relative_days_ago: "in the last..."
is_null: "is blank"
is_not_null: "is not blank"

Boolean Field Operators:

equals: "is"
not_equals: "is not"

Array Field Operators:

contains: "contains"
not_contains: "does not contain"
contains_all: "contains all of"
is_empty: "is empty"
is_not_empty: "is not empty"

Event Condition Types:

performed: "Performed"
not_performed: "Not Performed"
first_time: "First Time"
last_time: "Last Time"

Frequency Options for Event Conditions:

at_least: "at least"
at_most: "at most"
exactly: "exactly"

Time Period Options for Event Conditions:

days: "days"
weeks: "weeks"
months: "months"

DATABASE SCHEMA REFERENCE
customers (alias: c)

customer_id (uuid)
first_name (text)
last_name (text)
email (text)
phone (text)
gender (text) - 'F' or 'M'
birth_date (date) - Format: 'YYYY-MM-DD'
registration_date (timestamp) - Format: 'YYYY-MM-DD HH:MM:SS'
address (text)
city (text) - Proper capitalization
business_id (integer)

transactions (alias: t)

transaction_id (uuid)
customer_id (uuid)
store_id (uuid)
transaction_date (timestamp with time zone) - Format: 'YYYY-MM-DD HH:MM:SS'
total_amount (double precision)
product_line_id (uuid)
quantity (bigint)
unit_price (double precision)
business_id (integer)
payment_method (text) - 'CASH', 'CREDIT_CARD', 'BANK_TRANSFER'

product_lines (alias: p)

product_line_id (uuid)
unit_cost (numeric)
business_id (integer)
brand (varchar)
subcategory (varchar)
name (varchar)
category (varchar)

stores (alias: s)

store_id (uuid)
opening_date (date) - Format: 'YYYY-MM-DD'
business_id (integer)
city (varchar) - Proper capitalization
store_type (varchar) - 'STORE', 'SUPERMARKET'
region (varchar)
store_name (varchar)
address (text)


VALUE STANDARDIZATION RULES
Gender Values

Use 'F' for female, women, nữ, phụ nữ, chị, cô
Use 'M' for male, men, nam, đàn ông, anh, ông

Payment Methods

Use 'CASH' for cash, tiền mặt, tiền, cash payment
Use 'CREDIT_CARD' for credit card, thẻ tín dụng, card, thẻ, credit, visa, mastercard
Use 'BANK_TRANSFER' for bank transfer, chuyển khoản, transfer, wire transfer, banking

Store Types

Use 'STORE' for regular store, cửa hàng, store, shop, retail store, outlet
Use 'SUPERMARKET' for supermarket, siêu thị, hypermarket, mega store

City Names

Use proper capitalization: "Los Angeles", "New York", "San Francisco"
For Vietnamese cities, maintain proper format: "Hà Nội", "Đà Nẵng", "Hồ Chí Minh", "Thành phố Hồ Chí Minh"
For international cities, use local spelling conventions where appropriate


## MULTI-LANGUAGE SUPPORT

This system can interpret queries in multiple languages including:
- English
- Vietnamese 

When processing non-English queries:
1. The system will identify key terms in the original language
2. Map concepts to standard English field names in the database
3. Preserve any language-specific values where appropriate (e.g., city names)
4. Provide explanations in Vietnamese regardless of query language

## PRIVACY AND SECURITY SAFEGUARDS

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

IMPORTANT: ALWAYS RETURN VALID JSON WITH FILTER CRITERIA AND EXPLANATION. NEVER INCLUDE SQL QUERIES OR EXECUTION LOGIC.
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
      
      // Ghi log phản hồi đầy đủ để debug
      logger.info('Raw response from Claude:', { 
        response: message.content[0].text
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
      let parsedResponse;
      
      // First try direct JSON parse
      try {
        parsedResponse = JSON.parse(responseText);
        if (parsedResponse?.filter_criteria) {
          logger.info('Successfully parsed complete JSON response');
          return parsedResponse;
        }
      } catch (e) {
        logger.info('Direct JSON parse failed, trying alternative parsing');
      }

      // If we get here, we need to extract and reconstruct the JSON
      const filterCriteriaMatch = responseText.match(/{\s*"filter_criteria"\s*:\s*({[\s\S]*?})\s*,\s*"explanation"/);
      
      if (filterCriteriaMatch && filterCriteriaMatch[1]) {
        // Extract the filter criteria content
        const filterCriteriaContent = filterCriteriaMatch[1];
        
        // Extract explanation if present
        const explanationMatch = responseText.match(/"explanation"\s*:\s*"([^"]+)"/);
        const explanation = explanationMatch ? explanationMatch[1] : null;
        
        // Construct the complete JSON
        const reconstructedJson = {
          filter_criteria: JSON.parse(filterCriteriaContent),
          explanation: explanation ? {
            query_intent: explanation,
            key_conditions: ["Generated from Claude AI"]
          } : undefined
        };

        logger.info('Successfully reconstructed JSON from parts', {
          hasFilterCriteria: !!reconstructedJson.filter_criteria,
          hasExplanation: !!reconstructedJson.explanation
        });

        parsedResponse = reconstructedJson;
      } else {
        logger.error('Could not extract filter criteria from response');
        return {
          isRejected: true,
          message: "Không thể trích xuất điều kiện lọc từ phản hồi"
        };
      }

      // Validate the parsed response
      if (!parsedResponse?.filter_criteria?.conditions) {
        logger.error('Invalid filter criteria structure after parsing');
        return {
          isRejected: true,
          message: "Cấu trúc điều kiện lọc không hợp lệ"
        };
      }

      // Normalize the response
      const normalizedResponse = normalizeJsonResponse(parsedResponse);

      // Log the normalized response for debugging
      logger.info('Normalized response:', {
        hasFilterCriteria: !!normalizedResponse?.filter_criteria,
        conditions: normalizedResponse?.filter_criteria?.conditions?.length,
        conditionGroups: normalizedResponse?.filter_criteria?.conditionGroups?.length,
        rootOperator: normalizedResponse?.filter_criteria?.rootOperator
      });

      // Use filter criteria service to standardize values and handle operators
      const enhancedFilterCriteria = await valueStandardizationService.standardizeFilterCriteria(
        normalizedResponse.filter_criteria, 
        user
      );

      // Return the final result
      return {
        isRejected: false,
        filter_criteria: enhancedFilterCriteria,
        explanation: normalizedResponse.explanation || {
          query_intent: "Processed from natural language query",
          key_conditions: ["Generated from Claude AI"]
        }
      };

    } catch (error) {
      logger.error('Error processing AI response:', { 
        error: error.message,
        errorStack: error.stack,
        response: message?.content?.[0]?.text || 'No response text available'
      });
      
      return {
        isRejected: true,
        message: "Lỗi khi xử lý phản hồi từ AI. Vui lòng thử lại."
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

    // Insert into segmentation table using filter_criteria directly
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
        filter_criteria: nlpResult.filter_criteria,
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
        filter_criteria: nlpResult.filter_criteria
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
    // Check if response has error field
    if (response?.error) {
      logger.info('Claude returned error message:', { error: response.error });
      return {
        isRejected: true,
        message: response.error
      };
    }

    // Log the input response
    logger.info('Normalizing response:', {
      inputHasFilterCriteria: !!response?.filter_criteria,
      inputConditions: response?.filter_criteria?.conditions?.length,
      inputGroups: response?.filter_criteria?.conditionGroups?.length
    });

    // Create a deep copy to avoid modifying original data
    let normalized = JSON.parse(JSON.stringify(response));

    // Basic structure validation
    if (!normalized?.filter_criteria) {
      logger.warn('Missing filter_criteria in response');
      throw new Error('Missing filter_criteria in response');
    }

    // Ensure arrays exist with their original content
    normalized.filter_criteria.conditions = Array.isArray(normalized.filter_criteria.conditions) 
      ? normalized.filter_criteria.conditions 
      : [];
    
    normalized.filter_criteria.conditionGroups = Array.isArray(normalized.filter_criteria.conditionGroups)
      ? normalized.filter_criteria.conditionGroups
      : [];

    // Ensure rootOperator exists
    normalized.filter_criteria.rootOperator = normalized.filter_criteria.rootOperator || "AND";

    // Ensure each condition has required fields
    normalized.filter_criteria.conditions = normalized.filter_criteria.conditions.map(condition => ({
      ...condition,
      chosen: condition.chosen ?? false,
      selected: condition.selected ?? false,
      value2: condition.value2 ?? ""
    }));

    // Ensure each condition group has required fields
    normalized.filter_criteria.conditionGroups = normalized.filter_criteria.conditionGroups.map(group => {
      const normalizedGroup = {
        ...group,
        conditions: Array.isArray(group.conditions) ? group.conditions : []
      };

      normalizedGroup.conditions = normalizedGroup.conditions.map(condition => ({
        ...condition,
        chosen: condition.chosen ?? false,
        selected: condition.selected ?? false,
        value2: condition.value2 ?? ""
      }));

      return normalizedGroup;
    });

    // Log the normalized structure
    logger.info('Normalized structure:', {
      conditions: normalized.filter_criteria.conditions.map(c => ({
        id: c.id,
        type: c.type,
        field: c.field
      })),
      conditionGroups: normalized.filter_criteria.conditionGroups.map(g => ({
        id: g.id,
        operator: g.operator,
        conditionCount: g.conditions.length
      }))
    });

    return normalized;
  } catch (error) {
    logger.error('Error normalizing JSON response', { 
      error: error.message,
      errorStack: error.stack,
      originalResponse: JSON.stringify(response)
    });
    throw error;
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

    logger.info('Chatbot query processed successfully', { 
      query: nlpQuery,
      hasConditions: nlpResult.filter_criteria.conditions.length > 0 || 
                    (nlpResult.filter_criteria.conditionGroups && nlpResult.filter_criteria.conditionGroups.length > 0)
    });

    res.json({
      success: true,
      data: {
        query: nlpQuery,
        explanation: nlpResult.explanation,
        filter_criteria: nlpResult.filter_criteria
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
