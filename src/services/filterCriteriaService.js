import { OPERATORS, EVENT_CONDITION_TYPES, FREQUENCY_OPTIONS, TIME_PERIOD_OPTIONS } from '../constants/operators.js';
import { logger } from '../data/database.js';

// Field mappings for each dataset
const FIELD_MAPPINGS = {
  customers: {
    gender: { type: 'text', values: { 'female': 'F', 'male': 'M' } },
    birth_date: { type: 'date' },
    registration_date: { type: 'datetime' },
    phone: { type: 'text' },
    address: { type: 'text' },
    city: { type: 'text' },
    first_name: { type: 'text' },
    last_name: { type: 'text' },
    email: { type: 'text' }
  },
  transactions: {
    transaction_date: { type: 'datetime' },
    total_amount: { type: 'number' },
    quantity: { type: 'number' },
    unit_price: { type: 'number' },
    payment_method: { type: 'text', values: { 'cash': 'CASH', 'credit card': 'CREDIT_CARD', 'bank transfer': 'BANK_TRANSFER' } }
  },
  product_lines: {
    category: { type: 'text' },
    subcategory: { type: 'text' },
    brand: { type: 'text' },
    name: { type: 'text' },
    unit_cost: { type: 'number' }
  },
  stores: {
    city: { type: 'text' },
    store_type: { type: 'text', values: { 'regular store': 'STORE', 'supermarket': 'SUPERMARKET' } },
    region: { type: 'text' },
    store_name: { type: 'text' },
    address: { type: 'text' },
    opening_date: { type: 'date' }
  }
};

// Helper function to get operator from natural language
const getOperatorFromText = (text, fieldType) => {
  const operators = OPERATORS[fieldType];
  for (const op of operators) {
    if (text.toLowerCase().includes(op.label.toLowerCase())) {
      return op.value;
    }
  }
  return 'equals'; // Default operator
};

// Helper function to get field type
const getFieldType = (dataset, field) => {
  return FIELD_MAPPINGS[dataset]?.[field]?.type || 'text';
};

// Helper function to get mapped value
const getMappedValue = (dataset, field, value) => {
  return FIELD_MAPPINGS[dataset]?.[field]?.values?.[value.toLowerCase()] || value;
};

// Main function to generate filter criteria
export const generateFilterCriteria = async (nlpQuery) => {
  try {
    logger.info('Generating filter criteria from NLP query', { nlpQuery });

    // Initialize the filter criteria structure
    const filterCriteria = {
      type: 'group',
      logic_operator: 'AND',
      conditions: []
    };

    // Parse gender condition
    if (nlpQuery.toLowerCase().includes('nữ') || nlpQuery.toLowerCase().includes('female')) {
      filterCriteria.conditions.push({
        dataset: 'customers',
        field: 'gender',
        operator: 'equals',
        value: 'F'
      });
    }

    // Parse age condition
    const ageMatch = nlpQuery.match(/tuổi từ (\d+) đến (\d+)/i);
    if (ageMatch) {
      filterCriteria.conditions.push({
        dataset: 'customers',
        field: 'age',
        operator: 'between',
        value: parseInt(ageMatch[1]),
        value2: parseInt(ageMatch[2])
      });
    }

    // Parse city condition
    const cityMatch = nlpQuery.match(/ở (?:thành phố )?([^,]+)/i);
    if (cityMatch) {
      const city = cityMatch[1].trim();
      filterCriteria.conditions.push({
        dataset: 'customers',
        field: 'city',
        operator: 'equals',
        value: city
      });
    }

    // Parse transaction event condition
    const purchaseMatch = nlpQuery.match(/đã mua hàng (?:ít nhất|nhiều nhất|chính xác)? (\d+) lần trong (\d+) (tháng|tuần|ngày)/i);
    if (purchaseMatch) {
      const frequency = purchaseMatch[1];
      const timeValue = purchaseMatch[2];
      const timeUnit = purchaseMatch[3];

      filterCriteria.conditions.push({
        type: 'event',
        event_name: 'purchase',
        event_condition_type: 'performed',
        frequency: {
          operator: 'at_least',
          value: parseInt(frequency)
        },
        time_period: {
          unit: timeUnit === 'tháng' ? 'months' : timeUnit === 'tuần' ? 'weeks' : 'days',
          value: parseInt(timeValue)
        }
      });
    }

    // If no conditions were found, throw error
    if (filterCriteria.conditions.length === 0) {
      throw new Error('Could not parse any valid conditions from the query');
    }

    logger.info('Generated filter criteria', { filterCriteria });
    return filterCriteria;
  } catch (error) {
    logger.error('Error generating filter criteria:', { error });
    throw error;
  }
};

// Function to convert filter criteria to SQL
export const convertToSQL = (filterCriteria) => {
  try {
    let sqlConditions = [];

    for (const condition of filterCriteria.conditions) {
      if (condition.type === 'event') {
        // Handle event conditions
        const eventSQL = generateEventSQL(condition);
        if (eventSQL) {
          sqlConditions.push(eventSQL);
        }
      } else {
        // Handle attribute conditions
        const { dataset, field, operator, value, value2 } = condition;
        const tableAlias = dataset.charAt(0);
        const fieldType = getFieldType(dataset, field);
        const mappedValue = getMappedValue(dataset, field, value);
        
        let conditionSQL = '';
        switch (operator) {
          case 'equals':
            conditionSQL = `${tableAlias}.${field} = '${mappedValue}'`;
            break;
          case 'not_equals':
            conditionSQL = `${tableAlias}.${field} != '${mappedValue}'`;
            break;
          case 'greater_than':
            conditionSQL = `${tableAlias}.${field} > ${mappedValue}`;
            break;
          case 'less_than':
            conditionSQL = `${tableAlias}.${field} < ${mappedValue}`;
            break;
          case 'between':
            conditionSQL = `${tableAlias}.${field} BETWEEN ${value} AND ${value2}`;
            break;
          case 'contains':
            conditionSQL = `${tableAlias}.${field} LIKE '%${mappedValue}%'`;
            break;
          case 'starts_with':
            conditionSQL = `${tableAlias}.${field} LIKE '${mappedValue}%'`;
            break;
          case 'ends_with':
            conditionSQL = `${tableAlias}.${field} LIKE '%${mappedValue}'`;
            break;
          case 'is_null':
            conditionSQL = `${tableAlias}.${field} IS NULL`;
            break;
          case 'is_not_null':
            conditionSQL = `${tableAlias}.${field} IS NOT NULL`;
            break;
        }
        
        if (conditionSQL) {
          sqlConditions.push(conditionSQL);
        }
      }
    }

    return sqlConditions.join(` ${filterCriteria.logic_operator} `);
  } catch (error) {
    logger.error('Error converting filter criteria to SQL:', { error });
    throw error;
  }
};

// Helper function to generate SQL for event conditions
const generateEventSQL = (eventCondition) => {
  const { event_name, event_condition_type, frequency, time_period } = eventCondition;
  
  if (event_name === 'purchase') {
    const timeValue = time_period.value;
    const timeUnit = time_period.unit;
    const frequencyValue = frequency.value;
    
    return `EXISTS (
      SELECT 1 FROM transactions t 
      WHERE t.customer_id = c.customer_id 
      AND t.transaction_date >= CURRENT_DATE - INTERVAL '${timeValue} ${timeUnit}'
      GROUP BY t.customer_id
      HAVING COUNT(*) >= ${frequencyValue}
    )`;
  }
  
  return null;
}; 