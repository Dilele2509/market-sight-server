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

    // Step 1: Preprocessing
    const preprocessedQuery = preprocessQuery(nlpQuery);
    
    // Step 2: Information Extraction
    const extractedEntities = extractEntities(preprocessedQuery);
    
    // Step 3: Semantic Mapping
    const mappedConditions = mapToConditions(extractedEntities);
    
    // Step 4: Build Filter
    const filterCriteria = buildFilterCriteria(mappedConditions);
    
    logger.info('Generated filter criteria', { filterCriteria });
    return filterCriteria;
  } catch (error) {
    logger.error('Error generating filter criteria:', { error });
    throw error;
  }
};

/**
 * Step 1: Preprocess the query to normalize text
 */
const preprocessQuery = (query) => {
  // Convert to lowercase
  let processed = query.toLowerCase();
  
  // Normalize Vietnamese accents if needed
  // processed = normalizeAccents(processed);
  
  // Replace common synonyms
  const synonymMap = {
    'nam': 'male',
    'nữ': 'female',
    'tp.': 'thành phố',
    'tp': 'thành phố',
    'đã mua': 'mua',
    'đã thanh toán': 'thanh toán',
    'khách hàng': '',
    'người': ''
  };
  
  Object.entries(synonymMap).forEach(([key, value]) => {
    processed = processed.replace(new RegExp(`\\b${key}\\b`, 'g'), value);
  });
  
  return processed;
};

/**
 * Step 2: Extract entities from preprocessed query
 */
const extractEntities = (query) => {
  const entities = [];
  
  // Extract gender
  if (query.includes('female') || query.includes('nữ')) {
    entities.push({
      type: 'attribute',
      dataset: 'customers',
      field: 'gender',
      value: 'female',
      operator: 'equals'
    });
  } else if (query.includes('male') || query.includes('nam')) {
    entities.push({
      type: 'attribute',
      dataset: 'customers',
      field: 'gender',
      value: 'male',
      operator: 'equals'
    });
  }
  
  // Extract city
  const cityPatterns = [
    /(?:ở|tại|trong) (?:thành phố|tỉnh|tp|tp\.|city)? ?([a-zÀ-ỹ ]+?)(?:,|\.|$|\s+và|\s+hoặc|\s+or|\s+and)/i,
    /(?:thành phố|tỉnh|tp|tp\.|city) ([a-zÀ-ỹ ]+?)(?:,|\.|$|\s+và|\s+hoặc|\s+or|\s+and)/i
  ];
  
  for (const pattern of cityPatterns) {
    const match = query.match(pattern);
    if (match && match[1]) {
      const city = match[1].trim();
      if (city && city.length > 1) { // Avoid single character matches
        entities.push({
          type: 'attribute',
          dataset: 'customers',
          field: 'city',
          value: city,
          operator: 'equals'
        });
        break;
      }
    }
  }
  
  // Extract age range
  const ageRangePattern = /(?:tuổi|age) (?:từ|from) (\d+) (?:đến|to) (\d+)/i;
  const ageMatch = query.match(ageRangePattern);
  if (ageMatch) {
    entities.push({
      type: 'attribute',
      dataset: 'customers',
      field: 'birth_date',
      operator: 'age_between',
      value: parseInt(ageMatch[1]),
      value2: parseInt(ageMatch[2])
    });
  }
  
  // Extract purchase frequency
  const purchasePattern = /(?:mua|purchase|bought) (?:ít nhất|at least|nhiều nhất|at most|chính xác|exactly)? ?(\d+) (?:lần|times) (?:trong|in|within) (\d+) (tháng|months|tuần|weeks|ngày|days)/i;
  const purchaseMatch = query.match(purchasePattern);
  if (purchaseMatch) {
    const frequency = purchaseMatch[1];
    const timeValue = purchaseMatch[2];
    const timeUnit = purchaseMatch[3].toLowerCase();
    
    let unit = 'days';
    if (timeUnit.includes('tháng') || timeUnit.includes('month')) {
      unit = 'months';
    } else if (timeUnit.includes('tuần') || timeUnit.includes('week')) {
      unit = 'weeks';
    }
    
    let operator = 'at_least';
    if (query.includes('nhiều nhất') || query.includes('at most')) {
      operator = 'at_most';
    } else if (query.includes('chính xác') || query.includes('exactly')) {
      operator = 'exactly';
    }
    
    entities.push({
      type: 'event',
      event_name: 'purchase',
      event_condition_type: 'performed',
      frequency: {
        operator: operator,
        value: parseInt(frequency)
      },
      time_period: {
        unit: unit,
        value: parseInt(timeValue)
      }
    });
  }
  
  // Extract amount spent
  const amountPattern = /(?:chi tiêu|spent|paid) (?:ít nhất|at least|nhiều nhất|at most|chính xác|exactly)? ?(\d+(?:\.\d+)?) ?(?:đồng|vnd|usd|\$)?/i;
  const amountMatch = query.match(amountPattern);
  if (amountMatch) {
    let operator = 'greater_than_equals';
    if (query.includes('nhiều nhất') || query.includes('at most')) {
      operator = 'less_than_equals';
    } else if (query.includes('chính xác') || query.includes('exactly')) {
      operator = 'equals';
    }
    
    entities.push({
      type: 'event',
      event_name: 'purchase',
      event_condition_type: 'amount',
      operator: operator,
      value: parseFloat(amountMatch[1])
    });
  }
  
  // Extract product category
  const categoryPattern = /(?:mua|purchase|bought) (?:sản phẩm|product|hàng hóa)? ?(?:thuộc|in|from)? ?(?:danh mục|category|loại)? ?([a-zÀ-ỹ ]+?)(?:,|\.|$|\s+và|\s+hoặc|\s+or|\s+and)/i;
  const categoryMatch = query.match(categoryPattern);
  if (categoryMatch && categoryMatch[1]) {
    const category = categoryMatch[1].trim();
    if (category && category.length > 1) {
      entities.push({
        type: 'attribute',
        dataset: 'product_lines',
        field: 'category',
        value: category,
        operator: 'equals'
      });
    }
  }
  
  return entities;
};

/**
 * Step 3: Map extracted entities to filter conditions
 */
const mapToConditions = (entities) => {
  return entities.map(entity => {
    // Handle special cases like age calculation from birth date
    if (entity.field === 'birth_date' && entity.operator === 'age_between') {
      const currentYear = new Date().getFullYear();
      const minYear = currentYear - entity.value2;
      const maxYear = currentYear - entity.value;
      
      return {
        dataset: entity.dataset,
        field: entity.field,
        operator: 'between',
        value: `${minYear}-01-01`,
        value2: `${maxYear}-12-31`
      };
    }
    
    // Map entity values to database values if needed
    if (entity.type === 'attribute') {
      const fieldMapping = FIELD_MAPPINGS[entity.dataset]?.[entity.field];
      if (fieldMapping && fieldMapping.values && fieldMapping.values[entity.value]) {
        entity.value = fieldMapping.values[entity.value];
      }
      
      return {
        dataset: entity.dataset,
        field: entity.field,
        operator: entity.operator,
        value: entity.value,
        value2: entity.value2
      };
    }
    
    // Handle event conditions
    if (entity.type === 'event') {
      if (entity.event_name === 'purchase') {
        if (entity.event_condition_type === 'performed') {
          return {
            type: 'event',
            event_name: entity.event_name,
            event_condition_type: entity.event_condition_type,
            frequency: entity.frequency,
            time_period: entity.time_period
          };
        } else if (entity.event_condition_type === 'amount') {
          return {
            type: 'event',
            event_name: entity.event_name,
            event_condition_type: 'amount',
            operator: entity.operator,
            value: entity.value
          };
        }
      }
    }
    
    return entity;
  });
};

/**
 * Step 4: Build the final filter criteria structure
 */
const buildFilterCriteria = (conditions) => {
  // If no conditions were found, return a default structure
  if (conditions.length === 0) {
    return {
      type: 'group',
      logic_operator: 'AND',
      conditions: []
    };
  }
  
  return {
    type: 'group',
    logic_operator: 'AND',
    conditions: conditions
  };
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