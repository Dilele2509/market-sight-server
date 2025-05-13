import { supabase } from '../../config.js';
import stringSimilarity from 'string-similarity';
import { removeAccents } from '../utils/textUtils.js';
import { logger } from '../data/database.js';
import { OPERATORS, EVENT_CONDITION_TYPES, FREQUENCY_OPTIONS, TIME_PERIOD_OPTIONS } from '../constants/operators.js';

class ValueStandardizationService {
  constructor() {
    this.cache = new Map();
    this.businessMappingsCache = new Map();
    this.similarityThreshold = 0.8;
    this.defaultMappings = {
      gender: {
        'female': 'F', 
        'women': 'F', 
        'woman': 'F', 
        'nữ': 'F',
        'male': 'M', 
        'men': 'M', 
        'man': 'M', 
        'nam': 'M'
      },
      payment_method: {
        'cash': 'CASH', 
        'tiền mặt': 'CASH',
        'credit card': 'CREDIT_CARD', 
        'credit': 'CREDIT_CARD', 
        'card': 'CREDIT_CARD', 
        'thẻ tín dụng': 'CREDIT_CARD',
        'bank transfer': 'BANK_TRANSFER', 
        'transfer': 'BANK_TRANSFER', 
        'chuyển khoản': 'BANK_TRANSFER'
      },
      store_type: {
        'regular store': 'STORE', 
        'store': 'STORE', 
        'cửa hàng': 'STORE',
        'supermarket': 'SUPERMARKET', 
        'siêu thị': 'SUPERMARKET'
      }
    };
  }

  // Normalize text by removing accents and converting to lowercase
  normalizeText(text) {
    if (!text) return '';
    return removeAccents(String(text).toLowerCase().trim());
  }

  // Get business-specific mappings
  async getBusinessMappings(businessId) {
    if (!businessId) return this.defaultMappings;

    // Check cache first
    const cacheKey = `business_mappings:${businessId}`;
    if (this.businessMappingsCache.has(cacheKey)) {
      return this.businessMappingsCache.get(cacheKey);
    }

    try {
      // Fetch business-specific mappings from database
      const { data: businessMappings, error } = await supabase
        .from('business_value_mappings')
        .select('*')
        .eq('business_id', businessId);

      if (error) {
        logger.error('Error fetching business mappings:', { error, businessId });
        return this.defaultMappings;
      }

      if (!businessMappings || businessMappings.length === 0) {
        this.businessMappingsCache.set(cacheKey, this.defaultMappings);
        return this.defaultMappings;
      }

      // Transform the data into a structured object
      const mappings = { ...this.defaultMappings };
      
      for (const mapping of businessMappings) {
        const { mapping_type, input_value, standard_value } = mapping;
        
        if (!mappings[mapping_type]) {
          mappings[mapping_type] = {};
        }
        
        mappings[mapping_type][this.normalizeText(input_value)] = standard_value;
      }

      // Cache the result
      this.businessMappingsCache.set(cacheKey, mappings);
      return mappings;
    } catch (error) {
      logger.error('Error processing business mappings:', { error, businessId });
      return this.defaultMappings;
    }
  }

  // Get field schema
  async getBusinessFieldSchema(businessId) {
    if (!businessId) return null;

    // Check cache first
    const cacheKey = `business_field_schema:${businessId}`;
    if (this.businessMappingsCache.has(cacheKey)) {
      return this.businessMappingsCache.get(cacheKey);
    }

    try {
      // Fetch business-specific field schema
      const { data: fieldSchema, error } = await supabase
        .from('business_field_schema')
        .select('*')
        .eq('business_id', businessId);

      if (error || !fieldSchema || fieldSchema.length === 0) {
        return null;
      }

      // Transform to usable format
      const schema = {};
      for (const field of fieldSchema) {
        if (!schema[field.table_name]) {
          schema[field.table_name] = {};
        }
        schema[field.table_name][field.field_name] = field.field_type;
      }

      // Cache the result
      this.businessMappingsCache.set(cacheKey, schema);
      return schema;
    } catch (error) {
      logger.error('Error fetching business field schema:', { error, businessId });
      return null;
    }
  }

  // Get standard value using multiple approaches
  async getStandardValue(mappingType, inputValue, businessId) {
    if (!inputValue) return inputValue;
    
    const normalizedInput = this.normalizeText(inputValue);
    const cacheKey = `${businessId}:${mappingType}:${normalizedInput}`;

    // Check cache first
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    // 1. Try exact match from business-specific mappings
    const businessMappings = await this.getBusinessMappings(businessId);
    if (businessMappings[mappingType] && 
        businessMappings[mappingType][normalizedInput]) {
      const value = businessMappings[mappingType][normalizedInput];
      this.cache.set(cacheKey, value);
      return value;
    }

    // 2. Try database exact match
    const { data: exactMatch } = await supabase
      .from('business_value_mappings')
      .select('standard_value')
      .eq('mapping_type', mappingType)
      .eq('input_value', normalizedInput)
      .single();

    if (exactMatch) {
      this.cache.set(cacheKey, exactMatch.standard_value);
      return exactMatch.standard_value;
    }

    // 3. Try fuzzy matching against business mappings
    if (businessMappings[mappingType]) {
      const businessMappingKeys = Object.keys(businessMappings[mappingType]);
      if (businessMappingKeys.length > 0) {
        const bestMatch = stringSimilarity.findBestMatch(
          normalizedInput,
          businessMappingKeys
        );

        if (bestMatch.bestMatch.rating >= this.similarityThreshold) {
          const matchedKey = businessMappingKeys[bestMatch.bestMatchIndex];
          const value = businessMappings[mappingType][matchedKey];
          this.cache.set(cacheKey, value);
          return value;
        }
      }
    }

    // 4. Try fuzzy matching against general database
    const { data: allMappings } = await supabase
      .from('business_value_mappings')
      .select('input_value, standard_value')
      .eq('mapping_type', mappingType);

    if (allMappings && allMappings.length > 0) {
      const bestMatch = stringSimilarity.findBestMatch(
        normalizedInput,
        allMappings.map(m => m.input_value)
      );

      if (bestMatch.bestMatch.rating >= this.similarityThreshold) {
        const matchedMapping = allMappings[bestMatch.bestMatchIndex];
        this.cache.set(cacheKey, matchedMapping.standard_value);
        return matchedMapping.standard_value;
      }
    }

    // 5. Apply general business rules as last resort
    const standardizedValue = this.applyBusinessRules(mappingType, normalizedInput);
    this.cache.set(cacheKey, standardizedValue);
    return standardizedValue;
  }

  // Apply business rules for specific mapping types
  applyBusinessRules(mappingType, normalizedInput) {
    switch (mappingType) {
      case 'gender':
        return this.standardizeGender(normalizedInput);
      case 'city':
        return this.standardizeCity(normalizedInput);
      case 'payment_method':
        return this.standardizePaymentMethod(normalizedInput);
      case 'store_type':
        return this.standardizeStoreType(normalizedInput);
      default:
        return normalizedInput;
    }
  }

  // Gender standardization rules
  standardizeGender(input) {
    const femalePatterns = ['nu', 'nữ', 'female', 'f'];
    const malePatterns = ['nam', 'male', 'm'];

    if (femalePatterns.some(pattern => input.includes(pattern))) {
      return 'F';
    }
    if (malePatterns.some(pattern => input.includes(pattern))) {
      return 'M';
    }
    return input;
  }

  // City standardization rules
  standardizeCity(input) {
    // Capitalize first letter of each word
    return input
      .split(' ')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }

  // Payment method standardization rules
  standardizePaymentMethod(input) {
    const patterns = {
      'CASH': ['tien mat', 'cash', 'tien'],
      'CREDIT_CARD': ['the tin dung', 'credit card', 'the'],
      'BANK_TRANSFER': ['chuyen khoan', 'bank transfer', 'transfer']
    };

    for (const [standard, patternList] of Object.entries(patterns)) {
      if (patternList.some(pattern => input.includes(pattern))) {
        return standard;
      }
    }
    return input;
  }

  // Store type standardization rules
  standardizeStoreType(input) {
    const patterns = {
      'STORE': ['cua hang', 'store', 'shop'],
      'SUPERMARKET': ['sieu thi', 'supermarket', 'market']
    };

    for (const [standard, patternList] of Object.entries(patterns)) {
      if (patternList.some(pattern => input.includes(pattern))) {
        return standard;
      }
    }
    return input;
  }

  // New method for standardizing filter criteria from Claude's response
  async standardizeFilterCriteria(filterCriteria, user) {
    try {
      logger.info('Standardizing filter criteria values', { businessId: user?.business_id });
      
      // Kiểm tra cấu trúc filter criteria
      const hasOldStructure = filterCriteria && filterCriteria.conditions && Array.isArray(filterCriteria.conditions);
      const hasNewStructure = filterCriteria && filterCriteria.conditionGroups && Array.isArray(filterCriteria.conditionGroups);
      
      if (!hasOldStructure && !hasNewStructure) {
        logger.error('Invalid filter criteria structure', { filterCriteria });
        return filterCriteria;
      }
      
      // Create a deep copy to avoid modifying the original
      const standardizedCriteria = JSON.parse(JSON.stringify(filterCriteria));
      
      // Get business-specific field schema if available
      const businessFieldSchema = await this.getBusinessFieldSchema(user?.business_id);
      
      // Helper function to process a single condition
      const processCondition = async (condition) => {
        // Handle regular attribute conditions
        if (condition.type === 'attribute' && condition.field) {
          // Get field type - first from business schema, fallback to default
          let fieldType;
          const dataset = condition.dataset || condition.datasetKey || 'customers';
          
          if (businessFieldSchema && 
              businessFieldSchema[dataset] && 
              businessFieldSchema[dataset][condition.field]) {
            fieldType = businessFieldSchema[dataset][condition.field];
          } else {
            fieldType = this.getFieldType(dataset, condition.field);
          }
          
          // Validate operator
          condition.operator = this.validateOperator(fieldType, condition.operator);
          
          // Standardize city values with proper capitalization
          if (condition.field === 'city') {
            condition.value = this.capitalizeCityName(condition.value);
            if (condition.value2) {
              condition.value2 = this.capitalizeCityName(condition.value2);
            }
          }
          
          // Standardize enumerated values using business-specific mappings
          if (condition.value) {
            if (['gender', 'payment_method', 'store_type'].includes(condition.field) || 
                (await this.getBusinessMappings(user?.business_id))[condition.field]) {
              condition.value = await this.getStandardValue(condition.field, condition.value, user?.business_id);
            }
            
            // Handle value2 if present (for between operators)
            if (condition.value2 && condition.operator === 'between') {
              if (['gender', 'payment_method', 'store_type'].includes(condition.field) || 
                  (await this.getBusinessMappings(user?.business_id))[condition.field]) {
                condition.value2 = await this.getStandardValue(condition.field, condition.value2, user?.business_id);
              }
            }
          }
        }
        
        // Handle event conditions
        if (condition.type === 'event') {
          // Handle new structure (eventType) or old structure (event_condition_type)
          if (condition.eventType) {
            condition.eventType = this.validateEventConditionType(condition.eventType);
          } else if (condition.event_condition_type) {
            condition.event_condition_type = this.validateEventConditionType(condition.event_condition_type);
          }
          
          // Handle frequency and time period for purchase events - new structure
          if (condition.frequency && condition.timePeriod) {
            condition.frequency = this.validateFrequencyOperator(condition.frequency);
            condition.timePeriod = this.validateTimePeriodUnit(condition.timePeriod);
          }
          // Handle frequency and time period - old structure
          else if (condition.frequency && condition.time_period) {
            condition.frequency.operator = this.validateFrequencyOperator(condition.frequency.operator);
            condition.time_period.unit = this.validateTimePeriodUnit(condition.time_period.unit);
          }
          
          // Handle amount conditions
          if ((condition.eventType === 'amount' || condition.event_condition_type === 'amount') && condition.operator) {
            condition.operator = this.validateOperator('number', condition.operator);
          }
          
          // Process attribute conditions if any
          if (condition.attributeConditions && Array.isArray(condition.attributeConditions)) {
            condition.attributeConditions = await Promise.all(
              condition.attributeConditions.map(async (attr) => {
                // Determine field type
                let attrFieldType = 'text';
                if (attr.field) {
                  if (attr.field.includes('date') || attr.field.includes('time')) {
                    attrFieldType = 'datetime';
                  } else if (attr.field.includes('amount') || attr.field.includes('quantity') || 
                           attr.field.includes('price') || attr.field.includes('cost')) {
                    attrFieldType = 'number';
                  }
                }
                
                // Validate operator
                attr.operator = this.validateOperator(attrFieldType, attr.operator);
                
                return attr;
              })
            );
          }
        }
        
        return condition;
      };
      
      // Process old structure with conditions array
      if (hasOldStructure) {
        standardizedCriteria.conditions = await Promise.all(
          standardizedCriteria.conditions.map(processCondition)
        );
      }
      
      // Process new structure with conditionGroups
      if (hasNewStructure) {
        for (const group of standardizedCriteria.conditionGroups) {
          if (group.conditions && Array.isArray(group.conditions)) {
            group.conditions = await Promise.all(
              group.conditions.map(processCondition)
            );
          }
        }
      }
      
      logger.info('Filter criteria standardization completed');
      return standardizedCriteria;
    } catch (error) {
      logger.error('Error standardizing filter criteria', { error });
      return filterCriteria; // Return original if processing fails
    }
  }

  // Function to capitalize city names properly
  capitalizeCityName(city) {
    if (!city) return city;
    
    // Split by spaces and handle each word
    return city.split(' ')
      .map(word => {
        // Handle special cases
        if (word.toLowerCase() === 'tp.' || word.toLowerCase() === 'tp') return 'TP.';
        if (word.toLowerCase() === 'hcm') return 'HCM';
        
        // Handle regular words
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
      })
      .join(' ');
  }

  // Validate operator against allowed operators
  validateOperator(fieldType, operator) {
    if (!OPERATORS[fieldType]) return 'equals'; // Default fallback
    
    const validOperators = OPERATORS[fieldType].map(op => op.value);
    if (validOperators.includes(operator)) {
      return operator;
    }
    
    // Try to find a close match
    const operatorLabels = OPERATORS[fieldType].map(op => op.label.toLowerCase());
    const index = operatorLabels.findIndex(label => label === operator.toLowerCase());
    
    if (index !== -1) {
      return OPERATORS[fieldType][index].value;
    }
    
    return 'equals'; // Default fallback
  }

  // Validate event condition type 
  validateEventConditionType(conditionType) {
    const validTypes = EVENT_CONDITION_TYPES.map(type => type.value);
    if (validTypes.includes(conditionType)) {
      return conditionType;
    }
    
    // Try to find a close match by label
    const typeLabels = EVENT_CONDITION_TYPES.map(type => type.label.toLowerCase());
    const index = typeLabels.findIndex(label => label === conditionType.toLowerCase());
    
    if (index !== -1) {
      return EVENT_CONDITION_TYPES[index].value;
    }
    
    return 'performed'; // Default fallback
  }

  // Validate frequency operator
  validateFrequencyOperator(operator) {
    const validOperators = FREQUENCY_OPTIONS.map(op => op.value);
    if (validOperators.includes(operator)) {
      return operator;
    }
    
    // Try to find a close match
    const operatorLabels = FREQUENCY_OPTIONS.map(op => op.label.toLowerCase());
    const index = operatorLabels.findIndex(label => label === operator.toLowerCase());
    
    if (index !== -1) {
      return FREQUENCY_OPTIONS[index].value;
    }
    
    return 'at_least'; // Default fallback
  }

  // Validate time period unit
  validateTimePeriodUnit(unit) {
    const validUnits = TIME_PERIOD_OPTIONS.map(option => option.value);
    if (validUnits.includes(unit)) {
      return unit;
    }
    
    // Try to find a close match
    const unitLabels = TIME_PERIOD_OPTIONS.map(option => option.label.toLowerCase());
    const index = unitLabels.findIndex(label => label === unit.toLowerCase());
    
    if (index !== -1) {
      return TIME_PERIOD_OPTIONS[index].value;
    }
    
    return 'days'; // Default fallback
  }

  // Get field type based on dataset and field name
  getFieldType(dataset, field) {
    const schema = {
      customers: {
        gender: 'text',
        city: 'text',
        first_name: 'text',
        last_name: 'text',
        email: 'text',
        phone: 'text',
        address: 'text',
        birth_date: 'datetime',
        registration_date: 'datetime'
      },
      transactions: {
        transaction_date: 'datetime',
        payment_method: 'text',
        total_amount: 'number',
        quantity: 'number',
        unit_price: 'number'
      },
      product_lines: {
        category: 'text',
        subcategory: 'text',
        brand: 'text',
        name: 'text',
        unit_cost: 'number'
      },
      stores: {
        city: 'text',
        store_type: 'text',
        region: 'text',
        store_name: 'text',
        address: 'text',
        opening_date: 'datetime'
      }
    };
    
    return schema[dataset]?.[field] || 'text';
  }
}

export const valueStandardizationService = new ValueStandardizationService(); 