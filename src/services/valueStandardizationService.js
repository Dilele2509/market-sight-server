import { supabase } from '../../config.js';
import stringSimilarity from 'string-similarity';
import { removeAccents } from '../utils/textUtils.js';

class ValueStandardizationService {
  constructor() {
    this.cache = new Map();
    this.similarityThreshold = 0.8;
  }

  // Normalize text by removing accents and converting to lowercase
  normalizeText(text) {
    return removeAccents(text.toLowerCase().trim());
  }

  // Get standard value using multiple approaches
  async getStandardValue(mappingType, inputValue) {
    const normalizedInput = this.normalizeText(inputValue);
    const cacheKey = `${mappingType}:${normalizedInput}`;

    // Check cache first
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    // 1. Try exact match from database
    const { data: exactMatch } = await supabase
      .from('value_mappings')
      .select('standard_value')
      .eq('mapping_type', mappingType)
      .eq('input_value', normalizedInput)
      .single();

    if (exactMatch) {
      this.cache.set(cacheKey, exactMatch.standard_value);
      return exactMatch.standard_value;
    }

    // 2. Try fuzzy matching
    const { data: allMappings } = await supabase
      .from('value_mappings')
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

    // 3. Apply business rules for specific mapping types
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
}

export const valueStandardizationService = new ValueStandardizationService(); 