// Condition operators by data type
export const OPERATORS = {
    text: [
        { value: 'equals', label: 'is' },
        { value: 'not_equals', label: 'is not' },
        { value: 'contains', label: 'contains' },
        { value: 'not_contains', label: 'does not contain' },
        { value: 'starts_with', label: 'starts with' },
        { value: 'ends_with', label: 'ends with' },
        { value: 'is_null', label: 'is blank' },
        { value: 'is_not_null', label: 'is not blank' }
    ],
    number: [
        { value: 'equals', label: 'equals' },
        { value: 'not_equals', label: 'does not equal' },
        { value: 'greater_than', label: 'more than' },
        { value: 'less_than', label: 'less than' },
        { value: 'between', label: 'between' },
        { value: 'is_null', label: 'is blank' },
        { value: 'is_not_null', label: 'is not blank' }
    ],
    datetime: [
        { value: 'after', label: 'after' },
        { value: 'before', label: 'before' },
        { value: 'on', label: 'on' },
        { value: 'not_on', label: 'not on' },
        { value: 'between', label: 'between' },
        { value: 'relative_days_ago', label: 'in the last...' },
        { value: 'is_null', label: 'is blank' },
        { value: 'is_not_null', label: 'is not blank' }
    ],
    boolean: [
        { value: 'equals', label: 'is' },
        { value: 'not_equals', label: 'is not' }
    ],
    array: [
        { value: 'contains', label: 'contains' },
        { value: 'not_contains', label: 'does not contain' },
        { value: 'contains_all', label: 'contains all of' },
        { value: 'is_empty', label: 'is empty' },
        { value: 'is_not_empty', label: 'is not empty' }
    ]
};

// Event condition types
export const EVENT_CONDITION_TYPES = [
    { value: 'performed', label: 'Performed' },
    { value: 'not_performed', label: 'Not Performed' },
    { value: 'first_time', label: 'First Time' },
    { value: 'last_time', label: 'Last Time' }
];

// Frequency options for event conditions
export const FREQUENCY_OPTIONS = [
    { value: 'at_least', label: 'at least' },
    { value: 'at_most', label: 'at most' },
    { value: 'exactly', label: 'exactly' }
];

// Time period options for event conditions
export const TIME_PERIOD_OPTIONS = [
    { value: 'days', label: 'days' },
    { value: 'weeks', label: 'weeks' },
    { value: 'months', label: 'months' }
]; 