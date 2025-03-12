const { createClient } = require('@supabase/supabase-js');
const dotenv = require('dotenv');
const assert = require('assert');

dotenv.config();

const { SUPABASE_URL, SUPABASE_KEY } = process.env;

assert(SUPABASE_URL, 'SUPABASE_URL is required');
assert(SUPABASE_KEY, 'SUPABASE_KEY is required');

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function testConnection() {
  const { data, error } = await supabase.from('customers').select('*').limit(1);

  if (error) {
    console.error('Supabase connection failed:', error);
  } else {
    console.log('Supabase connected successfully:', data);
  }
}

testConnection(); 

module.exports = { supabase };
