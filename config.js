import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import assert from 'assert';

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

export { supabase };
