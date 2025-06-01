import { createClient } from '@supabase/supabase-js';
import { supabase } from '../../config.js';
import pkg from 'pg';
const { Pool } = pkg;
import winston from 'winston';


// const getAllTable = async () => {
//   const { data, error } = await supabase
//     .from('table_schema_view') // ← từ view bạn vừa tạo
//     .select('table_name, column_name, data_type')
//     .order('table_name', { ascending: true });

//   if (error) {
//     console.error('Error:', error);
//     return null;
//   }

//   const result = {};
//   data.forEach(row => {
//     const { table_name, column_name, data_type } = row;
//     if (!result[table_name]) {
//       result[table_name] = {};
//     }
//     result[table_name][column_name] = data_type;
//   });

//   //console.log('result get tables: ', result);

//   return result;
// };


// Create sensitive filter format

const getAllTable = async () => {
  const allowedTables = ['customers', 'product_lines', 'stores', 'transactions'];

  const { data, error } = await supabase
    .from('table_schema_view')
    .select('table_name, column_name, data_type')
    .in('table_name', allowedTables) // ← lọc theo danh sách tên bảng
    .order('table_name', { ascending: true });

  if (error) {
    console.error('Error:', error);
    return null;
  }

  const result = {};
  data.forEach(row => {
    const { table_name, column_name, data_type } = row;
    if (!result[table_name]) {
      result[table_name] = {};
    }
    result[table_name][column_name] = data_type;
  });

  return result;
};

const getRelatedTables = async (target_table) => {
  const { data, error } = await supabase
    .rpc('get_related_tables', { target_table: target_table });

  if (error) {
    console.error('RPC Error:', error);
    return null
  }
  else {
    //console.log('Related Tables:', data.map(d => d.related_table)); 
    return data.map(d => d.related_table);
  }
};

const sensitiveFilter = winston.format((info) => {
  if (typeof info.message === 'string') {
    // Mask PostgreSQL connection URLs in logs
    info.message = info.message.replace(
      /postgresql:\/\/([^:]+):([^@]+)@/g,
      'postgresql://$1:********@'
    );

    // Mask connection_url query parameters
    info.message = info.message.replace(
      /connection_url=postgresql:\/\/([^:]+):([^@]+)@/g,
      'connection_url=postgresql://$1:********@'
    );

    // Mask password parameters
    info.message = info.message.replace(/password=([^&\s]+)/g, 'password=********');
  }
  return info;
})();

// Configure logging
const logger = winston.createLogger({
  level: 'info',
  levels: {
    error: 0,
    warn: 1,
    info: 2,
    http: 3,
    debug: 4
  },
  format: winston.format.combine(
    sensitiveFilter,
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console()
  ]
});

// Supabase configuration
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_KEY;

const createSourceConnection = async (connection_details) => {
  try {
    // Parse the connection URL
    const host = connection_details.host;
    const port = connection_details.port;
    const database = connection_details.database;
    const username = connection_details.username;
    const password = connection_details.password;
    const connectionString = `postgresql://${username}:${password}@${host}:${port}/${database}`;

    logger.info(`Attempting to create connection with details:`, {
      host,
      port,
      database,
      username,
      // Don't log password for security
      hasPassword: !!password
    });

    const pool = new Pool({
      connectionString,
      ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
    });

    // Test the connection
    return pool.query('SELECT 1')
      .then(() => {
        logger.info('Source database connection successful');
        return pool;
      })
      .catch(error => {
        logger.error(`Error connecting to source database: ${error.message}`, {
          errorCode: error.code,
          errorDetail: error.detail,
          errorHint: error.hint
        });
        throw error;
      });
  } catch (error) {
    logger.error(`Error creating source connection: ${error.message}`, {
      errorStack: error.stack
    });
    throw error;
  }
};

const testDatabaseConnection = async (host, port, database, username, password) => {
  try {
    const pool = new Pool({
      user: username,
      host: host,
      database: database,
      password: password,
      port: port,
    });

    const res = await pool.query('SELECT NOW()');
    pool.end();
    return {
      success: true,
      message: 'Connection successful',
      data: res.rows,
    };
  } catch (error) {
    throw new Error(`Connection failed: ${error.message}`);
  }
}

const getSupabase = () => {
  try {
    logger.info(`Connecting to Supabase at: ${supabaseUrl}`);
    const client = createClient(supabaseUrl, supabaseKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false
      },
      global: {
        headers: {
          'Authorization': `Bearer ${supabaseKey}`
        }
      }
    });
    return client;
  } catch (error) {
    logger.error(`Error connecting to Supabase: ${error.message}`);
    throw error;
  }
};

export { logger, getAllTable, testDatabaseConnection, getSupabase, createSourceConnection, getRelatedTables }; 