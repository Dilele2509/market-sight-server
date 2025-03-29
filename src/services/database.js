import { createClient } from '@supabase/supabase-js';
import pkg from 'pg';
const { Pool } = pkg;
import winston from 'winston';

// Create sensitive filter format
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

export const createSourceConnection = async (connectionUrl) => {
  try {
    // Parse the connection URL
    const url = new URL(connectionUrl);
    const username = url.username;
    const password = url.password;
    const [host, port] = url.hostname.split(':');
    const database = url.pathname.slice(1);

    logger.info(`Attempting to connect to PostgreSQL at ${host}:${port}/${database} with user ${username}`);

    const pool = new Pool({
      host,
      port: parseInt(port),
      database,
      user: username,
      password: password,
      ssl: {
        rejectUnauthorized: false // Required for Supabase connections
      }
    });

    // Test the connection
    await pool.query('SELECT 1');
    logger.info(`Connected to source database at: ${host}:${port}/${database}`);
    
    return pool;
  } catch (error) {
    logger.error(`Error connecting to source database: ${error.message}`);
    throw error;
  }
};

export const getSupabase = () => {
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

export { logger }; 