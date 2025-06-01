import { createSourceConnection, getAllTable, getRelatedTables, getSupabase, logger, testDatabaseConnection } from '../data/database.js';
import pkg from 'pg';
const { Pool } = pkg;
import { SCHEMA_MAPPINGS } from '../services/schemaMapping.js';
import { v4 as uuidv4 } from 'uuid';

// Helper function to convert data to JSON-serializable format
const convertToJsonSerializable = (obj) => {
  if (obj instanceof Date) {
    return obj.toISOString();
  } else if (typeof obj === 'object' && obj !== null) {
    if (Array.isArray(obj)) {
      return obj.map(item => convertToJsonSerializable(item));
    } else {
      const result = {};
      for (const key in obj) {
        result[key] = convertToJsonSerializable(obj[key]);
      }
      return result;
    }
  }
  return obj;
}

const validateAndMapData = (data, tableName) => {
  try {
    const schema = SCHEMA_MAPPINGS[tableName];
    if (!schema) {
      throw new Error(`No schema mapping found for table '${tableName}'`);
    }
    const requiredFields = schema.required_fields;


    // Create a mapping of source columns to target columns
    const columnMapping = {};
    for (const reqField of requiredFields) {
      // Try exact match first
      if (data.some(row => reqField in row)) {
        columnMapping[reqField] = reqField;
        continue;
      }

      // Try case-insensitive match
      for (const row of data) {
        for (const col in row) {
          if (col.toLowerCase() === reqField.toLowerCase()) {
            columnMapping[col] = reqField;
            break;
          }
        }
      }
    }

    // Check for missing required fields
    const mappedFields = new Set(Object.values(columnMapping));
    const missingFields = requiredFields.filter(field => !mappedFields.has(field));

    if (missingFields.length > 0) {
      logger.warn(`Missing fields will be filled with defaults: ${missingFields.join(', ')}`);

      // Add default values for missing fields
      for (const row of data) {
        for (const field of missingFields) {
          if (field.toLowerCase().includes('date')) {
            row[field] = new Date();
          } else if (field.toLowerCase().includes('id')) {
            row[field] = data.indexOf(row) + 1;
          } else if (field.toLowerCase().includes('amount') ||
            field.toLowerCase().includes('cost') ||
            field.toLowerCase().includes('price')) {
            row[field] = 0.0;
          } else if (field.toLowerCase().includes('quantity')) {
            row[field] = 1;
          } else {
            row[field] = "Unknown";
          }
        }
      }
    }

    // Select required fields
    const resultData = data.map(row => {
      const newRow = {};
      for (const field of requiredFields) {
        if (field in row) {
          newRow[field] = row[field];
        } else {
          newRow[field] = null;
        }
      }
      return newRow;
    });

    // Convert date columns
    // const dateColumns = {
    //   "customers": ["birth_date", "registration_date"],
    //   "transactions": ["transaction_date"],
    //   "stores": ["opening_date"],
    //   "product_lines": []
    // };

    // for (const dateCol of dateColumns[tableName] || []) {
    //   for (const row of resultData) {
    //     if (row[dateCol]) {
    //       row[dateCol] = new Date(row[dateCol]);
    //     }
    //   }
    // }

    return resultData;
  } catch (error) {
    logger.error(`Error in data validation and mapping: ${error.message}`);
    throw new Error(`Data validation failed: ${error.message}`);
  }
}

const getTables = async (req, res) => {
  const pool = await getAllTable()
  if (pool) {
    res.status(200).json({
      message: 'Get all table successful',
      data: pool
    })
  } else {
    res.status(400).json({
      message: 'Get all table fail',
      data: pool
    })
  }
}

const getRelated = async (req, res) => {
  try {
    const { tableName } = req.body;
    const pool = await getRelatedTables(tableName);
    if (pool) {
      res.status(200).json({
        message: 'Get all related tables successful',
        data: pool
      })
    } else {
      res.status(400).json({
        message: 'Get all related tables fail',
        data: pool
      })
    }
  } catch (error) {
    logger.error('Error when getting related tables:', error);
    return res.status(400).json({
      detail: `Error when getting related tables: ${error.message}`
    });
  }
}


const testConnection = async (req, res) => {
  const { connection_url } = req.body;
  let username, password, host, port, database;

  if (connection_url) {
    console.log('Received connection_url:', connection_url);
    try {
      // Log the URL before parsing
      logger.debug('URL to parse:', connection_url.replace(/\/\/[^:]+:[^@]+@/, '//***:***@'));

      // Sử dụng URL constructor để phân tích cú pháp URL
      const url = new URL(connection_url);

      username = url.username;
      password = url.password;
      host = url.hostname;
      port = url.port || '6543';  // Nếu không có port thì mặc định là 6543
      database = url.pathname.replace('/', '') || 'postgres';  // Đảm bảo có database, nếu không dùng 'postgres'
    } catch (error) {
      logger.error('Error parsing connection URL:', error);
      return res.status(400).json({
        detail: `Invalid connection URL format: ${error.message}`
      });
    }
  }

  // Kiểm tra các tham số kết nối
  const missingParams = [];
  if (!host) missingParams.push('host');
  if (!port) missingParams.push('port');
  if (!database) missingParams.push('database');
  if (!username) missingParams.push('username');
  if (!password) missingParams.push('password');

  if (missingParams.length > 0) {
    logger.warn('Missing parameters detected:', {
      missingParams,
      receivedParams: {
        host: host || 'undefined',
        port: port || 'undefined',
        database: database || 'undefined',
        username: username || 'undefined',
        hasPassword: !!password
      }
    });
    return res.status(400).json({
      detail: `Missing required connection parameters: ${missingParams.join(', ')}`
    });
  }

  // Kiểm tra định dạng Supabase
  if (!host.includes('pooler.supabase.com')) {
    logger.warn('Invalid Supabase pooler URL format', {
      host,
      expectedFormat: 'pooler.supabase.com'
    });
    return res.status(400).json({
      detail: "Please use the Supabase Transaction Pooler URL format"
    });
  }

  // Kiểm tra nếu cổng là một số hợp lệ
  const portNumber = parseInt(port);
  if (isNaN(portNumber)) {
    logger.warn('Invalid port number', { port });
    return res.status(400).json({
      detail: "Port must be a valid number"
    });
  }

  // Gọi hàm từ data layer để kiểm tra kết nối
  try {
    const result = await testDatabaseConnection(host, portNumber, database, username, password);
    logger.info('Connection test completed successfully');
    return res.json(result);
  } catch (error) {
    logger.error('Connection test failed', {
      error: error.message,
      errorCode: error.code,
      errorDetail: error.detail,
      errorHint: error.hint,
      errorStack: error.stack
    });
    return res.status(400).json({
      detail: `Connection failed: ${error.message}`
    });
  }
};

const getPostgresTables = async (req, res) => {
  const { connection_url } = req.body;
  console.log("connect url: ", connection_url);
  let sourceDb = null;

  try {
    logger.info("Starting get_postgres_tables endpoint");

    if (!connection_url) {
      throw new Error("No connection URL provided. Please provide a PostgreSQL connection URL.");
    }

    try {
      sourceDb = await createSourceConnection(connection_url);
      logger.info("Successfully connected to PostgreSQL database");
    } catch (error) {
      logger.error(`Error connecting to database: ${error.message}`);
      throw new Error("Failed to connect to database. Please verify your connection URL is correct.");
    }

    // Query to get all tables and columns
    const query = `
      SELECT 
        t.table_schema as schema_name,
        t.table_name,
        c.column_name,
        pg_catalog.obj_description(pgc.oid, 'pg_class') as description
      FROM 
        information_schema.tables t
      JOIN 
        information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
      JOIN 
        pg_catalog.pg_class pgc ON pgc.relname = t.table_name
      WHERE 
        t.table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY 
        t.table_schema, t.table_name, c.ordinal_position
    `;

    const result = await sourceDb.query(query);
    logger.info(`Query returned ${result.rows.length} rows`);

    // Process results into a structured format
    const tables = {};
    for (const row of result.rows) {
      const { schema_name, table_name, column_name, description } = row;
      const tableKey = `${schema_name}.${table_name}`;

      if (!tables[tableKey]) {
        tables[tableKey] = {
          schema_name,
          table_name,
          description: description || `${table_name} table`,
          columns: []
        };
      }

      tables[tableKey].columns.push(column_name);
    }

    logger.info(`Successfully processed ${Object.keys(tables).length} tables`);
    res.json(Object.values(tables));

  } catch (error) {
    logger.error(`Error in get_postgres_tables: ${error.message}`);
    res.status(500).json({ error: error.message });
  } finally {
    if (sourceDb) {
      await sourceDb.end();
    }
  }
};

const automateDataMapping = async (req, res) => {
  const { source_connection_url, table, query } = req.body;
  let sourceDb = null;

  try {
    logger.info(`Starting automated data mapping process for table: ${table}`);

    // Step 1: Test connection to source database
    sourceDb = await createSourceConnection(source_connection_url);
    logger.info("Successfully connected to source database");

    // Step 2: Execute user's query
    logger.info(`Executing query: ${query}`);
    const result = await sourceDb.query(query);
    const data = result.rows;

    if (!data.length) {
      return res.json({
        success: true,
        message: "No data found to process",
        data: [],
        row_count: 0,
        columns: []
      });
    }

    // Step 3: Map data according to schema
    logger.info(`Mapping ${data.length} rows to standard schema`);
    const mappedData = validateAndMapData(data, table);
    logger.info("Data mapping completed");

    // Step 4: Insert mapped data into Supabase
    const supabase = getSupabase();
    logger.info(`Inserting ${mappedData.length} records into Supabase`);

    let insertedCount = 0;
    try {
      // Insert records one by one to better handle errors
      for (const record of mappedData) {
        const { data: insertResponse, error } = await supabase
          .from(table)
          .upsert(record, {
            onConflict: 'customer_id',
            ignoreDuplicates: false
          });

        if (error) {
          logger.warn(`Failed to insert record: ${error.message}`);
          continue;
        }
        if (insertResponse) {
          insertedCount++;
        }
      }
    } catch (error) {
      logger.warn(`Insert failed: ${error.message}`);
    }

    const response = {
      success: true,
      message: `Successfully processed ${data.length} rows and inserted ${insertedCount} rows`,
      data: convertToJsonSerializable(data),
      row_count: data.length,
      columns: Object.keys(data[0]),
      inserted_count: insertedCount,
      mapping_details: {
        source_table: table,
        mapped_fields: SCHEMA_MAPPINGS[table].required_fields,
        total_rows: data.length,
        inserted_rows: insertedCount
      }
    };

    logger.info(`Automated process completed successfully`);
    res.json(response);

  } catch (error) {
    logger.error(`Error in automated process: ${error.message}`);
    res.status(400).json({
      error: error.message,
      details: "Failed to complete the automated data mapping process"
    });
  } finally {
    if (sourceDb) {
      await sourceDb.end();
    }
  }
};

// Helper function to validate UUID
function isValidUUID(uuid) {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return uuidRegex.test(uuid);
}

const executeQuery = async (req, res) => {
  const { table, query, connection_details } = req.body;
  const user = req.user;

  if (!user || !user.user_id) {
    return res.status(400).json({
      success: false,
      error: "User authentication required",
      detail: "No user_id found in user session"
    });
  }

  try {
    const supabase = getSupabase();
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError) {
      throw new Error(`Failed to get user's business_id: ${userError.message}`);
    }

    if (!userData || !userData.business_id) {
      return res.status(400).json({
        success: false,
        error: "Business ID not found",
        detail: "User does not have an associated business_id"
      });
    }

    const business_id = userData.business_id;
    let pool = null;

    try {
      pool = await createSourceConnection(connection_details);
      const result = await pool.query(query);
      const rows = result.rows;
      const columns = result.fields.map(field => field.name);

      if (rows.length === 0) {
        return res.json({
          success: true,
          data: [],
          row_count: 0,
          columns: []
        });
      }

      const mappedData = validateAndMapData(rows, table);
      const records = mappedData.map(record => ({
        ...record,
        customer_id: record.customer_id && isValidUUID(record.customer_id)
          ? record.customer_id
          : uuidv4(),
        business_id: business_id
      }));

      let insertedCount = 0;
      let updatedCount = 0;

      const tableUniqueFields = {
        'customers': ['email', 'phone'],
        'product_lines': ['name', 'brand'],
        'stores': ['store_name', 'address']
      };

      const uniqueFields = tableUniqueFields[table] || [];

      if (uniqueFields.length > 0) {
        for (const record of records) {
          try {
            const conditions = {};
            let hasValidFields = false;

            uniqueFields.forEach(field => {
              if (record[field]) {
                conditions[field] = record[field];
                hasValidFields = true;
              }
            });

            if (!hasValidFields) continue;

            const { data: existingRecords, error: checkError } = await supabase
              .from(table)
              .select('*')
              .match(conditions);

            if (checkError) continue;

            if (existingRecords && existingRecords.length > 0) {
              const existingRecord = existingRecords[0];
              const idField = table.slice(0, -1) + '_id';
              const existingId = existingRecord[idField];

              const { error: updateError } = await supabase
                .from(table)
                .update(record)
                .eq(idField, existingId);

              if (!updateError) updatedCount++;
            } else {
              const { error: insertError } = await supabase
                .from(table)
                .insert(record);

              if (!insertError) insertedCount++;
            }
          } catch (error) {
            logger.error(`Error processing record: ${error.message}`);
            continue;
          }
        }
      } else {
        const { error } = await supabase
          .from(table)
          .insert(records);

        if (error) throw error;
        insertedCount = records.length;
      }

      const response = {
        success: true,
        data: convertToJsonSerializable(rows),
        row_count: rows.length,
        columns: columns,
        inserted_count: insertedCount,
        updated_count: updatedCount,
        business_id: business_id
      };

      res.json(response);

    } catch (error) {
      logger.error(`Error in query execution: ${error.message}`);
      res.status(400).json({
        detail: `Error executing query: ${error.message}`
      });
    } finally {
      if (pool) await pool.end();
    }

  } catch (error) {
    logger.error(`Error processing request: ${error.message}`);
    res.status(400).json({
      success: false,
      error: error.message,
      detail: "Failed to process request"
    });
  }
};

export {
  getTables,
  getRelated,
  executeQuery,
  testConnection,
  getPostgresTables,
  automateDataMapping
}