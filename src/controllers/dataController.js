import { createSourceConnection, getSupabase, logger } from '../services/database.js';
import pkg from 'pg';
const { Pool } = pkg;
import multer from 'multer';
import { parse } from 'csv-parse';
import xlsx from 'xlsx';
import crypto from 'crypto';

// Schema mappings for each table
const SCHEMA_MAPPINGS = {
  customers: {
    required_fields: [
      "customer_id", "first_name", "last_name", "email", 
      "phone", "gender", "birth_date", "registration_date", 
      "address", "city"
    ]
  },
  transactions: {
    required_fields: [
      "transaction_id", "customer_id", "store_id", 
      "transaction_date", "total_amount", "payment_method",
      "product_line_id", "quantity", "unit_price"
    ]
  },
  stores: {
    required_fields: [
      "store_id", "store_name", "address", "city",
      "store_type", "opening_date", "region"
    ]
  },
  product_lines: {
    required_fields: [
      "product_line_id", "name", "category", "subcategory",
      "brand", "unit_cost"
    ]
  }
};

// Helper function to convert data to JSON-serializable format
const convertToJsonSerializable = (obj) => {
  if (obj instanceof Date) {
    return obj.toISOString();
  } else if (typeof obj === 'number' && Number.isInteger(obj)) {
    return parseInt(obj);
  } else if (typeof obj === 'number') {
    return parseFloat(obj);
  } else if (Array.isArray(obj)) {
    return obj.map(item => convertToJsonSerializable(item));
  } else if (obj && typeof obj === 'object') {
    return Object.fromEntries(
      Object.entries(obj).map(([key, value]) => [key, convertToJsonSerializable(value)])
    );
  }
  return obj;
};

const validateAndMapData = (data, tableName) => {
  try {
    const requiredFields = SCHEMA_MAPPINGS[tableName].required_fields;
    const mappedData = [];

    for (const row of data) {
      const mappedRow = {};
      
      // Map existing fields
      for (const field of requiredFields) {
        if (row[field]) {
          mappedRow[field] = row[field];
        } else {
          // Handle missing required fields with defaults
          if (field.includes('date')) {
            mappedRow[field] = new Date().toISOString();
          } else if (field.includes('id')) {
            mappedRow[field] = mappedData.length + 1;
          } else if (field.includes('amount') || field.includes('cost') || field.includes('price')) {
            mappedRow[field] = 0.0;
          } else if (field.includes('quantity')) {
            mappedRow[field] = 1;
          } else {
            mappedRow[field] = "Unknown";
          }
        }
      }

      mappedData.push(mappedRow);
    }

    return mappedData;
  } catch (error) {
    logger.error(`Error in data validation and mapping: ${error.message}`);
    throw new Error(`Data validation failed: ${error.message}`);
  }
};

export const getTables = async (req, res) => {
  try {
    const tables = {
      "Customer Profile": {
        name: "customers",
        fields: SCHEMA_MAPPINGS.customers.required_fields,
        description: "Customer information"
      },
      "Transactions": {
        name: "transactions",
        fields: SCHEMA_MAPPINGS.transactions.required_fields,
        description: "Transaction records"
      },
      "Stores": {
        name: "stores",
        fields: SCHEMA_MAPPINGS.stores.required_fields,
        description: "Store information"
      },
      "Product Line": {
        name: "product_lines",
        fields: SCHEMA_MAPPINGS.product_lines.required_fields,
        description: "Product information"
      }
    };

    console.log('now getting tables');
    res.json(tables);
  } catch (error) {
    logger.error(`Error getting tables: ${error.message}`);
    res.status(500).json({ error: error.message });
  }
};

export const executeQuery = async (req, res) => {
  const { table, query, connection_url } = req.body;
  let sourceDb = null;

  try {
    logger.info(`Executing query for table: ${table}`);
    
    // Create connection to source database
    sourceDb = await createSourceConnection(connection_url);

    // Execute query
    logger.info(`Executing query on source database: ${query}`);
    const result = await sourceDb.query(query);
    const data = result.rows;
    
    if (!data.length) {
      return res.json({
        success: true,
        data: [],
        row_count: 0,
        columns: []
      });
    }

    // Map data to standard schema
    const mappedData = validateAndMapData(data, table);
    logger.info("Data mapped to standard schema");

    // Insert into Supabase
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
      data: convertToJsonSerializable(data),
      row_count: data.length,
      columns: Object.keys(data[0]),
      inserted_count: insertedCount
    };

    logger.info(`Successfully processed ${response.row_count} rows and inserted ${insertedCount} rows`);
    res.json(response);

  } catch (error) {
    logger.error(`Error in query execution: ${error.message}`);
    res.status(400).json({ error: error.message });
  } finally {
    if (sourceDb) {
      await sourceDb.end();
    }
  }
};

export const uploadFile = async (req, res) => {
  const { table_name } = req.params;
  const file = req.file;

  try {
    logger.info(`Received file upload request for table: ${table_name}`);
    
    let data;
    if (file.originalname.endsWith('.csv')) {
      data = await new Promise((resolve, reject) => {
        const results = [];
        file.buffer
          .toString()
          .pipe(parse({ columns: true }))
          .on('data', (data) => results.push(data))
          .on('end', () => resolve(results))
          .on('error', reject);
      });
    } else if (file.originalname.match(/\.(xlsx|xls)$/)) {
      const workbook = xlsx.read(file.buffer);
      const sheetName = workbook.SheetNames[0];
      data = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
    } else {
      throw new Error("Unsupported file format. Please upload CSV or Excel files.");
    }

    logger.info(`File read successfully. Found ${data.length} rows`);

    // Map data to standard schema
    const mappedData = validateAndMapData(data, table_name);

    // Insert into Supabase
    const supabase = getSupabase();
    logger.info(`Inserting ${mappedData.length} records into Supabase`);

    let insertedCount = 0;
    try {
      // Insert records one by one to better handle errors
      for (const record of mappedData) {
        const { data: insertResponse, error } = await supabase
          .from(table_name)
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

    res.json({
      success: true,
      message: `Successfully processed ${mappedData.length} rows and inserted ${insertedCount} rows`,
      rows_processed: mappedData.length,
      rows_inserted: insertedCount
    });

  } catch (error) {
    logger.error(`Error processing file: ${error.message}`);
    res.status(400).json({ error: error.message });
  }
};

export const testConnection = async (req, res) => {
  const { connection_url } = req.body;
  let sourceDb = null;

  try {
    sourceDb = await createSourceConnection(connection_url);
    await sourceDb.query('SELECT 1');
    
    res.json({
      success: true,
      message: "Connection successful"
    });
  } catch (error) {
    logger.error(`Connection test failed: ${error.message}`);
    res.status(400).json({ error: error.message });
  } finally {
    if (sourceDb) {
      await sourceDb.end();
    }
  }
};

export const getPostgresTables = async (req, res) => {
  const { connection_url } = req.body;
  console.log("connect url: ",connection_url);
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

export const automateDataMapping = async (req, res) => {
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