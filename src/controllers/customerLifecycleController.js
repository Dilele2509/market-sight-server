import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';
import {
  getNewCustomersMetricsQuery,
  getEarlyLifeCustomersMetricsQuery,
  getMatureCustomersMetricsQuery,
  getLoyalCustomersMetricsQuery,
  updateCustomerSegmentsQuery,
  getCustomerJourneyQuery,
  updateBusinessIdsQuery
} from '../data/customerLifecycleQueries.js';

// Main controller function for customer lifecycle analysis
const getCustomerLifecycleMetrics = async (req, res) => {


  const { start_date, end_date } = req.body;
  const user = req.user;
  let userData;

  logger.info('Starting customer lifecycle metrics analysis', {
    user_id: user?.user_id,
    request_body: req.body
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  // Validate date parameters
  if (!start_date || !end_date) {
    logger.warn('Missing date parameters', { 
      start_date, 
      end_date,
      request_body: req.body
    });
    return res.status(400).json({
      success: false,
      error: "Both start_date and end_date are required",
      detail: "Please provide start_date and end_date in the request body"
    });
  }

  try {
    const supabase = getSupabase();
    logger.info('Retrieving business_id for user', { user_id: user.user_id });

    // Get user's business_id first
    const { data, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError) {
      logger.error('Error retrieving business_id', {
        error: userError.message,
        user_id: user.user_id
      });
      throw new Error(`Failed to get user's business_id: ${userError.message}`);
    }

    userData = data;

    if (!userData || !userData.business_id) {
      logger.warn('No business_id found for user', {
        user_id: user.user_id,
        userData
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found",
        detail: "User does not have an associated business_id"
      });
    }

    const business_id = userData.business_id;
    logger.info('Successfully retrieved business_id', { 
      business_id,
      user_id: user.user_id 
    });


    const [
      newCustomers,
      earlyLifeCustomers,
      matureCustomers,
      loyalCustomers
    ] = await Promise.all([
      getNewCustomersMetrics(supabase, start_date, end_date, business_id),
      getEarlyLifeCustomersMetrics(supabase, start_date, end_date, business_id),
      getMatureCustomersMetrics(supabase, start_date, end_date, business_id),
      getLoyalCustomersMetrics(supabase, start_date, end_date, business_id)
    ]);

    // Check for errors in any of the queries
    const errors = [
      newCustomers.error,
      earlyLifeCustomers.error,
      matureCustomers.error,
      loyalCustomers.error
    ].filter(Boolean);

    if (errors.length > 0) {
      logger.error('Errors in metrics queries', {
        business_id,
        start_date,
        end_date,
        errors: errors.map(e => e.message)
      });
      throw new Error(`Errors in metrics queries: ${errors.map(e => e.message).join(', ')}`);
    }

    logger.info('Successfully retrieved all customer metrics', {
      business_id,
      start_date,
      end_date,
      new_customers_count: newCustomers.data?.[0]?.customer_count,
      early_life_customers_count: earlyLifeCustomers.data?.[0]?.customer_count,
      mature_customers_count: matureCustomers.data?.[0]?.customer_count,
      loyal_customers_count: loyalCustomers.data?.[0]?.customer_count
    });

    const response = {
      success: true,
      data: {
        new_customers: newCustomers.data?.[0] || {},
        early_life_customers: earlyLifeCustomers.data?.[0] || {},
        mature_customers: matureCustomers.data?.[0] || {},
        loyal_customers: loyalCustomers.data?.[0] || {}
      }
    };

    res.json(response);
  } catch (error) {
    logger.error('Error in customer lifecycle analysis', {
      error: error.message,
      stack: error.stack,
      user_id: user?.user_id,
      business_id: userData?.business_id,
      start_date,
      end_date
    });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

// Helper functions for each customer segment
const getNewCustomersMetrics = async (supabase, start_date, end_date, business_id) => {
  const { data, error } = await supabase.rpc('execute_sql', {
    params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
    sql: getNewCustomersMetricsQuery
  });

  if (error) {
    logger.error('Error in execute_sql RPC:', {
      error: error.message,
      params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
      sql: getNewCustomersMetricsQuery
    });
    throw error;
  }

  return data?.[0] || {};
};

const getEarlyLifeCustomersMetrics = async (supabase, start_date, end_date, business_id) => {
  const { data, error } = await supabase.rpc('execute_sql', {
    params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
    sql: getEarlyLifeCustomersMetricsQuery
  });

  if (error) {
    logger.error('Error in execute_sql RPC:', {
      error: error.message,
      params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
      sql: getEarlyLifeCustomersMetricsQuery
    });
    throw error;
  }

  return data?.[0] || {};
};

const getMatureCustomersMetrics = async (supabase, start_date, end_date, business_id) => {
  const { data, error } = await supabase.rpc('execute_sql', {
    params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
    sql: getMatureCustomersMetricsQuery
  });

  if (error) {
    logger.error('Error in execute_sql RPC:', {
      error: error.message,
      params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
      sql: getMatureCustomersMetricsQuery
    });
    throw error;
  }

  return data?.[0] || {};
};

const getLoyalCustomersMetrics = async (supabase, start_date, end_date, business_id) => {
  const { data, error } = await supabase.rpc('execute_sql', {
    params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
    sql: getLoyalCustomersMetricsQuery
  });

  if (error) {
    logger.error('Error in execute_sql RPC:', {
      error: error.message,
      params: [Number(business_id), new Date(start_date).toISOString(), new Date(end_date).toISOString()],
      sql: getLoyalCustomersMetricsQuery
    });
    throw error;
  }

  return data?.[0] || {};
};

// Function to update customer segments
const updateCustomerSegments = async (req, res) => {
  const user = req.user;

  logger.info('Starting customer segments update', { user_id: user?.user_id });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  try {
    const supabase = getSupabase();
    logger.info('Retrieving business_id for user', { user_id: user.user_id });

    // Get user's business_id first
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError) {
      logger.error('Error retrieving business_id', {
        error: userError.message,
        user_id: user.user_id
      });
      throw new Error(`Failed to get user's business_id: ${userError.message}`);
    }

    if (!userData || !userData.business_id) {
      logger.warn('No business_id found for user', {
        user_id: user.user_id,
        userData
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found",
        detail: "User does not have an associated business_id"
      });
    }

    const business_id = userData.business_id;
    logger.info('Successfully retrieved business_id', { business_id });

    // Update segments for all customers
    logger.info('Updating customer segments', { business_id });

    const { data, error } = await supabase.rpc('execute_sql', {
      params: { business_id },
      sql: updateCustomerSegmentsQuery
    });

    if (error) {
      logger.error('Error updating customer segments', {
        error: error.message,
        business_id
      });
      throw error;
    }

    logger.info('Successfully updated customer segments', {
      business_id,
      affected_rows: data?.length || 0
    });

    res.json({
      success: true,
      message: "Customer segments updated successfully",
      affected_rows: data?.length || 0
    });
  } catch (error) {
    logger.error('Error updating customer segments', {
      error: error.message,
      stack: error.stack,
      user_id: user?.user_id
    });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

// Function to get customer journey visualization
const getCustomerJourney = async (req, res) => {
  const { start_date, end_date } = req.query;
  const user = req.user;

  logger.info('Starting customer journey analysis', {
    start_date,
    end_date,
    user_id: user?.user_id
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  try {
    const supabase = getSupabase();
    logger.info('Retrieving business_id for user', { user_id: user.user_id });

    // Get user's business_id first
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError) {
      logger.error('Error retrieving business_id', {
        error: userError.message,
        user_id: user.user_id
      });
      throw new Error(`Failed to get user's business_id: ${userError.message}`);
    }

    if (!userData || !userData.business_id) {
      logger.warn('No business_id found for user', {
        user_id: user.user_id,
        userData
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found",
        detail: "User does not have an associated business_id"
      });
    }

    const business_id = userData.business_id;
    logger.info('Successfully retrieved business_id', { business_id });

    logger.info('Fetching customer journey data', {
      business_id,
      start_date,
      end_date
    });

    const { data, error } = await supabase.rpc('execute_sql', {
      params: { start_date, end_date, business_id },
      sql: getCustomerJourneyQuery
    });

    if (error) {
      logger.error('Error fetching customer journey data', {
        error: error.message,
        business_id,
        start_date,
        end_date
      });
      throw error;
    }

    logger.info('Successfully fetched customer journey data', {
      business_id,
      data_count: data?.length
    });

    res.json({
      success: true,
      data
    });
  } catch (error) {
    logger.error('Error getting customer journey', {
      error: error.message,
      stack: error.stack,
      user_id: user?.user_id
    });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

// Function to update business_ids in related tables
const updateBusinessIds = async (req, res) => {
  const user = req.user;

  logger.info('Starting business_id update process', { user_id: user?.user_id });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  try {
    const supabase = getSupabase();
    logger.info('Retrieving business_id for user', { user_id: user.user_id });

    // Get user's business_id first
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError) {
      logger.error('Error retrieving business_id', {
        error: userError.message,
        user_id: user.user_id
      });
      throw new Error(`Failed to get user's business_id: ${userError.message}`);
    }

    if (!userData || !userData.business_id) {
      logger.warn('No business_id found for user', {
        user_id: user.user_id,
        userData
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found",
        detail: "User does not have an associated business_id"
      });
    }

    const business_id = userData.business_id;
    logger.info('Successfully retrieved business_id', { 
      business_id,
      user_id: user.user_id 
    });

    // Update business_ids in related tables
    logger.info('Updating business_ids in related tables', { business_id });

    const { data, error } = await supabase.rpc('execute_sql', {
      params: [business_id],
      sql: updateBusinessIdsQuery
    });

    if (error) {
      logger.error('Error updating business_ids', {
        error: error.message,
        business_id
      });
      throw error;
    }

    logger.info('Successfully updated business_ids', {
      business_id,
      updates: data?.[0]
    });

    res.json({
      success: true,
      message: "Business IDs updated successfully",
      updates: data?.[0]
    });
  } catch (error) {
    logger.error('Error updating business_ids', {
      error: error.message,
      stack: error.stack,
      user_id: user?.user_id
    });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

export {
  getCustomerLifecycleMetrics,
  updateCustomerSegments,
  getCustomerJourney,
  updateBusinessIds
};