import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';
import {
  updateCustomerSegmentsQuery,
  getCustomerJourneyQuery,
  updateBusinessIdsQuery,
} from '../data/customerLifecycleQueries.js';

// Helper functions for each customer segment
const getNewCustomersMetrics = async (req, res) => {
  const user = req.user;

  logger.info('Starting new customers metrics analysis', {
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
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError || !userData?.business_id) {
      logger.error('Error retrieving business_id', {
        error: userError?.message,
        user_id: user.user_id
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found"
      });
    }

    // Get metrics for last 30 days using the RPC function
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_new_customers_metrics', {
      p_business_id: Number(userData.business_id)
    });

    if (metricsError) {
      logger.error('Error in get_new_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_new_customers_info', {
      p_business_id: Number(userData.business_id)
    });

    if (customersError) {
      logger.error('Error in get_detailed_new_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data - expecting a single row with all metrics
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        first_purchase_gmv: metrics.first_purchase_gmv || 0,
        avg_first_purchase_value: metrics.avg_first_purchase_value || 0,
        conversion_to_second_purchase_rate: metrics.conversion_to_second_purchase_rate || 0
      },
      customers: customersData || []
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in new customers metrics analysis', {
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

const getEarlyLifeCustomersMetrics = async (req, res) => {
  const user = req.user;

  logger.info('Starting early life customers metrics analysis', {
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
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError || !userData?.business_id) {
      logger.error('Error retrieving business_id', {
        error: userError?.message,
        user_id: user.user_id
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found"
      });
    }

    // Get metrics using the RPC function
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_early_life_customers_metrics', {
      p_business_id: Number(userData.business_id)
    });

    if (metricsError) {
      logger.error('Error in get_early_life_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_early_life_customers_info', {
      p_business_id: Number(userData.business_id)
    });

    if (customersError) {
      logger.error('Error in get_detailed_early_life_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data - expecting a single row with all metrics
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        repeat_purchase_rate: metrics.repeat_purchase_rate || 0,
        avg_time_between_purchases: metrics.avg_time_between_purchases || 0,
        avg_order_value: metrics.avg_order_value || 0,
        orders: metrics.orders || 0,
        aov: metrics.aov || 0,
        arpu: metrics.arpu || 0,
        orders_per_day: metrics.orders_per_day || 0
      },
      customers: customersData || []
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in early life customers metrics analysis', {
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

const getMatureCustomersMetrics = async (req, res) => {
  const user = req.user;

  logger.info('Starting mature customers metrics analysis', {
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
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError || !userData?.business_id) {
      logger.error('Error retrieving business_id', {
        error: userError?.message,
        user_id: user.user_id
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found"
      });
    }

    // Get metrics using the RPC function
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_mature_customers_metrics', {
      p_business_id: Number(userData.business_id)
    });

    if (metricsError) {
      logger.error('Error in get_mature_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_mature_customers_info', {
      p_business_id: Number(userData.business_id)
    });

    if (customersError) {
      logger.error('Error in get_detailed_mature_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data - expecting a single row with all metrics
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        purchase_frequency: metrics.purchase_frequency || 0,
        avg_basket_size: metrics.avg_basket_size || 0,
        monthly_spend: metrics.monthly_spend || 0
      },
      customers: customersData || []
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in mature customers metrics analysis', {
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

const getLoyalCustomersMetrics = async (req, res) => {
  const user = req.user;

  logger.info('Starting loyal customers metrics analysis', {
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
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError || !userData?.business_id) {
      logger.error('Error retrieving business_id', {
        error: userError?.message,
        user_id: user.user_id
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found"
      });
    }

    // Get metrics using the RPC function
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_loyal_customers_metrics', {
      p_business_id: Number(userData.business_id)
    });

    if (metricsError) {
      logger.error('Error in get_loyal_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_loyal_customers_info', {
      p_business_id: Number(userData.business_id)
    });

    if (customersError) {
      logger.error('Error in get_detailed_loyal_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data - expecting a single row with all metrics
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        annual_customer_value: metrics.annual_customer_value || 0,
        purchase_frequency: metrics.purchase_frequency || 0,
        category_penetration: metrics.category_penetration || 0
      },
      customers: customersData || []
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in loyal customers metrics analysis', {
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

const getDetailedCustomerStageInfo = async (req, res) => {
  const user = req.user;
  const { stage } = req.params;

  logger.info('Starting detailed customer stage info analysis', {
    user_id: user?.user_id,
    stage
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!stage) {
    logger.warn('Stage parameter missing');
    return res.status(400).json({
      success: false,
      error: "Stage parameter is required"
    });
  }

  // Map URL parameter to actual customer segment name
  const stageMapping = {
    'new-customers': 'New Customer',
    'early-life-customers': 'Early-life Customer',
    'mature-customers': 'Mature Customer',
    'loyal-customers': 'Loyal Customer',
    'inactive': 'Inactive'
  };

  const customerSegment = stageMapping[stage];
  if (!customerSegment) {
    logger.warn('Invalid stage parameter', { stage });
    return res.status(400).json({
      success: false,
      error: "Invalid stage parameter. Must be one of: new-customers, early-life-customers, mature-customers, loyal-customers, inactive"
    });
  }

  try {
    const supabase = getSupabase();
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError || !userData?.business_id) {
      logger.error('Error retrieving business_id', {
        error: userError?.message,
        user_id: user.user_id
      });
      return res.status(400).json({
        success: false,
        error: "Business ID not found"
      });
    }

    // Get detailed customer information using the PostgreSQL function
    const { data, error } = await supabase.rpc('get_detailed_customer_stage_info', {
      p_business_id: userData.business_id,
      p_customer_segment: customerSegment
    });

    if (error) {
      logger.error('Error in get_detailed_customer_stage_info:', {
        error: error.message,
        business_id: userData.business_id,
        stage: customerSegment
      });
      throw error;
    }

    logger.info('Successfully retrieved detailed customer stage info', {
      business_id: userData.business_id,
      stage: customerSegment,
      customer_count: data?.length || 0
    });

    res.json({
      success: true,
      data: {
        stage: customerSegment,
        customers: data || []
      }
    });
  } catch (error) {
    logger.error('Error in detailed customer stage info analysis', {
      error: error.message,
      stack: error.stack,
      user_id: user?.user_id,
      stage: customerSegment
    });
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

export {
  updateCustomerSegments,
  getCustomerJourney,
  updateBusinessIds,
  getNewCustomersMetrics,
  getEarlyLifeCustomersMetrics,
  getMatureCustomersMetrics,
  getLoyalCustomersMetrics,
  getDetailedCustomerStageInfo
};