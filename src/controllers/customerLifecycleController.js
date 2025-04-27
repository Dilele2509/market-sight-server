import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';
import {
  updateBusinessIdsQuery,
} from '../data/customerLifecycleQueries.js';

// Function to get New customers metrics

const getNewCustomersMetrics = async (req, res) => {
  const user = req.user;
  const { reference_date, time_range } = req.body;

  logger.info('Starting new customers metrics analysis', {
    user_id: user?.user_id,
    reference_date,
    time_range
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!reference_date || !time_range) {
    logger.warn('Missing required parameters', { reference_date, time_range });
    return res.status(400).json({
      success: false,
      error: "Reference date and time range are required"
    });
  }

  try {
    // Validate date format
    const referenceDate = new Date(reference_date);
    if (isNaN(referenceDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid reference_date format. Use YYYY-MM-DD"
      });
    }

    // Validate time_range
    if (![3, 6, 9, 12].includes(Number(time_range))) {
      return res.status(400).json({
        success: false,
        error: "Invalid time_range. Must be one of: 3, 6, 9, 12 months"
      });
    }

    // Validate reference_date is not in future
    if (referenceDate > new Date()) {
      return res.status(400).json({
        success: false,
        error: "Reference date cannot be in the future"
      });
    }

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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_new_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (metricsError) {
      logger.error('Error in get_new_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_new_customers_info', {
      p_business_id: Number(userData.business_id),
      p_reference_date: reference_date,
      p_time_range: Number(time_range)
    });

    if (customersError) {
      logger.error('Error in get_detailed_new_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        first_purchase_gmv: metrics.first_purchase_gmv || 0,
        avg_first_purchase_value: metrics.avg_first_purchase_value || 0,
        conversion_to_second_purchase_rate: metrics.conversion_to_second_purchase_rate || 0
      },
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metrics.start_date,
        end_date: metrics.end_date
      }
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

// Function to get Early life customers metrics 

const getEarlyLifeCustomersMetrics = async (req, res) => {
  const user = req.user;
  const { reference_date, time_range } = req.body;

  logger.info('Starting early life customers metrics analysis', {
    user_id: user?.user_id,
    reference_date,
    time_range
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!reference_date || !time_range) {
    logger.warn('Missing required parameters', { reference_date, time_range });
    return res.status(400).json({
      success: false,
      error: "Reference date and time range are required"
    });
  }

  try {
    // Validate date format
    const referenceDate = new Date(reference_date);
    if (isNaN(referenceDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid reference_date format. Use YYYY-MM-DD"
      });
    }

    // Validate time_range
    if (![3, 6, 9, 12].includes(Number(time_range))) {
      return res.status(400).json({
        success: false,
        error: "Invalid time_range. Must be one of: 3, 6, 9, 12 months"
      });
    }

    // Validate reference_date is not in future
    if (referenceDate > new Date()) {
      return res.status(400).json({
        success: false,
        error: "Reference date cannot be in the future"
      });
    }

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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_early_life_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (metricsError) {
      logger.error('Error in get_early_life_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_early_life_customers_info', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (customersError) {
      logger.error('Error in get_detailed_early_life_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data
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
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metrics.start_date,
        end_date: metrics.end_date
      }
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

// Function to get Mature customers metrics

const getMatureCustomersMetrics = async (req, res) => {
  const user = req.user;
  const { reference_date, time_range } = req.body;

  logger.info('Starting mature customers metrics analysis', {
    user_id: user?.user_id,
    reference_date,
    time_range
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!reference_date || !time_range) {
    logger.warn('Missing required parameters', { reference_date, time_range });
    return res.status(400).json({
      success: false,
      error: "Reference date and time range are required"
    });
  }

  try {
    // Validate date format
    const referenceDate = new Date(reference_date);
    if (isNaN(referenceDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid reference_date format. Use YYYY-MM-DD"
      });
    }

    // Validate time_range
    if (![3, 6, 9, 12].includes(Number(time_range))) {
      return res.status(400).json({
        success: false,
        error: "Invalid time_range. Must be one of: 3, 6, 9, 12 months"
      });
    }

    // Validate reference_date is not in future
    if (referenceDate > new Date()) {
      return res.status(400).json({
        success: false,
        error: "Reference date cannot be in the future"
      });
    }

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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_mature_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (metricsError) {
      logger.error('Error in get_mature_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_mature_customers_info', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (customersError) {
      logger.error('Error in get_detailed_mature_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        purchase_frequency: metrics.purchase_frequency || 0,
        avg_basket_size: metrics.avg_basket_size || 0,
        monthly_spend: metrics.monthly_spend || 0
      },
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metrics.start_date,
        end_date: metrics.end_date
      }
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

// Function to get Loyal customers metrics

const getLoyalCustomersMetrics = async (req, res) => {
  const user = req.user;
  const { reference_date, time_range } = req.body;

  logger.info('Starting loyal customers metrics analysis', {
    user_id: user?.user_id,
    reference_date,
    time_range
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!reference_date || !time_range) {
    logger.warn('Missing required parameters', { reference_date, time_range });
    return res.status(400).json({
      success: false,
      error: "Reference date and time range are required"
    });
  }

  try {
    // Validate date format
    const referenceDate = new Date(reference_date);
    if (isNaN(referenceDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid reference_date format. Use YYYY-MM-DD"
      });
    }

    // Validate time_range
    if (![3, 6, 9, 12].includes(Number(time_range))) {
      return res.status(400).json({
        success: false,
        error: "Invalid time_range. Must be one of: 3, 6, 9, 12 months"
      });
    }

    // Validate reference_date is not in future
    if (referenceDate > new Date()) {
      return res.status(400).json({
        success: false,
        error: "Reference date cannot be in the future"
      });
    }

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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_loyal_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (metricsError) {
      logger.error('Error in get_loyal_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_loyal_customers_info', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (customersError) {
      logger.error('Error in get_detailed_loyal_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    // Process the metrics data
    const metrics = Array.isArray(metricsData) && metricsData.length > 0 ? metricsData[0] : {};

    // Return both metrics and detailed customer information
    const response = {
      metrics: {
        customer_count: metrics.customer_count || 0,
        annual_customer_value: metrics.annual_customer_value || 0,
        purchase_frequency: metrics.purchase_frequency || 0,
        category_penetration: metrics.category_penetration || 0
      },
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metrics.start_date,
        end_date: metrics.end_date
      }
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

const getCustomerStagePeriodChanges = async (req, res) => {
  const user = req.user;
  const { reference_date, time_range } = req.body;

  logger.info('Starting customer stage period changes analysis', {
    user_id: user?.user_id,
    reference_date,
    time_range
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!reference_date || !time_range) {
    logger.warn('Missing required parameters', { reference_date, time_range });
    return res.status(400).json({
      success: false,
      error: "Reference date and time range are required"
    });
  }

  try {
    // Validate date format
    const referenceDate = new Date(reference_date);
    if (isNaN(referenceDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid reference_date format. Use YYYY-MM-DD"
      });
    }

    // Validate time_range
    if (![3, 6, 9, 12].includes(Number(time_range))) {
      return res.status(400).json({
        success: false,
        error: "Invalid time_range. Must be one of: 3, 6, 9, 12 months"
      });
    }

    // Validate reference_date is not in future
    if (referenceDate > new Date()) {
      return res.status(400).json({
        success: false,
        error: "Reference date cannot be in the future"
      });
    }

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

    // Get period changes using the RPC function
    const { data: periodChanges, error: periodError } = await supabase.rpc('get_customer_stage_period_changes', {
      p_business_id: Number(userData.business_id),
      p_reference_date: referenceDate.toISOString().split('T')[0],
      p_time_range: Number(time_range)
    });

    if (periodError) {
      logger.error('Error in get_customer_stage_period_changes RPC:', {
        error: periodError.message,
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      throw periodError;
    }

    // Format the response
    const response = {
      period_changes: periodChanges.map(stage => ({
        stage: stage.stage,
        current_period: {
          count: stage.current_period_count,
          start_date: stage.current_period_start,
          end_date: stage.current_period_end
        },
        previous_period: {
          count: stage.previous_period_count,
          start_date: stage.previous_period_start,
          end_date: stage.previous_period_end
        },
        change_percentage: stage.change_percentage
      })),
      time_window: {
        reference_date,
        time_range
      }
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in customer stage period changes analysis', {
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
  updateBusinessIds,
  getNewCustomersMetrics,
  getEarlyLifeCustomersMetrics,
  getMatureCustomersMetrics,
  getLoyalCustomersMetrics,
  getCustomerStagePeriodChanges
};