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

    // Validate time_range is a positive number
    const timeRange = Number(time_range);
    if (isNaN(timeRange) || timeRange <= 0) {
      return res.status(400).json({
        success: false,
        error: "Time range must be a positive number"
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
      p_reference_date: referenceDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_time_range: Number(time_range)
    });

    if (customersError) {
      logger.error('Error in get_detailed_new_customers_info RPC:', {
        error: customersError.message,
        business_id: userData.business_id
      });
      throw customersError;
    }

    if (!metricsData || metricsData.length === 0) {
      logger.warn('No data returned from metrics function', {
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      return res.json({
        success: true,
        data: {
          segment: "New Customers",
          metrics: [],
          customers: [],
          time_window: {
            reference_date,
            time_range
          }
        }
      });
    }

    // Format the response with monthly breakdown
    const response = {
      segment: "New Customers",
      metrics: metricsData.map(period => ({
        period: {
          start_date: period.period_start,
          end_date: period.period_end
        },
        values: {
          customer_count: Number(period.customer_count || 0),
          gmv: Number(period.gmv || 0),
          orders: Number(period.orders || 0),
          unique_customers: Number(period.unique_customers || 0),
          aov: Number(period.aov || 0),
          avg_bill_per_user: Number(period.avg_bill_per_user || 0),
          arpu: Number(period.arpu || 0),
          orders_per_day: Number(period.orders_per_day || 0),
          orders_per_day_per_store: Number(period.orders_per_day_per_store || 0),
          first_purchase_gmv: Number(period.first_purchase_gmv || 0),
          avg_first_purchase_value: Number(period.avg_first_purchase_value || 0),
          conversion_to_second_purchase_rate: Number(period.conversion_to_second_purchase_rate || 0)
        }
      })),
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metricsData[0]?.period_start,
        end_date: metricsData[metricsData.length - 1]?.period_end
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

    // Validate time_range is a positive number
    const timeRange = Number(time_range);
    if (isNaN(timeRange) || timeRange <= 0) {
      return res.status(400).json({
        success: false,
        error: "Time range must be a positive number"
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

    if (!metricsData || metricsData.length === 0) {
      logger.warn('No data returned from metrics function', {
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      return res.json({
        success: true,
        data: {
          segment: "Early-life Customers",
          metrics: [],
          customers: [],
          time_window: {
            reference_date,
            time_range
          }
        }
      });
    }

    // Format the response with monthly breakdown
    const response = {
      segment: "Early-life Customers",
      metrics: metricsData.map(period => ({
        period: {
          start_date: period.period_start,
          end_date: period.period_end
        },
        values: {
          customer_count: Number(period.customer_count || 0),
          gmv: Number(period.gmv || 0),
          orders: Number(period.orders || 0),
          unique_customers: Number(period.unique_customers || 0),
          aov: Number(period.aov || 0),
          avg_bill_per_user: Number(period.avg_bill_per_user || 0),
          arpu: Number(period.arpu || 0),
          orders_per_day: Number(period.orders_per_day || 0),
          orders_per_day_per_store: Number(period.orders_per_day_per_store || 0),
          repeat_purchase_rate: Number(period.repeat_purchase_rate || 0),
          avg_time_between_purchases: Number(period.avg_time_between_purchases || 0),
          avg_order_value: Number(period.avg_order_value || 0)
        }
      })),
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metricsData[0]?.period_start,
        end_date: metricsData[metricsData.length - 1]?.period_end
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

    // Validate time_range is a positive number
    const timeRange = Number(time_range);
    if (isNaN(timeRange) || timeRange <= 0) {
      return res.status(400).json({
        success: false,
        error: "Time range must be a positive number"
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

    if (!metricsData || metricsData.length === 0) {
      logger.warn('No data returned from metrics function', {
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      return res.json({
        success: true,
        data: {
          segment: "Mature Customers",
          metrics: [],
          customers: [],
          time_window: {
            reference_date,
            time_range
          }
        }
      });
    }

    // Format the response with monthly breakdown
    const response = {
      segment: "Mature Customers",
      metrics: metricsData.map(period => ({
        period: {
          start_date: period.period_start,
          end_date: period.period_end
        },
        values: {
          customer_count: Number(period.customer_count || 0),
          gmv: Number(period.gmv || 0),
          orders: Number(period.orders || 0),
          unique_customers: Number(period.unique_customers || 0),
          aov: Number(period.aov || 0),
          avg_bill_per_user: Number(period.avg_bill_per_user || 0),
          arpu: Number(period.arpu || 0),
          orders_per_day: Number(period.orders_per_day || 0),
          orders_per_day_per_store: Number(period.orders_per_day_per_store || 0),
          purchase_frequency: Number(period.purchase_frequency || 0),
          avg_basket_size: Number(period.avg_basket_size || 0),
          monthly_spend: Number(period.monthly_spend || 0)
        }
      })),
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metricsData[0]?.period_start,
        end_date: metricsData[metricsData.length - 1]?.period_end
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

    // Validate time_range is a positive number
    const timeRange = Number(time_range);
    if (isNaN(timeRange) || timeRange <= 0) {
      return res.status(400).json({
        success: false,
        error: "Time range must be a positive number"
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
      logger.error('Error in get_loyal_customers_metrics_monthly RPC:', {
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

    if (!metricsData || metricsData.length === 0) {
      logger.warn('No data returned from metrics function', {
        business_id: userData.business_id,
        reference_date,
        time_range
      });
      return res.json({
        success: true,
        data: {
          segment: "Loyal Customers",
          metrics: [],
          customers: [],
          time_window: {
            reference_date,
            time_range
          }
        }
      });
    }

    // Format the response with monthly breakdown
    const response = {
      segment: "Loyal Customers",
      metrics: metricsData.map(period => ({
        period: {
          start_date: period.period_start,
          end_date: period.period_end
        },
        values: {
          customer_count: Number(period.customer_count || 0),
          gmv: Number(period.gmv || 0),
          orders: Number(period.orders || 0),
          unique_customers: Number(period.unique_customers || 0),
          aov: Number(period.aov || 0),
          avg_bill_per_user: Number(period.avg_bill_per_user || 0),
          arpu: Number(period.arpu || 0),
          orders_per_day: Number(period.orders_per_day || 0),
          orders_per_day_per_store: Number(period.orders_per_day_per_store || 0),
          annual_customer_value: Number(period.annual_customer_value || 0),
          purchase_frequency: Number(period.purchase_frequency || 0),
          category_penetration: Number(period.category_penetration || 0)
        }
      })),
      customers: customersData || [],
      time_window: {
        reference_date,
        time_range,
        start_date: metricsData[0]?.period_start,
        end_date: metricsData[metricsData.length - 1]?.period_end
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

// const getCustomerStagePeriodChanges = async (req, res) => {
//   const user = req.user;
//   const { reference_date, time_range } = req.body;

//   logger.info('Starting customer stage period changes analysis', {
//     user_id: user?.user_id,
//     reference_date,
//     time_range
//   });

//   if (!user || !user.user_id) {
//     logger.warn('User authentication missing', { user });
//     return res.status(400).json({
//       success: false,
//       error: "User authentication required"
//     });
//   }

//   if (!reference_date || !time_range) {
//     logger.warn('Missing required parameters', { reference_date, time_range });
//     return res.status(400).json({
//       success: false,
//       error: "Reference date and time range are required"
//     });
//   }

//   try {
//     // Validate date format
//     const referenceDate = new Date(reference_date);
//     if (isNaN(referenceDate.getTime())) {
//       return res.status(400).json({
//         success: false,
//         error: "Invalid reference_date format. Use YYYY-MM-DD"
//       });
//     }

//     // Validate time_range is a positive number
//     const timeRange = Number(time_range);
//     if (isNaN(timeRange) || timeRange <= 0) {
//       return res.status(400).json({
//         success: false,
//         error: "Time range must be a positive number"
//       });
//     }

//     // Validate reference_date is not in future
//     if (referenceDate > new Date()) {
//       return res.status(400).json({
//         success: false,
//         error: "Reference date cannot be in the future"
//       });
//     }

//     const supabase = getSupabase();
//     const { data: userData, error: userError } = await supabase
//       .from('users')
//       .select('business_id')
//       .eq('user_id', user.user_id)
//       .single();

//     if (userError || !userData?.business_id) {
//       logger.error('Error retrieving business_id', {
//         error: userError?.message,
//         user_id: user.user_id
//       });
//       return res.status(400).json({
//         success: false,
//         error: "Business ID not found"
//       });
//     }

//     // Get period changes using the RPC function
//     const { data: periodChanges, error: periodError } = await supabase.rpc('get_customer_stage_period_changes', {
//       p_business_id: Number(userData.business_id),
//       p_reference_date: referenceDate.toISOString().split('T')[0],
//       p_time_range: Number(time_range)
//     });

//     if (periodError) {
//       logger.error('Error in get_customer_stage_period_changes RPC:', {
//         error: periodError.message,
//         business_id: userData.business_id,
//         reference_date,
//         time_range
//       });
//       throw periodError;
//     }

//     // Format the response
//     const response = {
//       period_changes: periodChanges.map(stage => ({
//         stage: stage.stage,
//         current_period: {
//           count: stage.current_period_count,
//           start_date: stage.current_period_start,
//           end_date: stage.current_period_end
//         },
//         previous_period: {
//           count: stage.previous_period_count,
//           start_date: stage.previous_period_start,
//           end_date: stage.previous_period_end
//         },
//         change_percentage: stage.change_percentage !== null ? Number(stage.change_percentage.toFixed(2)) : null
//       })),
//       time_window: {
//         reference_date,
//         time_range
//       }
//     };

//     res.json({
//       success: true,
//       data: response
//     });
//   } catch (error) {
//     logger.error('Error in customer stage period changes analysis', {
//       error: error.message,
//       stack: error.stack,
//       user_id: user?.user_id
//     });
//     res.status(400).json({
//       success: false,
//       error: error.message
//     });
//   }
// };

const getToplineMetricsBreakdown = async (req, res) => {
  const user = req.user;
  const { start_date, end_date } = req.body;

  logger.info('Starting topline metrics breakdown analysis', {
    user_id: user?.user_id,
    start_date,
    end_date
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!start_date || !end_date) {
    logger.warn('Date parameters missing', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Start date and end date are required"
    });
  }

  try {
    // Validate date format
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid date format. Use YYYY-MM-DD"
      });
    }

    // Validate date range
    if (startDate > endDate) {
      return res.status(400).json({
        success: false,
        error: "Start date cannot be after end date"
      });
    }

    // Validate end date is not in future
    if (endDate > new Date()) {
      return res.status(400).json({
        success: false,
        error: "End date cannot be in the future"
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

    // Get metrics breakdown
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_topline_metrics_breakdown', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0],
      p_end_date: endDate.toISOString().split('T')[0]
    });

    if (metricsError) {
      logger.error('Error in get_topline_metrics_breakdown RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        start_date,
        end_date
      });
      throw metricsError;
    }

    if (!metricsData || metricsData.length === 0) {
      logger.warn('No data returned from breakdown function', {
        business_id: userData.business_id,
        start_date,
        end_date
      });
      return res.json({
        success: true,
        data: {
          metrics: [],
          is_monthly_breakdown: false,
          time_window: {
            start_date,
            end_date
          }
        }
      });
    }

    // Format the response
    const response = {
      metrics: metricsData.map(period => ({
        period: {
          start_date: period.period_start,
          end_date: period.period_end
        },
        values: {
          gmv: Number(period.gmv || 0),
          orders: Number(period.orders || 0),
          unique_customers: Number(period.unique_customers || 0),
          aov: Number(period.aov || 0),
          avg_bill_per_user: Number(period.avg_bill_per_user || 0),
          arpu: Number(period.arpu || 0),
          orders_per_day: Number(period.orders_per_day || 0),
          orders_per_day_per_store: Number(period.orders_per_day_per_store || 0)
        },
        changes: {
          gmv: Number((period.gmv_change || 0).toFixed(2)),
          orders: Number((period.orders_change || 0).toFixed(2)),
          unique_customers: Number((period.unique_customers_change || 0).toFixed(2)),
          aov: Number((period.aov_change || 0).toFixed(2)),
          avg_bill_per_user: Number((period.avg_bill_per_user_change || 0).toFixed(2)),
          arpu: Number((period.arpu_change || 0).toFixed(2)),
          orders_per_day: Number((period.orders_per_day_change || 0).toFixed(2)),
          orders_per_day_per_store: Number((period.orders_per_day_per_store_change || 0).toFixed(2))
        }
      })),
      is_monthly_breakdown: metricsData[0]?.is_monthly_breakdown || false,
      time_window: {
        start_date,
        end_date
      }
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in topline metrics breakdown analysis', {
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

const getCustomerStageMonthlyBreakdown = async (req, res) => {
  const user = req.user;
  const { reference_date, time_range } = req.body;

  logger.info('Starting customer stage monthly breakdown analysis', {
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

    // Validate time_range is a positive number
    const timeRange = Number(time_range);
    if (isNaN(timeRange) || timeRange <= 0) {
      return res.status(400).json({
        success: false,
        error: "Time range must be a positive number"
      });
    }

    // Validate reference_date is not in future
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    if (referenceDate > today) {
      return res.status(400).json({
        success: false,
        error: "Reference date cannot be in the future"
      });
    }

    const supabase = getSupabase();
    
    // Get business_id
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

    // Format date for RPC call
    const formattedReferenceDate = referenceDate.toISOString().split('T')[0];

    // Log RPC call parameters
    logger.info('Calling get_customer_stage_monthly_breakdown with params:', {
      p_business_id: Number(userData.business_id),
      p_reference_date: formattedReferenceDate,
      p_time_range: timeRange
    });

    // Get monthly breakdown using the RPC function
    const { data: breakdownData, error: breakdownError } = await supabase.rpc(
      'get_customer_stage_monthly_breakdown', 
      {
        p_business_id: Number(userData.business_id),
        p_reference_date: formattedReferenceDate,
        p_time_range: timeRange
      }
    );

    if (breakdownError) {
      logger.error('Error in get_customer_stage_monthly_breakdown RPC:', {
        error: breakdownError.message,
        details: breakdownError.details,
        hint: breakdownError.hint,
        business_id: userData.business_id,
        reference_date: formattedReferenceDate,
        time_range: timeRange
      });
      return res.status(400).json({
        success: false,
        error: breakdownError.message,
        details: breakdownError.details || 'No additional details'
      });
    }

    if (!breakdownData || breakdownData.length === 0) {
      logger.warn('No data returned from breakdown function', {
        business_id: userData.business_id,
        reference_date: formattedReferenceDate,
        time_range: timeRange
      });
      return res.json({
        success: true,
        data: {
          monthly_breakdown: [],
          time_window: {
            reference_date: formattedReferenceDate,
            time_range: timeRange
          }
        }
      });
    }

    // Format the response by grouping data by month
    const monthlyData = breakdownData.reduce((acc, record) => {
      const monthKey = record.month_start;
      if (!acc[monthKey]) {
        acc[monthKey] = {
          period: {
            start_date: record.month_start,
            end_date: record.month_end
          },
          stages: {}
        };
      }
      
      // Add stage data
      acc[monthKey].stages[record.stage] = {
        customer_count: record.customer_count,
        metrics: record.metrics
      };

      return acc;
    }, {});

    // Convert to array and sort by month
    const formattedData = Object.values(monthlyData).sort((a, b) => 
      new Date(a.period.start_date) - new Date(b.period.start_date)
    );

    const response = {
      monthly_breakdown: formattedData,
      time_window: {
        reference_date: formattedReferenceDate,
        time_range: timeRange
      }
    };

    logger.info('Successfully generated monthly breakdown', {
      business_id: userData.business_id,
      months_count: formattedData.length
    });

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    logger.error('Error in customer stage monthly breakdown analysis', {
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
  getToplineMetricsBreakdown,
  getCustomerStageMonthlyBreakdown
};