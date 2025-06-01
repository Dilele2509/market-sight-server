import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';
import {
  updateBusinessIdsQuery,
} from '../data/customerLifecycleQueries.js';

// Function to get New customers metrics

const getNewCustomersMetrics = async (req, res) => {
  const user = req.user;
  const { start_date, end_date } = req.body;

  logger.info('Starting new customers metrics analysis', {
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
    logger.warn('Missing required parameters', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Start date and end date are required"
    });
  }

  try {
    // Validate date format
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    // Convert to UTC midnight for consistent date handling
    startDate.setUTCHours(0, 0, 0, 0);
    endDate.setUTCHours(23, 59, 59, 999); // End of day in UTC

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
    const now = new Date();
    now.setUTCHours(23, 59, 59, 999); // End of current day in UTC

    if (endDate > now) {
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

    // Call the updated RPC function that handles monthly breakdown and zero values internally
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_new_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0],
      p_end_date: endDate.toISOString().split('T')[0]
    });

    if (metricsError) {
      logger.error('Error in get_new_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        start_date,
        end_date
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_new_customers_info', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0],
      p_end_date: endDate.toISOString().split('T')[0]
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
        start_date,
        end_date
      });
      return res.json({
        success: true,
        data: {
          segment: "New Customers",
          metrics: [],
          customers: [],
          time_window: {
            start_date: startDate.toISOString().split('T')[0],
            end_date: endDate.toISOString().split('T')[0],
            is_monthly_breakdown: false
          }
        }
      });
    }

    // Check if we got a monthly breakdown (more than one period)
    const isMonthlyBreakdown = metricsData.length > 1;

    // Format the response
    const metricsWithData = metricsData.map(period => ({
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
    }));

    // Calculate aggregated metrics across all periods
    const aggregated_metrics = (() => {
      // Calculate total values across all periods
      const totalGMV = metricsData.reduce((sum, period) => sum + Number(period.gmv || 0), 0);
      const totalOrders = metricsData.reduce((sum, period) => sum + Number(period.orders || 0), 0);
      const totalUniqueCustomers = metricsData.reduce((sum, period) => sum + Number(period.unique_customers || 0), 0);
      const totalCustomerCount = metricsData.reduce((sum, period) => sum + Number(period.customer_count || 0), 0);
      const totalFirstPurchaseGMV = metricsData.reduce((sum, period) => sum + Number(period.first_purchase_gmv || 0), 0);

      // Calculate total days in the time range
      const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;

      // Calculate weighted averages for metrics that need it
      const weightedConversionRate = totalCustomerCount > 0 ?
        metricsData.reduce((sum, period) => {
          const weight = Number(period.customer_count || 0);
          return sum + (Number(period.conversion_to_second_purchase_rate || 0) * weight);
        }, 0) / totalCustomerCount : 0;

      return {
        gmv: totalGMV,
        orders: totalOrders,
        unique_customers: totalUniqueCustomers,
        aov: totalOrders > 0 ? totalGMV / totalOrders : 0,
        avg_bill_per_user: totalUniqueCustomers > 0 ? totalGMV / totalUniqueCustomers : 0,
        arpu: totalCustomerCount > 0 ? totalGMV / totalCustomerCount : 0,
        orders_per_day: totalDays > 0 ? totalOrders / totalDays : 0,
        orders_per_day_per_store: totalDays > 0 ? totalOrders / totalDays : 0,
        first_purchase_gmv: totalFirstPurchaseGMV,
        avg_first_purchase_value: totalCustomerCount > 0 ? totalFirstPurchaseGMV / totalCustomerCount : 0,
        conversion_to_second_purchase_rate: weightedConversionRate
      };
    })();

    // Format customer data
    const formattedCustomers = customersData ? customersData.map(customer => ({
      customer_id: customer.customer_id,
      first_name: customer.first_name,
      last_name: customer.last_name,
      email: customer.email,
      phone: customer.phone,
      gender: customer.gender,
      birth_date: customer.birth_date,
      registration_date: customer.registration_date,
      address: customer.address,
      city: customer.city,
      date: customer.first_purchase_date,
      amount: Number(customer.first_purchase_amount || 0),
      has_second_purchase: customer.has_second_purchase,
      total_purchases: Number(customer.total_purchases || 0),
      total_spent: Number(customer.total_spent || 0),
      avg_order_value: Number(customer.avg_order_value || 0),
      categories_purchased: Number(customer.categories_purchased || 0),
      purchase_categories: customer.purchase_categories,
      brands_purchased: Number(customer.brands_purchased || 0),
      brand_names: customer.brand_names,
      stores_visited: Number(customer.stores_visited || 0),
      store_names: customer.store_names,
      payment_methods: customer.payment_methods
    })) : [];

    const response = {
      segment: "New Customers",
      metrics: metricsWithData,
      aggregated_metrics,
      customers: formattedCustomers,
      time_window: {
        start_date: startDate.toISOString().split('T')[0],
        end_date: endDate.toISOString().split('T')[0],
        is_monthly_breakdown: isMonthlyBreakdown
      }
    };

    logger.info('Successfully generated new customers metrics', {
      business_id: userData.business_id,
      is_monthly_breakdown: isMonthlyBreakdown,
      metrics_count: metricsWithData.length
    });

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
  const { start_date, end_date } = req.body;

  logger.info('Starting early life customers metrics analysis', {
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
    logger.warn('Missing required parameters', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Start date and end date are required"
    });
  }

  try {
    // Validate date format
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    if (isNaN(startDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid start_date format. Use YYYY-MM-DD"
      });
    }

    if (isNaN(endDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid end_date format. Use YYYY-MM-DD"
      });
    }

    // Validate date range
    if (startDate > endDate) {
      return res.status(400).json({
        success: false,
        error: "Start date cannot be after end date"
      });
    }

    // Validate end_date is not in future
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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_early_life_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_end_date: endDate.toISOString().split('T')[0] // Convert to YYYY-MM-DD
    });

    if (metricsError) {
      logger.error('Error in get_early_life_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        start_date,
        end_date
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_early_life_customers_info', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD 
      p_end_date: endDate.toISOString().split('T')[0] // Convert to YYYY-MM-DD
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
        start_date,
        end_date
      });
      return res.json({
        success: true,
        data: {
          segment: "Early-life Customers",
          metrics: [],
          customers: [],
          time_window: {
            start_date,
            end_date
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
      aggregated_metrics: (() => {
        // Calculate total values across all periods
        const totalGMV = metricsData.reduce((sum, period) => sum + Number(period.gmv || 0), 0);
        const totalOrders = metricsData.reduce((sum, period) => sum + Number(period.orders || 0), 0);
        const totalUniqueCustomers = metricsData.reduce((sum, period) => sum + Number(period.unique_customers || 0), 0);
        const totalCustomerCount = metricsData.reduce((sum, period) => sum + Number(period.customer_count || 0), 0);

        // Calculate total days in the time range
        const firstPeriodStart = new Date(metricsData[0]?.period_start);
        const lastPeriodEnd = new Date(metricsData[metricsData.length - 1]?.period_end);
        const totalDays = Math.ceil((lastPeriodEnd - firstPeriodStart) / (1000 * 60 * 60 * 24)) + 1;

        // Calculate weighted averages for metrics that need it
        const weightedRepeatPurchaseRate = metricsData.reduce((sum, period) => {
          const weight = Number(period.unique_customers || 0);
          return sum + (Number(period.repeat_purchase_rate || 0) * weight);
        }, 0) / (totalUniqueCustomers || 1);

        const weightedAvgTimeBetweenPurchases = metricsData.reduce((sum, period) => {
          const weight = Number(period.orders || 0);
          return sum + (Number(period.avg_time_between_purchases || 0) * weight);
        }, 0) / (totalOrders || 1);

        const weightedAvgOrderValue = metricsData.reduce((sum, period) => {
          const weight = Number(period.orders || 0);
          return sum + (Number(period.avg_order_value || 0) * weight);
        }, 0) / (totalOrders || 1);

        return {
          gmv: totalGMV,
          orders: totalOrders,
          unique_customers: totalUniqueCustomers,
          aov: totalOrders > 0 ? totalGMV / totalOrders : 0,
          avg_bill_per_user: totalUniqueCustomers > 0 ? totalGMV / totalUniqueCustomers : 0,
          arpu: totalCustomerCount > 0 ? totalGMV / totalCustomerCount : 0,
          orders_per_day: totalDays > 0 ? totalOrders / totalDays : 0,
          orders_per_day_per_store: totalDays > 0 ? totalOrders / totalDays : 0, // Assuming single store for now
          repeat_purchase_rate: weightedRepeatPurchaseRate,
          avg_time_between_purchases: weightedAvgTimeBetweenPurchases,
          avg_order_value: weightedAvgOrderValue
        };
      })(),
      customers: customersData || [],
      time_window: {
        start_date,
        end_date,
        start_date_formatted: startDate.toISOString().split('T')[0],
        end_date_formatted: endDate.toISOString().split('T')[0]
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
  const { start_date, end_date } = req.body;

  logger.info('Starting mature customers metrics analysis', {
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
    logger.warn('Missing required parameters', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Start date and end date are required"
    });
  }

  try {
    // Validate date format
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    if (isNaN(startDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid start_date format. Use YYYY-MM-DD"
      });
    }

    if (isNaN(endDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid end_date format. Use YYYY-MM-DD"
      });
    }

    // Validate date range
    if (startDate > endDate) {
      return res.status(400).json({
        success: false,
        error: "Start date cannot be after end date"
      });
    }

    // Validate end_date is not in future
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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_mature_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_end_date: endDate.toISOString().split('T')[0] // Convert to YYYY-MM-DD
    });

    if (metricsError) {
      logger.error('Error in get_mature_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        start_date,
        end_date
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_mature_customers_info', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_end_date: endDate.toISOString().split('T')[0] // Convert to YYYY-MM-DD
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
        start_date,
        end_date
      });
      return res.json({
        success: true,
        data: {
          segment: "Mature Customers",
          metrics: [],
          customers: [],
          time_window: {
            start_date,
            end_date
          }
        }
      });
    }

    // Format the response with monthly breakdown for mature customers
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
      aggregated_metrics: (() => {
        // Calculate total values across all periods
        const totalGMV = metricsData.reduce((sum, period) => sum + Number(period.gmv || 0), 0);
        const totalOrders = metricsData.reduce((sum, period) => sum + Number(period.orders || 0), 0);
        const totalUniqueCustomers = metricsData.reduce((sum, period) => sum + Number(period.unique_customers || 0), 0);
        const totalCustomerCount = metricsData.reduce((sum, period) => sum + Number(period.customer_count || 0), 0);

        // Calculate total days in the time range
        const firstPeriodStart = new Date(metricsData[0]?.period_start);
        const lastPeriodEnd = new Date(metricsData[metricsData.length - 1]?.period_end);
        const totalDays = Math.ceil((lastPeriodEnd - firstPeriodStart) / (1000 * 60 * 60 * 24)) + 1;

        // Calculate weighted averages for metrics that need it
        const weightedPurchaseFrequency = metricsData.reduce((sum, period) => {
          const weight = Number(period.unique_customers || 0);
          return sum + (Number(period.purchase_frequency || 0) * weight);
        }, 0) / (totalUniqueCustomers || 1);

        const weightedAvgBasketSize = metricsData.reduce((sum, period) => {
          const weight = Number(period.orders || 0);
          return sum + (Number(period.avg_basket_size || 0) * weight);
        }, 0) / (totalOrders || 1);

        const weightedMonthlySpend = metricsData.reduce((sum, period) => {
          const weight = Number(period.unique_customers || 0);
          return sum + (Number(period.monthly_spend || 0) * weight);
        }, 0) / (totalUniqueCustomers || 1);

        return {
          gmv: totalGMV,
          orders: totalOrders,
          unique_customers: totalUniqueCustomers,
          aov: totalOrders > 0 ? totalGMV / totalOrders : 0,
          avg_bill_per_user: totalUniqueCustomers > 0 ? totalGMV / totalUniqueCustomers : 0,
          arpu: totalCustomerCount > 0 ? totalGMV / totalCustomerCount : 0,
          orders_per_day: totalDays > 0 ? totalOrders / totalDays : 0,
          orders_per_day_per_store: totalDays > 0 ? totalOrders / totalDays : 0, // Assuming single store for now
          purchase_frequency: weightedPurchaseFrequency,
          avg_basket_size: weightedAvgBasketSize,
          monthly_spend: weightedMonthlySpend
        };
      })(),
      customers: customersData || [],
      time_window: {
        start_date,
        end_date,
        start_date_formatted: startDate.toISOString().split('T')[0],
        end_date_formatted: endDate.toISOString().split('T')[0]
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
  const { start_date, end_date } = req.body;

  logger.info('Starting loyal customers metrics analysis', {
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
    logger.warn('Missing required parameters', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Start date and end date are required"
    });
  }

  try {
    // Validate date format
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    if (isNaN(startDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid start_date format. Use YYYY-MM-DD"
      });
    }

    if (isNaN(endDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid end_date format. Use YYYY-MM-DD"
      });
    }

    // Validate date range
    if (startDate > endDate) {
      return res.status(400).json({
        success: false,
        error: "Start date cannot be after end date"
      });
    }

    // Validate end_date is not in future
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

    // Get metrics using the RPC function with new parameters
    const { data: metricsData, error: metricsError } = await supabase.rpc('get_loyal_customers_metrics', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_end_date: endDate.toISOString().split('T')[0] // Convert to YYYY-MM-DD
    });

    if (metricsError) {
      logger.error('Error in get_loyal_customers_metrics RPC:', {
        error: metricsError.message,
        business_id: userData.business_id,
        start_date,
        end_date
      });
      throw metricsError;
    }

    // Get detailed customer information
    const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_loyal_customers_info', {
      p_business_id: Number(userData.business_id),
      p_start_date: startDate.toISOString().split('T')[0], // Convert to YYYY-MM-DD
      p_end_date: endDate.toISOString().split('T')[0] // Convert to YYYY-MM-DD
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
        start_date,
        end_date
      });
      return res.json({
        success: true,
        data: {
          segment: "Loyal Customers",
          metrics: [],
          customers: [],
          time_window: {
            start_date,
            end_date
          }
        }
      });
    }

    // Format the response with monthly breakdown for loyal customers
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
      aggregated_metrics: (() => {
        // Calculate total values across all periods
        const totalGMV = metricsData.reduce((sum, period) => sum + Number(period.gmv || 0), 0);
        const totalOrders = metricsData.reduce((sum, period) => sum + Number(period.orders || 0), 0);
        const totalUniqueCustomers = metricsData.reduce((sum, period) => sum + Number(period.unique_customers || 0), 0);
        const totalCustomerCount = metricsData.reduce((sum, period) => sum + Number(period.customer_count || 0), 0);

        // Calculate total days in the time range
        const firstPeriodStart = new Date(metricsData[0]?.period_start);
        const lastPeriodEnd = new Date(metricsData[metricsData.length - 1]?.period_end);
        const totalDays = Math.ceil((lastPeriodEnd - firstPeriodStart) / (1000 * 60 * 60 * 24)) + 1;

        // Calculate weighted averages for metrics that need it
        const weightedAnnualCustomerValue = metricsData.reduce((sum, period) => {
          const weight = Number(period.unique_customers || 0);
          return sum + (Number(period.annual_customer_value || 0) * weight);
        }, 0) / (totalUniqueCustomers || 1);

        const weightedPurchaseFrequency = metricsData.reduce((sum, period) => {
          const weight = Number(period.unique_customers || 0);
          return sum + (Number(period.purchase_frequency || 0) * weight);
        }, 0) / (totalUniqueCustomers || 1);

        const weightedCategoryPenetration = metricsData.reduce((sum, period) => {
          const weight = Number(period.unique_customers || 0);
          return sum + (Number(period.category_penetration || 0) * weight);
        }, 0) / (totalUniqueCustomers || 1);

        return {
          gmv: totalGMV,
          orders: totalOrders,
          unique_customers: totalUniqueCustomers,
          aov: totalOrders > 0 ? totalGMV / totalOrders : 0,
          avg_bill_per_user: totalUniqueCustomers > 0 ? totalGMV / totalUniqueCustomers : 0,
          arpu: totalCustomerCount > 0 ? totalGMV / totalCustomerCount : 0,
          orders_per_day: totalDays > 0 ? totalOrders / totalDays : 0,
          orders_per_day_per_store: totalDays > 0 ? totalOrders / totalDays : 0, // Assuming single store for now
          annual_customer_value: weightedAnnualCustomerValue,
          purchase_frequency: weightedPurchaseFrequency,
          category_penetration: weightedCategoryPenetration
        };
      })(),
      customers: customersData || [],
      time_window: {
        start_date,
        end_date,
        start_date_formatted: startDate.toISOString().split('T')[0],
        end_date_formatted: endDate.toISOString().split('T')[0]
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

    // Convert to UTC midnight for consistent date handling
    startDate.setUTCHours(0, 0, 0, 0);
    endDate.setUTCHours(23, 59, 59, 999); // End of day in UTC

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
    const now = new Date();
    now.setUTCHours(23, 59, 59, 999); // End of current day in UTC

    if (endDate > now) {
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
          metrics: [{
            period: {
              start_date: startDate.toISOString().split('T')[0],
              end_date: endDate.toISOString().split('T')[0]
            },
            values: {
              gmv: 0,
              orders: 0,
              unique_customers: 0,
              aov: 0,
              avg_bill_per_user: 0,
              arpu: 0,
              orders_per_day: 0,
              orders_per_day_per_store: 0
            },
            changes: {
              gmv: 0,
              orders: 0,
              unique_customers: 0,
              aov: 0,
              avg_bill_per_user: 0,
              arpu: 0,
              orders_per_day: 0,
              orders_per_day_per_store: 0
            }
          }],
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
  const { start_date, end_date } = req.body;

  logger.info('Starting customer stage monthly breakdown analysis', {
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
    logger.warn('Missing required parameters', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Start date and end date are required"
    });
  }

  try {
    // Validate date format
    const startDate = new Date(start_date);
    const endDate = new Date(end_date);

    if (isNaN(startDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid start_date format. Use YYYY-MM-DD"
      });
    }

    if (isNaN(endDate.getTime())) {
      return res.status(400).json({
        success: false,
        error: "Invalid end_date format. Use YYYY-MM-DD"
      });
    }

    // Validate date range
    if (startDate > endDate) {
      return res.status(400).json({
        success: false,
        error: "Start date cannot be after end date"
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

    // Format date for RPC call - simply use the original format
    const formattedStartDate = startDate.toISOString().split('T')[0];
    const formattedEndDate = endDate.toISOString().split('T')[0];

    // Get monthly breakdown using the RPC function
    const { data: breakdownData, error: breakdownError } = await supabase.rpc(
      'get_customer_stage_monthly_breakdown',
      {
        p_business_id: Number(userData.business_id),
        p_start_date: formattedStartDate,
        p_end_date: formattedEndDate
      }
    );

    if (breakdownError) {
      logger.error('Error in get_customer_stage_monthly_breakdown RPC:', {
        error: breakdownError.message,
        details: breakdownError.details,
        hint: breakdownError.hint,
        business_id: userData.business_id,
        start_date: formattedStartDate,
        end_date: formattedEndDate
      });
      return res.status(400).json({
        success: false,
        error: breakdownError.message,
        details: breakdownError.details || 'No additional details'
      });
    }

    // Initialize monthly data structure
    const monthlyData = {};

    // Map returned data to standardized stage names
    const stageNameMapping = {
      "New Customers": "New",
      "Early Life Customers": "Early-life",
      "Mature Customers": "Mature",
      "Loyal Customers": "Loyal"
    };

    if (breakdownData && breakdownData.length > 0) {
      // Group data by month directly using the date values from RPC
      breakdownData.forEach(record => {
        const monthKey = record.month_start;

        if (!monthlyData[monthKey]) {
          monthlyData[monthKey] = {
            period: {
              start_date: record.month_start,
              end_date: record.month_end
            },
            stages: {}
          };
        }

        // Map the stage name
        const stageName = stageNameMapping[record.stage] || record.stage;

        // Add this stage's data to the month
        monthlyData[monthKey].stages[stageName] = {
          customer_count: Number(record.customer_count) || 0,
          metrics: record.metrics || {}
        };
      });
    }

    // Convert to array and sort by month
    const formattedData = Object.values(monthlyData).sort((a, b) =>
      new Date(a.period.start_date) - new Date(b.period.start_date)
    );

    // Ensure all months have all stages
    formattedData.forEach(month => {
      Object.keys(stageNameMapping).forEach(originalStageName => {
        const stageName = stageNameMapping[originalStageName];
        if (!month.stages[stageName]) {
          month.stages[stageName] = {
            customer_count: 0,
            metrics: {}
          };
        }
      });
    });

    const response = {
      monthly_breakdown: formattedData,
      time_window: {
        start_date: formattedStartDate,
        end_date: formattedEndDate,
        calendar_months: {
          start_month: formattedStartDate.substring(0, 7),
          end_month: formattedEndDate.substring(0, 7)
        }
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