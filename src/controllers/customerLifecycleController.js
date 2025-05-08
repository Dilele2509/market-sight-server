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

    // Generate all months in range
    const months = [];
    let isMonthlyBreakdown = false;

    // Tính khoảng cách tháng chính xác hơn
    function getMonthDiff(startDate, endDate) {
      // Ensure we're working with UTC date values for consistent timezone handling
      const start = new Date(startDate);
      const end = new Date(endDate);
      
      // Set to UTC hours (beginning of day)
      start.setUTCHours(0, 0, 0, 0);
      end.setUTCHours(0, 0, 0, 0);
      
      // Tính số tháng giữa hai ngày sử dụng UTC để nhất quán
      const startYear = start.getUTCFullYear();
      const startMonth = start.getUTCMonth();
      const endYear = end.getUTCFullYear();
      const endMonth = end.getUTCMonth();
      
      // Tính khoảng cách tháng cơ bản
      const monthDiff = (endYear - startYear) * 12 + (endMonth - startMonth);
      
      // Nếu cùng tháng hoặc dưới 1 tháng
      if (monthDiff === 0) {
        return 0;
      }
      
      // Kiểm tra trường hợp chính xác 1 tháng
      if (monthDiff === 1) {
        const startDay = start.getUTCDate();
        // Ngày 1 của tháng kế tiếp từ startDate
        const nextMonth = new Date(Date.UTC(
          start.getUTCFullYear(),
          start.getUTCMonth() + 1,
          startDay
        ));
        
        // Nếu endDate <= nextMonth thì coi là chưa đủ 1 tháng đầy đủ
        if (end <= nextMonth) {
          return 0;
        }
      }
      
      return monthDiff;
    }

    const monthDiff = getMonthDiff(startDate, endDate);
    // Trường hợp 1: Không cần breakdown theo tháng (thời gian <= 1 tháng hoặc cùng tháng)
    if (monthDiff === 0) {
      months.push({
        period_start: startDate.toISOString().split('T')[0],
        period_end: endDate.toISOString().split('T')[0]
      });
    } 
    // Trường hợp 2: Cần breakdown theo tháng (thời gian > 1 tháng)
    else {
      isMonthlyBreakdown = true;
      
      // Tạo bản sao của startDate và endDate để không làm ảnh hưởng đến giá trị gốc
      let currentStart = new Date(startDate);
      const finalEnd = new Date(endDate);
      
      // Đảm bảo chỉ làm việc với ngày tháng (loại bỏ thời gian)
      currentStart.setUTCHours(0, 0, 0, 0);
      finalEnd.setUTCHours(0, 0, 0, 0);
      
      while (currentStart <= finalEnd) {
        const currentYear = currentStart.getUTCFullYear();
        const currentMonth = currentStart.getUTCMonth();
        
        // Tính ngày cuối của tháng hiện tại
        const lastDayOfMonth = new Date(Date.UTC(currentYear, currentMonth + 1, 0));
        lastDayOfMonth.setUTCHours(0, 0, 0, 0);
        
        // Xác định period_end: ngày cuối của tháng hoặc endDate (lấy ngày nào sớm hơn)
        const periodEnd = lastDayOfMonth < finalEnd ? lastDayOfMonth : finalEnd;
        
        // Thêm khoảng thời gian vào danh sách
        months.push({
          period_start: currentStart.toISOString().split('T')[0],
          period_end: periodEnd.toISOString().split('T')[0]
        });
        
        // Chuyển đến ngày 1 của tháng tiếp theo
        currentStart = new Date(Date.UTC(currentYear, currentMonth + 1, 1));
      }
    }

    // Lấy dữ liệu cho từng tháng riêng biệt thay vì sử dụng dữ liệu chung cho toàn khoảng thời gian
    const metricsWithData = [];

    // Kết quả tổng hợp cho toàn khoảng thời gian (dùng để tính metrics tổng)
    let aggregatedMetricsData = null;

    // Trường hợp không breakdown theo tháng: chỉ gọi RPC một lần cho toàn bộ khoảng thời gian
    if (!isMonthlyBreakdown) {
      const { data: periodData, error: periodError } = await supabase.rpc('get_new_customers_metrics', {
        p_business_id: userData.business_id,
        p_start_date: new Date(startDate.toISOString().split('T')[0]).toISOString().split('T')[0],
        p_end_date: new Date(endDate.toISOString().split('T')[0]).toISOString().split('T')[0]
      });

      if (periodError) {
        logger.error('Error fetching metrics for period:', {
          error: periodError.message,
          period: months[0]
        });
      } else {
        aggregatedMetricsData = periodData && periodData.length > 0 ? periodData[0] : null;
        
        if (aggregatedMetricsData) {
          metricsWithData.push({
            period: {
              start_date: months[0].period_start,
              end_date: months[0].period_end
            },
            values: {
              customer_count: Number(aggregatedMetricsData.customer_count || 0),
              gmv: Number(aggregatedMetricsData.gmv || 0),
              orders: Number(aggregatedMetricsData.orders || 0),
              unique_customers: Number(aggregatedMetricsData.unique_customers || 0),
              aov: Number(aggregatedMetricsData.aov || 0),
              avg_bill_per_user: Number(aggregatedMetricsData.avg_bill_per_user || 0),
              arpu: Number(aggregatedMetricsData.arpu || 0),
              orders_per_day: Number(aggregatedMetricsData.orders_per_day || 0),
              orders_per_day_per_store: Number(aggregatedMetricsData.orders_per_day_per_store || 0),
              first_purchase_gmv: Number(aggregatedMetricsData.first_purchase_gmv || 0),
              avg_first_purchase_value: Number(aggregatedMetricsData.avg_first_purchase_value || 0),
              conversion_to_second_purchase_rate: Number(aggregatedMetricsData.conversion_to_second_purchase_rate || 0)
            }
          });
        } else {
          // Nếu không có dữ liệu, vẫn trả về một khoảng thời gian với các metrics bằng 0
          metricsWithData.push({
            period: {
              start_date: months[0].period_start,
              end_date: months[0].period_end
            },
            values: {
              customer_count: 0,
              gmv: 0,
              orders: 0,
              unique_customers: 0,
              aov: 0,
              avg_bill_per_user: 0,
              arpu: 0,
              orders_per_day: 0,
              orders_per_day_per_store: 0,
              first_purchase_gmv: 0,
              avg_first_purchase_value: 0,
              conversion_to_second_purchase_rate: 0
            }
          });
        }
      }
    } 
    // Trường hợp breakdown theo tháng: gọi RPC riêng cho từng tháng 
    else {
      // Đầu tiên, lấy dữ liệu tổng hợp cho toàn khoảng thời gian (sử dụng cho tính toán tổng)
      const { data: fullRangeData, error: fullRangeError } = await supabase.rpc('get_new_customers_metrics', {
        p_business_id: userData.business_id,
        p_start_date: new Date(startDate.toISOString().split('T')[0]).toISOString().split('T')[0],
        p_end_date: new Date(endDate.toISOString().split('T')[0]).toISOString().split('T')[0]
      });

      if (!fullRangeError && fullRangeData && fullRangeData.length > 0) {
        aggregatedMetricsData = fullRangeData[0];
      }

      // Sau đó, lấy dữ liệu chi tiết cho từng tháng 
      for (const month of months) {
        const { data: periodData, error: periodError } = await supabase.rpc('get_new_customers_metrics', {
          p_business_id: userData.business_id,
          p_start_date: month.period_start,
          p_end_date: month.period_end
        });

        if (periodError) {
          logger.error('Error fetching metrics for period:', {
            error: periodError.message,
            period: month
          });
          
          // Nếu có lỗi, vẫn đảm bảo có một entry với giá trị 0
          metricsWithData.push({
            period: {
              start_date: month.period_start,
              end_date: month.period_end
            },
            values: {
              customer_count: 0,
              gmv: 0,
              orders: 0,
              unique_customers: 0,
              aov: 0,
              avg_bill_per_user: 0,
              arpu: 0,
              orders_per_day: 0,
              orders_per_day_per_store: 0,
              first_purchase_gmv: 0,
              avg_first_purchase_value: 0,
              conversion_to_second_purchase_rate: 0
            }
          });
        } else if (periodData && periodData.length > 0) {
          // Nếu có dữ liệu cho tháng này, sử dụng dữ liệu đó
          const monthData = periodData[0];
          
          metricsWithData.push({
            period: {
              start_date: month.period_start,
              end_date: month.period_end
            },
            values: {
              customer_count: Number(monthData.customer_count || 0),
              gmv: Number(monthData.gmv || 0),
              orders: Number(monthData.orders || 0),
              unique_customers: Number(monthData.unique_customers || 0),
              aov: Number(monthData.aov || 0),
              avg_bill_per_user: Number(monthData.avg_bill_per_user || 0),
              arpu: Number(monthData.arpu || 0),
              orders_per_day: Number(monthData.orders_per_day || 0),
              orders_per_day_per_store: Number(monthData.orders_per_day_per_store || 0),
              first_purchase_gmv: Number(monthData.first_purchase_gmv || 0),
              avg_first_purchase_value: Number(monthData.avg_first_purchase_value || 0),
              conversion_to_second_purchase_rate: Number(monthData.conversion_to_second_purchase_rate || 0)
            }
          });
        } else {
          // Nếu không có dữ liệu cho tháng này (tức là không có customer nào trong khoảng thời gian)
          // Thêm một entry với tất cả giá trị bằng 0
          metricsWithData.push({
            period: {
              start_date: month.period_start,
              end_date: month.period_end
            },
            values: {
              customer_count: 0,
              gmv: 0,
              orders: 0,
              unique_customers: 0,
              aov: 0,
              avg_bill_per_user: 0,
              arpu: 0,
              orders_per_day: 0,
              orders_per_day_per_store: 0,
              first_purchase_gmv: 0,
              avg_first_purchase_value: 0,
              conversion_to_second_purchase_rate: 0
            }
          });
        }
      }
    }

    logger.info('Final metrics data:', {
      is_monthly_breakdown: isMonthlyBreakdown,
      metrics_count: metricsWithData.length,
      periods: metricsWithData.map(m => ({ 
        start: m.period.start_date, 
        end: m.period.end_date,
        customer_count: m.values.customer_count
      }))
    });

    // Format the response
    const response = {
      segment: "New Customers",
      metrics: metricsWithData,
      aggregated_metrics: (() => {
        // Calculate total values across all periods
        const totalGMV = metricsWithData.reduce((sum, period) => sum + Number(period.values.gmv || 0), 0);
        const totalOrders = metricsWithData.reduce((sum, period) => sum + Number(period.values.orders || 0), 0);
        const totalUniqueCustomers = metricsWithData.reduce((sum, period) => sum + Number(period.values.unique_customers || 0), 0);
        const totalCustomerCount = metricsWithData.reduce((sum, period) => sum + Number(period.values.customer_count || 0), 0);
        const totalFirstPurchaseGMV = metricsWithData.reduce((sum, period) => sum + Number(period.values.first_purchase_gmv || 0), 0);
        
        // Calculate total days in the time range
        const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;

        // Calculate weighted averages for metrics that need it
        const weightedConversionRate = totalCustomerCount > 0 ? 
          metricsWithData.reduce((sum, period) => {
            const weight = Number(period.values.customer_count || 0);
            return sum + (Number(period.values.conversion_to_second_purchase_rate || 0) * weight);
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
      })(),
      customers: await (async () => {
        // Get detailed customer information
        const { data: customersData, error: customersError } = await supabase.rpc('get_detailed_new_customers_info', {
          p_business_id: userData.business_id,
          p_start_date: new Date(startDate.toISOString().split('T')[0]).toISOString().split('T')[0],
          p_end_date: new Date(endDate.toISOString().split('T')[0]).toISOString().split('T')[0]
        });
        
        if (customersError) {
          logger.error('Error in get_detailed_new_customers_info RPC:', {
            error: customersError.message,
            business_id: userData.business_id
          });
          return [];
        }
        
        return customersData ? customersData.map(customer => ({
          customer_id: customer.customer_id,
          profile: {
            first_name: customer.first_name,
            last_name: customer.last_name,
            email: customer.email,
            phone: customer.phone,
            gender: customer.gender,
            birth_date: customer.birth_date,
            registration_date: customer.registration_date,
            address: customer.address,
            city: customer.city
          },
          first_purchase: {
            date: customer.first_purchase_date,
            amount: Number(customer.first_purchase_amount || 0),
            has_second_purchase: customer.has_second_purchase
          },
          transactions: {
            total_purchases: Number(customer.total_purchases || 0),
            total_spent: Number(customer.total_spent || 0),
            avg_order_value: Number(customer.avg_order_value || 0)
          },
          products: {
            categories_purchased: Number(customer.categories_purchased || 0),
            purchase_categories: customer.purchase_categories,
            brands_purchased: Number(customer.brands_purchased || 0),
            brand_names: customer.brand_names
          },
          stores: {
            stores_visited: Number(customer.stores_visited || 0),
            store_names: customer.store_names
          },
          payment: {
            payment_methods: customer.payment_methods
          }
        })) : [];
      })(),
      time_window: {
        start_date: startDate.toISOString().split('T')[0],
        end_date: endDate.toISOString().split('T')[0],
        is_monthly_breakdown: isMonthlyBreakdown
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
        const startDate = new Date(metricsData[0]?.period_start);
        const endDate = new Date(metricsData[metricsData.length - 1]?.period_end);
        const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;

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
        const startDate = new Date(metricsData[0]?.period_start);
        const endDate = new Date(metricsData[metricsData.length - 1]?.period_end);
        const totalDays = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24)) + 1;

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

    // Calculate date range based on calendar months
    const endDate = new Date(referenceDate); // LỖI #2 ĐÃ SỬA: Sử dụng referenceDate thay vì reference_date
    const startDate = new Date(referenceDate);
    
    // Set end date to the reference date
    endDate.setHours(0, 0, 0, 0);
    
    // Set start date to first day of the month that is 'timeRange' months before reference date
    startDate.setDate(1); // First set to 1st of current month
    startDate.setMonth(startDate.getMonth() - timeRange); // Then go back timeRange months

    // Format date for RPC call
    const formattedReferenceDate = referenceDate.toISOString().split('T')[0];

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

    // Initialize monthly data structure for all months in range
    const monthlyData = {};
    const currentDate = new Date(startDate.getFullYear(), startDate.getMonth(), 1); // Always start from 1st
    
    // Define the standardized stage keys we want to use
    const stageKeys = ["New", "Early-life", "Mature", "Loyal"];
    
    // Create structure for each month in the range
    while (currentDate <= endDate) {
      const monthStart = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1);
      // Always show full calendar month in period display
      const monthEnd = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0);
      const monthKey = monthStart.toISOString().split('T')[0];
      
      monthlyData[monthKey] = {
        period: {
          start_date: monthStart.toISOString().split('T')[0],
          end_date: monthEnd.toISOString().split('T')[0]
        },
        stages: {}
      };
      
      // Initialize all stage values
      stageKeys.forEach(stage => {
        monthlyData[monthKey].stages[stage] = { 
          customer_count: 0, 
          metrics: {}
        };
      });
      
      currentDate.setMonth(currentDate.getMonth() + 1);
    }

    // Map returned data to standardized stage names
    const stageNameMapping = {
      "New Customers": "New",
      "Early Life Customers": "Early-life",
      "Mature Customers": "Mature", 
      "Loyal Customers": "Loyal"
    };

    if (breakdownData && breakdownData.length > 0) {
      breakdownData.forEach(record => {
        // LỖI #1 ĐÃ SỬA: Chuẩn hóa record.month_start thành ngày đầu tiên của tháng
        const recordDate = new Date(record.month_start);
        const monthStart = new Date(recordDate.getFullYear(), recordDate.getMonth(), 1);
        const monthKey = monthStart.toISOString().split('T')[0];
        
        // Only include data up to reference_date for the final month
        const isLastMonth = monthStart.getMonth() === endDate.getMonth() && 
                          monthStart.getFullYear() === endDate.getFullYear();
        const recordEndDate = new Date(record.month_end);
        
        if (monthlyData[monthKey] && (!isLastMonth || recordEndDate <= endDate)) {
          // Map the stage name if needed
          const stageName = stageNameMapping[record.stage] || record.stage;
          
          // Only use the stage if it's one of our defined stages
          if (stageKeys.includes(stageName)) {
            monthlyData[monthKey].stages[stageName] = {
              customer_count: record.customer_count || 0,
              metrics: record.metrics || {}
            };
          }
        }
      });
    }

    // Convert to array and sort by month
    const formattedData = Object.values(monthlyData).sort((a, b) => 
      new Date(a.period.start_date) - new Date(b.period.start_date)
    );

    const response = {
      monthly_breakdown: formattedData,
      time_window: {
        reference_date: formattedReferenceDate,
        time_range: timeRange,
        start_date: startDate.toISOString().split('T')[0],
        end_date: endDate.toISOString().split('T')[0],
        calendar_months: {
          start_month: startDate.toISOString().split('T')[0].substring(0, 7),
          end_month: endDate.toISOString().split('T')[0].substring(0, 7)
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