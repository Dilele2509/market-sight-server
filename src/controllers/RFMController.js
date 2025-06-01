import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';

// Phân tích RFM cho một khoảng thời gian cụ thể
const analyzeRFMForPeriod = async (req, res) => {
  const user = req.user;
  const { start_date, end_date } = req.body;
  
  logger.info('Starting RFM analysis for period', { 
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
  
  if (!start_date || !end_date) {
    logger.warn('Missing required parameters', { start_date, end_date });
    return res.status(400).json({
      success: false,
      error: "Both start_date and end_date are required"
    });
  }
  
  try {
    // Validate date format
    const startDateObj = new Date(start_date);
    const endDateObj = new Date(end_date);
    
    if (isNaN(startDateObj.getTime()) || isNaN(endDateObj.getTime())) {
      logger.warn('Invalid date format', { start_date, end_date });
      return res.status(400).json({
        success: false,
        error: "Invalid date format. Use ISO format (YYYY-MM-DD)"
      });
    }
    
    if (endDateObj < startDateObj) {
      logger.warn('End date before start date', { start_date, end_date });
      return res.status(400).json({
        success: false,
        error: "End date must be after start date"
      });
    }
    
    const supabase = getSupabase();
    
    // Lấy business_id của user
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();
      
    if (userError || !userData) {
      logger.error('Error fetching business_id', { error: userError });
      return res.status(400).json({
        success: false,
        error: "Could not retrieve business information"
      });
    }
    
    const business_id = userData.business_id;
    
    // Gọi hàm phân tích RFM
    const { data, error } = await supabase.rpc('analyze_rfm_for_period', {
      target_business_id: business_id,
      start_date: start_date,
      end_date: end_date
    });
    
    if (error) {
      logger.error('Error analyzing RFM for period', { error });
      return res.status(500).json({
        success: false,
        error: "Failed to analyze RFM for the specified period"
      });
    }
    
    // Lấy số lượng khách hàng đã phân tích
    const { count, error: countError } = await supabase
      .from('rfm_scores')
      .select('customer_id', { count: 'exact' })
      .eq('business_id', business_id);
      
    if (countError) {
      logger.error('Error counting analyzed customers', { error: countError });
      // Still return success as the analysis was completed
    }
    
    // Lấy dữ liệu RFM scores đã tính toán để trả về trong response
    const { data: rfmScores, error: rfmError } = await supabase
      .from('rfm_scores')
      .select('*')
      .eq('business_id', business_id)
      .order('segment', { ascending: true });
      
    if (rfmError) {
      logger.error('Error fetching RFM scores', { error: rfmError });
    }
    
    // Danh sách đầy đủ các segment RFM
    const allSegments = [
      'Champions',
      'Loyal Customers',
      'Potential Loyalist',
      'New Customers',
      'Promising',
      'Need Attention',
      'About To Sleep',
      'At Risk',
      'Can\'t Lose Them',
      'Hibernating',
      'Lost'
    ];
    
    // Tính toán số lượng khách hàng theo từng segment và phần trăm
    const segmentCounts = {};
    const totalCustomers = rfmScores?.length || 0;
    
    // Khởi tạo counts với 0 cho tất cả segments
    allSegments.forEach(segment => {
      segmentCounts[segment] = 0;
    });
    
    // Đếm số lượng khách hàng trong mỗi segment
    if (rfmScores && totalCustomers > 0) {
      rfmScores.forEach(score => {
        if (score.segment && segmentCounts.hasOwnProperty(score.segment)) {
          segmentCounts[score.segment]++;
        }
      });
    }
    
    // Tính phần trăm và tạo object thống kê cho tất cả segments
    const segmentStats = allSegments.map(segment => {
      const count = segmentCounts[segment] || 0;
      const percentage = totalCustomers > 0 
        ? parseFloat(((count / totalCustomers) * 100).toFixed(2)) 
        : 0;
      
      return {
        segment,
        count,
        percentage
      };
    });
    
    // Sắp xếp theo số lượng giảm dần
    segmentStats.sort((a, b) => b.count - a.count);
    
    logger.info('RFM analysis completed successfully', { 
      analyzed_customers: count || 'unknown',
      business_id
    });
    
    return res.status(200).json({
      success: true,
      message: "RFM analysis completed successfully",
      data: {
        analyzed_customers: totalCustomers,
        period: {
          start_date: start_date,
          end_date: end_date
        },
        segment_stats: segmentStats,
        rfm_scores: rfmScores || []
      }
    });
    
  } catch (error) {
    logger.error('Unexpected error in analyzeRFMForPeriod', { error });
    return res.status(500).json({
      success: false,
      error: "An unexpected error occurred"
    });
  }
};

// Lấy thông tin chi tiết của khách hàng theo RFM segment và khoảng thời gian
const getRFMSegmentCustomers = async (req, res) => {
  const user = req.user;
  const { segment } = req.params;
  const { start_date, end_date } = req.query;
  
  logger.info('Fetching detailed customer data for RFM segment', { 
    segment, 
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
    
    // Lấy business_id của user
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();
      
    if (userError || !userData) {
      logger.error('Error fetching business_id', { error: userError });
      return res.status(400).json({
        success: false,
        error: "Could not retrieve business information"
      });
    }
    
    const business_id = userData.business_id;
    
    // Validate dates if provided
    if (start_date && end_date) {
      const startDateObj = new Date(start_date);
      const endDateObj = new Date(end_date);
      
      if (isNaN(startDateObj.getTime()) || isNaN(endDateObj.getTime())) {
        logger.warn('Invalid date format', { start_date, end_date });
        return res.status(400).json({
          success: false,
          error: "Invalid date format. Use ISO format (YYYY-MM-DD)"
        });
      }
      
      if (endDateObj < startDateObj) {
        logger.warn('End date before start date', { start_date, end_date });
        return res.status(400).json({
          success: false,
          error: "End date must be after start date"
        });
      }
    }
    
    // RPC params
    const rpcParams = {
      target_business_id: business_id,
      target_segment: segment || null,
      start_date: start_date || null,
      end_date: end_date || null
    };
    
    // Danh sách đầy đủ các segment RFM
    const allSegments = [
      'Champions',
      'Loyal Customers',
      'Potential Loyalist',
      'New Customers',
      'Promising',
      'Need Attention',
      'About To Sleep',
      'At Risk',
      'Can\'t Lose Them',
      'Hibernating',
      'Lost'
    ];
    
    // Nếu có segment cụ thể, chỉ lấy dữ liệu cho segment đó
    const segmentsToQuery = segment ? [segment] : allSegments;
    
    // Tạo object để lưu kết quả phân nhóm theo segment
    const segmentedCustomers = {};
    let totalCustomers = 0;
    
    // Khởi tạo cấu trúc rỗng cho tất cả segment
    segmentsToQuery.forEach(seg => {
      segmentedCustomers[seg] = {
        segment: seg,
        customers: [],
        count: 0
      };
    });
    
    // Nếu có segment cụ thể, lấy tất cả khách hàng cho segment đó
    if (segment) {
      const { data: customers, error, count } = await supabase.rpc(
        'get_rfm_segment_customers',
        rpcParams
      ).select('*', { count: 'exact' });
      
      if (error) {
        logger.error('Error fetching RFM segment customers', { error });
        return res.status(500).json({
          success: false,
          error: "Failed to fetch customers for RFM segment"
        });
      }
      
      // Format dữ liệu khách hàng
      const formattedCustomers = customers.map(customer => {
        return {
          ...customer,
          birth_date: customer.birth_date ? new Date(customer.birth_date).toISOString().split('T')[0] : null,
          registration_date: customer.registration_date ? new Date(customer.registration_date).toISOString() : null,
          last_updated: customer.last_updated ? new Date(customer.last_updated).toISOString() : null,
          full_name: `${customer.first_name || ''} ${customer.last_name || ''}`.trim()
        };
      });
      
      segmentedCustomers[segment].customers = formattedCustomers;
      segmentedCustomers[segment].count = count;
      totalCustomers = count;
    } else {
      // Lấy tất cả khách hàng và nhóm theo segment
      const { data: allCustomers, error, count } = await supabase.rpc(
        'get_rfm_segment_customers',
        rpcParams
      ).select('*', { count: 'exact' });
      
      if (error) {
        logger.error('Error fetching all RFM segment customers', { error });
        return res.status(500).json({
          success: false,
          error: "Failed to fetch customers for all RFM segments"
        });
      }
      
      totalCustomers = count || 0;
      
      // Phân nhóm khách hàng theo segment
      if (allCustomers && allCustomers.length > 0) {
        allCustomers.forEach(customer => {
          const formattedCustomer = {
            ...customer,
            birth_date: customer.birth_date ? new Date(customer.birth_date).toISOString().split('T')[0] : null,
            registration_date: customer.registration_date ? new Date(customer.registration_date).toISOString() : null,
            last_updated: customer.last_updated ? new Date(customer.last_updated).toISOString() : null,
            full_name: `${customer.first_name || ''} ${customer.last_name || ''}`.trim()
          };
          
          if (customer.segment && segmentedCustomers[customer.segment]) {
            segmentedCustomers[customer.segment].customers.push(formattedCustomer);
            segmentedCustomers[customer.segment].count++;
          }
        });
      }
    }
    
    // Tạo mảng chứa các segment có dữ liệu, sắp xếp theo số lượng khách hàng giảm dần
    const segmentResults = Object.values(segmentedCustomers)
      .filter(segment => segment.count > 0)
      .sort((a, b) => b.count - a.count);
    
    logger.info('RFM segment customers retrieved successfully', { 
      segment: segment || 'All',
      totalCustomers
    });
    
    return res.status(200).json({
      success: true,
      data: {
        segments: segmentResults,
        total_customers: totalCustomers,
        filter: {
          segment: segment || 'All Segments',
          period: start_date && end_date ? {
            start_date,
            end_date
          } : 'Latest Analysis'
        }
      }
    });
    
  } catch (error) {
    logger.error('Unexpected error in getRFMSegmentCustomers', { error });
    return res.status(500).json({
      success: false,
      error: "An unexpected error occurred"
    });
  }
};

export {
 analyzeRFMForPeriod,
 getRFMSegmentCustomers
};