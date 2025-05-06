import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';

// Tính toán RFM cho toàn bộ khách hàng của một business
const calculateRFMForBusiness = async (req, res) => {
  const user = req.user;
  
  logger.info('Starting RFM calculation process', { user_id: user?.user_id });
  
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
    logger.info('Found business_id', { business_id });
    
    // Gọi hàm tính toán RFM trong PostgreSQL
    const { data, error } = await supabase.rpc('calculate_rfm_scores', {
      target_business_id: business_id
    });
    
    if (error) {
      logger.error('Error calculating RFM scores', { error });
      return res.status(500).json({
        success: false,
        error: "Failed to calculate RFM scores"
      });
    }
    
    logger.info('RFM calculation completed successfully');
    return res.status(200).json({
      success: true,
      message: "RFM scores calculated successfully"
    });
    
  } catch (error) {
    logger.error('Unexpected error in RFM calculation', { error });
    return res.status(500).json({
      success: false,
      error: "An unexpected error occurred"
    });
  }
};

// Lấy thông tin RFM của một khách hàng cụ thể
const getCustomerRFM = async (req, res) => {
  const user = req.user;
  const { customer_id } = req.body;
  
  logger.info('Fetching RFM data for customer', { customer_id });
  
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
    
    // Lấy thông tin RFM của khách hàng
    const { data: rfmData, error: rfmError } = await supabase
      .from('rfm_scores')
      .select('*')
      .eq('customer_id', customer_id)
      .eq('business_id', business_id)
      .single();
      
    if (rfmError) {
      logger.error('Error fetching RFM data', { error: rfmError });
      return res.status(404).json({
        success: false,
        error: "RFM data not found for this customer"
      });
    }
    
    logger.info('RFM data retrieved successfully', { customer_id });
    return res.status(200).json({
      success: true,
      data: rfmData
    });
    
  } catch (error) {
    logger.error('Unexpected error in getCustomerRFM', { error });
    return res.status(500).json({
      success: false,
      error: "An unexpected error occurred"
    });
  }
};

// Lấy danh sách khách hàng theo segment
const getCustomersBySegment = async (req, res) => {
  const user = req.user;
  const { segment } = req.params;
  const { page = 1, limit = 20 } = req.query;
  
  logger.info('Fetching customers by segment', { segment, page, limit });
  
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
    const offset = (page - 1) * limit;
    
    // Lấy số lượng khách hàng trong segment
    const { count, error: countError } = await supabase
      .from('rfm_scores')
      .select('*', { count: 'exact', head: true })
      .eq('business_id', business_id)
      .eq('segment', segment);
      
    if (countError) {
      logger.error('Error counting customers in segment', { error: countError });
      return res.status(500).json({
        success: false,
        error: "Failed to count customers in segment"
      });
    }
    
    // Lấy danh sách khách hàng trong segment
    const { data: customers, error: customersError } = await supabase
    .from('rfm_scores')
    .select(`
      *,
      customers:customer_id (
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        gender,
        birth_date,
        registration_date,
        address,
        city
      )
    `)
    .eq('business_id', business_id)
    .eq('segment', segment)
    .range(offset, offset + limit - 1);
  
    if (customersError) {
      logger.error('Error fetching customers in segment', { error: customersError });
      return res.status(500).json({
        success: false,
        error: "Failed to fetch customers in segment"
      });
    }
    
    logger.info('Customers by segment retrieved successfully', { segment, count });
    return res.status(200).json({
      success: true,
      data: customers,
    });
    
  } catch (error) {
    logger.error('Unexpected error in getCustomersBySegment', { error });
    return res.status(500).json({
      success: false,
      error: "An unexpected error occurred"
    });
  }
};

// Tính lại RFM cho một khách hàng cụ thể
const recalculateCustomerRFM = async (req, res) => {
 const user = req.user;
 const { customer_id } = req.params;
 
 logger.info('Recalculating RFM for customer', { customer_id });
 
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
   
   // Gọi hàm tính RFM cho khách hàng cụ thể
   const { data, error } = await supabase.rpc('calculate_rfm_for_customer', {
     target_customer_id: customer_id,
     target_business_id: business_id
   });
   
   if (error) {
     logger.error('Error recalculating RFM', { error });
     return res.status(500).json({
       success: false,
       error: "Failed to recalculate RFM"
     });
   }
   
   // Lấy thông tin RFM mới của khách hàng
   const { data: rfmData, error: rfmError } = await supabase
     .from('rfm_scores')
     .select('*')
     .eq('customer_id', customer_id)
     .eq('business_id', business_id)
     .single();
     
   if (rfmError) {
     logger.error('Error fetching updated RFM data', { error: rfmError });
     return res.status(404).json({
       success: false,
       error: "Updated RFM data not found"
     });
   }
   
   logger.info('RFM recalculation completed successfully', { customer_id });
   return res.status(200).json({
     success: true,
     data: rfmData
   });
   
 } catch (error) {
   logger.error('Unexpected error in recalculateCustomerRFM', { error });
   return res.status(500).json({
     success: false,
     error: "An unexpected error occurred"
   });
 }
};

// Lấy thống kê RFM theo segment
const getRFMSegmentStatistics = async (req, res) => {
  const user = req.user;
  
  logger.info('Fetching RFM segment statistics');
  
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
    
    // Gọi stored procedure để lấy thống kê
    const { data: stats, error: statsError } = await supabase.rpc('get_rfm_segment_statistics', {
      target_business_id: business_id
    });
    
    if (statsError) {
      logger.error('Error fetching RFM statistics', { error: statsError });
      return res.status(500).json({
        success: false,
        error: "Failed to fetch RFM statistics"
      });
    }
    
    // Thêm các segment không có dữ liệu với giá trị 0
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
    
    const completeStats = allSegments.map(segment => {
      const existingStat = stats.find(s => s.segment === segment);
      return existingStat || {
        segment,
        customer_count: 0,
        percentage: 0,
        total_monetary: 0,
        recency_value: 0,
        frequency_value: 0,
        monetary_value: 0,
        r_score: 0,
        f_score: 0,
        m_score: 0
      };
    });

    // Calculate average RFM scores for each segment
    const segmentRFMScores = await Promise.all(completeStats.map(async (stat) => {
      if (stat.customer_count === 0) return stat;

      const { data: rfmScores, error: rfmError } = await supabase
        .from('rfm_scores')
        .select('recency_value, frequency_value, monetary_value, r_score, f_score, m_score')
        .eq('business_id', business_id)
        .eq('segment', stat.segment);

      if (rfmError) {
        logger.error('Error fetching RFM scores for segment', { 
          error: rfmError,
          segment: stat.segment 
        });
        return stat;
      }

      if (rfmScores && rfmScores.length > 0) {
        const totalCustomers = rfmScores.length;
        
        // Calculate averages
        const avgRecencyValue = rfmScores.reduce((sum, score) => sum + (score.recency_value || 0), 0) / totalCustomers;
        const avgFrequencyValue = rfmScores.reduce((sum, score) => sum + (score.frequency_value || 0), 0) / totalCustomers;
        const avgMonetaryValue = rfmScores.reduce((sum, score) => sum + (Number(score.monetary_value) || 0), 0) / totalCustomers;
        const avgRScore = rfmScores.reduce((sum, score) => sum + (score.r_score || 0), 0) / totalCustomers;
        const avgFScore = rfmScores.reduce((sum, score) => sum + (score.f_score || 0), 0) / totalCustomers;
        const avgMScore = rfmScores.reduce((sum, score) => sum + (score.m_score || 0), 0) / totalCustomers;

        return {
          segment: stat.segment,
          customer_count: stat.customer_count,
          percentage: stat.percentage,
          total_monetary: stat.total_monetary,
          recency_value: Math.round(avgRecencyValue),
          frequency_value: Math.round(avgFrequencyValue),
          monetary_value: Number(avgMonetaryValue.toFixed(2)),
          r_score: Math.round(avgRScore),
          f_score: Math.round(avgFScore),
          m_score: Math.round(avgMScore)
        };
      }

      return stat;
    }));
    
    logger.info('RFM segment statistics retrieved successfully');
    return res.status(200).json({
      success: true,
      data: segmentRFMScores
    });
    
  } catch (error) {
    logger.error('Unexpected error in getRFMSegmentStatistics', { error });
    return res.status(500).json({
      success: false,
      error: "An unexpected error occurred"
    });
  }
};

export {
 calculateRFMForBusiness,
 getCustomerRFM,
 getCustomersBySegment,
 recalculateCustomerRFM,
 getRFMSegmentStatistics
};
