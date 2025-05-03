import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';
import { google } from 'googleapis';
import { OAuth2Client } from 'google-auth-library';

// Initialize Google Sheets API
const sheets = google.sheets('v4');

// Initialize OAuth2 client
const oauth2Client = new OAuth2Client(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);

// Function to get preview data for a segment
const getSegmentPreview = async (req, res) => {
  const user = req.user;
  const { segment_name, reference_date, time_range } = req.body;

  logger.info('Starting segment preview generation', {
    user_id: user?.user_id,
    segment_name,
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

  if (!segment_name || !reference_date || !time_range) {
    logger.warn('Missing required parameters', { segment_name, reference_date, time_range });
    return res.status(400).json({
      success: false,
      error: "Segment name, reference date, and time range are required"
    });
  }

  try {
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

    // Get segment data based on segment name
    let segmentData;
    switch (segment_name.toLowerCase()) {
      case 'new customers':
        const { data: newCustomersData, error: newCustomersError } = await supabase.rpc(
          'get_detailed_new_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (newCustomersError) throw newCustomersError;
        segmentData = newCustomersData;
        break;

      case 'early-life customers':
        const { data: earlyLifeData, error: earlyLifeError } = await supabase.rpc(
          'get_detailed_early_life_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (earlyLifeError) throw earlyLifeError;
        segmentData = earlyLifeData;
        break;

      case 'mature customers':
        const { data: matureData, error: matureError } = await supabase.rpc(
          'get_detailed_mature_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (matureError) throw matureError;
        segmentData = matureData;
        break;

      case 'loyal customers':
        const { data: loyalData, error: loyalError } = await supabase.rpc(
          'get_detailed_loyal_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (loyalError) throw loyalError;
        segmentData = loyalData;
        break;

      default:
        return res.status(400).json({
          success: false,
          error: "Invalid segment name"
        });
    }

    // Format preview data
    const previewData = {
      segment_name,
      record_count: segmentData.length,
      sample_data: segmentData.slice(0, 5), // Show first 5 records as preview
      metrics: {
        total_customers: segmentData.length,
        // Add other relevant metrics based on segment type
      },
      time_window: {
        reference_date,
        time_range
      }
    };

    res.json({
      success: true,
      data: previewData
    });
  } catch (error) {
    logger.error('Error generating segment preview', {
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

// Function to sync segment data to Google Sheets
const syncSegmentToSheet = async (req, res) => {
  const user = req.user;
  const { segment_name, reference_date, time_range, sheet_id, sheet_name } = req.body;

  logger.info('Starting segment sync to Google Sheets', {
    user_id: user?.user_id,
    segment_name,
    reference_date,
    time_range,
    sheet_id,
    sheet_name
  });

  if (!user || !user.user_id) {
    logger.warn('User authentication missing', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required"
    });
  }

  if (!segment_name || !reference_date || !time_range || !sheet_id || !sheet_name) {
    logger.warn('Missing required parameters', { 
      segment_name, 
      reference_date, 
      time_range, 
      sheet_id, 
      sheet_name 
    });
    return res.status(400).json({
      success: false,
      error: "All parameters are required"
    });
  }

  try {
    const supabase = getSupabase();
    
    // Get business_id and Google tokens
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

    // Get Google tokens
    const { data: tokenData, error: tokenError } = await supabase
      .from('google_tokens')
      .select('*')
      .eq('user_id', user.user_id)
      .single();

    if (tokenError || !tokenData) {
      logger.error('Error retrieving Google tokens', {
        error: tokenError?.message,
        user_id: user.user_id
      });
      return res.status(400).json({
        success: false,
        error: "Google account not connected. Please connect your Google account first."
      });
    }

    // Check if token is expired
    if (tokenData.expiry_date < Date.now()) {
      // Refresh token
      oauth2Client.setCredentials({
        refresh_token: tokenData.refresh_token
      });

      const { credentials } = await oauth2Client.refreshAccessToken();
      
      // Update stored tokens
      const { error: updateError } = await supabase
        .from('google_tokens')
        .update({
          access_token: credentials.access_token,
          expiry_date: credentials.expiry_date,
          updated_at: new Date().toISOString()
        })
        .eq('user_id', user.user_id);

      if (updateError) {
        throw updateError;
      }

      oauth2Client.setCredentials(credentials);
    } else {
      oauth2Client.setCredentials({
        access_token: tokenData.access_token,
        refresh_token: tokenData.refresh_token
      });
    }

    // Get segment data
    let segmentData;
    switch (segment_name.toLowerCase()) {
      case 'new customers':
        const { data: newCustomersData, error: newCustomersError } = await supabase.rpc(
          'get_detailed_new_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (newCustomersError) throw newCustomersError;
        segmentData = newCustomersData;
        break;

      case 'early-life customers':
        const { data: earlyLifeData, error: earlyLifeError } = await supabase.rpc(
          'get_detailed_early_life_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (earlyLifeError) throw earlyLifeError;
        segmentData = earlyLifeData;
        break;

      case 'mature customers':
        const { data: matureData, error: matureError } = await supabase.rpc(
          'get_detailed_mature_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (matureError) throw matureError;
        segmentData = matureData;
        break;

      case 'loyal customers':
        const { data: loyalData, error: loyalError } = await supabase.rpc(
          'get_detailed_loyal_customers_info',
          {
            p_business_id: Number(userData.business_id),
            p_reference_date: reference_date,
            p_time_range: Number(time_range)
          }
        );
        if (loyalError) throw loyalError;
        segmentData = loyalData;
        break;

      default:
        return res.status(400).json({
          success: false,
          error: "Invalid segment name"
        });
    }

    // Prepare data for Google Sheets
    const headers = Object.keys(segmentData[0]);
    const values = [
      headers,
      ...segmentData.map(record => headers.map(header => record[header]))
    ];

    // Update Google Sheet
    await sheets.spreadsheets.values.update({
      auth: oauth2Client,
      spreadsheetId: sheet_id,
      range: `${sheet_name}!A1`,
      valueInputOption: 'RAW',
      resource: {
        values
      }
    });

    // Log sync attempt
    const { error: syncLogError } = await supabase
      .from('sheet_sync_history')
      .insert({
        user_id: user.user_id,
        business_id: userData.business_id,
        segment_name,
        record_count: segmentData.length,
        status: 'completed',
        sheet_id,
        sheet_name,
        sync_date: new Date().toISOString()
      });

    if (syncLogError) {
      logger.error('Error logging sync attempt', {
        error: syncLogError.message,
        user_id: user.user_id
      });
    }

    res.json({
      success: true,
      message: "Segment data successfully synced to Google Sheets",
      data: {
        record_count: segmentData.length,
        sheet_id,
        sheet_name
      }
    });
  } catch (error) {
    logger.error('Error syncing segment to Google Sheets', {
      error: error.message,
      stack: error.stack,
      user_id: user?.user_id
    });

    // Log failed sync attempt
    try {
      await supabase
        .from('sheet_sync_history')
        .insert({
          user_id: user.user_id,
          business_id: userData.business_id,
          segment_name,
          record_count: 0,
          status: 'failed',
          sheet_id,
          sheet_name,
          sync_date: new Date().toISOString(),
          error_message: error.message
        });
    } catch (logError) {
      logger.error('Error logging failed sync attempt', {
        error: logError.message,
        user_id: user.user_id
      });
    }

    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

// Function to get sync history
const getSyncHistory = async (req, res) => {
  const user = req.user;
  const { limit = 10, offset = 0 } = req.query;

  logger.info('Retrieving sync history', {
    user_id: user?.user_id,
    limit,
    offset
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
    
    // Get sync history
    const { data: historyData, error: historyError, count } = await supabase
      .from('sheet_sync_history')
      .select('*', { count: 'exact' })
      .eq('user_id', user.user_id)
      .order('sync_date', { ascending: false })
      .range(offset, offset + limit - 1);

    if (historyError) {
      throw historyError;
    }

    // Calculate success rate
    const successCount = historyData.filter(record => record.status === 'completed').length;
    const successRate = historyData.length > 0 ? (successCount / historyData.length) * 100 : 0;

    res.json({
      success: true,
      data: {
        history: historyData,
        pagination: {
          total: count,
          limit: Number(limit),
          offset: Number(offset)
        },
        metrics: {
          success_rate: Number(successRate.toFixed(2)),
          total_syncs: historyData.length,
          successful_syncs: successCount
        }
      }
    });
  } catch (error) {
    logger.error('Error retrieving sync history', {
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
  getSegmentPreview,
  syncSegmentToSheet,
  getSyncHistory
}; 