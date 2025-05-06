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

// Function to refresh Google access token
const refreshGoogleToken = async (refreshToken) => {
  try {
    oauth2Client.setCredentials({
      refresh_token: refreshToken
    });

    const { credentials } = await oauth2Client.refreshAccessToken();
    
    return {
      access_token: credentials.access_token,
      expiry_date: credentials.expiry_date
    };
  } catch (error) {
    logger.error('Error refreshing Google token', {
      error: error.message,
      stack: error.stack
    });
    throw new Error('Failed to refresh Google access token');
  }
};

// Function to check and refresh token if needed
const ensureValidToken = async (tokenData) => {
  try {
    // Check if token is expired or will expire in the next 5 minutes
    const isExpired = tokenData.expiry_date && 
      (tokenData.expiry_date - 5 * 60 * 1000) < Date.now();

    if (isExpired) {
      const newTokenData = await refreshGoogleToken(tokenData.refresh_token);
      
      // Update token in database
      const supabase = getSupabase();
      const { error: updateError } = await supabase
        .from('google_tokens')
        .update({
          access_token: newTokenData.access_token,
          expiry_date: newTokenData.expiry_date
        })
        .eq('user_id', tokenData.user_id);

      if (updateError) {
        logger.error('Error updating refreshed token', {
          error: updateError.message,
          user_id: tokenData.user_id
        });
        throw new Error('Failed to update refreshed token');
      }

      return {
        ...tokenData,
        access_token: newTokenData.access_token,
        expiry_date: newTokenData.expiry_date
      };
    }

    return tokenData;
  } catch (error) {
    logger.error('Error in ensureValidToken', {
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
};

// Get available segments for a business
const getAvailableSegments = async (req, res) => {
  const user = req.user;

  logger.info('Getting available segments', {
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

    // Get business_id
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('business_id')
      .eq('user_id', user.user_id)
      .single();

    if (userError || !userData?.business_id) {
      throw new Error('Business ID not found');
    }

    // Get segments
    const { data: segments, error: segmentsError } = await supabase
      .from('segmentation')
      .select('*')
      .eq('business_id', userData.business_id)
      .eq('status', 'active');

    if (segmentsError) {
      throw segmentsError;
    }

    res.json({
      success: true,
      data: segments
    });
  } catch (error) {
    logger.error('Error getting segments', {
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

// Sync segment to Google Sheets
const syncSegmentToSheet = async (req, res) => {
  const user = req.user;
  const { segment_id, sheet_url, new_file_name, create_new } = req.body;

  // Convert create_new to boolean if it's a string
  const shouldCreateNew = create_new === true || create_new === 'true';

  logger.info('Syncing segment to Google Sheets', {
    user_id: user?.user_id,
    segment_id,
    sheet_url,
    new_file_name,
    create_new: shouldCreateNew
  });

  if (!user || !user.user_id || !user.business_id) {
    logger.warn('User authentication missing or incomplete', { user });
    return res.status(400).json({
      success: false,
      error: "User authentication required with business_id"
    });
  }

  if (!segment_id || (!sheet_url && !shouldCreateNew)) {
    logger.warn('Missing required parameters', { segment_id, sheet_url, shouldCreateNew });
    return res.status(400).json({
      success: false,
      error: "Segment ID and either sheet URL or create_new flag are required"
    });
  }

  try {
    const supabase = getSupabase();

    // Get segment details
    const { data: segment, error: segmentError } = await supabase
      .from('segmentation')
      .select('*')
      .eq('segment_id', segment_id)
      .eq('business_id', user.business_id)
      .single();

    if (segmentError || !segment) {
      throw new Error('Segment not found');
    }

    // Get segment customers with their details
    const { data: customers, error: customersError } = await supabase
      .from('segment_customers')
      .select(`
        customer_id,
        assigned_at,
        customers!inner (
          birth_date,
          registration_date,
          business_id,
          phone,
          gender,
          address,
          city,
          first_name,
          last_name,
          email
        )
      `)
      .eq('segment_id', segment_id);

    if (customersError) {
      throw customersError;
    }

    // Get Google tokens
    const { data: tokenData, error: tokenError } = await supabase
      .from('google_tokens')
      .select('*')
      .eq('user_id', user.user_id)
      .single();

    if (tokenError || !tokenData) {
      throw new Error('Google account not connected');
    }

    // Ensure token is valid
    const validTokenData = await ensureValidToken(tokenData);

    // Set up OAuth2 client with tokens
    oauth2Client.setCredentials({
      access_token: validTokenData.access_token,
      refresh_token: validTokenData.refresh_token
    });

    let targetSheetId;
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    const drive = google.drive({ version: 'v3', auth: oauth2Client });

    // If creating new file
    if (shouldCreateNew) {
      // Create new spreadsheet with custom name or default name
      const fileMetadata = {
        name: new_file_name || `${segment.segment_name} - Customer Segment`,
        mimeType: 'application/vnd.google-apps.spreadsheet'
      };

      const file = await drive.files.create({
        requestBody: fileMetadata,
        fields: 'id'
      });

      targetSheetId = file.data.id;
    } else {
      // Extract Sheet ID from URL
      const sheetIdMatch = sheet_url.match(/\/d\/([a-zA-Z0-9-_]+)/);
      if (!sheetIdMatch) {
        throw new Error('Invalid Google Sheets URL format');
      }
      targetSheetId = sheetIdMatch[1];
    }

    // Use segment name as sheet name
    const sheetName = segment.segment_name;

    // Check if sheet exists in the file
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: targetSheetId,
      fields: 'sheets.properties'
    });

    const existingSheets = spreadsheet.data.sheets.map(sheet => sheet.properties.title);
    
    if (existingSheets.includes(sheetName)) {
      // If sheet exists, delete it first
      const sheetId = spreadsheet.data.sheets.find(s => s.properties.title === sheetName).properties.sheetId;
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: targetSheetId,
        requestBody: {
          requests: [{
            deleteSheet: {
              sheetId: sheetId
            }
          }]
        }
      });
    }

    // Create new sheet with segment name
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: targetSheetId,
      requestBody: {
        requests: [{
          addSheet: {
            properties: {
              title: sheetName
            }
          }
        }]
      }
    });

    // Prepare data for Google Sheets
    const values = [
      [
        'Customer ID',
        'First Name',
        'Last Name',
        'Email',
        'Phone',
        'Gender',
        'Birth Date',
        'Registration Date',
        'Address',
        'City',
        'Segment Name',
        'Assigned At'
      ],
      ...customers.map(customer => [
        customer.customer_id,
        customer.customers.first_name || '',
        customer.customers.last_name || '',
        customer.customers.email || '',
        customer.customers.phone || '',
        customer.customers.gender || '',
        customer.customers.birth_date ? new Date(customer.customers.birth_date).toISOString().split('T')[0] : '',
        customer.customers.registration_date ? new Date(customer.customers.registration_date).toISOString() : '',
        customer.customers.address || '',
        customer.customers.city || '',
        segment.segment_name,
        customer.assigned_at
      ])
    ];

    // Update Google Sheet
    await sheets.spreadsheets.values.update({
      auth: oauth2Client,
      spreadsheetId: targetSheetId,
      range: `${sheetName}!A1`,
      valueInputOption: 'RAW',
      requestBody: {
        values
      }
    });

    // Auto-resize columns for better readability
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: targetSheetId,
      requestBody: {
        requests: [{
          autoResizeDimensions: {
            dimensions: {
              sheetId: 0,
              dimension: 'COLUMNS',
              startIndex: 0,
              endIndex: values[0].length
            }
          }
        }]
      }
    });

    // Log sync attempt
    const { error: syncLogError } = await supabase
      .from('sheet_sync_history')
      .insert({
        user_id: user.user_id,
        business_id: user.business_id,
        segment_id,
        sheet_id: targetSheetId,
        status: 'completed',
        created_at: new Date().toISOString()
      });

    if (syncLogError) {
      logger.error('Error logging sync attempt', {
        error: syncLogError.message,
        user_id: user.user_id
      });
    }

    res.json({
      success: true,
      message: "Segment successfully synced to Google Sheets",
      data: {
        sheet_id: targetSheetId,
        sheet_name: sheetName,
        customer_count: customers.length
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
      const supabase = getSupabase();
      await supabase
        .from('sheet_sync_history')
        .insert({
          user_id: user.user_id,
          business_id: user.business_id,
          segment_id,
          sheet_id: targetSheetId,
          status: 'failed',
          error_message: error.message,
          created_at: new Date().toISOString()
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

// Get sync history
const getSyncHistory = async (req, res) => {
  const user = req.user;

  logger.info('Getting sync history', {
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

    // Get sync history
    const { data: history, error: historyError } = await supabase
      .from('sheet_sync_history')
      .select(`
        *,
        segmentation:segment_id (
          segment_name,
          description
        )
      `)
      .eq('user_id', user.user_id)
      .order('created_at', { ascending: false });

    if (historyError) {
      throw historyError;
    }

    res.json({
      success: true,
      data: history
    });
  } catch (error) {
    logger.error('Error getting sync history', {
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

// Test token refresh
const testTokenRefresh = async (req, res) => {
  const user = req.user;

  logger.info('Testing token refresh', {
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

    // Get current token data
    const { data: tokenData, error: tokenError } = await supabase
      .from('google_tokens')
      .select('*')
      .eq('user_id', user.user_id)
      .single();

    if (tokenError || !tokenData) {
      throw new Error('Google account not connected');
    }

    // Get new token data
    const newTokenData = await refreshGoogleToken(tokenData.refresh_token);

    // Format expiry date for database
    const expiryDate = new Date(newTokenData.expiry_date);
    const formattedExpiryDate = expiryDate.toISOString().replace('T', ' ').replace('Z', '');

    // Update token in database
    const { error: updateError } = await supabase
      .from('google_tokens')
      .update({
        access_token: newTokenData.access_token,
        expiry_date: formattedExpiryDate,
        updated_at: new Date().toISOString().replace('T', ' ').replace('Z', '')
      })
      .eq('user_id', user.user_id);

    if (updateError) {
      logger.error('Error updating token', {
        error: updateError.message,
        user_id: user.user_id
      });
      throw new Error('Failed to update refreshed token');
    }

    res.json({
      success: true,
      message: "Token refreshed successfully",
      data: {
        old_token: {
          access_token: tokenData.access_token,
          expiry_date: tokenData.expiry_date
        },
        new_token: {
          access_token: newTokenData.access_token,
          expiry_date: formattedExpiryDate
        }
      }
    });
  } catch (error) {
    logger.error('Error in test token refresh', {
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
  getAvailableSegments,
  syncSegmentToSheet,
  getSyncHistory,
  refreshGoogleToken,
  ensureValidToken,
  testTokenRefresh
}; 