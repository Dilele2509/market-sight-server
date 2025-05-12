import { getSupabase } from '../data/database.js';
import { logger } from '../data/database.js';
import { OAuth2Client } from 'google-auth-library';

// Initialize OAuth2 client
const oauth2Client = new OAuth2Client(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);

// Scopes required for Google Sheets access
const SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/drive.file'
];

// Generate OAuth URL and redirect to Google
const initiateGoogleAuth = async (req, res) => {
  const user = req.user;

  console.log('check gg: ',user);
  logger.info('Initiating Google OAuth flow', {
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
    // Generate state parameter to prevent CSRF
    const state = Buffer.from(JSON.stringify({
      user_id: user.user_id,
      timestamp: Date.now()
    })).toString('base64');

    // Generate OAuth URL
    const authUrl = oauth2Client.generateAuthUrl({
      access_type: 'offline',
      scope: SCOPES,
      state,
      prompt: 'consent' // Force consent screen to get refresh token
    });

    // Return the OAuth URL instead of redirecting
    res.json({
      success: true,
      data: {
        authUrl
      }
    });
  } catch (error) {
    logger.error('Error initiating OAuth flow', {
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

// Handle OAuth callback
const handleOAuthCallback = async (req, res) => {
  const { code, state, scope } = req.query;

  logger.info('Handling OAuth callback', {
    state,
    scope
  });

  if (!code || !state) {
    logger.warn('Missing required parameters', { code, state });
    return res.status(400).json({
      success: false,
      error: "Code and state are required"
    });
  }

  try {
    // Verify state parameter
    const decodedState = JSON.parse(Buffer.from(state, 'base64').toString());
    const { user_id } = decodedState;

    // Exchange code for tokens
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // Get user info
    const supabase = getSupabase();
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('user_id, business_id')
      .eq('user_id', user_id)
      .single();

    if (userError || !userData) {
      throw new Error('User not found');
    }

    // Store tokens securely
    const { error: tokenError } = await supabase
      .from('google_tokens')
      .upsert({
        user_id,
        business_id: userData.business_id,
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        expiry_date: new Date(tokens.expiry_date).toISOString(),
        scope: tokens.scope,
        token_type: tokens.token_type,
        updated_at: new Date().toISOString()
      }, {
        onConflict: 'user_id,business_id'
      });

    if (tokenError) {
      throw tokenError;
    }

    // Return success response
    res.json({
      success: true,
      message: "Google OAuth successful",
      data: {
        user_id,
        business_id: userData.business_id,
        scope: tokens.scope,
        expiry_date: new Date(tokens.expiry_date).toISOString(),
        token_type: tokens.token_type
      }
    });
  } catch (error) {
    logger.error('Error handling OAuth callback', {
      error: error.message,
      stack: error.stack
    });
    
    res.status(400).json({
      success: false,
      error: error.message
    });
  }
};

// Revoke Google access
const revokeAccess = async (req, res) => {
  const user = req.user;

  logger.info('Revoking Google access', {
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

    // Get stored tokens
    const { data: tokenData, error: tokenError } = await supabase
      .from('google_tokens')
      .select('access_token')
      .eq('user_id', user.user_id)
      .single();

    if (tokenError) {
      throw tokenError;
    }

    if (tokenData?.access_token) {
      // Revoke token
      await oauth2Client.revokeToken(tokenData.access_token);
    }

    // Delete stored tokens
    const { error: deleteError } = await supabase
      .from('google_tokens')
      .delete()
      .eq('user_id', user.user_id);

    if (deleteError) {
      throw deleteError;
    }

    res.json({
      success: true,
      message: "Google access revoked successfully"
    });
  } catch (error) {
    logger.error('Error revoking Google access', {
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

// Get OAuth status
const getAuthStatus = async (req, res) => {
  const user = req.user;

  logger.info('Getting OAuth status', {
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

    // Get stored tokens
    const { data: tokenData, error: tokenError } = await supabase
      .from('google_tokens')
      .select('*')
      .eq('user_id', user.user_id)
      .single();

    if (tokenError && tokenError.code !== 'PGRST116') { // PGRST116 is "no rows returned"
      throw tokenError;
    }

    // Check if token is expired
    const isExpired = tokenData?.expiry_date && tokenData.expiry_date < Date.now();

    res.json({
      success: true,
      data: {
        is_connected: !!tokenData && !isExpired,
        expiry_date: tokenData?.expiry_date,
        scope: tokenData?.scope
      }
    });
  } catch (error) {
    logger.error('Error getting OAuth status', {
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
  initiateGoogleAuth,
  handleOAuthCallback,
  revokeAccess,
  getAuthStatus
}; 