import express from 'express';
import { authenticationToken } from '../controllers/authController.js';
import {
  initiateGoogleAuth,
  handleOAuthCallback,
  revokeAccess,
  getAuthStatus
} from '../controllers/googleAuthController.js';

const router = express.Router();

// Initiate Google OAuth flow
router.get('/auth/google', authenticationToken, initiateGoogleAuth);

// Handle OAuth callback
router.get('/oauth2callback', handleOAuthCallback);

// Revoke Google access
router.post('/revoke', authenticationToken, revokeAccess);

// Get OAuth status
router.get('/status', authenticationToken, getAuthStatus);

export default router; 