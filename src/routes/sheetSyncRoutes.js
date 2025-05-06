import express from 'express';
import { authenticationToken } from '../controllers/authController.js';
import {
  getAvailableSegments,
  syncSegmentToSheet,
  getSyncHistory,
  testTokenRefresh
} from '../controllers/sheetSyncController.js';

const router = express.Router();

// Get available segments for a business
router.get('/sync/segments', authenticationToken, getAvailableSegments);

// Sync segment to Google Sheets
router.post('/sync', authenticationToken, syncSegmentToSheet);

// Get sync history
router.get('/sync/history', authenticationToken, getSyncHistory);

// Test token refresh
router.post('/sync/test-refresh', authenticationToken, testTokenRefresh);

export default router; 