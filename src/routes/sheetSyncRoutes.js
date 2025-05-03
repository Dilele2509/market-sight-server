import express from 'express';
import { authenticationToken } from '../controllers/authController.js';
import {
  getSegmentPreview,
  syncSegmentToSheet,
  getSyncHistory
} from '../controllers/sheetSyncController.js';

const router = express.Router();

// Get preview data for a segment
router.post('/sync/preview', authenticationToken, getSegmentPreview);

// Sync segment data to Google Sheets
router.post('/sync/sheetSync', authenticationToken, syncSegmentToSheet);

// Get sync history
router.get('/sync/history', authenticationToken, getSyncHistory);

export default router; 