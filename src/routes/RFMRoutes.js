import express from 'express';
import { 
    analyzeRFMForPeriod,
    getRFMSegmentCustomers
} from '../controllers/RFMController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

// Analyze RFM for a specific time period
router.post('/rfm/analyze-period', authenticationToken, analyzeRFMForPeriod);

// Get detailed customer information for a specific RFM segment
router.get('/rfm/segment-customers/:segment?', authenticationToken, getRFMSegmentCustomers);

export default router;

