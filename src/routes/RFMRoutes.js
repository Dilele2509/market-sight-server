import express from 'express';
import { 
    calculateRFMForBusiness,
    getCustomerRFM,
    getCustomersBySegment,
    recalculateCustomerRFM,
    getRFMSegmentStatistics
} from '../controllers/RFMController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

// Calculate RFM scores for all customers of a business
router.post('/rfm/calculate', authenticationToken, calculateRFMForBusiness);

// Get RFM analysis for a specific customer
router.get('/rfm/rfm-customer/:customer_id', authenticationToken, getCustomerRFM);

// Get customers by segment
router.get('/rfm/rfm-segment/:segment', authenticationToken, getCustomersBySegment);

// Recalculate RFM for a specific customer
router.post('/rfm/recalculate/:customer_id', authenticationToken, recalculateCustomerRFM);

// get RFM statistics
router.get('/rfm/rfm-statistic', authenticationToken, getRFMSegmentStatistics)

export default router;

