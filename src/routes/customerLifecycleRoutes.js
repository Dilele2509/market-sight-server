import express from 'express';
import { 
  getNewCustomersMetrics,
  getEarlyLifeCustomersMetrics,
  getMatureCustomersMetrics,
  getLoyalCustomersMetrics,
  // getCustomerStagePeriodChanges,
  getToplineMetricsBreakdown,
  getCustomerStageMonthlyBreakdown
} from '../controllers/customerLifecycleController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

// Individual Customer Stage Metrics Routes
router.post('/customer-lifecycle/new-customers', authenticationToken, getNewCustomersMetrics);
router.post('/customer-lifecycle/early-life-customers', authenticationToken, getEarlyLifeCustomersMetrics);
router.post('/customer-lifecycle/mature-customers', authenticationToken, getMatureCustomersMetrics);
router.post('/customer-lifecycle/loyal-customers', authenticationToken, getLoyalCustomersMetrics);

// Topline Metrics Route  
router.post('/customer-lifecycle/topline-metrics', authenticationToken, getToplineMetricsBreakdown);
// Customer Stage Period Changes Route
// router.get('/customer-lifecycle/stage-period-changes', authenticationToken, getCustomerStagePeriodChanges);
router.post('/customer-lifecycle/stage-breakdown', authenticationToken, getCustomerStageMonthlyBreakdown);

export default router;
