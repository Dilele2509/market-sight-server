import express from 'express';
import { 
  updateCustomerSegments,
  getCustomerJourney,
  getNewCustomersMetrics,
  getEarlyLifeCustomersMetrics,
  getMatureCustomersMetrics,
  getLoyalCustomersMetrics
} from '../controllers/customerLifecycleController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

// Customer Lifecycle Analysis Routes
router.post('/customer-lifecycle/update-segments', authenticationToken, updateCustomerSegments);
router.get('/customer-lifecycle/journey', authenticationToken, getCustomerJourney);

// Individual Customer Stage Metrics Routes
router.post('/customer-lifecycle/new-customers', authenticationToken, getNewCustomersMetrics);
router.post('/customer-lifecycle/early-life-customers', authenticationToken, getEarlyLifeCustomersMetrics);
router.post('/customer-lifecycle/mature-customers', authenticationToken, getMatureCustomersMetrics);
router.post('/customer-lifecycle/loyal-customers', authenticationToken, getLoyalCustomersMetrics);

export default router;
