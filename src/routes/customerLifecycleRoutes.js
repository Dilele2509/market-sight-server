import express from 'express';
import { 
  getCustomerLifecycleMetrics,
  updateCustomerSegments,
  getCustomerJourney
} from '../controllers/customerLifecycleController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

// Customer Lifecycle Analysis Routes
router.get('/customer-lifecycle/metrics', authenticationToken, getCustomerLifecycleMetrics);
router.post('/customer-lifecycle/update-segments', authenticationToken, updateCustomerSegments);
router.get('/customer-lifecycle/journey', authenticationToken, getCustomerJourney);

export default router;
