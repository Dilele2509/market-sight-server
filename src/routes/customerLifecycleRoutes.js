import express from 'express';
import { 
  getNewCustomersMetrics,
  getEarlyLifeCustomersMetrics,
  getMatureCustomersMetrics,
  getLoyalCustomersMetrics
} from '../controllers/customerLifecycleController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();


// Individual Customer Stage Metrics Routes
router.post('/customer-lifecycle/new-customers', authenticationToken, getNewCustomersMetrics);
router.post('/customer-lifecycle/early-life-customers', authenticationToken, getEarlyLifeCustomersMetrics);
router.post('/customer-lifecycle/mature-customers', authenticationToken, getMatureCustomersMetrics);
router.post('/customer-lifecycle/loyal-customers', authenticationToken, getLoyalCustomersMetrics);



export default router;
