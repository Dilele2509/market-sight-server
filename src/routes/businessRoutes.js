import express from 'express';
import { getAllBusinesses } from '../controllers/businessController.js';

const router = express.Router();

router.get('/businesses', getAllBusinesses);

export default router;
