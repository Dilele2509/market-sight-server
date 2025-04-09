import express from 'express';
import { authenticationToken } from '../controllers/authController.js';
import { allSegmentByUser, checkSegment, saveSegment } from '../controllers/segmentController.js';

const router = express.Router();

router.post('/segment/save-segment', authenticationToken, saveSegment);
router.post('/segment/check-exits', authenticationToken, checkSegment);
router.post('/segment/get-all-by-user', authenticationToken, allSegmentByUser)

export default router;
