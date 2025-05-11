import express from 'express';
import { authenticationToken } from '../controllers/authController.js';
import { allSegmentByUser, checkSegment, deleteSegment, saveSegment, updateStatusSegment } from '../controllers/segmentController.js';
import { previewSegmentation, createSegmentationFromNLP } from '../controllers/nlpSegmentController.js';

const router = express.Router();

router.post('/segment/save-segment', authenticationToken, saveSegment);
router.post('/segment/check-exits', authenticationToken, checkSegment);
router.post('/segment/get-all-by-user', authenticationToken, allSegmentByUser);
router.put('/segment/update-status', authenticationToken, updateStatusSegment);
router.put('/segment/delete', authenticationToken, deleteSegment);

// New NLP-based segmentation routes
router.post('/segment/nlp/preview', authenticationToken, previewSegmentation);
router.post('/segment/nlp/create', authenticationToken, createSegmentationFromNLP);

export default router;
