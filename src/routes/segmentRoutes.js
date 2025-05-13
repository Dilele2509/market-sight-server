import express from 'express';
import { authenticationToken } from '../controllers/authController.js';
import { allSegmentByUser, checkSegment, deleteSegment, insertSegmentCustomer, saveSegment, updateStatusSegment } from '../controllers/segmentController.js';
import { createSegmentationFromNLP, processChatbotQuery } from '../controllers/nlpSegmentController.js';

const router = express.Router();

router.post('/segment/save-segment', authenticationToken, saveSegment);
router.post('/segment/check-exits', authenticationToken, checkSegment);
router.post('/segment/get-all-by-user', authenticationToken, allSegmentByUser);
router.put('/segment/update-status', authenticationToken, updateStatusSegment);
router.put('/segment/delete', authenticationToken, deleteSegment);

router.post('/segment/add-state-sync', authenticationToken, insertSegmentCustomer)

// New NLP-based segmentation routes
router.post('/segment/nlp/create', authenticationToken, createSegmentationFromNLP);
router.post('/segment/nlp/chatbot', authenticationToken, processChatbotQuery);

export default router;
