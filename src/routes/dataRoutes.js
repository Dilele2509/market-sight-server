import express from 'express';
import multer from 'multer';
import { 
  getTables, 
  executeQuery, 
  uploadFile, 
  testConnection, 
  getPostgresTables,
  automateDataMapping 
} from '../controllers/dataController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

// Configure multer for file uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
});

// Routes
router.get('/data/tables',authenticationToken, getTables);
router.post('/data/query',authenticationToken, executeQuery);
router.post('/data/upload/:table_name',authenticationToken, upload.single('file'), uploadFile);
router.post('/data/test-connection',authenticationToken, testConnection);
router.post('/data/datasources/postgres/tables',authenticationToken, getPostgresTables);
router.post('/data/automate-mapping',authenticationToken, automateDataMapping);

export default router; 