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

const router = express.Router();

// Configure multer for file uploads
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB limit
  }
});

// Routes
router.get('/data/tables', getTables);
router.post('/data/query', executeQuery);
router.post('/data/upload/:table_name', upload.single('file'), uploadFile);
router.post('/data/test-connection', testConnection);
router.get('/data/datasources/postgres/tables', getPostgresTables);
router.post('/data/automate-mapping', automateDataMapping);

export default router; 