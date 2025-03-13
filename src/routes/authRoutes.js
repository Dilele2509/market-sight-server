import express from 'express';
import { authenticationToken, login } from '../controllers/authController.js';

const router = express.Router();

router.post('/login', login);
router.post('/authorization', authenticationToken);
router.post('/refresh-token', authenticationToken);

export default router;
