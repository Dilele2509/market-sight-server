import express from 'express';
import { authenticationToken, login, logout } from '../controllers/authController.js';

const router = express.Router();

router.post('/login', login);
router.post('/authorization', authenticationToken);
router.post ('/logout', logout);
router.post('/refresh-token', authenticationToken);

export default router;
