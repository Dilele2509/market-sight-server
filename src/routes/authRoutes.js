import express from 'express';
import dotenv from 'dotenv'
import { authenticationToken, login, verifyEmailToken } from '../controllers/authController.js';
import { addUser } from '../controllers/userController.js';

dotenv.config()
const router = express.Router();

router.post('/login', login);
router.post('/authorization', authenticationToken);
router.post('/refresh-token', authenticationToken);
router.post('/register', addUser);
// Route to handle email verification
router.post('/verify-email', verifyEmailToken);


export default router;
