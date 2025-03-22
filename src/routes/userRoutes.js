import express from 'express';
import { getAllUsers } from '../controllers/userController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

router.get('/users', authenticationToken, getAllUsers);

export default router;
