import express from 'express';
import { getAllUsers, getUserDataByEmail } from '../controllers/userController.js';
import { authenticationToken } from '../controllers/authController.js';

const router = express.Router();

router.get('/users', authenticationToken, getAllUsers);
router.get('/user-profile', authenticationToken, getUserDataByEmail);

export default router;
