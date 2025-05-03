import express from 'express';
import dotenv from 'dotenv'
import { authenticationToken, login, checkRegister, verifyEmailToken } from '../controllers/authController.js';
import { addUser } from '../controllers/userController.js';
import { addBusiness } from '../controllers/businessController.js';

dotenv.config()
const router = express.Router();

router.post('/login', login);
router.post('/authorization', authenticationToken);
router.post('/refresh-token', authenticationToken);
router.post('/register', checkRegister, (req, res) => {
    if (req.nextHandler === 'addUser') {
        return addUser(req, res);
    } else if (req.nextHandler === 'addBusiness') {
        return addBusiness(req, res);
    }
});
// Route to handle email verification
router.post('/verify-email', verifyEmailToken);


export default router;
