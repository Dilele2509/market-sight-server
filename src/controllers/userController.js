import { getUserByEmail, getUsers, insertUser } from '../data/userData.js';
import { sendVerificationEmail } from '../services/nodemailer.js';

const getAllUsers = async (req, res) => {
    try {
        const users = await getUsers();
        res.status(200).json(users);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Failed to get users' });
    }
}

const getUserDataByEmail = async (req, res) => {
    try {
        //console.log(req.user);
        const email = req.user.email;
        const user = await getUserByEmail(email);
        res.status(200).json(user);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Failed to get user data' });
    }
}

const addUser = async (req, res) => {
    try {
        const data = req.body
        console.log(data);
        const response = await insertUser(data)
        if (response.status !== 200) {
            res.status(response.status).json({
                message: response.detail
            })
        }
        else if (response.status === 200) {
            sendVerificationEmail(response.data.email, response.token)
        }
        res.status(200).json({
            message: 'Register successfully, check your email to activate account',
            data: response.data
        })
    } catch (error) {
        console.error(error);
        res.status(500).json({ error });
    }
}

export { getAllUsers, getUserDataByEmail, addUser }