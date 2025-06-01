import { getUserByEmail, getUsers, insertUser } from '../data/userData.js';
import { sendVerificationEmail } from '../services/nodemailer.js';
import { broadcast } from '../services/websocketService.js';

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
        
        // Broadcast new user registration to all connected clients
        console.log(response.data);
        broadcast({
            type: 'NEW_USER_REGISTERED',
            data: {
                user_id: response.data.user_id,
                email: response.data.email,
                first_name: response.data.first_name,
                last_name: response.data.last_name,
                created_at: response.data.created_at
            }
        });

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