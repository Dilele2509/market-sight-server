import { getUserByEmail, getUsers } from '../data/userData.js';

const getAllUsers = async (req, res) =>{
    try {
        const users = await getUsers();
        res.status(200).json(users);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Failed to get users' });
    }
}

const getUserDataByEmail = async (req, res) =>{
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

export {getAllUsers, getUserDataByEmail}