import { getUsers } from '../data/userData.js';

const getAllUsers = async (req, res) =>{
    try {
        const users = await getUsers();
        res.status(200).json(users);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Failed to get users' });
    }
}

export {getAllUsers}