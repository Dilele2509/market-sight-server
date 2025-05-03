import { supabase } from '../../config.js';
import dotenv from 'dotenv'
import jwt from 'jsonwebtoken'

dotenv.config()

const getUsers = async () => {
    const { data, error } = await supabase.from('users').select('*');
    if (error) throw error;

    data.forEach(userData => {
        // Xóa password_hash trước khi trả về
        delete userData.password_hash;
    });
    return data;
};

const getUserByEmail = async (email) => {
    const { data: userData, error } = await supabase
        .from('users')
        .select('user_id, business_id, role_id, first_name, last_name, email, created_at, updated_at')
        .eq('email', email)
        .single(); // Chỉ lấy một user duy nhất

    if (error) throw error;

    return { status: 200, data: userData };
}

const insertUser = async (data) => {
    if (data.password !== data.confirmPassword) {
        return { status: 400, data: 'Password and confirm password do not match' };
    }

    const { data: resData, error } = await supabase
        .from('users')
        .insert([
            {
                business_id: Number(data.business_id),
                role_id: Number(data.role),
                first_name: data.firstName,
                last_name: data.lastName,
                email: data.email,
                password_hash: data.password,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
            }
        ])
        .select();

    if (error) throw error;

    const insertedUser = resData?.[0];
    if (!insertedUser) {
        return { status: 500, data: 'Failed to insert user' };
    }

    const user_id = insertedUser.user_id;
    // Generate a JWT token
    const token = jwt.sign({ email: insertedUser.email }, process.env.ACCESS_TOKEN_KEY, { expiresIn: '1h' });

    const { error: emailInsertError } = await supabase
        .from('email_waiting_verify')
        .insert([
            {
                user_id: user_id,
                token: token
            }
        ]);

    if (emailInsertError) throw emailInsertError;

    return { status: 200, data: insertedUser, token: token };
};

export { getUsers, getUserByEmail, insertUser };
