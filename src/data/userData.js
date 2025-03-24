import { supabase } from '../../config.js';

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

export { getUsers, getUserByEmail };
