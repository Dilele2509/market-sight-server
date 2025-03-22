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

export { getUsers };
