import { supabase } from '../../config.js';
// import bcrypt from 'bcryptjs';

const checkAccountAvailable = async (email, password) => {
    const { data: userData, error: userError } = await supabase
        .from('users')
        .select('user_id, business_id, role_id, first_name, last_name, email, created_at, updated_at, password_hash')
        .eq('email', email)
        .single(); // Chỉ lấy một user duy nhất

    if (userError || !userData) {
        return { status: 401, data: { message: 'Email does not exist' } };
    }

    // Kiểm tra mật khẩu
    // const isMatch = await bcrypt.compare(password, userData.password_hash);
    if (password !== userData.password_hash) {
        return { status: 401, data: { message: 'Wrong password' } };
    }

    // Xóa password_hash trước khi trả về
    delete userData.password_hash;
    console.log('login successful');
    return { status: 200, data: userData };
};

export { checkAccountAvailable };
