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

    const { data, error } = await supabase
        .from('email_waiting_verify')
        .select('id')
        .eq('user_id', userData.user_id)

    if (data.id) return { status: 401, data: { message: 'Please activate your account!' } };

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

const checkTokenEmail = async (token) => {
    const { data, error } = await supabase
        .from('email_waiting_verify')
        .select('id, token')
        .eq('token', token)

    //console.log(data);
    if (error) {
        console.error('Error fetching data:', error);
        return false;
    }

    // Kiểm tra nếu có kết quả trả về và token hợp lệ
    if (data && data.length > 0) {
        const id = data[0].id;  // Lấy id của hàng đầu tiên

        // Xóa bản ghi có id tương ứng trong bảng
        const { error: deleteError } = await supabase
            .from('email_waiting_verify')
            .delete()
            .eq('id', id);

        // Kiểm tra nếu có lỗi khi xóa
        if (deleteError) {
            console.error('Error deleting data:', deleteError);
            return false;
        }

        // Trả về true nếu token hợp lệ và xóa thành công
        return true;
    } else {
        // Nếu không có token hợp lệ
        console.log('Token not found');
        return false;
    }
};


export { checkAccountAvailable, checkTokenEmail };
