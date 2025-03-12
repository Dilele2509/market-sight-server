const {supabase} = require('../../config');

const getUsers = async () => {
    const { data, error } = await supabase.from('users').select('*');
    if (error) throw error;
    return data;
};

module.exports = { getUsers };
