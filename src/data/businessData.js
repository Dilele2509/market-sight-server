import { supabase } from '../../config.js';

const getBusinesses = async () => {
    const { data, error } = await supabase.from('business').select('*');
    if (error) throw error;

    return data;
};

export { getBusinesses };
