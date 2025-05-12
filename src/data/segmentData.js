import { supabase } from '../../config.js';

const getSegmentByUser = async (user_id) => {
    const { data, error } = await supabase
        .from('segmentation')
        .select('*')
        .eq('created_by_user_id', user_id)

    if (error) {
        return ({
            statusCode: 401,
            message: 'Failed to get segment list',
            data: error
        })
    } else return ({
        statusCode: 200,
        message: 'Loaded segment list successful',
        data: data
    })
}

const checkSegmentExists = async (segment_id) => {
    try {
        const { data, error } = await supabase
            .from('segmentation')
            .select('segment_id, created_at')  // Lấy thêm created_at
            .eq('segment_id', segment_id)
            .maybeSingle();

        if (error) {
            //console.error('Error checking segment existence:', error);
            return { exists: false };
        }

        if (data) {
            return { exists: true, created_at: data.created_at };
        } else {
            return { exists: false, created_at: null };
        }
    } catch (error) {
        console.error('Error checking segment existence:', error);
        return { exists: false };
    }
};

const saveOrUpdateSegment = async (segment) => {
    try {
        const { exists } = await checkSegmentExists(segment.segment_id);

        if (exists) {
            // Update the segment
            return await updateSegment(segment);
        } else {
            // Save new segment
            return await insertSegment(segment);
        }
    } catch (error) {
        console.error('Error saving or updating segment:', error);
        return { statusCode: 500, message: 'Failed to save or update segment.' };
    }
};

// Update the segment in Supabase
const updateSegment = async (segment) => {
    try {
        const { error } = await supabase
            .from('segmentation')
            .update({
                segment_name: segment.segment_name,
                created_by_user_id: segment.created_by_user_id,
                dataset: segment.dataset,
                description: segment.description,
                created_at: segment.created_at,
                updated_at: new Date().toISOString(),
                status: segment.status,
                filter_criteria: segment.filter_criteria,
            })
            .eq('segment_id', segment.segment_id);

        if (error) {
            console.error('Error updating segment:', error);
            return { statusCode: 500, message: 'Failed to update segment.' };
        }

        return { statusCode: 200, message: 'Segment updated successfully.' };
    } catch (error) {
        console.error('Error updating segment:', error);
        return { statusCode: 500, message: 'Failed to update segment.' };
    }
};

// Insert the segment into Supabase
const insertSegment = async (segment) => {
    //console.log('show segment req:', JSON.stringify(segment, null, 2));

    try {
        const { data, error } = await supabase
            .from('segmentation')
            .insert([
                {
                    segment_id: segment.segment_id,
                    segment_name: segment.segment_name,
                    created_by_user_id: segment.created_by_user_id,
                    dataset: segment.dataset,
                    description: segment.description,
                    created_at: segment.created_at,
                    updated_at: new Date().toISOString(),
                    status: segment.status,
                    filter_criteria: segment.filter_criteria,
                }
            ]);

        console.log(error);

        if (error) {
            console.error('Error inserting segment:', error);
            return {
                statusCode: 500,
                message: 'Failed to insert segment.',
                error: error
            };
        }

        return {
            statusCode: 201,
            message: 'Segment created successfully.',
            insert_data: data
        };
    } catch (error) {
        console.error('Error inserting segment:', error);
        return { statusCode: 500, message: 'Failed to insert segment.' };
    }
};

const updateStatus = async (segment_id, status) => {
    try {
        const { error } = await supabase
            .from('segmentation')
            .update({ status: status })
            .eq('segment_id', segment_id);

        if (error) throw error;
    } catch (error) {
        console.error('Error updating segment:', error);
        return { statusCode: 500, message: 'Failed to update segment.' };
    }
}

const deleteSegmentItem = async (segment_id) => {
    try {
        const { error } = await supabase
            .from('segmentation')
            .delete()
            .eq('segment_id', segment_id);

        if (error) throw error;

        return { statusCode: 200, message: 'Segment deleted successfully' };
    } catch (error) {
        console.error('Error deleting segment:', error);
        return { statusCode: 500, message: 'Failed to delete segment.' };
    }
}

const insertSegmentCustomersToSupabase = async (segment_id, data) => {
    if (!Array.isArray(data) || data.length === 0) {
        throw new Error('Invalid or empty data array');
    }

    const customerIds = data.map(item => item.customer_id);

    // 1. Get existing records
    const { data: existingRecords, error: fetchError } = await supabase
        .from('segment_customers')
        .select('customer_id')
        .eq('segment_id', segment_id)
        .in('customer_id', customerIds);

    if (fetchError) {
        throw new Error(fetchError.message);
    }

    const existingCustomerIds = new Set((existingRecords || []).map(rec => rec.customer_id));

    // 2. Filter out already existing customer_ids
    const newEntries = data.filter(item => !existingCustomerIds.has(item.customer_id));

    if (newEntries.length === 0) {
        return []; // All already existed
    }

    const assigned_at = new Date().toISOString();
    const insertPayload = newEntries.map(item => ({
        segment_id,
        customer_id: item.customer_id,
        assigned_at,
    }));

    const { data: inserted, error: insertError } = await supabase
        .from('segment_customers')
        .insert(insertPayload);

    if (insertError) {
        throw new Error(insertError.message);
    }

    return inserted;
};

export {
    saveOrUpdateSegment,
    checkSegmentExists,
    getSegmentByUser,
    updateStatus,
    deleteSegmentItem,
    insertSegmentCustomersToSupabase
};
