import { checkSegmentExists, saveOrUpdateSegment, getSegmentByUser, updateStatus, insertSegmentCustomersToSupabase } from "../data/segmentData.js";

const allSegmentByUser = async (req, res) => {
    try {
        const { user_id } = req.body;
        const response = await getSegmentByUser(user_id)
        console.log(response);

        if (response) {
            return res.status(response.statusCode).json({
                message: response.message,
                data: response.data
            })
        } else return res.status(401).json({
            message: 'fail to get all segmentation'
        })
    } catch (error) {
        console.error('Error get segment:', error);
        return res.status(500).json({
            message: 'An error occurred while get the segment.'
        });
    }
}

const saveSegment = async (req, res) => {
    try {
        const segment = req.body;

        if (!segment.segment_id || !segment.segment_name) {
            return res.status(400).json({
                message: 'segment_id and segment_name are required.'
            });
        }

        // Call the data layer to save or update the segment
        const result = await saveOrUpdateSegment(segment);
        return res.status(result.statusCode).json({
            message: result.message
        });
    } catch (error) {
        console.error('Error saving segment:', error);
        return res.status(500).json({
            message: 'An error occurred while saving the segment.'
        });
    }
};

const checkSegment = async (req, res) => {
    try {
        const { segment_id } = req.body;
        // Check if segment already exists
        const { exists, created_at } = await checkSegmentExists(segment_id);

        if (exists) {
            return res.status(200).json({
                message: 'segment has already in storage',
                created_at: created_at
            });;
        } else {
            return res.status(200).json({
                message: 'segment has not in storage',
                created_at: created_at
            });;
        }
    } catch (error) {
        return res.status(400).json({
            message: 'has error when checked segment exits'
        })
    }
}

const updateStatusSegment = async (req, res) => {
    try {
        const { segment_id, status } = req.body;

        if (!segment_id || typeof status === 'undefined') {
            return res.status(400).json({ error: "Missing segment_id or status" });
        }

        let newStatus;
        if (status === 'active') newStatus = 'inactive';
        else if (status === 'inactive') newStatus = 'active';
        else newStatus = 'active';

        console.log(newStatus);
        await updateStatus(segment_id, newStatus);

        res.status(200).json({ message: "Segment status updated successfully", newStatus });
    } catch (error) {
        console.error("Update status failed:", error);
        res.status(500).json({ error: "Internal server error" });
    }
};

const deleteSegment = async (req, res) => {
    try {
        const { segment_id } = req.body;
        console.log(segment_id);
        if (!segment_id) {
            return res.status(400).json({ error: "Missing segment_id" });
        }

        const result = await deleteSegmentItem(segment_id);

        if (result.statusCode === 500) {
            return res.status(500).json({ error: result.message });
        }

        return res.status(200).json({ message: "Segment deleted successfully" });
    } catch (error) {
        console.error("Error deleting segment:", error);
        return res.status(500).json({ error: "Internal server error" });
    }
}

const insertSegmentCustomer = async (req, res) => {
    try {
        const { segment_id, data } = req.body;

        const inserted = await insertSegmentCustomersToSupabase(segment_id, data);

        res.status(200).json({
            message: 'Data inserted successfully',
            inserted,
        });
    } catch (err) {
        console.error('Insert segment error:', err.message);
        res.status(500).json({ error: err.message });
    }
};

export {
    saveSegment,
    checkSegment,
    allSegmentByUser,
    updateStatusSegment,
    deleteSegment,
    insertSegmentCustomer
};
