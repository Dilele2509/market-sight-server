import { checkSegmentExists, saveOrUpdateSegment, getSegmentByUser } from "../data/segmentData.js";

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

export {
    saveSegment,
    checkSegment,
    allSegmentByUser
};
