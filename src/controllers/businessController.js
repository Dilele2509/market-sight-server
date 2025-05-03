import { getBusinesses } from "../data/businessData.js";

const getAllBusinesses = async (req, res) => {
    try {
        const businesses = await getBusinesses();
        res.status(200).json(businesses);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Failed to get businesses' });
    }
}

const addBusiness = async (req, res) => {
    try {
        console.log(req);
        res.status(200)
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Failed to add business data' });
    }
}

export { getAllBusinesses, addBusiness }