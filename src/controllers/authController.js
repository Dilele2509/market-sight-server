import jwt from 'jsonwebtoken';
import { checkAccountAvailable, checkTokenEmail } from '../data/authData.js'

const { ACCESS_TOKEN_KEY } = process.env

const login = async (req, res) => {
    //authentication
    const { email, password_hash } = req.body;

    console.log(email, password_hash);

    if (!email) {
        return res.status(400).json({ message: 'Missing email' });
    } else if (!password_hash) {
        return res.status(400).json({ message: 'Missing password' });
    }

    const accountCheckData = await checkAccountAvailable(email, password_hash);

    console.log('check result: ', accountCheckData);
    if (accountCheckData.status !== 200) {
        return res.status(401).json({ message: accountCheckData.data.message });
    }

    //authorization
    const accessToken = jwt.sign(accountCheckData.data, ACCESS_TOKEN_KEY, { expiresIn: '3d' })

    res.json({
        accessToken
    })
}

const authenticationToken = async (req, res, next) => {
    const authenticationHeader = req.headers['authorization'];

    if (!authenticationHeader) {
        return res.status(401).json({ error: 'Unauthorized: No token provided' });
    }

    const token = authenticationHeader.split(' ')[1];

    //console.log("key: ", ACCESS_TOKEN_KEY, "   token: ", token);

    if (!token) {
        return res.status(401).json({ error: 'Missing token' });
    }

    jwt.verify(token, ACCESS_TOKEN_KEY, (err, data) => {
        //console.log('error: ', err, 'data: ', data);
        console.log('in auth token');

        if (err) {
            return res.status(403).json({ error: 'Forbidden: Invalid token' });
        }

        req.user = data; // Lưu thông tin user từ token vào request
        next(); // Chỉ tiếp tục nếu token hợp lệ
    });
};

const verifyEmailToken = async (req, res) => {
    const { token } = req.query;
    try {
        // Xác thực token
        const decoded = jwt.verify(token, process.env.ACCESS_TOKEN_KEY);
        const email = decoded.email;
        console.log('Email:', email);

        // Gọi hàm checkTokenEmail để kiểm tra token và xóa dữ liệu
        const isValidToken = await checkTokenEmail(token);

        if (isValidToken) {
            res.status(200).send('Email verified successfully');
        } else {
            res.status(400).send('Invalid or expired token');
        }
    } catch (error) {
        console.error('Error verifying email:', error);
        res.status(400).send('Invalid or expired token');
    }
};


export { login, authenticationToken, verifyEmailToken };