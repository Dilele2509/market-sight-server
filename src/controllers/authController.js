import jwt from 'jsonwebtoken';
import { checkAccountAvailable } from '../data/authData.js'

const { ACCESS_TOKEN_KEY } = process.env

const login = async (req, res) => {
    //authentication
    const { email, password_hash } = req.body;

    //console.log(email, password_hash);
    console.log(ACCESOKEN_KEY);

    if (!email) {
        return res.status(400).json({ error: 'Missing email' });
    } else if (!password_hash) {
        return res.status(400).json({ error: 'Missing password' });
    }

    const accountCheckData = await checkAccountAvailable(email, password_hash);

    console.log('check result: ', accountCheckData);
    if (accountCheckData.status !== 200) {
        return res.status(401).json({ error: 'Invalid credentials' });
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

    console.log("key: ", ACCESS_TOKEN_KEY, "   token: ", token);

    if (!token) {
        return res.status(401).json({ error: 'Missing token' });
    }

    jwt.verify(token, ACCESS_TOKEN_KEY, (err, data) => {
        console.log('error: ', err, 'data: ', data);

        if (err) {
            return res.status(403).json({ error: 'Forbidden: Invalid token' });
        }

        req.user = data; // Lưu thông tin user từ token vào request
        next(); // Chỉ tiếp tục nếu token hợp lệ
    });
};

const logout = async (req, res) => {
    const accessToken = req.body.token;
    accessToken = accessToken.filter(accToken => accToken !== accessToken)

    res.status(200).json({message: 'logout successful'})
};


export { login, authenticationToken, logout };