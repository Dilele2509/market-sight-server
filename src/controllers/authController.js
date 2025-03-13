import jwt from 'jsonwebtoken';
import { checkAccountAvailable } from '../data/authData';

const { ACCESS_TOKEN_KEY } = process.env

const login = async (res, req, next) =>{
    //authentication
    const {email, password} = res.body;

    if (!email) {
        return res.status(400).json({ error: 'Missing email' });
    }else if (!password) {
        return res.status(400).json({ error: 'Missing password' });
    }

    const accountCheckData = await checkAccountAvailable(email, password);
    if (accountCheckData.status !== 200) {
        return res.status(401).json({ error: 'Invalid credentials' });
    }

    //authorization
    const accessToken = jwt.sign(accountCheckData, ACCESS_TOKEN_KEY, { expiresIn: '30s'})

    res.json(accessToken)
}

const authenticationToken = async (res, req, next) => {
    const authenticationHeader = req.headers['authorization'];
    //'Bearer [token]'
    const token = authenticationHeader.split(' ')[1];

    if (!token) return res.status(401).json({ error: 'Missing token' });

    jwt.verify(token, ACCESS_TOKEN_KEY, (err, data) => {
        console.log(err, data);
        if(err) res.status(403, err);

        next();
    })
}

export { login, authenticationToken }