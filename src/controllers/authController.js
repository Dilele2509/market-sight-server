import jwt from 'jsonwebtoken';
import {checkAccountAvailable} from '../data/authData.js'

const { ACCESS_TOKEN_KEY } = process.env

const login = async (req, res, next) =>{
    //authentication
    const {email, password_hash} = req.body;

    //console.log(email, password_hash);

    if (!email) {
        return res.status(400).json({ error: 'Missing email' });
    }else if (!password_hash) {
        return res.status(400).json({ error: 'Missing password' });
    }

    const accountCheckData = await checkAccountAvailable(email, password_hash);

    //console.log('check result: ', accountCheckData);
    if (accountCheckData.status !== 200) {
        return res.status(401).json({ error: 'Invalid credentials' });
    }

    //authorization
    const accessToken = jwt.sign(accountCheckData.data, ACCESS_TOKEN_KEY, { expiresIn: '30s'})

    res.json({
        accessToken,
        accountData: accountCheckData.data
    })
}

const authenticationToken = async (req, res, next) => {
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