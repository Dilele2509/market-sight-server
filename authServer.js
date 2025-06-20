'use strict';
import dotenv from 'dotenv';
import express from 'express';
import cors from 'cors';
import authRoutes from './src/routes/authRoutes.js';

dotenv.config();

const { HOST, AUTH_PORT } = process.env;

const app = express();

const corsOptions = {
  origin: [
    'http://retailsight.pantech.vn',         
    'http://retailsight.pantech.vn:80',
    'http://retailsight.pantech.vn:8080',
    'http://retailsight.pantech.vn:81'
  ],
  credentials: true,               
};

app.use(cors(corsOptions));

app.use(express.json());

app.use('/api/', authRoutes);

// Start the server
app.listen(AUTH_PORT || 5500, HOST || '0.0.0.0', () => {
  console.log(`App listening on url http://${HOST || '0.0.0.0'}:${AUTH_PORT || 5500}`);
});
