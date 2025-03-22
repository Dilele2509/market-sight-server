'use strict';
import dotenv from 'dotenv';
import express from 'express';
import authRoutes from './src/routes/authRoutes.js';

dotenv.config();

const { HOST, AUTH_PORT } = process.env;

const app = express();
app.use(express.json());

app.use('/api/', authRoutes);

// Start the server
app.listen(AUTH_PORT || 3002, HOST || '0.0.0.0', () => {
  console.log(`App listening on url http://${HOST || '0.0.0.0'}:${AUTH_PORT || 3002}`);
});
