'use strict';
import dotenv from 'dotenv';
import express from 'express';
import userRoutes from './src/routes/userRoutes.js';

dotenv.config();

const { HOST, PORT } = process.env;

const app = express();
app.use(express.json());

app.use('/api/', userRoutes);

// Start the server
app.listen(PORT || 3001, HOST || '0.0.0.0', () => {
  console.log(`App listening on url http://${HOST || '0.0.0.0'}:${PORT || 3001}`);
});
