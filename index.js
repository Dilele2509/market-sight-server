'use strict';
import dotenv from 'dotenv';
import express from 'express';
import userRoutes from './src/routes/userRoutes.js';
import dataRoutes from './src/routes/dataRoutes.js';
import cors from 'cors';

dotenv.config();

const { HOST, PORT } = process.env;

const app = express();
app.use(cors({
  origin: 'http://localhost:8080',
  credentials: true // Cho phép gửi cookie (nếu có)
}));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use('/api/', userRoutes);
app.use('/api/', dataRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: 'Internal Server Error',
    message: process.env.NODE_ENV === 'development' ? err.message : 'Something went wrong'
  });
});

// Start the server
app.listen(PORT || 3001, HOST || '0.0.0.0', () => {
  console.log(`App listening on url http://${HOST || '0.0.0.0'}:${PORT || 3001}`);
});
