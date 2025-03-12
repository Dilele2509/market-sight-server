'use strict';
const dotenv = require('dotenv');
const express = require('express');
const config = require('./config.js');

dotenv.config();

const { HOST, PORT } = process.env;

const users = require('./src/routes/userRoutes'); 
const app = express();

app.use(express.json());

app.use('/api/', users);
// Start the server
app.listen(PORT, HOST || '0.0.0.0', () => {
  console.log(`App listening on url http://${HOST || '0.0.0.0'}:${PORT}`);
});

