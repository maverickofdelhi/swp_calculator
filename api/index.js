const serverless = require('serverless-http');
const { app } = require('../swp_server');

module.exports = serverless(app);