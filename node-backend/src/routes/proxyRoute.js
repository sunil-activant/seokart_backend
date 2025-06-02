// Add this to your AWS server's Express app (e.g., in your routes file or app.js)

const express = require('express');
const { handleProxyRotateRequest } = require('../services/scraperService.js'); // Adjust path accordingly

const router = express.Router();

// Route for handling proxy rotation requests
router.post('/', async (req, res) => {
  try {
    await handleProxyRotateRequest(req, res);
  } catch (error) {
    console.error('Error in proxy_rotate route:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message
    });
  }
});

// Health check route for monitoring
router.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: 'aws_proxy_rotation'
  });
});

module.exports = router;

// If you're adding directly to app.js, use:
// app.post('/api/proxy_rotate', handleProxyRotateRequest);
// app.get('/api/proxy_rotate/health', (req, res) => {
//   res.json({
//     status: 'healthy',
//     timestamp: new Date().toISOString(),
//     service: 'aws_proxy_rotation'
//   });
// });