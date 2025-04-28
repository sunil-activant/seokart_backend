// routes/webpageRoutes.js
const express = require('express');
const router = express.Router();
const { 
  getPaginatedWebpages, 
  getWebpageById, 
  getWebpageStats 
} = require('../controllers/webpageController');
const { auth } = require('../middleware/authMiddleware');

router.use(auth);

// Get paginated webpages for a website
router.get('/:activityId', getPaginatedWebpages);

// Get webpage statistics for a website
router.get('/:websiteUrl/stats', getWebpageStats);

// Get a single webpage by ID
router.get('/detail/:id', getWebpageById);

module.exports = router;