
const express = require('express');
const scraperController = require('../controllers/scraperController');
const { auth } = require('../middleware/authMiddleware');

const router = express.Router();

router.use(auth);

router.post('/scrape', scraperController.handleSitemapCrawl);
router.get('/status/:activityId', scraperController.checkCrawlStatus); 
router.get('/get-activities', scraperController.getUserActivities);

module.exports = router;