const axios = require("axios");
const Sitemap = require("../models/Sitemap");
const UserActivity = require("../models/UserActivity");
const Webpages = require("../models/Webpages");
const mongoose = require("mongoose");
const {validateWebsite , calculateTimeRemaining , processAllSitemapsAndWebpages} = require("../services/scraperService")


const handleSitemapCrawl = async (req, res) => {
  try {
    const { websiteUrl, concurrency = 5 } = req.body;
    const userId = req.user.id

    console.log(req.user)
    
    // Validate website and check for sitemaps
    const validation = await validateWebsite(websiteUrl);
    
    if (!validation.isValid) {
      return res.status(400).json({
        success: false,
        message: validation.message
      });
    }
    
    // Check if we already have an active crawl for this website/user
    const existingActivity = await UserActivity.findOne({
      userId,
      websiteUrl,
      $or: [
        { isSitemapCrawling: 1 },
        { isWebpageCrawling: 1 },
        { status: "processing" }
      ]
    });
    
    if (existingActivity) {
      return res.status(200).json({
        success: true,
        message: "A crawl is already in progress for this website",
        status: "processing",
        activityId: existingActivity._id
      });
    }
    
    // Create initial user activity record with zero counts
    const userActivity = await UserActivity.create({
      userId,
      websiteUrl,
      isSitemapCrawling: 1,
      isWebpageCrawling: 1,
      startTime: new Date(),
      status: "processing",
      sitemapCount: 0,
      webpageCount: 0,
      webpagesSuccessful: 0,
      webpagesFailed: 0,
      progress: 0,
      errorMessages: []
    });
    
    // Start background processing (don't await)
    processAllSitemapsAndWebpages(validation.sitemapUrls, userId, websiteUrl, concurrency)
      .then(result => {
        console.log("Background sitemap and webpage processing completed:", result);
      })
      .catch(error => {
        console.error("Background sitemap and webpage processing failed:", error);
      });
    
    // Respond immediately with status
    return res.status(200).json({
      success: true,
      message: "Sitemap and webpage crawling started",
      sitemapCount: validation.sitemapUrls.length,
      activityId: userActivity._id,
      status: "processing"
    });
  } catch (error) {
    console.error("Error in handleSitemapCrawl:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while processing the request",
      error: error.message
    });
  }
};

const checkCrawlStatus = async (req, res) => {
  try {
    const { activityId } = req.params;
    const userId = req.user ? req.user._id : new mongoose.Types.ObjectId('6618dde65a25055d0ff67579');

    const activity = await UserActivity.findOne({
      _id: activityId,
      userId
    });

    if (!activity) {
      return res.status(404).json({
        success: false,
        message: "Activity not found"
      });
    }

    // Return the activity details with progress information
    return res.status(200).json({
      success: true,
      status: activity.status,
      progress: activity.progress || 0,
      isSitemapCrawling: activity.isSitemapCrawling,
      isWebpageCrawling: activity.isWebpageCrawling,
      sitemapCount: activity.sitemapCount || 0,
      webpageCount: activity.webpageCount || 0,
      webpagesSuccessful: activity.webpagesSuccessful || 0,
      webpagesFailed: activity.webpagesFailed || 0,
      startTime: activity.startTime,
      endTime: activity.endTime,
      errorMessages: activity.errorMessages || [],  
      estimatedTimeRemaining: activity.progress < 100 ? calculateTimeRemaining(activity) : 0
    });
  } catch (error) {
    console.error("Error in checkCrawlStatus:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while checking status",
      error: error.message
    });
  }
};


const getUserActivities = async (req, res) => {
  try {
    const userId = req.user.id; 

    const userActivities = await UserActivity.find({ userId });

    if (!userActivities || userActivities.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No activities found for this user"
      });
    }  

    return res.status(200).json({
      success: true,
      count: userActivities.length,
      data: userActivities
    });
  } catch (error) {
    console.error('Error fetching user activities:', error);
    return res.status(500).json({
      success: false,
      message: 'Server error',
      error: error.message
    });
  }
};

module.exports = { 
  handleSitemapCrawl,
  checkCrawlStatus,
  getUserActivities,
};