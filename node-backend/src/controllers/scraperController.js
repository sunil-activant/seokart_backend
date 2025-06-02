const axios = require("axios");
const Sitemap = require("../models/Sitemap");
const UserActivity = require("../models/UserActivity");
const Webpages = require("../models/Webpages");
const mongoose = require("mongoose");
const {validateWebsite , calculateTimeRemaining , processAllSitemapsAndWebpages} = require("../services/scraperService")
const { getBacklinkSummary } = require("../services/backlinkService");
const socketService = require("../services/socketService");

const handleSitemapCrawl = async (req, res) => {
  try {
    const { websiteUrl, concurrency = 5, forceRecrawl = false } = req.body;
    const userId = req.user.id;

    console.log(`Crawl request from user ${userId} for website: ${websiteUrl}`);
    
    // Validate website and check for sitemaps
    const validation = await validateWebsite(websiteUrl);
    
    if (!validation.isValid) {
      return res.status(400).json({
        success: false,
        message: validation.message
      });
    }
    
    // Use the new findOrCreateActivity method
    const { activity, isNewCrawl, canCrawl } = await UserActivity.findOrCreateActivity(userId, websiteUrl);
    
    if (!canCrawl && !forceRecrawl) {
      // Emit current activity status via socket
      socketService.emitActivityUpdate(userId, {
        activityId: activity._id,
        websiteUrl: activity.websiteUrl,
        status: activity.status,
        progress: activity.progress || 0,
        isSitemapCrawling: activity.isSitemapCrawling,
        isWebpageCrawling: activity.isWebpageCrawling,
        isBacklinkFetching: activity.isBacklinkFetching,
        sitemapCount: activity.sitemapCount || 0,
        webpageCount: activity.webpageCount || 0,
        webpagesSuccessful: activity.webpagesSuccessful || 0,
        webpagesFailed: activity.webpagesFailed || 0,
        estimatedTimeRemaining: calculateTimeRemaining(activity),
        message: "Crawl already in progress"
      });

      return res.status(200).json({
        success: true,
        message: "A crawl is already in progress for this website",
        status: activity.status,
        activityId: activity._id,
        progress: activity.progress,
        currentCrawlStarted: activity.lastCrawlStarted,
        crawlCount: activity.crawlCount,
        isCurrentlyCrawling: true,
        sitemapCrawling: activity.isSitemapCrawling === 1,
        webpageCrawling: activity.isWebpageCrawling === 1,
        backlinkFetching: activity.isBacklinkFetching === 1
      });
    }

    // Update activity with latest crawl information
    activity.isSitemapCrawling = 1;
    activity.isWebpageCrawling = 1;
    activity.isBacklinkFetching = 0;
    activity.startTime = isNewCrawl ? new Date() : activity.startTime;
    activity.lastCrawlStarted = new Date();
    activity.status = "processing";
    activity.progress = 0;
    activity.sitemapCount = validation.sitemapUrls.length;
    activity.webpageCount = 0;
    activity.webpagesSuccessful = 0;
    activity.webpagesFailed = 0;
    activity.estimatedTimeRemaining = 0;
    activity.estimatedTotalUrls = 0;
    activity.errorMessages = [];
    activity.crawlCount = (activity.crawlCount || 0) + (isNewCrawl ? 1 : 0);
    
    // If force recrawl is requested
    if (forceRecrawl && !canCrawl) {
      console.log(`Force recrawl requested for ${websiteUrl}, resetting active crawl`);
      activity.crawlCount = (activity.crawlCount || 0) + 1;
    }
    
    // IMPORTANT: Save the activity first
    await activity.save();
    console.log(`✅ Activity saved for user ${userId}, activity ${activity._id}`);

    // Emit immediate crawl started notification
    socketService.emitCrawlStarted(userId, {
      activityId: activity._id,
      websiteUrl,
      status: "processing",
      sitemapCount: validation.sitemapUrls.length,
      crawlCount: activity.crawlCount,
      isNewCrawl,
      message: isNewCrawl ? "New crawl started" : "Crawl restarted"
    });

    // Check if we have backlink data for this website
    let backlinkSummary = null;
    try {
      const backlinkResult = await getBacklinkSummary(websiteUrl, userId);
      if (backlinkResult.success) {
        backlinkSummary = backlinkResult.data;
      }
    } catch (error) {
      console.warn(`Could not retrieve existing backlink summary: ${error.message}`);
    }
    
    // Start background processing (don't await)
    processAllSitemapsAndWebpages(validation.sitemapUrls, userId, websiteUrl, { 
      sitemapConcurrency: concurrency,
      webpageConcurrency: concurrency 
    })
      .then(async result => {
        console.log("✅ Background sitemap and webpage processing completed:", {
          userId,
          websiteUrl,
          totalSitemaps: result.totalSitemaps,
          savedPages: result.savedPages,
          failedPages: result.failedPages,
          success: result.success
        });

        // Emit final completion to update activities list
        const updatedActivities = await UserActivity.find({ userId }).sort({ lastCrawlStarted: -1 });
        socketService.emitUserActivitiesUpdate(userId, updatedActivities);
      })
      .catch(async error => {
        console.error("❌ Background sitemap and webpage processing failed:", {
          userId,
          websiteUrl,
          error: error.message
        });

        // Emit error notification
        socketService.emitError(userId, {
          websiteUrl,
          message: error.message,
          activityId: activity._id
        });

        // Emit updated activities list
        const updatedActivities = await UserActivity.find({ userId }).sort({ lastCrawlStarted: -1 });
        socketService.emitUserActivitiesUpdate(userId, updatedActivities);
      });
    
    // Respond immediately with status
    return res.status(200).json({
      success: true,
      message: isNewCrawl 
        ? "Sitemap and webpage crawling started for the first time" 
        : "Sitemap and webpage crawling restarted",
      sitemapCount: validation.sitemapUrls.length,
      activityId: activity._id,
      status: "processing",
      crawlCount: activity.crawlCount,
      lastCrawlStarted: activity.lastCrawlStarted,
      isNewCrawl,
      backlinkSummary: backlinkSummary ? {
        hasBacklinkData: true,
        backlinks: backlinkSummary.backlinks,
        refdomains: backlinkSummary.refdomains,
        lastFetched: backlinkSummary.lastFetched,
        dominatingAnchor: backlinkSummary.dominatingAnchor
      } : {
        hasBacklinkData: false,
        message: "Backlink data will be fetched during crawling"
      }
    });
  } catch (error) {
    console.error("Error in handleSitemapCrawl:", error);
    
    // Try to update activity status to failed if we have the info
    try {
      if (req.body.websiteUrl && req.user?.id) {
        const failedActivity = await UserActivity.findOne({
          userId: req.user.id,
          websiteUrl: req.body.websiteUrl,
          status: "processing"
        });
        
        if (failedActivity) {
          await failedActivity.completeCrawl(false, error.message);

          // Emit error via socket
          socketService.emitError(req.user.id, {
            websiteUrl: req.body.websiteUrl,
            message: error.message,
            activityId: failedActivity._id
          });
        }
      }
    } catch (updateError) {
      console.error("Failed to update activity status on error:", updateError);
    }
    
    return res.status(500).json({
      success: false,
      message: "An error occurred while processing the request",
      error: error.message
    });
  }
};

const handleGetBacklinkSummary = async (req, res) => {
  try {
    const { websiteUrl } = req.params;
    const userId = req.user.id;
    
    if (!websiteUrl) {
      return res.status(400).json({
        success: false,
        message: "Website URL is required"
      });
    }
    
    const result = await getBacklinkSummary(decodeURIComponent(websiteUrl), userId);
    
    if (!result.success) {
      return res.status(404).json(result);
    }
    
    return res.status(200).json({
      success: true,
      data: result.data
    });
  } catch (error) {
    console.error("Error in handleGetBacklinkSummary:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while retrieving backlink summary",
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

    const responseData = {
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
    };

    return res.status(200).json(responseData);
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

    const userActivities = await UserActivity.find({ userId }).sort({ lastCrawlStarted: -1 });

    if (!userActivities || userActivities.length === 0) {
      return res.status(404).json({
        success: false,
        message: "No activities found for this user"
      });
    }  

    const responseData = {
      success: true,
      count: userActivities.length,
      data: userActivities
    };

    return res.status(200).json(responseData);

  } catch (error) {
    console.error('Error fetching user activities:', error);
    return res.status(500).json({
      success: false,
      message: 'Server error',
      error: error.message
    });
  }
};

// New function to initialize socket service when the module loads
const initializeSocket = (io) => {
  socketService.init(io);
  console.log('🔌 Socket service initialized in scraper controller');
};

module.exports = { 
  handleSitemapCrawl,
  checkCrawlStatus,
  getUserActivities,
  handleGetBacklinkSummary,
  initializeSocket
};