const mongoose = require("mongoose");

const UserActivitySchema = new mongoose.Schema(
  {
    userId: { 
      type: mongoose.Schema.Types.ObjectId, 
      ref: "User", 
      required: true,
      index: true 
    },
    websiteUrl: { 
      type: String, 
      required: true,
      trim: true,
      index: true 
    },
    
    // Crawling status flags
    isSitemapCrawling: { 
      type: Number, 
      enum: [0, 1], 
      default: 1 
    }, // 0 = No, 1 = Yes
    isWebpageCrawling: { 
      type: Number, 
      enum: [0, 1], 
      default: 1 
    }, // 0 = No, 1 = Yes
    isBacklinkFetching: { 
      type: Number, 
      enum: [0, 1], 
      default: 0 
    }, // 0 = No, 1 = Yes (NEW)
    
    // Timing information
    startTime: { type: Date },
    endTime: { type: Date },
    lastCrawlStarted: { type: Date }, // NEW: Track when last crawl started
    lastCrawlCompleted: { type: Date }, // NEW: Track when last crawl completed
    
    // Status tracking
    status: { 
      type: String, 
      enum: ["pending", "processing", "completed", "completed_with_errors", "failed"], 
      default: "pending",
      index: true 
    },
    
    // Progress and statistics
    sitemapCount: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    webpageCount: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    webpagesSuccessful: { 
      type: Number, 
      default: 0,
      min: 0 
    }, // Number of successfully crawled and saved webpages
    webpagesFailed: { 
      type: Number, 
      default: 0,
      min: 0 
    }, // Number of failed webpage crawls
    progress: { 
      type: Number, 
      default: 0,
      min: 0,
      max: 100 
    }, // Overall progress percentage (0-100)
    
    // Error handling
    errorMessages: [{ 
      type: String,
      maxlength: 1000 
    }], // Array of error messages
    
    // Time estimation
    estimatedTimeRemaining: { 
      type: Number, 
      default: 0,
      min: 0 
    }, // Estimated time remaining in seconds
    estimatedTotalUrls: { 
      type: Number, 
      default: 0,
      min: 0 
    }, // Estimated total URLs to process
    
    // NEW: Backlink summary tracking
    backlinkSummaryStatus: { 
      type: String, 
      enum: ["pending", "fetching", "completed", "failed", "skipped"], 
      default: "pending" 
    },
    backlinkSummaryId: { 
      type: mongoose.Schema.Types.ObjectId, 
      ref: "BacklinkSummary" 
    },
    backlinkFetchedAt: { type: Date },
    backlinkError: { 
      type: String,
      maxlength: 500 
    },
    
    // NEW: Crawl tracking to prevent duplicates
    crawlCount: { 
      type: Number, 
      default: 0,
      min: 0 
    }, // Track number of times this website has been crawled
    
    // Session tracking
    sessionId: { 
      type: String,
      index: true 
    }, // For tracking active sessions
    
    // Additional metadata
    userAgent: { type: String },
    ipAddress: { type: String },
    lastUpdated: { type: Date, default: Date.now }
  },
  { 
    timestamps: true,
    // Compound indexes for better performance
    indexes: [
      { userId: 1, websiteUrl: 1 }, // Primary compound index for finding existing activities
      { status: 1, createdAt: -1 },
      { userId: 1, createdAt: -1 },
      { websiteUrl: 1, createdAt: -1 },
      { sessionId: 1 },
      { userId: 1, status: 1 }
    ]
  }
);

// Virtual for calculating duration
UserActivitySchema.virtual('duration').get(function() {
  if (!this.startTime) return null;
  const endTime = this.endTime || new Date();
  return Math.round((endTime - this.startTime) / 1000); // Duration in seconds
});

// Virtual for checking if currently active
UserActivitySchema.virtual('isCurrentlyActive').get(function() {
  return this.isSitemapCrawling === 1 || 
         this.isWebpageCrawling === 1 || 
         this.isBacklinkFetching === 1 ||
         this.status === "processing";
});

// Virtual for success rate
UserActivitySchema.virtual('successRate').get(function() {
  const total = this.webpagesSuccessful + this.webpagesFailed;
  if (total === 0) return 0;
  return Math.round((this.webpagesSuccessful / total) * 100);
});

// Static method to find or update existing activity
UserActivitySchema.statics.findOrCreateActivity = async function(userId, websiteUrl) {
  try {
    let activity = await this.findOne({ userId, websiteUrl });
    
    if (activity) {
      // Check if currently crawling
      const isCurrentlyCrawling = activity.isSitemapCrawling === 1 || 
                                 activity.isWebpageCrawling === 1 || 
                                 activity.isBacklinkFetching === 1 ||
                                 activity.status === "processing";
      
      if (isCurrentlyCrawling) {
        return { activity, isNewCrawl: false, canCrawl: false };
      }
      
      // Update existing activity for new crawl
      activity.isSitemapCrawling = 1;
      activity.isWebpageCrawling = 1;
      activity.isBacklinkFetching = 0;
      activity.startTime = new Date();
      activity.lastCrawlStarted = new Date();
      activity.endTime = undefined;
      activity.status = "processing";
      activity.progress = 0;
      activity.sitemapCount = 0;
      activity.webpageCount = 0;
      activity.webpagesSuccessful = 0;
      activity.webpagesFailed = 0;
      activity.estimatedTimeRemaining = 0;
      activity.estimatedTotalUrls = 0;
      activity.errorMessages = [];
      activity.crawlCount = (activity.crawlCount || 0) + 1;
      activity.lastUpdated = new Date();
      
      // Reset backlink status only if it hasn't been fetched before
      if (!activity.backlinkSummaryId) {
        activity.backlinkSummaryStatus = "pending";
        activity.backlinkError = undefined;
      }
      
      await activity.save();
      return { activity, isNewCrawl: true, canCrawl: true };
    } else {
      // Create new activity
      activity = await this.create({
        userId,
        websiteUrl,
        isSitemapCrawling: 1,
        isWebpageCrawling: 1,
        isBacklinkFetching: 0,
        startTime: new Date(),
        lastCrawlStarted: new Date(),
        status: "processing",
        sitemapCount: 0,
        webpageCount: 0,
        webpagesSuccessful: 0,
        webpagesFailed: 0,
        progress: 0,
        errorMessages: [],
        crawlCount: 1,
        backlinkSummaryStatus: "pending",
        lastUpdated: new Date()
      });
      
      return { activity, isNewCrawl: true, canCrawl: true };
    }
  } catch (error) {
    throw new Error(`Error finding or creating activity: ${error.message}`);
  }
};

// Method to check if crawling is allowed
UserActivitySchema.methods.canStartCrawl = function() {
  const isCurrentlyCrawling = this.isSitemapCrawling === 1 || 
                             this.isWebpageCrawling === 1 || 
                             this.isBacklinkFetching === 1 ||
                             this.status === "processing";
  
  return !isCurrentlyCrawling;
};

// Method to mark crawl as completed
UserActivitySchema.methods.completeCrawl = async function(success = true, errorMessage = null) {
  try {
    this.isSitemapCrawling = 0;
    this.isWebpageCrawling = 0;
    this.isBacklinkFetching = 0;
    this.endTime = new Date();
    this.lastCrawlCompleted = new Date();
    this.progress = 100;
    this.estimatedTimeRemaining = 0;
    this.lastUpdated = new Date();
    
    if (success) {
      this.status = this.errorMessages && this.errorMessages.length > 0 ? "completed_with_errors" : "completed";
    } else {
      this.status = "failed";
      if (errorMessage && !this.errorMessages.includes(errorMessage)) {
        this.errorMessages.push(errorMessage);
      }
    }
    
    return await this.save();
  } catch (error) {
    throw new Error(`Error completing crawl: ${error.message}`);
  }
};

// Method to update progress with validation
UserActivitySchema.methods.updateProgress = async function(updates) {
  try {
    // Validate progress
    if (updates.progress !== undefined) {
      updates.progress = Math.max(0, Math.min(100, updates.progress));
    }
    
    // Update lastUpdated timestamp
    updates.lastUpdated = new Date();
    
    // Apply updates
    Object.assign(this, updates);
    
    return await this.save();
  } catch (error) {
    throw new Error(`Error updating progress: ${error.message}`);
  }
};

// Method to get activity summary
UserActivitySchema.methods.getSummary = function() {
  return {
    id: this._id,
    websiteUrl: this.websiteUrl,
    status: this.status,
    progress: this.progress,
    duration: this.duration,
    successRate: this.successRate,
    crawlCount: this.crawlCount,
    isCurrentlyActive: this.isCurrentlyActive,
    hasBacklinkData: !!this.backlinkSummaryId,
    backlinkStatus: this.backlinkSummaryStatus,
    startTime: this.startTime,
    endTime: this.endTime,
    lastCrawlStarted: this.lastCrawlStarted,
    webpagesProcessed: {
      successful: this.webpagesSuccessful,
      failed: this.webpagesFailed,
      total: this.webpagesSuccessful + this.webpagesFailed
    },
    estimatedTimeRemaining: this.estimatedTimeRemaining
  };
};

// Pre-save middleware to update lastUpdated
UserActivitySchema.pre('save', function(next) {
  this.lastUpdated = new Date();
  next();
});

// Static method to cleanup old completed activities
UserActivitySchema.statics.cleanupOldActivities = async function(daysOld = 30) {
  try {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysOld);

    const result = await this.deleteMany({
      status: { $in: ["completed", "failed"] },
      endTime: { $lt: cutoffDate }
    });

    return { deletedCount: result.deletedCount };
  } catch (error) {
    throw new Error(`Error cleaning up old activities: ${error.message}`);
  }
};

// Static method to get user statistics
UserActivitySchema.statics.getUserStats = async function(userId) {
  try {
    const stats = await this.aggregate([
      { $match: { userId: mongoose.Types.ObjectId(userId) } },
      {
        $group: {
          _id: null,
          totalCrawls: { $sum: 1 },
          totalWebsites: { $addToSet: "$websiteUrl" },
          totalWebpagesProcessed: { $sum: { $add: ["$webpagesSuccessful", "$webpagesFailed"] } },
          totalWebpagesSuccessful: { $sum: "$webpagesSuccessful" },
          totalWebpagesFailed: { $sum: "$webpagesFailed" },
          avgSuccessRate: { 
            $avg: { 
              $cond: [
                { $gt: [{ $add: ["$webpagesSuccessful", "$webpagesFailed"] }, 0] },
                { $multiply: [{ $divide: ["$webpagesSuccessful", { $add: ["$webpagesSuccessful", "$webpagesFailed"] }] }, 100] },
                0
              ]
            }
          }
        }
      },
      {
        $project: {
          totalCrawls: 1,
          totalWebsites: { $size: "$totalWebsites" },
          totalWebpagesProcessed: 1,
          totalWebpagesSuccessful: 1,
          totalWebpagesFailed: 1,
          avgSuccessRate: { $round: ["$avgSuccessRate", 2] }
        }
      }
    ]);

    return stats[0] || {
      totalCrawls: 0,
      totalWebsites: 0,
      totalWebpagesProcessed: 0,
      totalWebpagesSuccessful: 0,
      totalWebpagesFailed: 0,
      avgSuccessRate: 0
    };
  } catch (error) {
    throw new Error(`Error getting user stats: ${error.message}`);
  }
};

// Ensure virtuals are included when converting to JSON
UserActivitySchema.set('toJSON', { virtuals: true });
UserActivitySchema.set('toObject', { virtuals: true });

module.exports = mongoose.model("UserActivity", UserActivitySchema);