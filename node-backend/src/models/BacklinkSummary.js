const mongoose = require("mongoose");

const BacklinkSummarySchema = new mongoose.Schema(
  {
    userId: { 
      type: mongoose.Schema.Types.ObjectId, 
      ref: "User", 
      required: true,
      index: true 
    },
    userActivityId: { 
      type: mongoose.Schema.Types.ObjectId, 
      ref: "UserActivity", 
      required: true 
    },
    websiteUrl: { 
      type: String, 
      required: true, 
      index: true,
      trim: true 
    },
    
    // Main backlink metrics
    backlinks: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    refdomains: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    subnets: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    ips: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    nofollow_backlinks: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    dofollow_backlinks: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    inlink_rank: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    anchors: { 
      type: Number, 
      default: 0,
      min: 0 
    },
    
    // Top anchors by backlinks
    top_anchors_by_backlinks: [{
      anchor: { 
        type: String, 
        required: true,
        maxlength: 200 
      },
      backlinks: { 
        type: Number, 
        required: true,
        min: 0 
      }
    }],
    
    // Top pages by backlinks
    top_pages_by_backlinks: [{
      url: { 
        type: String, 
        required: true,
        maxlength: 500 
      },
      backlinks: { 
        type: Number, 
        required: true,
        min: 0 
      }
    }],
    
    // Top pages by referring domains
    top_pages_by_refdomains: [{
      url: { 
        type: String, 
        required: true,
        maxlength: 500 
      },
      refdomains: { 
        type: Number, 
        required: true,
        min: 0 
      }
    }],
    
    // API response metadata
    lastFetched: { 
      type: Date, 
      default: Date.now,
      index: true 
    },
    apiResponseTime: { 
      type: Number,
      min: 0 
    }, // Response time in milliseconds
    apiStatus: { 
      type: String, 
      enum: ["success", "failed", "partial"], 
      default: "success" 
    },
    errorMessage: { 
      type: String,
      maxlength: 1000 
    },
    
    // Calculated metrics
    dominatingAnchor: { 
      type: String,
      maxlength: 200 
    }, // Most used anchor text
    dominatingPage: { 
      type: String,
      maxlength: 500 
    }, // Page with most backlinks
    backlinkDiversity: { 
      type: Number,
      min: 0,
      max: 1 
    }, // Calculated diversity score (0-1)
  },
  { 
    timestamps: true,
    // Compound indexes for better performance
    indexes: [
      { userId: 1, websiteUrl: 1 },
      { userActivityId: 1 },
      { websiteUrl: 1 },
      { lastFetched: -1 },
      { userId: 1, lastFetched: -1 }
    ]
  }
);

// Virtual for checking if data is fresh (within cache duration)
BacklinkSummarySchema.virtual('isFresh').get(function() {
  const cacheDuration = (process.env.BACKLINK_CACHE_DURATION_DAYS || 7) * 24 * 60 * 60 * 1000;
  return this.lastFetched && (Date.now() - this.lastFetched.getTime()) < cacheDuration;
});

// Virtual for data age in hours
BacklinkSummarySchema.virtual('ageInHours').get(function() {
  if (!this.lastFetched) return null;
  return Math.round((Date.now() - this.lastFetched.getTime()) / (1000 * 60 * 60));
});

// Calculate dominating anchor and page before saving
BacklinkSummarySchema.pre('save', function(next) {
  try {
    // Find dominating anchor (most used)
    if (this.top_anchors_by_backlinks && this.top_anchors_by_backlinks.length > 0) {
      this.dominatingAnchor = this.top_anchors_by_backlinks[0].anchor;
    }
    
    // Find dominating page (most backlinks)
    if (this.top_pages_by_backlinks && this.top_pages_by_backlinks.length > 0) {
      this.dominatingPage = this.top_pages_by_backlinks[0].url;
    }
    
    // Calculate backlink diversity (referring domains / total backlinks)
    if (this.backlinks > 0 && this.refdomains > 0) {
      this.backlinkDiversity = Math.round((this.refdomains / this.backlinks) * 100) / 100;
    } else {
      this.backlinkDiversity = 0;
    }
    
    next();
  } catch (error) {
    next(error);
  }
});

// Static method to find or create backlink summary
BacklinkSummarySchema.statics.findOrCreate = async function(websiteUrl, userId, userActivityId) {
  try {
    let summary = await this.findOne({ websiteUrl, userId });
    
    if (!summary) {
      summary = new this({
        userId,
        userActivityId,
        websiteUrl,
        lastFetched: new Date()
      });
    } else {
      // Update the userActivityId for the current crawl
      summary.userActivityId = userActivityId;
    }
    
    return summary;
  } catch (error) {
    throw new Error(`Error finding or creating backlink summary: ${error.message}`);
  }
};

// Static method to get fresh data or null if cache expired
BacklinkSummarySchema.statics.getFreshData = async function(websiteUrl, userId) {
  try {
    const summary = await this.findOne({ websiteUrl, userId });
    
    if (!summary) return null;
    
    const cacheDuration = (process.env.BACKLINK_CACHE_DURATION_DAYS || 7) * 24 * 60 * 60 * 1000;
    const isExpired = !summary.lastFetched || 
                     (Date.now() - summary.lastFetched.getTime()) > cacheDuration;
    
    return isExpired ? null : summary;
  } catch (error) {
    throw new Error(`Error getting fresh backlink data: ${error.message}`);
  }
};

// Instance method to check if refresh is needed
BacklinkSummarySchema.methods.needsRefresh = function() {
  const cacheDuration = (process.env.BACKLINK_CACHE_DURATION_DAYS || 7) * 24 * 60 * 60 * 1000;
  return !this.lastFetched || (Date.now() - this.lastFetched.getTime()) > cacheDuration;
};

// Instance method to get summary statistics
BacklinkSummarySchema.methods.getSummaryStats = function() {
  return {
    totalBacklinks: this.backlinks,
    totalRefdomains: this.refdomains,
    totalAnchors: this.anchors,
    dofollowRatio: this.backlinks > 0 ? 
                   Math.round((this.dofollow_backlinks / this.backlinks) * 100) : 0,
    backlinkDiversity: this.backlinkDiversity,
    dominatingAnchor: this.dominatingAnchor,
    dominatingPage: this.dominatingPage,
    lastUpdated: this.lastFetched,
    ageInHours: this.ageInHours,
    isFresh: this.isFresh
  };
};

// Ensure virtuals are included when converting to JSON
BacklinkSummarySchema.set('toJSON', { virtuals: true });
BacklinkSummarySchema.set('toObject', { virtuals: true });

module.exports = mongoose.model("BacklinkSummary", BacklinkSummarySchema);