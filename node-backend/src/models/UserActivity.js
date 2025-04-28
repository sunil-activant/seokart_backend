const mongoose = require("mongoose");

const UserActivitySchema = new mongoose.Schema(
  {
    userId: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true },
    websiteUrl: { type: String, required: true },
    isSitemapCrawling: { type: Number, enum: [0, 1], default: 1 }, // 0 = No, 1 = Yes
    isWebpageCrawling: { type: Number, enum: [0, 1], default: 1 }, // 0 = No, 1 = Yes
    startTime: { type: Date },
    endTime: { type: Date },
    status: { 
      type: String, 
      enum: ["pending", "processing", "completed", "completed_with_errors", "failed"], 
      default: "pending" 
    },
    sitemapCount: { type: Number, default: 0 },
    webpageCount: { type: Number, default: 0 },
    webpagesSuccessful: { type: Number, default: 0 }, // Number of successfully crawled and saved webpages
    webpagesFailed: { type: Number, default: 0 }, // Number of failed webpage crawls
    progress: { type: Number, default: 0 }, // Overall progress percentage (0-100)
    errorMessages: [{ type: String }], // Renamed from 'errors' to 'errorMessages'
    estimatedTimeRemaining: { type: Number, default: 0 }, // Estimated time remaining in seconds
    estimatedTotalUrls: { type: Number, default: 0 } // Estimated total URLs to process
  },
  { timestamps: true }
);

module.exports = mongoose.model("UserActivity", UserActivitySchema);