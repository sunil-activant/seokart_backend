const axios = require("axios");
const BacklinkSummary = require("../models/BacklinkSummary"); // Adjust path as needed
const UserActivity = require("../models/UserActivity"); // Adjust path as needed

/**
 * Configuration for SEO PowerSuite backlink API
 */
const BACKLINK_API_CONFIG = {
  ENDPOINT:
    process.env.BACKLINK_API_ENDPOINT ||
    "https://api.seopowersuite.com/backlinks/v1.0/get-summary",
  API_KEY:
    process.env.BACKLINK_API_KEY || "125ee6fe-ba8b-8137-31d0-12ef83093d04",
  TIMEOUT: 30000,
  RETRY_ATTEMPTS: 3,
  RETRY_DELAY: 2000,
  CACHE_DURATION_DAYS: parseInt(process.env.BACKLINK_CACHE_DURATION_DAYS) || 7,
  MODE: "url",
  OUTPUT: "json",
};

/**
 * Enhanced logger for backlink service
 */
const backlinkLogger = {
  info: (message, userId = null) =>
    console.log(
      `[BACKLINK-INFO]${userId ? ` [User:${userId}]` : ""} ${message}`
    ),
  warn: (message, userId = null) =>
    console.warn(
      `[BACKLINK-WARN]${userId ? ` [User:${userId}]` : ""} ⚠️ ${message}`
    ),
  error: (message, error, userId = null) => {
    const errorMsg = error instanceof Error ? error.message : error;
    console.error(
      `[BACKLINK-ERROR]${userId ? ` [User:${userId}]` : ""} ❌ ${message}`,
      errorMsg
    );
  },
  debug: (message, userId = null) => {
    if (process.env.NODE_ENV === "development") {
      console.log(
        `[BACKLINK-DEBUG]${userId ? ` [User:${userId}]` : ""} ${message}`
      );
    }
  },
};

/**
 * Normalize URL for SEO PowerSuite API
 */
function normalizeUrlForBacklinkAPI(websiteUrl) {
  try {
    let normalizedUrl = websiteUrl.trim();

    if (
      !normalizedUrl.startsWith("http://") &&
      !normalizedUrl.startsWith("https://")
    ) {
      normalizedUrl = "https://" + normalizedUrl;
    }

    const url = new URL(normalizedUrl);

    // Keep trailing slash for root domains
    if (url.pathname === "" || url.pathname === "/") {
      normalizedUrl = url.origin + "/";
    } else {
      normalizedUrl = url.href;
    }

    return normalizedUrl;
  } catch (error) {
    backlinkLogger.error(`Error normalizing URL ${websiteUrl}:`, error);

    let fallbackUrl = websiteUrl.trim();
    if (
      !fallbackUrl.startsWith("http://") &&
      !fallbackUrl.startsWith("https://")
    ) {
      fallbackUrl = "https://" + fallbackUrl;
    }

    return fallbackUrl;
  }
}

/**
 * Validate and sanitize SEO PowerSuite API response
 * UPDATED: Now filters out empty/null anchors and URLs to prevent validation errors
 */
function validateBacklinkResponse(data, userId = null) {
  const safeNumber = (value) => {
    const num = parseInt(value) || 0;
    return isNaN(num) ? 0 : num;
  };

  const safeArray = (value, maxItems = 10) => {
    if (!Array.isArray(value)) return [];
    return value.slice(0, maxItems);
  };

  // Helper function to validate and clean anchor data
  const validateAnchors = (anchors) => {
    const validAnchors = safeArray(anchors)
      .map((item) => {
        const anchor = String(item.anchor || "").trim();
        if (!anchor || anchor.length === 0) {
          backlinkLogger.debug(
            `Skipping empty anchor item: ${JSON.stringify(item)}`,
            userId
          );
          return null; // Mark invalid items for removal
        }
        return {
          anchor: anchor.substring(0, 200),
          backlinks: safeNumber(item.backlinks),
        };
      })
      .filter((item) => item !== null); // Remove invalid items

    const originalCount = safeArray(anchors).length;
    const validCount = validAnchors.length;
    if (originalCount !== validCount) {
      backlinkLogger.debug(
        `Filtered anchors: ${originalCount} -> ${validCount} (removed ${
          originalCount - validCount
        } empty anchors)`,
        userId
      );
    }

    return validAnchors;
  };

  // Helper function to validate and clean page data
  const validatePages = (pages, urlKey, valueKey) => {
    const validPages = safeArray(pages)
      .map((item) => {
        const url = String(item[urlKey] || "").trim();
        if (!url || url.length === 0) {
          backlinkLogger.debug(
            `Skipping empty URL item: ${JSON.stringify(item)}`,
            userId
          );
          return null; // Mark invalid items for removal
        }
        return {
          [urlKey]: url.substring(0, 500),
          [valueKey]: safeNumber(item[valueKey]),
        };
      })
      .filter((item) => item !== null); // Remove invalid items

    const originalCount = safeArray(pages).length;
    const validCount = validPages.length;
    if (originalCount !== validCount) {
      backlinkLogger.debug(
        `Filtered pages (${valueKey}): ${originalCount} -> ${validCount} (removed ${
          originalCount - validCount
        } empty URLs)`,
        userId
      );
    }

    return validPages;
  };

  return {
    backlinks: safeNumber(data.backlinks),
    refdomains: safeNumber(data.refdomains),
    subnets: safeNumber(data.subnets),
    ips: safeNumber(data.ips),
    nofollow_backlinks: safeNumber(data.nofollow_backlinks),
    dofollow_backlinks: safeNumber(data.dofollow_backlinks),
    inlink_rank: safeNumber(data.inlink_rank),
    anchors: safeNumber(data.anchors),

    // Validate and filter anchors to remove empty ones
    top_anchors_by_backlinks: validateAnchors(data.top_anchors_by_backlinks),

    // Validate and filter pages to remove empty URLs
    top_pages_by_backlinks: validatePages(
      data.top_pages_by_backlinks,
      "url",
      "backlinks"
    ),

    // Validate and filter pages by refdomains
    top_pages_by_refdomains: validatePages(
      data.top_pages_by_refdomains,
      "url",
      "refdomains"
    ),
  };
}

/**
 * Fetch backlink summary from SEO PowerSuite API
 */
async function fetchBacklinkSummaryFromAPI(
  websiteUrl,
  userId = null,
  retryCount = 0
) {
  const startTime = Date.now();

  try {
    const normalizedTarget = normalizeUrlForBacklinkAPI(websiteUrl);

    backlinkLogger.info(
      `Fetching backlink summary for target: ${normalizedTarget}`,
      userId
    );

    const params = {
      apikey: BACKLINK_API_CONFIG.API_KEY,
      target: normalizedTarget,
      mode: BACKLINK_API_CONFIG.MODE,
      output: BACKLINK_API_CONFIG.OUTPUT,
    };

    const requestConfig = {
      method: "GET",
      url: BACKLINK_API_CONFIG.ENDPOINT,
      params: params,
      timeout: BACKLINK_API_CONFIG.TIMEOUT,
      headers: {
        "User-Agent": "SEOKart-Backlink-Fetcher/1.0",
      },
    };

    backlinkLogger.debug(
      `Making request to: ${BACKLINK_API_CONFIG.ENDPOINT}`,
      userId
    );
    backlinkLogger.debug(`Request params: ${JSON.stringify(params)}`, userId);

    const response = await axios(requestConfig);
    const responseTime = Date.now() - startTime;

    if (!response.data) {
      throw new Error("Empty response from SEO PowerSuite API");
    }

    backlinkLogger.debug(
      `Raw API response: ${JSON.stringify(response.data)}`,
      userId
    );

    // Handle SEO PowerSuite specific response format
    let backlinkData = response.data;

    // Check if response contains error
    if (backlinkData.error) {
      throw new Error(`API Error: ${backlinkData.error}`);
    }

    // SEO PowerSuite API returns data in a 'summary' array
    if (
      backlinkData.summary &&
      Array.isArray(backlinkData.summary) &&
      backlinkData.summary.length > 0
    ) {
      backlinkData = backlinkData.summary[0]; // Get the first summary object
      backlinkLogger.debug(
        `Extracted data from summary array: backlinks=${backlinkData.backlinks}, refdomains=${backlinkData.refdomains}`,
        userId
      );
    } else {
      throw new Error("Invalid response format: no summary array found");
    }

    // Validate that we have the required backlinks data
    if (typeof backlinkData.backlinks === "undefined") {
      throw new Error(
        "Invalid response format: missing backlinks data in summary object"
      );
    }

    // UPDATED: Pass userId to validation function for better logging
    const validatedData = validateBacklinkResponse(backlinkData, userId);

    // Additional validation: ensure we have some data to work with
    if (validatedData.backlinks === 0 && validatedData.refdomains === 0) {
      backlinkLogger.warn(
        `Warning: No backlink data found for ${normalizedTarget}`,
        userId
      );
    }

    // Log the number of top items we're saving
    backlinkLogger.debug(
      `Saving ${validatedData.top_anchors_by_backlinks.length} anchors, ${validatedData.top_pages_by_backlinks.length} pages by backlinks, ${validatedData.top_pages_by_refdomains.length} pages by refdomains`,
      userId
    );

    backlinkLogger.info(
      `✅ Successfully fetched backlink data for ${normalizedTarget} (${responseTime}ms) - ${validatedData.backlinks} backlinks from ${validatedData.refdomains} domains`,
      userId
    );

    return {
      success: true,
      data: validatedData,
      responseTime,
      retryCount,
    };
  } catch (error) {
    const responseTime = Date.now() - startTime;

    backlinkLogger.error(
      `❌ SEO PowerSuite API request failed for ${websiteUrl}: ${error.message}`,
      null,
      userId
    );

    if (error.response) {
      backlinkLogger.error(
        `API Response Status: ${error.response.status}`,
        null,
        userId
      );
      backlinkLogger.error(
        `API Response Data: ${JSON.stringify(error.response.data)}`,
        null,
        userId
      );
    }

    // Retry logic with exponential backoff
    if (retryCount < BACKLINK_API_CONFIG.RETRY_ATTEMPTS) {
      const delay = BACKLINK_API_CONFIG.RETRY_DELAY * Math.pow(2, retryCount);

      backlinkLogger.info(
        `Retrying SEO PowerSuite API request for ${websiteUrl} in ${delay}ms (attempt ${
          retryCount + 1
        })`,
        userId
      );

      await new Promise((resolve) => setTimeout(resolve, delay));
      return fetchBacklinkSummaryFromAPI(websiteUrl, userId, retryCount + 1);
    }

    return {
      success: false,
      error: error.message,
      responseTime,
      retryCount,
    };
  }
}

/**
 * Main function to fetch and save backlink summary
 */
async function fetchAndSaveBacklinkSummary(websiteUrl, userId, userActivityId) {
  try {
    let backlinkSummary = await BacklinkSummary.findOne({ websiteUrl, userId });

    const CACHE_DURATION =
      BACKLINK_API_CONFIG.CACHE_DURATION_DAYS * 24 * 60 * 60 * 1000;
    if (
      backlinkSummary &&
      backlinkSummary.lastFetched &&
      Date.now() - backlinkSummary.lastFetched.getTime() < CACHE_DURATION
    ) {
      backlinkLogger.info(
        `Using cached backlink data for ${websiteUrl} (age: ${Math.round(
          (Date.now() - backlinkSummary.lastFetched.getTime()) /
            (1000 * 60 * 60)
        )} hours)`,
        userId
      );

      await UserActivity.findByIdAndUpdate(userActivityId, {
        backlinkSummaryStatus: "completed",
        backlinkSummaryId: backlinkSummary._id,
        backlinkFetchedAt: backlinkSummary.lastFetched,
      });

      return {
        success: true,
        cached: true,
        data: backlinkSummary,
      };
    }

    await UserActivity.findByIdAndUpdate(userActivityId, {
      isBacklinkFetching: 1,
      backlinkSummaryStatus: "fetching",
    });

    const apiResult = await fetchBacklinkSummaryFromAPI(websiteUrl, userId);

    if (!apiResult.success) {
      await UserActivity.findByIdAndUpdate(userActivityId, {
        isBacklinkFetching: 0,
        backlinkSummaryStatus: "failed",
        backlinkError: apiResult.error,
      });

      return {
        success: false,
        error: apiResult.error,
      };
    }

    if (!backlinkSummary) {
      backlinkSummary = new BacklinkSummary({
        userId,
        userActivityId,
        websiteUrl,
        ...apiResult.data,
        lastFetched: new Date(),
        apiResponseTime: apiResult.responseTime,
        apiStatus: "success",
      });
    } else {
      Object.assign(backlinkSummary, {
        userActivityId,
        ...apiResult.data,
        lastFetched: new Date(),
        apiResponseTime: apiResult.responseTime,
        apiStatus: "success",
        errorMessage: undefined,
      });
    }

    // Additional validation before saving to catch any remaining issues
    try {
      await backlinkSummary.save();
    } catch (saveError) {
      if (saveError.name === "ValidationError") {
        backlinkLogger.error(
          `Validation error saving backlink summary: ${saveError.message}`,
          null,
          userId
        );

        // Log the problematic data for debugging
        backlinkLogger.debug(
          `Problematic backlink data: ${JSON.stringify(apiResult.data)}`,
          userId
        );

        // Try to fix common validation issues
        if (backlinkSummary.top_anchors_by_backlinks) {
          backlinkSummary.top_anchors_by_backlinks =
            backlinkSummary.top_anchors_by_backlinks.filter(
              (item) => item.anchor && item.anchor.trim().length > 0
            );
        }

        if (backlinkSummary.top_pages_by_backlinks) {
          backlinkSummary.top_pages_by_backlinks =
            backlinkSummary.top_pages_by_backlinks.filter(
              (item) => item.url && item.url.trim().length > 0
            );
        }

        if (backlinkSummary.top_pages_by_refdomains) {
          backlinkSummary.top_pages_by_refdomains =
            backlinkSummary.top_pages_by_refdomains.filter(
              (item) => item.url && item.url.trim().length > 0
            );
        }

        // Try saving again
        await backlinkSummary.save();
        backlinkLogger.info(
          `Fixed validation issues and saved backlink summary for ${websiteUrl}`,
          userId
        );
      } else {
        throw saveError;
      }
    }

    await UserActivity.findByIdAndUpdate(userActivityId, {
      isBacklinkFetching: 0,
      backlinkSummaryStatus: "completed",
      backlinkSummaryId: backlinkSummary._id,
      backlinkFetchedAt: new Date(),
      backlinkError: undefined,
    });

    backlinkLogger.info(
      `✅ Successfully saved backlink summary for ${websiteUrl} - ${apiResult.data.backlinks} backlinks from ${apiResult.data.refdomains} domains`,
      userId
    );

    return {
      success: true,
      cached: false,
      data: backlinkSummary,
    };
  } catch (error) {
    backlinkLogger.error(
      `Error in fetchAndSaveBacklinkSummary for ${websiteUrl}:`,
      error,
      userId
    );

    try {
      await UserActivity.findByIdAndUpdate(userActivityId, {
        isBacklinkFetching: 0,
        backlinkSummaryStatus: "failed",
        backlinkError: error.message,
      });
    } catch (updateError) {
      backlinkLogger.error(
        `Failed to update UserActivity with error:`,
        updateError,
        userId
      );
    }

    return {
      success: false,
      error: error.message,
    };
  }
}

/**
 * Get backlink summary for a website
 */
async function getBacklinkSummary(websiteUrl, userId) {
  try {
    const backlinkSummary = await BacklinkSummary.findOne({
      websiteUrl,
      userId,
    })
      .populate("userActivityId", "startTime endTime status")
      .lean();

    if (!backlinkSummary) {
      return {
        success: false,
        message: "No backlink summary found for this website",
      };
    }

    return {
      success: true,
      data: backlinkSummary,
    };
  } catch (error) {
    backlinkLogger.error(
      `Error retrieving backlink summary for ${websiteUrl}:`,
      error,
      userId
    );
    return {
      success: false,
      error: error.message,
    };
  }
}

/**
 * Force refresh backlink summary
 */
async function forceRefreshBacklinkSummary(websiteUrl, userId, userActivityId) {
  try {
    backlinkLogger.info(
      `Force refreshing backlink summary for ${websiteUrl}`,
      userId
    );

    await UserActivity.findByIdAndUpdate(userActivityId, {
      isBacklinkFetching: 1,
      backlinkSummaryStatus: "fetching",
    });

    const apiResult = await fetchBacklinkSummaryFromAPI(websiteUrl, userId);

    if (!apiResult.success) {
      await UserActivity.findByIdAndUpdate(userActivityId, {
        isBacklinkFetching: 0,
        backlinkSummaryStatus: "failed",
        backlinkError: apiResult.error,
      });

      return {
        success: false,
        error: apiResult.error,
      };
    }

    let backlinkSummary = await BacklinkSummary.findOne({ websiteUrl, userId });

    if (!backlinkSummary) {
      backlinkSummary = new BacklinkSummary({
        userId,
        userActivityId,
        websiteUrl,
        ...apiResult.data,
        lastFetched: new Date(),
        apiResponseTime: apiResult.responseTime,
        apiStatus: "success",
      });
    } else {
      Object.assign(backlinkSummary, {
        userActivityId,
        ...apiResult.data,
        lastFetched: new Date(),
        apiResponseTime: apiResult.responseTime,
        apiStatus: "success",
        errorMessage: undefined,
      });
    }

    await backlinkSummary.save();

    await UserActivity.findByIdAndUpdate(userActivityId, {
      isBacklinkFetching: 0,
      backlinkSummaryStatus: "completed",
      backlinkSummaryId: backlinkSummary._id,
      backlinkFetchedAt: new Date(),
      backlinkError: undefined,
    });

    backlinkLogger.info(
      `✅ Successfully force refreshed backlink summary for ${websiteUrl}`,
      userId
    );

    return {
      success: true,
      cached: false,
      data: backlinkSummary,
    };
  } catch (error) {
    backlinkLogger.error(
      `Error force refreshing backlink summary for ${websiteUrl}:`,
      error,
      userId
    );

    try {
      await UserActivity.findByIdAndUpdate(userActivityId, {
        isBacklinkFetching: 0,
        backlinkSummaryStatus: "failed",
        backlinkError: error.message,
      });
    } catch (updateError) {
      backlinkLogger.error(
        `Failed to update UserActivity with error:`,
        updateError,
        userId
      );
    }

    return {
      success: false,
      error: error.message,
    };
  }
}

/**
 * Get backlink statistics for dashboard/analytics
 */
async function getBacklinkStatistics(userId) {
  try {
    const stats = await BacklinkSummary.aggregate([
      { $match: { userId: userId } },
      {
        $group: {
          _id: null,
          totalWebsites: { $sum: 1 },
          totalBacklinks: { $sum: "$backlinks" },
          totalRefdomains: { $sum: "$refdomains" },
          avgBacklinksPerSite: { $avg: "$backlinks" },
          avgRefdomainsPerSite: { $avg: "$refdomains" },
          mostBacklinks: { $max: "$backlinks" },
          mostRefdomains: { $max: "$refdomains" },
        },
      },
    ]);

    const topSites = await BacklinkSummary.find({ userId })
      .sort({ backlinks: -1 })
      .limit(5)
      .select("websiteUrl backlinks refdomains dominatingAnchor lastFetched")
      .lean();

    return {
      success: true,
      data: {
        summary: stats[0] || {
          totalWebsites: 0,
          totalBacklinks: 0,
          totalRefdomains: 0,
          avgBacklinksPerSite: 0,
          avgRefdomainsPerSite: 0,
          mostBacklinks: 0,
          mostRefdomains: 0,
        },
        topSites,
      },
    };
  } catch (error) {
    backlinkLogger.error(`Error getting backlink statistics:`, error, userId);
    return {
      success: false,
      error: error.message,
    };
  }
}

/**
 * Test the SEO PowerSuite API connection
 */
async function testAPIConnection() {
  try {
    backlinkLogger.info("Testing SEO PowerSuite API connection...");

    const result = await fetchBacklinkSummaryFromAPI(
      "https://seokart.com/",
      "test-user"
    );

    if (result.success) {
      backlinkLogger.info("✅ SEO PowerSuite API test successful:", {
        backlinks: result.data.backlinks,
        refdomains: result.data.refdomains,
        responseTime: result.responseTime,
        dominatingAnchor: result.data.top_anchors_by_backlinks?.[0]?.anchor,
      });
      return { success: true, data: result.data };
    } else {
      backlinkLogger.error("❌ SEO PowerSuite API test failed:", result.error);
      return { success: false, error: result.error };
    }
  } catch (error) {
    backlinkLogger.error("❌ SEO PowerSuite API test error:", error);
    return { success: false, error: error.message };
  }
}

module.exports = {
  fetchAndSaveBacklinkSummary,
  getBacklinkSummary,
  forceRefreshBacklinkSummary,
  fetchBacklinkSummaryFromAPI,
  getBacklinkStatistics,
  normalizeUrlForBacklinkAPI,
  testAPIConnection,
  BACKLINK_API_CONFIG,
};
