const axios = require("axios");
const Sitemap = require("../models/Sitemap");
const UserActivity = require("../models/UserActivity");
const Sitemapper = require("sitemapper");
const Webpage = require("../models/Webpages");
const { spawn } = require('child_process');
const path = require('path');
const { URL } = require("url");
const socketNotifications = require('../helper/socketNotifications');

/**
 * Configuration constants
 */
const CONFIG = {
  CONCURRENCY: {
    DEFAULT: 5,
    SITEMAP: 3,
    WEBPAGE: 10
  },
  TIMEOUTS: {
    URL_CHECK: 5000,
    SITEMAP_FETCH: 15000
  },
  PROGRESS: {
    SITEMAP_PHASE: 30,
    CHILD_SITEMAP_PHASE: 20,
    WEBPAGE_PHASE: 50
  },
  BATCH_SIZE: 100 // Increased batch size for bulk operations
};

/**
 * Logger utility for consistent logging
 */
const logger = {
  info: (message) => console.log(`[INFO] ${message}`),
  warn: (message) => console.warn(`[WARN] ⚠️ ${message}`),
  error: (message, error) => console.error(`[ERROR] ❌ ${message}`, error)
};

/**
 * Custom concurrency limiter for CommonJS
 * Improved version with better queue management
 * @param {number} concurrency - Maximum concurrent tasks
 * @returns {Function} - A limiting function
 */
function createLimiter(concurrency) {
  const queue = [];
  let activeCount = 0;

  // Process next item in queue
  const next = () => {
    activeCount--;
    if (queue.length > 0) {
      const { fn, resolve, reject } = queue.shift();
      run(fn).then(resolve, reject);
    }
  };

  // Run a function with active count tracking
  const run = async (fn) => {
    activeCount++;
    try {
      return await fn();
    } finally {
      next();
    }
  };

  // Add a function to the queue
  const limit = (fn) => {
    if (activeCount < concurrency) {
      return run(fn);
    }
    
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
    });
  };

  // Add utility to get queue status
  limit.activeCount = () => activeCount;
  limit.pendingCount = () => queue.length;

  return limit;
}

/**
 * Scrape a webpage and return comprehensive SEO metrics using the Python script.
 * @param {string} url - The URL to scrape
 * @returns {Promise<Object>} Object containing scraped data
 */
const scrapeWebpage = async (url) => {
  // Validate URL
  let validUrl;
  try {
    validUrl = new URL(url);
    if (!validUrl.protocol || !["http:", "https:"].includes(validUrl.protocol)) {
      throw new Error("Invalid URL protocol. Must be http or https.");
    }
  } catch (error) {
    return { url, error: `Invalid URL: ${error.message}` };
  }

  return new Promise((resolve, reject) => {
    // Path to Python script
    const pythonScript = path.join(__dirname, '../../../python_crawler/web_scraper.py');
    
    // Spawn Python process
    const pythonProcess = spawn('python', [pythonScript, url]);
    
    let dataString = '';
    let errorString = '';

    // Collect data from stdout
    pythonProcess.stdout.on('data', (data) => {
      dataString += data.toString();
    });

    // Collect any errors from stderr
    pythonProcess.stderr.on('data', (data) => {
      errorString += data.toString();
    });

    // Handle process completion
    pythonProcess.on('close', (code) => {
      if (code !== 0) {
        return reject(new Error(`Python process exited with code ${code}: ${errorString}`));
      }

      try {
        // Parse the JSON output from the Python script
        const result = JSON.parse(dataString);
        resolve(result);
      } catch (error) {
        reject(new Error(`Failed to parse Python script output: ${error.message}`));
      }
    });

    // Handle any errors in spawning the process
    pythonProcess.on('error', (error) => {
      reject(new Error(`Failed to start Python process: ${error.message}`));
    });
  });
};

/**
 * Calculate time remaining based on current progress
 * @param {Object} activity - User activity object
 * @return {Number} - Time remaining in seconds
 */
function calculateTimeRemaining(activity) {
  if (!activity.startTime || activity.progress >= 100) return 0;

  const elapsedTime = Date.now() - new Date(activity.startTime).getTime(); // in ms
  const progress = Math.max(activity.progress || 1, 1); // ensure minimum 1% to avoid division by zero

  // Calculate estimated total time based on current progress and elapsed time
  const estimatedTotalTime = (elapsedTime * 100) / progress;
  const estimatedTimeRemaining = estimatedTotalTime - elapsedTime;

  // Return time in seconds, rounded to nearest integer
  return Math.round(estimatedTimeRemaining / 1000);
}
/**
 * Validate a website URL and check if it's accessible
 * @param {String} websiteUrl - URL to validate
 * @returns {Promise<Object>} - Validation result
 */
const validateWebsite = async (websiteUrl) => {
  try {
    // Validate URL format
    if (!websiteUrl.startsWith("http://") && !websiteUrl.startsWith("https://")) {
      return {
        isValid: false,
        message: "Invalid URL format. URL must start with 'http://' or 'https://'"
      };
    }

    // Parse URL
    let parsedUrl;
    try {
      parsedUrl = new URL(websiteUrl);
    } catch (error) {
      return {
        isValid: false,
        message: "Invalid URL structure"
      };
    }

    // Test if website is accessible
    try {
      await axios.head(websiteUrl, { timeout: CONFIG.TIMEOUTS.URL_CHECK });
    } catch (error) {
      return {
        isValid: false,
        message: "Website is not accessible"
      };
    }

    // Check for sitemaps
    const sitemapUrls = await findSitemap(websiteUrl);
    
    if (sitemapUrls.error) {
      return {
        isValid: false,
        message: sitemapUrls.error
      };
    }

    if (Array.isArray(sitemapUrls) && sitemapUrls.length === 0) {
      return {
        isValid: false,
        message: "No sitemaps found for this website"
      };
    }

    return { isValid: true, sitemapUrls };
  } catch (error) {
    logger.error("Error validating website", error);
    return {
      isValid: false,
      message: "Error validating website: " + error.message
    };
  }
};

/**
 * Find sitemaps for a website
 * @param {String} websiteUrl - Website URL
 * @returns {Promise<Array|Object>} - Array of sitemap URLs or error object
 */
const findSitemap = async (websiteUrl) => {
  try {
    // Validate URL
    if (!websiteUrl.startsWith("http://") && !websiteUrl.startsWith("https://")) {
      return { error: "Invalid URL format. URL must start with 'http://' or 'https://'" };
    }

    const parsedUrl = new URL(websiteUrl);
    const robotsTxtUrl = `${parsedUrl.origin}/robots.txt`;
    const headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    };

    let sitemapUrls = new Set(); // Use Set to avoid duplicates

    // Fetch sitemaps from robots.txt
    try {
      const response = await axios.get(robotsTxtUrl, {
        headers,
        timeout: CONFIG.TIMEOUTS.URL_CHECK
      });
      
      const lines = response.data.split("\n");
      lines
        .filter((line) => line.toLowerCase().startsWith("sitemap:"))
        .map((line) => line.replace(/sitemap:/i, "").trim())
        .filter((url) => url.startsWith("http"))
        .forEach((url) => sitemapUrls.add(url));
    } catch (error) {
      logger.warn("robots.txt not accessible, checking alternative locations...");
    }

    // Extra sitemap locations to check - use Promise.all for parallel requests
    const extraSitemapLocations = [
      `${parsedUrl.origin}/sitemap.xml`,
      `${parsedUrl.origin}/sitemap_index.xml`,
      `${parsedUrl.origin}/sitemaps/sitemap.xml`,
      `${parsedUrl.origin}/sitemap1.xml`,
      `${parsedUrl.origin}/feeds/sitemap.xml`,
      `${parsedUrl.origin}/sitemap-pages.xml`,
      `${parsedUrl.origin}/xmlsitemap.php`
    ];

    // Check all sitemap locations in parallel
    const sitemapChecks = extraSitemapLocations.map(async (sitemapUrl) => {
      try {
        const res = await axios.head(sitemapUrl, { 
          headers, 
          timeout: CONFIG.TIMEOUTS.URL_CHECK,
          validateStatus: status => status === 200 // Only consider 200 OK responses
        });
        return sitemapUrl;
      } catch (error) {
        return null; // Ignore errors and return null
      }
    });

    // Wait for all checks to complete
    const validSitemaps = (await Promise.all(sitemapChecks)).filter(Boolean);
    validSitemaps.forEach(url => sitemapUrls.add(url));

    return Array.from(sitemapUrls);
  } catch (error) {
    logger.error("Error fetching sitemap", error);
    return { error: error.message };
  }
};

/**
 * Update activity progress in database
 * @param {String} userActivityId - Activity ID
 * @param {Object} updates - Fields to update
 * @returns {Promise<void>}
 */
const updateActivityProgress = async (userActivityId, updates) => {
  try {
    const updatedActivity = await UserActivity.findByIdAndUpdate(userActivityId, updates, { new: true });
    socketNotifications.updateCrawlProgress(updatedActivity);
    return updatedActivity;
  } catch (error) {
    logger.error("Error updating activity progress", error);
    return null;
  }
};
/**
 * Processes and saves webpage data to the database
 * @param {Object} scrapedData - Data returned from scrapeWebpage function
 * @param {String} userId - User ID
 * @param {String} userActivityId - UserActivity ID
 * @returns {Promise<Object>} - Saved webpage document
 */
async function saveWebpageData(scrapedData, userId, userActivityId) {
  try {
    // Check if data exists and has no errors
    if (!scrapedData || scrapedData.error) {
      logger.error(`Error in scraped data for ${scrapedData?.pageUrl || "unknown URL"}: ${scrapedData?.error}`);
      return null;
    }

    // Check if webpage already exists in DB
    let webpage = await Webpage.findOne({ pageUrl: scrapedData.pageUrl });

    // If webpage exists, update it; otherwise create a new one
    if (webpage) {
      // Map scraped data to webpage model
      Object.assign(webpage, mapScrapedDataToWebpageModel(scrapedData, userId, userActivityId));
    } else {
      // Create new webpage document
      webpage = new Webpage(mapScrapedDataToWebpageModel(scrapedData, userId, userActivityId));
    }

    // Calculate all SEO scores
    webpage.calculateSeoScore();

    socketNotifications.notifyNewWebpage(webpage);

    // Save to database
    await webpage.save();
    return webpage;
  } catch (error) {
    logger.error(`Error saving webpage data for ${scrapedData?.pageUrl || "unknown URL"}`, error);
    return null;
  }
}

/**
 * Maps data from scraper output to Webpage schema format
 * @param {Object} scrapedData - Data returned from scrapeWebpage function
 * @param {String} userId - User ID
 * @param {String} userActivityId - UserActivity ID
 * @returns {Object} - Data formatted for Webpage model
 */
function mapScrapedDataToWebpageModel(scrapedData, userId, userActivityId) {
  const safeString = (value) => value || "";
  const title = safeString(scrapedData.title);
  const metaDescription = safeString(scrapedData.metaDescription);

  return {
    userId,
    userActivityId,
    websiteUrl: safeString(scrapedData.websiteUrl),
    pageUrl: safeString(scrapedData.pageUrl),
    lastCrawled: scrapedData.lastCrawled || new Date(),

    // Title data - use the safely created variables
    title: title,
    titleLength: title.length,
    titleScore: scrapedData.titleScore || 0,
    titleIssues: scrapedData.titleIssues || {
      tooShort: false,
      tooLong: false,
      missing: !title,
      duplicate: false,
      multiple: false,
    },

    // Meta description data
    metaDescription: metaDescription,
    metaDescriptionLength: metaDescription.length,
    metaDescriptionScore: scrapedData.metaDescriptionScore || 0,
    metaDescriptionIssues: scrapedData.metaDescriptionIssues || {
      tooShort: false,
      tooLong: false,
      missing: !metaDescription,
      duplicate: false,
      multiple: false,
    },

    // Content data
    contentScore: scrapedData.contentScore || 0,
    contentIssues: scrapedData.contentIssues || {
      tooShort: true,
      lowKeywordDensity: false,
      poorReadability: false,
    },
    wordCount: scrapedData.wordCount || 0,
    readabilityScore: scrapedData.readabilityScore || 0,

    // Heading structure
    headingStructure: scrapedData.headingStructure || {
      h1Count: 0,
      h2Count: 0,
      h3Count: 0,
      h4Count: 0,
      h5Count: 0,
      h6Count: 0,
      h1Missing: true,
      h1Multiple: false,
      h2H3AtTop: false,
      headingScore: 0,
    },

    // URL data
    urlScore: scrapedData.urlScore || 0,
    urlIssues: scrapedData.urlIssues || {
      tooLong: false,
      containsSpecialChars: false,
      containsParams: false,
      nonDescriptive: false,
    },

    // Technical SEO data
    technicalSeo: scrapedData.technicalSeo || {
      canonicalTagExists: false,
      canonicalUrl: "",
      robotsDirectives: "",
      hreflangTags: [],
      structuredData: false,
      structuredDataTypes: [],
      technicalScore: 0,
    },

    // Images data
    images: scrapedData.images || {
      count: 0,
      optimizedCount: 0,
      altTextMissing: [],
      largeImages: [],
      imageScore: 0,
    },

    // Links data
    links: scrapedData.links || {
      internalCount: 0,
      externalCount: 0,
      brokenLinks: [],
      httpLinks: [],
      redirectLinks: [],
      linkScore: 0,
    },

    // Performance data
    performance: scrapedData.performance || {
      mobileResponsive: false,
      mobileIssues: [],
      pageSpeedScore: 0,
      coreWebVitals: {
        LCP: 0,
        FID: 0,
        CLS: 0,
      },
      performanceScore: 0,
    },

    // Content quality data
    contentQuality: scrapedData.contentQuality || {
      spellingErrors: [],
      grammarErrors: [],
      duplicateContent: [],
      contentQualityScore: 0,
    },

    // Duplicates data
    duplicates: scrapedData.duplicates || {
      titleDuplicates: [],
      descriptionDuplicates: [],
      duplicateScore: 10,
    },

    // SEO score components (using default weights from schema)
    seoScoreComponents: {
      titleWeight: 0.1,
      metaDescriptionWeight: 0.1,
      contentWeight: 0.15,
      headingsWeight: 0.1,
      urlWeight: 0.05,
      technicalWeight: 0.1,
      imagesWeight: 0.05,
      linksWeight: 0.1,
      performanceWeight: 0.15,
      contentQualityWeight: 0.1,
    },
  };
}

/**
 * Processes and saves webpages with concurrency control
 * Using custom limiter for CommonJS compatibility
 * @param {Array<string>} webpageUrls - Array of webpage URLs to process
 * @param {string} userId - User ID
 * @param {string} userActivityId - UserActivity ID
 * @param {number} concurrency - Maximum number of concurrent scraping operations
 * @returns {Promise<Object>} - Results statistics
 */
async function processAndSaveWebpages(webpageUrls, userId, userActivityId, concurrency = CONFIG.CONCURRENCY.WEBPAGE) {
  // Validate input
  if (!Array.isArray(webpageUrls)) {
    throw new Error("webpageUrls must be an array");
  }

  if (webpageUrls.length === 0) {
    return {
      success: true,
      message: "No webpages to process",
      processed: 0,
      failed: 0,
      total: 0,
    };
  }

  // Create a concurrency limiter
  const limit = createLimiter(concurrency);

  // Stats tracking
  const stats = {
    processed: 0,
    failed: 0,
    saved: 0,
    total: webpageUrls.length,
  };

  logger.info(`Starting to process ${webpageUrls.length} webpages with concurrency of ${concurrency}`);

  // Process in batches to avoid memory issues with large sites
  const batchSize = 1000;
  for (let i = 0; i < webpageUrls.length; i += batchSize) {
    const batch = webpageUrls.slice(i, i + batchSize);
    
    // Create an array of limited promises for this batch
    const promises = batch.map((url, index) => {
      return limit(async () => {
        try {
          // Log progress periodically
          if ((i + index) % 50 === 0 || index === 0 || index === batch.length - 1) {
            logger.info(`Processing webpage ${i + index + 1}/${webpageUrls.length}: ${url}`);
            
            // Update progress in database periodically
            await updateActivityProgress(userActivityId, {
              progress: 50 + Math.round(((i + index) / webpageUrls.length) * 50),
              webpagesSuccessful: stats.saved,
              webpagesFailed: stats.failed
            });
          }

          // Scrape the webpage
          const result = await scrapeWebpage(url);

          // Save to database if no error
          if (!result.error) {
            const savedWebpage = await saveWebpageData(result, userId, userActivityId);
            if (savedWebpage) {
              stats.saved++;
            }
          }

          // Track stats
          if (result.error) {
            stats.failed++;
            logger.warn(`Failed to scrape ${url}: ${result.error}`);
          } else {
            stats.processed++;
          }

          return result;
        } catch (error) {
          stats.failed++;
          logger.error(`Exception while processing ${url}`, error);
          return { url, error: error.message };
        }
      });
    });

    // Wait for this batch to complete
    await Promise.allSettled(promises);
  }

  logger.info(`Completed processing ${stats.processed} webpages successfully, ${stats.failed} failed, ${stats.saved} saved to database`);

  return {
    success: true,
    message: "Webpage processing and saving completed",
    ...stats,
  };
}

/**
 * Process a single sitemap and extract webpage URLs
 * @param {String} sitemapUrl - Sitemap URL
 * @param {String} userActivityId - User activity ID
 * @param {String} parentSitemapId - Parent sitemap ID (optional)
 * @returns {Promise<Object>} - Processing results with webpages and child sitemaps
 */
async function processSitemap(sitemapUrl, userActivityId, parentSitemapId = null) {
  try {
    // Create sitemap record
    let sitemap = await Sitemap.findOneAndUpdate(
      { url: sitemapUrl, urlType: 0 },
      { 
        userActivityId, 
        status: 1,
        ...(parentSitemapId ? { parentSitemaps: [parentSitemapId] } : {})
      },
      { upsert: true, new: true }
    );

    // Fetch and process the sitemap
    const sitemapFetcher = new Sitemapper({
      url: sitemapUrl,
      timeout: CONFIG.TIMEOUTS.SITEMAP_FETCH,
    });

    const result = await sitemapFetcher.fetch();
    const sites = result.sites;

    // Categorize URLs as either child sitemaps or webpages
    const childSitemaps = [];
    const webpages = [];

    sites.forEach(site => {
      if (site.includes("sitemap") && (site.endsWith(".xml") || site.endsWith(".xml.gz"))) {
        childSitemaps.push(site);
      } else {
        webpages.push(site);
      }
    });

    return {
      sitemapId: sitemap._id,
      childSitemaps,
      webpages,
      success: true
    };
  } catch (error) {
    logger.error(`Error processing sitemap ${sitemapUrl}`, error);
    return {
      sitemapUrl,
      success: false,
      error: error.message,
      childSitemaps: [],
      webpages: []
    };
  }
}

/**
 * Process all sitemaps and webpages for a website
 * Improved with better concurrency control and progress tracking
 * @param {Array<string>} sitemapUrls - Array of sitemap URLs
 * @param {string} userId - User ID
 * @param {string} websiteUrl - Website URL
 * @param {Object} options - Processing options
 * @returns {Promise<Object>} - Processing results
 */
async function processAllSitemapsAndWebpages(
  sitemapUrls,
  userId,
  websiteUrl,
  options = {}
) {
  // Default options
  const { 
    sitemapConcurrency = CONFIG.CONCURRENCY.SITEMAP,
    webpageConcurrency = CONFIG.CONCURRENCY.WEBPAGE
  } = options;
  
  try {
    // Create UserActivity record
    let userActivity = await UserActivity.findOneAndUpdate(
      { userId, websiteUrl },
      {
        isSitemapCrawling: 1,
        isWebpageCrawling: 1,
        startTime: new Date(),
        status: "processing",
        sitemapCount: 0,
        webpageCount: 0,
        progress: 0,
      },
      { upsert: true, new: true }
    );

    const userActivityId = userActivity._id;
    let totalSitemapCount = 0;
    let totalWebpageCount = 0;
    let errors = [];
    let allWebpageUrls = new Set(); // Use Set to avoid duplicates

    // First, count all potential sitemaps for better progress tracking
    const estimatedTotalUrls = sitemapUrls.length * 500; // Rough estimate

    // Update activity with initial sitemaps found
    await updateActivityProgress(userActivityId, {
      sitemapCount: sitemapUrls.length,
      estimatedTotalUrls,
    });

    logger.info(`Starting to process ${sitemapUrls.length} sitemaps`);

    // Process parent sitemaps with concurrency control
    const limit = createLimiter(sitemapConcurrency);
    const sitemapPromises = [];

    // Create processing tasks for each sitemap
    for (let i = 0; i < sitemapUrls.length; i++) {
      const url = sitemapUrls[i];
      sitemapPromises.push(
        limit(async () => {
          try {
            const result = await processSitemap(url, userActivityId);
            
            // Update progress after each parent sitemap is processed
            totalSitemapCount++;
            await updateActivityProgress(userActivityId, {
              sitemapCount: totalSitemapCount,
              progress: Math.round((i / sitemapUrls.length) * CONFIG.PROGRESS.SITEMAP_PHASE),
            });
            
            // Add webpages to collection
            result.webpages.forEach(page => allWebpageUrls.add(page));
            
            // Return data needed for child sitemaps
            return {
              success: true,
              sitemapId: result.sitemapId,
              childSitemaps: result.childSitemaps
            };
          } catch (error) {
            logger.error(`Failed to process sitemap ${url}`, error);
            errors.push(`Failed to process sitemap: ${url}`);
            return { success: false, childSitemaps: [] };
          }
        })
      );
    }

    // Process all parent sitemaps
    const parentResults = await Promise.all(sitemapPromises);
    
    // Process child sitemaps
    const childSitemapLimit = createLimiter(sitemapConcurrency);
    const allChildSitemaps = parentResults
      .filter(r => r.success)
      .flatMap(r => r.childSitemaps.map(url => ({ url, parentId: r.sitemapId })));

    logger.info(`Processing ${allChildSitemaps.length} child sitemaps`);
    
    if (allChildSitemaps.length > 0) {
      const childPromises = [];
      
      for (let i = 0; i < allChildSitemaps.length; i++) {
        const sitemap = allChildSitemaps[i];
        childPromises.push(
          childSitemapLimit(async () => {
            try {
              const result = await processSitemap(sitemap.url, userActivityId, sitemap.parentId);
              
              // Update count and progress
              totalSitemapCount++;
              await updateActivityProgress(userActivityId, {
                sitemapCount: totalSitemapCount,
                progress: CONFIG.PROGRESS.SITEMAP_PHASE + 
                  Math.round((i / allChildSitemaps.length) * CONFIG.PROGRESS.CHILD_SITEMAP_PHASE),
              });
              
              // Add webpages to collection
              result.webpages.forEach(page => allWebpageUrls.add(page));
              return { success: true };
            } catch (error) {
              logger.error(`Failed to process child sitemap ${sitemap.url}`, error);
              errors.push(`Failed to process child sitemap: ${sitemap.url}`);
              return { success: false };
            }
          })
        );
      }
      
      await Promise.all(childPromises);
    }

    // Save all webpage URLs to the database in batches
    const webpageUrlsArray = Array.from(allWebpageUrls);
    
    logger.info(`Saving ${webpageUrlsArray.length} webpage URLs to database in batches`);
    
    for (let i = 0; i < webpageUrlsArray.length; i += CONFIG.BATCH_SIZE) {
      const batch = webpageUrlsArray.slice(i, i + CONFIG.BATCH_SIZE);
      const operations = batch.map(url => ({
        updateOne: {
          filter: { url, urlType: 1 },
          update: {
            userActivityId,
            status: 1,
          },
          upsert: true,
        },
      }));

      if (operations.length > 0) {
        await Sitemap.bulkWrite(operations);
        
        // Update progress after each batch
        totalWebpageCount += batch.length;
        await updateActivityProgress(userActivityId, {
          webpageCount: totalWebpageCount,
          progress: CONFIG.PROGRESS.SITEMAP_PHASE + CONFIG.PROGRESS.CHILD_SITEMAP_PHASE + 
            Math.round((totalWebpageCount / webpageUrlsArray.length) * 20),
        });
      }
    }

    // Process and save webpages with SEO data
    logger.info(`Processing and saving ${webpageUrlsArray.length} webpages with SEO data...`);
    
    const scrapingResults = await processAndSaveWebpages(
      webpageUrlsArray,
      userId,
      userActivityId,
      webpageConcurrency
    );

    const userActivityUpdated = await UserActivity.findByIdAndUpdate(userActivityId, {
      sitemapCount: totalSitemapCount,
      webpageCount: totalWebpageCount,
      webpagesSuccessful: scrapingResults.saved,
      webpagesFailed: scrapingResults.failed,
      isSitemapCrawling: 0,
      isWebpageCrawling: 0,
      endTime: new Date(),
      status: errorMessages.length > 0 ? "completed_with_errors" : "completed",
      errorMessages: errorMessages.length > 0 ? errorMessages : undefined,
      progress: 100,
    }, { new: true });
  
    socketNotifications.updateCrawlProgress(userActivityUpdated);


    return {
      success: true,
      message: "Sitemap and webpage processing completed",
      totalSitemaps: totalSitemapCount,
      totalWebpages: totalWebpageCount,
      scrapedPages: scrapingResults.processed,
      savedPages: scrapingResults.saved,
      failedPages: scrapingResults.failed,
      errors: errors.length > 0 ? errors : [],
    };
  } catch (error) {
    logger.error("Error in processAllSitemapsAndWebpages", error);

    // Update user activity to show failure
    if (userId && websiteUrl) {
      try {
        await UserActivity.findOneAndUpdate(
          { userId, websiteUrl },
          {
            isSitemapCrawling: 0,
            isWebpageCrawling: 0,
            endTime: new Date(),
            status: "failed",
            errors: [error.message],
          }
        );
      } catch (updateError) {
        logger.error("Failed to update user activity status", updateError);
      }
    }

    return { error: error.message };
  }
}



module.exports = {
  validateWebsite,
  processAllSitemapsAndWebpages,
  scrapeWebpage,
  findSitemap,
  processSitemap,
}