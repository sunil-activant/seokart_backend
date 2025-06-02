const axios = require("axios");
const Sitemap = require("../models/Sitemap");
const UserActivity = require("../models/UserActivity");
const Sitemapper = require("sitemapper");
const Webpage = require("../models/Webpages");
const { spawn } = require("child_process");
const path = require("path");
const { URL } = require("url");
const cluster = require("cluster");
const os = require("os");
const Redis = require("ioredis");
const Bull = require("bull");
const { fetchAndSaveBacklinkSummary } = require("./backlinkService");
const socketService = require("./socketService");

/**
 * Enhanced Configuration with improved rate limiting
 */
const CONFIG = {
  CONCURRENCY: {
    DEFAULT: 1, // Reduced from 2
    SITEMAP: 1,
    WEBPAGE: 1, // Reduced from 2
    MAX_GLOBAL_WORKERS: Math.min(os.cpus().length, 4), // Reduced from 6
    MAX_CONCURRENT_USERS: 3, // Reduced from 5
    AWS_PROXY_LIMIT: 2, // Reduced from 5
  },
  TIMEOUTS: {
    URL_CHECK: 10000, // Increased from 5000
    SITEMAP_FETCH: 30000, // Increased from 15000
    SCRAPE_TIMEOUT: 60000, // Increased from 45000
    AWS_PROXY_TIMEOUT: 50000, // Increased from 40000
    PROCESS_KILL_TIMEOUT: 5000,
  },
  PROGRESS: {
    SITEMAP_PHASE: 30,
    CHILD_SITEMAP_PHASE: 20,
    WEBPAGE_PHASE: 50,
  },
  BATCH_SIZE: 100, // Reduced from 250
  QUEUE: {
    MAX_JOBS: 5000,
    STALLED_INTERVAL: 30000,
    MAX_STALLED_COUNT: 3,
  },
  MEMORY: {
    HEAP_LIMIT_MB: 2048,
    GC_THRESHOLD: 0.7,
  },
  RATE_LIMITING: {
    REQUESTS_PER_SECOND: 1, // Reduced from 5
    BURST_SIZE: 5, // Reduced from 20
    MIN_DELAY_BETWEEN_REQUESTS: 1000, // Increased from 200
    MIN_DELAY_BETWEEN_DOMAIN_REQUESTS: 2000, // New: minimum delay per domain
    MAX_RETRIES_BEFORE_BACKOFF: 2, // New: retries before increasing delay
    BACKOFF_MULTIPLIER: 2, // New: multiply delay on errors
    MAX_BACKOFF_DELAY: 30000, // New: maximum backoff delay
    RANDOM_DELAY_MIN: 500, // New: random delay component
    RANDOM_DELAY_MAX: 2000, // New: random delay component
  },
  RETRY: {
    MAX_ATTEMPTS: 3, // Increased from 2
    BACKOFF_DELAY: 5000, // Increased from 2000
    EXPONENTIAL_BASE: 2,
  },
  AWS_PROXY: {
    ENDPOINT:
      process.env.AWS_PROXY_ENDPOINT ||
      "https://b1anh9d0nl.execute-api.us-west-2.amazonaws.com/proxy_rotate/proxy",
    MAX_CONCURRENT_REQUESTS: 2, // Reduced from 5
    RETRY_ATTEMPTS: 3,
    TIMEOUT: 50000, // Increased from 40000
  },
  PYTHON: {
    MAX_CONCURRENT_PROCESSES: 5, // Reduced from 10
  },
};

/**
 * Enhanced Rate limiter with improved backoff and per-domain tracking
 */
class RateLimiter {
  constructor() {
    this.domainQueues = new Map();
    this.globalLastRequest = 0;
    this.domainErrors = new Map(); // Track errors per domain
    this.domainBackoff = new Map(); // Track backoff delays per domain
  }

  async acquire(domain) {
    const now = Date.now();

    // Global rate limiting
    const globalTimeSinceLastRequest = now - this.globalLastRequest;
    const minGlobalInterval = CONFIG.RATE_LIMITING.MIN_DELAY_BETWEEN_REQUESTS;

    if (globalTimeSinceLastRequest < minGlobalInterval) {
      await new Promise((resolve) =>
        setTimeout(resolve, minGlobalInterval - globalTimeSinceLastRequest)
      );
    }

    // Initialize domain data if not exists
    if (!this.domainQueues.has(domain)) {
      this.domainQueues.set(domain, {
        queue: [],
        lastRequest: 0,
        requestCount: 0,
        consecutiveErrors: 0,
      });
    }

    const domainData = this.domainQueues.get(domain);

    // Calculate delay with backoff if domain has errors
    let minInterval = CONFIG.RATE_LIMITING.MIN_DELAY_BETWEEN_DOMAIN_REQUESTS;
    const backoffDelay = this.domainBackoff.get(domain) || 0;
    if (backoffDelay > 0) {
      minInterval = Math.min(
        backoffDelay,
        CONFIG.RATE_LIMITING.MAX_BACKOFF_DELAY
      );
    }

    // Add random delay to appear more human-like
    const randomDelay = Math.floor(
      Math.random() *
        (CONFIG.RATE_LIMITING.RANDOM_DELAY_MAX -
          CONFIG.RATE_LIMITING.RANDOM_DELAY_MIN) +
        CONFIG.RATE_LIMITING.RANDOM_DELAY_MIN
    );
    minInterval += randomDelay;

    const timeSinceLastRequest = Date.now() - domainData.lastRequest;
    if (timeSinceLastRequest < minInterval) {
      const waitTime = minInterval - timeSinceLastRequest;
      logger.debug(`Rate limiting: waiting ${waitTime}ms for domain ${domain}`);
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }

    domainData.lastRequest = Date.now();
    domainData.requestCount++;
    this.globalLastRequest = Date.now();
  }

  // Report success - reduce backoff
  reportSuccess(domain) {
    if (this.domainQueues.has(domain)) {
      const domainData = this.domainQueues.get(domain);
      domainData.consecutiveErrors = 0;
    }
    this.domainBackoff.delete(domain);
    this.domainErrors.delete(domain);
  }

  // Report error - increase backoff
  reportError(domain, errorType = "general") {
    if (!this.domainQueues.has(domain)) {
      this.domainQueues.set(domain, {
        queue: [],
        lastRequest: 0,
        requestCount: 0,
        consecutiveErrors: 0,
      });
    }

    const domainData = this.domainQueues.get(domain);
    domainData.consecutiveErrors++;

    // Track error types
    const errors = this.domainErrors.get(domain) || { count: 0, types: [] };
    errors.count++;
    errors.types.push({ type: errorType, timestamp: Date.now() });
    this.domainErrors.set(domain, errors);

    // Calculate exponential backoff
    const currentBackoff =
      this.domainBackoff.get(domain) ||
      CONFIG.RATE_LIMITING.MIN_DELAY_BETWEEN_DOMAIN_REQUESTS;
    const newBackoff = Math.min(
      currentBackoff * CONFIG.RATE_LIMITING.BACKOFF_MULTIPLIER,
      CONFIG.RATE_LIMITING.MAX_BACKOFF_DELAY
    );
    this.domainBackoff.set(domain, newBackoff);

    logger.warn(
      `Domain ${domain} error #${domainData.consecutiveErrors}, backoff: ${newBackoff}ms`
    );
  }

  getDomainStats(domain) {
    const queueData = this.domainQueues.get(domain) || {
      requestCount: 0,
      consecutiveErrors: 0,
    };
    const errors = this.domainErrors.get(domain) || { count: 0, types: [] };
    const backoff = this.domainBackoff.get(domain) || 0;

    return {
      ...queueData,
      errors,
      currentBackoff: backoff,
    };
  }

  cleanup() {
    const cutoff = Date.now() - 60 * 60 * 1000;
    for (const [domain, data] of this.domainQueues.entries()) {
      if (data.lastRequest < cutoff) {
        this.domainQueues.delete(domain);
        this.domainErrors.delete(domain);
        this.domainBackoff.delete(domain);
      }
    }
  }

  // Check if domain should be temporarily blocked
  isDomainBlocked(domain) {
    const stats = this.getDomainStats(domain);
    return (
      stats.consecutiveErrors > 10 ||
      stats.currentBackoff >= CONFIG.RATE_LIMITING.MAX_BACKOFF_DELAY
    );
  }
}

const globalRateLimiter = new RateLimiter();

// Clean up old domain data periodically
setInterval(() => {
  globalRateLimiter.cleanup();
}, 10 * 60 * 1000);

/**
 * Enhanced AWS Proxy Request Manager with better error handling
 */
class AWSProxyManager {
  constructor(maxConcurrent = CONFIG.AWS_PROXY.MAX_CONCURRENT_REQUESTS) {
    this.maxConcurrent = maxConcurrent;
    this.activeRequests = 0;
    this.queue = [];
    this.stats = {
      totalRequests: 0,
      successfulRequests: 0,
      failedRequests: 0,
      averageResponseTime: 0,
    };
  }

  async scrapeWebpage(url, userId = null, retryCount = 0) {
    const parsedUrl = new URL(url);
    const domain = parsedUrl.hostname;

    // Check if domain is blocked due to too many errors
    if (globalRateLimiter.isDomainBlocked(domain)) {
      logger.warn(
        `Domain ${domain} is temporarily blocked due to too many errors`,
        userId
      );
      throw new Error(`Domain ${domain} is temporarily unavailable`);
    }

    return new Promise((resolve, reject) => {
      // Add to queue if too many active requests
      if (this.activeRequests >= this.maxConcurrent) {
        this.queue.push({ url, userId, retryCount, resolve, reject });
        return;
      }

      this._processRequest(url, userId, retryCount, resolve, reject);
    });
  }

  async _processRequest(url, userId, retryCount, resolve, reject) {
    const startTime = Date.now();
    const parsedUrl = new URL(url);
    const domain = parsedUrl.hostname;

    this.activeRequests++;
    this.stats.totalRequests++;

    try {
      // Apply rate limiting before making request
      await globalRateLimiter.acquire(domain);

      logger.debug(
        `Making AWS proxy request for: ${url} (attempt ${retryCount + 1})`,
        userId
      );

      const response = await axios.post(
        CONFIG.AWS_PROXY.ENDPOINT,
        { url },
        {
          timeout: CONFIG.AWS_PROXY.TIMEOUT,
          headers: {
            "Content-Type": "application/json",
            "User-Agent": "SEOKart-Scraper/1.0",
          },
          validateStatus: (status) => status < 500, // Don't throw on 4xx errors
        }
      );

      const responseTime = Date.now() - startTime;

      // Handle different response codes
      if (response.status === 429) {
        // Rate limited by server
        throw new Error("Rate limited by server");
      } else if (response.status === 403) {
        // Forbidden - likely blocked
        throw new Error("Access forbidden - possible bot detection");
      } else if (response.status >= 400) {
        throw new Error(`HTTP ${response.status} error`);
      }

      // Validate response
      if (!response.data) {
        throw new Error("Empty response from AWS proxy");
      }

      if (response.data.error) {
        throw new Error(response.data.error);
      }

      this._updateStats(responseTime, true);

      // Report success to rate limiter
      globalRateLimiter.reportSuccess(domain);

      // Extract base website URL from the page URL
      const validUrl = new URL(url);
      const websiteUrl = `${validUrl.protocol}//${validUrl.hostname}`;

      // Add metadata to the response
      const result = {
        ...response.data,
        websiteUrl: response.data.websiteUrl || websiteUrl,
        pageUrl: response.data.pageUrl || response.data.url || url,
        url: url,
        scrapedAt: new Date().toISOString(),
        responseTime,
        method: "aws_proxy",
        retryCount,
      };

      logger.info(
        `✅ Successfully scraped ${url} via AWS proxy (${responseTime}ms)`,
        userId
      );
      this.stats.successfulRequests++;
      resolve(result);
    } catch (error) {
      const responseTime = Date.now() - startTime;
      this._updateStats(responseTime, false);

      // Report error to rate limiter
      let errorType = "general";
      if (error.message.includes("Rate limited")) {
        errorType = "rate_limit";
      } else if (error.message.includes("timeout")) {
        errorType = "timeout";
      } else if (error.message.includes("forbidden")) {
        errorType = "forbidden";
      }
      globalRateLimiter.reportError(domain, errorType);

      logger.error(
        `❌ AWS proxy request failed for ${url}: ${error.message}`,
        null,
        userId
      );

      // Enhanced retry logic with exponential backoff
      if (retryCount < CONFIG.AWS_PROXY.RETRY_ATTEMPTS) {
        // Calculate delay based on error type and retry count
        let delay =
          CONFIG.RETRY.BACKOFF_DELAY *
          Math.pow(CONFIG.RETRY.EXPONENTIAL_BASE, retryCount);

        // Add extra delay for rate limiting errors
        if (errorType === "rate_limit") {
          delay *= 2;
        }

        logger.info(
          `Retrying AWS proxy request for ${url} in ${delay}ms (attempt ${
            retryCount + 1
          }/${CONFIG.AWS_PROXY.RETRY_ATTEMPTS})`,
          userId
        );

        setTimeout(() => {
          this.scrapeWebpage(url, userId, retryCount + 1)
            .then(resolve)
            .catch(reject);
        }, delay);
        return;
      }

      this.stats.failedRequests++;
      reject(
        new Error(
          `AWS proxy failed after ${CONFIG.AWS_PROXY.RETRY_ATTEMPTS} attempts: ${error.message}`
        )
      );
    } finally {
      this.activeRequests--;
      this._processNext();
    }
  }

  _processNext() {
    if (this.queue.length > 0 && this.activeRequests < this.maxConcurrent) {
      const { url, userId, retryCount, resolve, reject } = this.queue.shift();
      this._processRequest(url, userId, retryCount, resolve, reject);
    }
  }

  _updateStats(responseTime, success) {
    const totalResponses =
      this.stats.successfulRequests + this.stats.failedRequests;
    this.stats.averageResponseTime =
      (this.stats.averageResponseTime * (totalResponses - 1) + responseTime) /
      totalResponses;
  }

  getStats() {
    return {
      activeRequests: this.activeRequests,
      queueLength: this.queue.length,
      maxConcurrent: this.maxConcurrent,
      ...this.stats,
      successRate:
        this.stats.totalRequests > 0
          ? (
              (this.stats.successfulRequests / this.stats.totalRequests) *
              100
            ).toFixed(2) + "%"
          : "0%",
    };
  }

  cleanup() {
    this.queue = [];
  }
}

// Create global AWS proxy manager with reduced concurrency
const awsProxyManager = new AWSProxyManager(
  CONFIG.AWS_PROXY.MAX_CONCURRENT_REQUESTS
);

/**
 * Python Process Pool Manager with enhanced error handling
 */
class PythonProcessPool {
  constructor(poolSize = CONFIG.PYTHON.MAX_CONCURRENT_PROCESSES) {
    this.poolSize = poolSize;
    this.processes = new Map();
    this.queue = [];
    this.activeProcesses = 0;
    this.processErrors = new Map();
    this.isAwsProxyServer = process.env.IS_AWS_PROXY_SERVER === "true";
    this.enabled =
      this.isAwsProxyServer || process.env.USE_LOCAL_PYTHON === "true";
  }

  async executeScript(url, userId = null, retryCount = 0) {
    if (!this.enabled) {
      throw new Error(
        "Local Python processing is disabled. Use AWS proxy instead."
      );
    }

    const parsedUrl = new URL(url);
    const domain = parsedUrl.hostname;

    // Check if domain is blocked
    if (globalRateLimiter.isDomainBlocked(domain)) {
      throw new Error(`Domain ${domain} is temporarily unavailable`);
    }

    return new Promise((resolve, reject) => {
      if (this.activeProcesses >= this.poolSize) {
        this.queue.push({ url, userId, retryCount, resolve, reject });
        return;
      }

      this._processRequest(url, userId, retryCount, resolve, reject);
    });
  }

  async _processRequest(url, userId, retryCount, resolve, reject) {
    const processId = `${Date.now()}_${Math.random()}`;
    const parsedUrl = new URL(url);
    const domain = parsedUrl.hostname;

    this.activeProcesses++;

    try {
      // Apply rate limiting
      await globalRateLimiter.acquire(domain);

      const result = await this._spawnPythonProcess(url, processId, userId);

      // Report success
      globalRateLimiter.reportSuccess(domain);

      this.activeProcesses--;
      resolve(result);
    } catch (error) {
      this.activeProcesses--;

      // Report error
      let errorType = "general";
      if (error.message.includes("timeout")) {
        errorType = "timeout";
      } else if (
        error.message.includes("403") ||
        error.message.includes("forbidden")
      ) {
        errorType = "forbidden";
      }
      globalRateLimiter.reportError(domain, errorType);

      const errorCount = this.processErrors.get(processId) || 0;
      this.processErrors.set(processId, errorCount + 1);

      if (retryCount < CONFIG.RETRY.MAX_ATTEMPTS) {
        const delay =
          CONFIG.RETRY.BACKOFF_DELAY *
          Math.pow(CONFIG.RETRY.EXPONENTIAL_BASE, retryCount);

        logger.info(
          `Retrying Python script for ${url} in ${delay}ms (attempt ${
            retryCount + 1
          }/${CONFIG.RETRY.MAX_ATTEMPTS})`,
          userId
        );

        setTimeout(() => {
          this.executeScript(url, userId, retryCount + 1)
            .then(resolve)
            .catch(reject);
        }, delay);
        return;
      }

      reject(error);
    } finally {
      this._processNext();
    }
  }

  _processNext() {
    if (this.queue.length > 0 && this.activeProcesses < this.poolSize) {
      const { url, userId, retryCount, resolve, reject } = this.queue.shift();
      this._processRequest(url, userId, retryCount, resolve, reject);
    }
  }

  async _spawnPythonProcess(url, processId, userId) {
    return new Promise((resolve, reject) => {
      const pythonScript = path.join(
        __dirname,
        "../../../python_crawler/web_scraper.py"
      );
      const pythonExecutable =
        "/var/www/seokart_backend/python_crawler/venv/bin/python";

      const args = [
        pythonScript,
        url,
        "--method=requests",
        "--timeout=15", // Increased timeout
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "--max-retries=2", // Increased retries
        "--delay=2", // Add delay between retries
      ];

      if (process.env.NODE_ENV === "development") {
        args.push("--verbose");
      }

      const pythonProcess = spawn(pythonExecutable, args, {
        shell: false,
        timeout: CONFIG.TIMEOUTS.SCRAPE_TIMEOUT,
        stdio: ["pipe", "pipe", "pipe"],
        env: {
          ...process.env,
          PYTHONUNBUFFERED: "1",
          PYTHONIOENCODING: "utf-8",
        },
      });

      let dataString = "";
      let errorString = "";
      let processKilled = false;

      this.processes.set(processId, pythonProcess);

      pythonProcess.stdout.on("data", (data) => {
        dataString += data.toString("utf8");
      });

      pythonProcess.stderr.on("data", (data) => {
        errorString += data.toString("utf8");
      });

      pythonProcess.on("close", (code, signal) => {
        this.processes.delete(processId);

        if (processKilled) {
          return reject(
            new Error(`Process was killed due to timeout for ${url}`)
          );
        }

        if (code === null) {
          return reject(
            new Error(`Process was terminated by signal ${signal} for ${url}`)
          );
        }

        if (code !== 0) {
          const error = new Error(
            `Python process exited with code ${code} for ${url}: ${
              errorString || "No error details"
            }`
          );
          return reject(error);
        }

        try {
          if (!dataString.trim()) {
            throw new Error("Empty response from Python script");
          }

          const result = JSON.parse(dataString);

          const pageUrl = new URL(url);
          const websiteUrl = `${pageUrl.protocol}//${pageUrl.hostname}`;

          result.scrapedAt = new Date().toISOString();
          result.retryCount = retryCount;
          result.processingMethod = "local_python";
          result.processId = processId;
          result.websiteUrl = result.websiteUrl || websiteUrl;
          result.pageUrl = result.pageUrl || url;
          result.url = url;

          logger.info(`✅ Successfully scraped ${url}`, userId);
          resolve(result);
        } catch (parseError) {
          logger.error(
            `❌ Failed to parse result for ${url}: ${parseError.message}`,
            userId
          );
          reject(
            new Error(
              `Failed to parse Python script output: ${parseError.message}`
            )
          );
        }
      });

      pythonProcess.on("error", (error) => {
        this.processes.delete(processId);
        logger.error(
          `❌ Process spawn error for ${url}: ${error.message}`,
          userId
        );
        reject(new Error(`Failed to start Python process: ${error.message}`));
      });

      const timeoutHandler = setTimeout(() => {
        if (!processKilled && pythonProcess.pid) {
          processKilled = true;
          logger.warn(
            `⚠️ Killing process ${processId} due to timeout for ${url}`,
            userId
          );

          pythonProcess.kill("SIGTERM");

          setTimeout(() => {
            if (this.processes.has(processId)) {
              pythonProcess.kill("SIGKILL");
            }
          }, CONFIG.TIMEOUTS.PROCESS_KILL_TIMEOUT);
        }
      }, CONFIG.TIMEOUTS.SCRAPE_TIMEOUT);

      pythonProcess.on("close", () => {
        clearTimeout(timeoutHandler);
      });
    });
  }

  cleanup() {
    for (const [processId, process] of this.processes.entries()) {
      try {
        process.kill("SIGTERM");
        setTimeout(() => {
          if (this.processes.has(processId)) {
            process.kill("SIGKILL");
          }
        }, CONFIG.TIMEOUTS.PROCESS_KILL_TIMEOUT);
      } catch (error) {
        logger.warn(`Error killing process ${processId}: ${error.message}`);
      }
    }
    this.processes.clear();
    this.queue = [];
  }

  getStats() {
    return {
      enabled: this.enabled,
      isAwsProxyServer: this.isAwsProxyServer,
      activeProcesses: this.activeProcesses,
      queueLength: this.queue.length,
      poolSize: this.poolSize,
      totalProcesses: this.processes.size,
    };
  }
}

// Create Python process pool with reduced concurrency
const pythonProcessPool = new PythonProcessPool(
  CONFIG.PYTHON.MAX_CONCURRENT_PROCESSES
);

/**
 * Redis connection for distributed processing
 */
let redisClient;
let queueManager;

try {
  redisClient = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: process.env.REDIS_PORT || 6379,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3,
    lazyConnect: true,
    connectTimeout: 10000,
  });

  queueManager = {
    sitemapQueue: new Bull("sitemap processing", {
      redis: {
        host: process.env.REDIS_HOST || "localhost",
        port: process.env.REDIS_PORT || 6379,
      },
      defaultJobOptions: {
        removeOnComplete: 50,
        removeOnFail: 25,
        attempts: CONFIG.RETRY.MAX_ATTEMPTS,
        backoff: "exponential",
      },
    }),
    webpageQueue: new Bull("webpage processing", {
      redis: {
        host: process.env.REDIS_HOST || "localhost",
        port: process.env.REDIS_PORT || 6379,
      },
      defaultJobOptions: {
        removeOnComplete: 50,
        removeOnFail: 25,
        attempts: CONFIG.RETRY.MAX_ATTEMPTS,
        backoff: "exponential",
      },
    }),
  };
} catch (error) {
  console.warn("Redis not available, falling back to in-memory processing");
  redisClient = null;
  queueManager = null;
}

/**
 * Enhanced logger with user context and rate limiting
 */
const logger = {
  info: (message, userId = null) =>
    console.log(`[INFO]${userId ? ` [User:${userId}]` : ""} ${message}`),
  warn: (message, userId = null) =>
    console.warn(`[WARN]${userId ? ` [User:${userId}]` : ""} ⚠️ ${message}`),
  error: (message, error, userId = null) => {
    const errorMsg = error instanceof Error ? error.message : error;
    console.error(
      `[ERROR]${userId ? ` [User:${userId}]` : ""} ❌ ${message}`,
      errorMsg
    );
  },
  debug: (message, userId = null) => {
    if (process.env.NODE_ENV === "development") {
      console.log(`[DEBUG]${userId ? ` [User:${userId}]` : ""} ${message}`);
    }
  },
};

/**
 * Enhanced Memory monitoring and management
 */
class MemoryManager {
  static checkMemoryUsage() {
    const usage = process.memoryUsage();
    const heapUsedMB = usage.heapUsed / 1024 / 1024;
    const heapTotalMB = usage.heapTotal / 1024 / 1024;
    const rssUsedMB = usage.rss / 1024 / 1024;

    if (heapUsedMB > CONFIG.MEMORY.HEAP_LIMIT_MB * CONFIG.MEMORY.GC_THRESHOLD) {
      logger.warn(
        `High memory usage detected: ${heapUsedMB.toFixed(
          2
        )}MB heap, ${rssUsedMB.toFixed(2)}MB RSS`
      );
      if (global.gc) {
        global.gc();
        logger.info("Garbage collection triggered");
      }
    }

    return { heapUsedMB, heapTotalMB, rssUsedMB };
  }

  static async waitForMemoryAvailable() {
    const usage = this.checkMemoryUsage();
    if (
      usage.heapUsedMB >
      CONFIG.MEMORY.HEAP_LIMIT_MB * CONFIG.MEMORY.GC_THRESHOLD
    ) {
      logger.info("Waiting for memory to be available...");
      await new Promise((resolve) => setTimeout(resolve, 2000));
      return this.waitForMemoryAvailable();
    }
  }

  static async forceCleanup() {
    if (global.gc) {
      global.gc();
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

/**
 * Enhanced User session manager with better resource allocation
 */
class UserSessionManager {
  constructor() {
    this.userSessions = new Map();
    this.globalStats = {
      activeUsers: 0,
      totalRequests: 0,
      totalErrors: 0,
    };
  }

  createSession(userId, websiteUrl) {
    const sessionId = `${userId}_${Date.now()}`;
    const session = {
      userId,
      websiteUrl,
      sessionId,
      startTime: Date.now(),
      stats: {
        sitemapsProcessed: 0,
        webpagesProcessed: 0,
        webpagesSuccessful: 0,
        errors: 0,
        lastActivity: Date.now(),
      },
      limiters: {
        sitemap: new PriorityLimiter(CONFIG.CONCURRENCY.SITEMAP, userId),
        webpage: new PriorityLimiter(
          this.calculateWebpageConcurrency(),
          userId
        ),
      },
    };

    this.userSessions.set(sessionId, session);
    this.globalStats.activeUsers++;

    logger.info(
      `Created session for user ${userId}, total active users: ${this.globalStats.activeUsers}`,
      userId
    );
    return session;
  }

  getSession(sessionId) {
    const session = this.userSessions.get(sessionId);
    if (session) {
      session.stats.lastActivity = Date.now();
    }
    return session;
  }

  endSession(sessionId) {
    const session = this.userSessions.get(sessionId);
    if (session) {
      this.userSessions.delete(sessionId);
      this.globalStats.activeUsers--;
      logger.info(
        `Ended session for user ${session.userId}, remaining active users: ${this.globalStats.activeUsers}`,
        session.userId
      );
    }
  }

  calculateWebpageConcurrency() {
    const activeUsers = Math.max(this.globalStats.activeUsers, 1);
    const baseConcurrency = Math.max(
      1,
      Math.floor(CONFIG.CONCURRENCY.MAX_GLOBAL_WORKERS / activeUsers)
    );
    return Math.min(baseConcurrency, CONFIG.CONCURRENCY.WEBPAGE);
  }

  cleanupStaleSessions() {
    const now = Date.now();
    const staleThreshold = 30 * 60 * 1000;

    for (const [sessionId, session] of this.userSessions.entries()) {
      if (now - session.stats.lastActivity > staleThreshold) {
        this.endSession(sessionId);
      }
    }
  }

  getGlobalStats() {
    // Get rate limiter stats for all domains
    const domainStats = {};
    for (const [domain, data] of globalRateLimiter.domainQueues.entries()) {
      domainStats[domain] = globalRateLimiter.getDomainStats(domain);
    }

    return {
      ...this.globalStats,
      memoryUsage: MemoryManager.checkMemoryUsage(),
      awsProxyStats: awsProxyManager.getStats(),
      pythonProcessPool: pythonProcessPool.getStats(),
      rateLimiterStats: {
        totalDomains: globalRateLimiter.domainQueues.size,
        domains: domainStats,
      },
      activeSessions: Array.from(this.userSessions.values()).map((s) => ({
        userId: s.userId,
        websiteUrl: s.websiteUrl,
        stats: s.stats,
        limiterStats: {
          sitemap: s.limiters.sitemap.getStats(),
          webpage: s.limiters.webpage.getStats(),
        },
      })),
    };
  }
}

const sessionManager = new UserSessionManager();

/**
 * Enhanced concurrency limiter with priority queuing
 */
class PriorityLimiter {
  constructor(concurrency, userId = null) {
    this.concurrency = concurrency;
    this.userId = userId;
    this.running = 0;
    this.queue = [];
  }

  async execute(fn, priority = 1) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, priority, resolve, reject });
      this.queue.sort((a, b) => b.priority - a.priority);
      this.process();
    });
  }

  async process() {
    if (this.running >= this.concurrency || this.queue.length === 0) {
      return;
    }

    this.running++;
    const { fn, resolve, reject } = this.queue.shift();

    try {
      await MemoryManager.waitForMemoryAvailable();
      const result = await fn();
      resolve(result);
    } catch (error) {
      reject(error);
    } finally {
      this.running--;
      this.process();
    }
  }

  getStats() {
    return {
      running: this.running,
      queued: this.queue.length,
      concurrency: this.concurrency,
    };
  }
}

/**
 * Custom concurrency limiter for CommonJS compatibility
 */
function createLimiter(concurrency) {
  const queue = [];
  let activeCount = 0;

  const next = () => {
    activeCount--;
    if (queue.length > 0) {
      const { fn, resolve, reject } = queue.shift();
      run(fn).then(resolve, reject);
    }
  };

  const run = async (fn) => {
    activeCount++;
    try {
      return await fn();
    } finally {
      next();
    }
  };

  const limit = (fn) => {
    if (activeCount < concurrency) {
      return run(fn);
    }

    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
    });
  };

  limit.activeCount = () => activeCount;
  limit.pendingCount = () => queue.length;

  return limit;
}

/**
 * UPDATED webpage scraping with enhanced rate limiting
 */
const scrapeWebpage = async (url, userId = null, retryCount = 0) => {
  let validUrl;
  try {
    validUrl = new URL(url);
    if (
      !validUrl.protocol ||
      !["http:", "https:"].includes(validUrl.protocol)
    ) {
      throw new Error("Invalid URL protocol. Must be http or https.");
    }
  } catch (error) {
    return { url, error: `Invalid URL: ${error.message}` };
  }

  try {
    // Check if domain is blocked before attempting
    if (globalRateLimiter.isDomainBlocked(validUrl.hostname)) {
      logger.warn(`Skipping blocked domain: ${validUrl.hostname}`, userId);
      return {
        url,
        error: `Domain ${validUrl.hostname} is temporarily blocked due to repeated errors`,
      };
    }

    // Use AWS proxy manager with enhanced error handling
    const result = await awsProxyManager.scrapeWebpage(url, userId, retryCount);

    // Ensure websiteUrl and pageUrl are set in the result
    if (result && !result.error) {
      const websiteUrl = `${validUrl.protocol}//${validUrl.hostname}`;
      result.websiteUrl = result.websiteUrl || websiteUrl;
      result.pageUrl = result.pageUrl || result.url || url;
      result.url = result.url || url;

      logger.debug(
        `scrapeWebpage result fields - websiteUrl: ${result.websiteUrl}, pageUrl: ${result.pageUrl}`,
        userId
      );
    }

    return result;
  } catch (error) {
    logger.error(`❌ Failed to scrape ${url}: ${error.message}`, null, userId);
    return { url, error: error.message };
  }
};

/**
 * Route handler for /api/proxy_rotate with enhanced rate limiting
 */
const handleProxyRotateRequest = async (req, res) => {
  try {
    const { url } = req.body;

    // Validate URL
    if (!url) {
      return res.status(400).json({
        error: "URL is required",
      });
    }

    let validUrl;
    try {
      validUrl = new URL(url);
      if (
        !validUrl.protocol ||
        !["http:", "https:"].includes(validUrl.protocol)
      ) {
        throw new Error("Invalid URL protocol. Must be http or https.");
      }
    } catch (error) {
      return res.status(400).json({
        error: `Invalid URL: ${error.message}`,
      });
    }

    // Check if domain is blocked
    if (globalRateLimiter.isDomainBlocked(validUrl.hostname)) {
      return res.status(429).json({
        error: `Domain ${validUrl.hostname} is temporarily blocked due to repeated errors`,
        retryAfter: 300, // 5 minutes
      });
    }

    logger.info(`Processing proxy_rotate request for: ${url}`);

    // Force enable Python processing for AWS proxy server
    pythonProcessPool.enabled = true;
    pythonProcessPool.isAwsProxyServer = true;

    try {
      // Use the local Python process pool with rate limiting
      const result = await pythonProcessPool.executeScript(url);

      if (result.error) {
        return res.status(500).json({
          error: result.error,
          url: url,
        });
      }

      // Extract base website URL from the page URL
      const websiteUrl = `${validUrl.protocol}//${validUrl.hostname}`;

      // Ensure the result includes required fields
      const enrichedResult = {
        ...result,
        websiteUrl: result.websiteUrl || websiteUrl,
        pageUrl: result.pageUrl || url,
        url: url,
        processedAt: new Date().toISOString(),
        server: "aws_proxy",
      };

      // Return successful result
      res.json(enrichedResult);
    } catch (pythonError) {
      logger.error(`Python execution error: ${pythonError.message}`);

      // Check if it's a rate limit error
      if (pythonError.message.includes("temporarily unavailable")) {
        return res.status(429).json({
          error: pythonError.message,
          url: url,
          retryAfter: 300,
        });
      }

      // Return a structured error response
      res.status(500).json({
        error: `Failed to scrape webpage: ${pythonError.message}`,
        url: url,
        details:
          process.env.NODE_ENV === "development"
            ? pythonError.stack
            : undefined,
      });
    }
  } catch (error) {
    logger.error(`Error in proxy_rotate handler: ${error.message}`);
    res.status(500).json({
      error: error.message,
      url: req.body?.url,
    });
  }
};

// Calculate time remaining based on current progress
function calculateTimeRemaining(activity) {
  if (!activity.startTime || activity.progress >= 100) return 0;

  const elapsedTime = Date.now() - new Date(activity.startTime).getTime();
  const progress = Math.max(activity.progress || 1, 1);
  const estimatedTotalTime = (elapsedTime * 100) / progress;
  const estimatedTimeRemaining = estimatedTotalTime - elapsedTime;

  return Math.round(estimatedTimeRemaining / 1000);
}

/**
 * Enhanced website validation with timeout and retry
 */
const validateWebsite = async (websiteUrl, userId = null) => {
  try {
    if (
      !websiteUrl.startsWith("http://") &&
      !websiteUrl.startsWith("https://")
    ) {
      return {
        isValid: false,
        message:
          "Invalid URL format. URL must start with 'http://' or 'https://'",
      };
    }

    let parsedUrl;
    try {
      parsedUrl = new URL(websiteUrl);
    } catch (error) {
      return { isValid: false, message: "Invalid URL structure" };
    }

    // Check if domain is blocked
    if (globalRateLimiter.isDomainBlocked(parsedUrl.hostname)) {
      return {
        isValid: false,
        message: `Domain ${parsedUrl.hostname} is temporarily blocked. Please try again later.`,
      };
    }

    await globalRateLimiter.acquire(parsedUrl.hostname);

    try {
      await axios.head(websiteUrl, {
        timeout: CONFIG.TIMEOUTS.URL_CHECK,
        maxRedirects: 3,
        validateStatus: (status) => status < 500,
      });

      // Report success for validation
      globalRateLimiter.reportSuccess(parsedUrl.hostname);
    } catch (error) {
      globalRateLimiter.reportError(parsedUrl.hostname, "validation");
      return { isValid: false, message: "Website is not accessible" };
    }

    const sitemapUrls = await findSitemap(websiteUrl, userId);

    if (sitemapUrls.error) {
      return { isValid: false, message: sitemapUrls.error };
    }

    if (Array.isArray(sitemapUrls) && sitemapUrls.length === 0) {
      return { isValid: false, message: "No sitemaps found for this website" };
    }

    return { isValid: true, sitemapUrls };
  } catch (error) {
    logger.error("Error validating website", error, userId);
    return {
      isValid: false,
      message: "Error validating website: " + error.message,
    };
  }
};

/**
 * Enhanced sitemap discovery with rate limiting
 */
const findSitemap = async (websiteUrl, userId = null) => {
  try {
    if (
      !websiteUrl.startsWith("http://") &&
      !websiteUrl.startsWith("https://")
    ) {
      return {
        error:
          "Invalid URL format. URL must start with 'http://' or 'https://'",
      };
    }

    const parsedUrl = new URL(websiteUrl);
    const robotsTxtUrl = `${parsedUrl.origin}/robots.txt`;
    const headers = {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    };

    let sitemapUrls = new Set();

    await globalRateLimiter.acquire(parsedUrl.hostname);

    try {
      const response = await axios.get(robotsTxtUrl, {
        headers,
        timeout: CONFIG.TIMEOUTS.URL_CHECK,
        validateStatus: (status) => status < 500,
      });

      if (response.status === 200) {
        const lines = response.data.split("\n");
        lines
          .filter((line) => line.toLowerCase().startsWith("sitemap:"))
          .map((line) => line.replace(/sitemap:/i, "").trim())
          .filter((url) => url.startsWith("http"))
          .forEach((url) => sitemapUrls.add(url));

        globalRateLimiter.reportSuccess(parsedUrl.hostname);
      }
    } catch (error) {
      logger.warn(
        "robots.txt not accessible, checking alternative locations...",
        userId
      );
      globalRateLimiter.reportError(parsedUrl.hostname, "robots_txt");
    }

    const extraSitemapLocations = [
      `${parsedUrl.origin}/sitemap.xml`,
      `${parsedUrl.origin}/sitemap_index.xml`,
      `${parsedUrl.origin}/sitemaps/sitemap.xml`,
      `${parsedUrl.origin}/sitemap1.xml`,
      `${parsedUrl.origin}/feeds/sitemap.xml`,
      `${parsedUrl.origin}/sitemap-pages.xml`,
      `${parsedUrl.origin}/xmlsitemap.php`,
    ];

    // Check sitemaps with reduced concurrency
    const sitemapLimiter = createLimiter(2); // Only 2 concurrent checks
    const sitemapChecks = extraSitemapLocations.map((sitemapUrl) =>
      sitemapLimiter(async () => {
        try {
          await globalRateLimiter.acquire(parsedUrl.hostname);
          await axios.head(sitemapUrl, {
            headers,
            timeout: CONFIG.TIMEOUTS.URL_CHECK,
            validateStatus: (status) => status === 200,
          });
          globalRateLimiter.reportSuccess(parsedUrl.hostname);
          return sitemapUrl;
        } catch (error) {
          return null;
        }
      })
    );

    const validSitemaps = (await Promise.all(sitemapChecks)).filter(Boolean);
    validSitemaps.forEach((url) => sitemapUrls.add(url));

    return Array.from(sitemapUrls);
  } catch (error) {
    logger.error("Error fetching sitemap", error, userId);
    return { error: error.message };
  }
};

const updateActivityProgress = async (
  userActivityId,
  updates,
  userId = null
) => {
  try {
    if (updates.progress !== undefined) {
      const activity = await UserActivity.findById(userActivityId);
      if (activity) {
        updates.timeRemaining = calculateTimeRemaining({
          ...activity.toObject(),
          ...updates,
        });
      }
    }

    const updatedActivity = await UserActivity.findByIdAndUpdate(
      userActivityId,
      {
        ...updates,
        lastUpdated: new Date(),
      },
      { new: true }
    );

    if (updatedActivity && userId) {
      // Emit real-time update via socket
      socketService.emitActivityUpdate(userId, {
        activityId: userActivityId,
        websiteUrl: updatedActivity.websiteUrl,
        status: updatedActivity.status,
        progress: updatedActivity.progress || 0,
        isSitemapCrawling: updatedActivity.isSitemapCrawling,
        isWebpageCrawling: updatedActivity.isWebpageCrawling,
        isBacklinkFetching: updatedActivity.isBacklinkFetching,
        sitemapCount: updatedActivity.sitemapCount || 0,
        webpageCount: updatedActivity.webpageCount || 0,
        webpagesSuccessful: updatedActivity.webpagesSuccessful || 0,
        webpagesFailed: updatedActivity.webpagesFailed || 0,
        startTime: updatedActivity.startTime,
        endTime: updatedActivity.endTime,
        estimatedTimeRemaining: updates.timeRemaining || 0,
        errorMessages: updatedActivity.errorMessages || [],
      });

      if (redisClient) {
        try {
          await redisClient.setex(
            `activity:${userActivityId}`,
            300,
            JSON.stringify(updatedActivity)
          );
        } catch (redisError) {
          logger.warn("Redis caching failed", userId);
        }
      }
    }

    return updatedActivity;
  } catch (error) {
    logger.error("Error updating activity progress", error, userId);
    return null;
  }
};

/**
 * Data sanitization function to ensure all fields have proper default values
 */
function sanitizeWebpageData(data) {
  const safeString = (value) => {
    if (typeof value === "string") return value;
    if (typeof value === "object" && value !== null) {
      return String(value.text || value.content || "");
    }
    return String(value || "");
  };

  const safeNumber = (value) => {
    if (typeof value === "number" && !isNaN(value)) return value;
    const parsed = parseInt(value) || 0;
    return isNaN(parsed) ? 0 : parsed;
  };

  const safeArray = (value) => {
    return Array.isArray(value) ? value : [];
  };

  const safeObject = (value, defaultObj = {}) => {
    return typeof value === "object" && value !== null ? value : defaultObj;
  };

  // Ensure we have at least some URL data
  const pageUrl = safeString(data.pageUrl) || safeString(data.url);
  let websiteUrl = safeString(data.websiteUrl);

  if (!websiteUrl && pageUrl) {
    try {
      const url = new URL(pageUrl);
      websiteUrl = `${url.protocol}//${url.hostname}`;
    } catch (e) {
      websiteUrl = pageUrl;
    }
  }

  // Extract and sanitize title and meta description early
  const title = safeString(data.title);
  const metaDescription = safeString(data.metaDescription);

  return {
    ...data,

    // Ensure websiteUrl and pageUrl are present
    websiteUrl: websiteUrl || pageUrl || "unknown",
    pageUrl: pageUrl || "unknown",

    // Critical fields that need .length property - ensure they're strings
    title: title,
    titleLength: title.length,
    titleScore: safeNumber(data.titleScore),

    metaDescription: metaDescription,
    metaDescriptionLength: metaDescription.length,
    metaDescriptionScore: safeNumber(data.metaDescriptionScore),

    // Content fields
    contentScore: safeNumber(data.contentScore),
    wordCount: safeNumber(data.wordCount),
    readabilityScore: safeNumber(data.readabilityScore),

    urlScore: safeNumber(data.urlScore),

    // Issues objects with safe defaults
    titleIssues: safeObject(data.titleIssues, {
      tooShort: title.length < 30,
      tooLong: title.length > 60,
      missing: title.length === 0,
      duplicate: false,
      multiple: false,
    }),

    metaDescriptionIssues: safeObject(data.metaDescriptionIssues, {
      tooShort: metaDescription.length < 120,
      tooLong: metaDescription.length > 160,
      missing: metaDescription.length === 0,
      duplicate: false,
      multiple: false,
    }),

    contentIssues: safeObject(data.contentIssues, {
      tooShort: safeNumber(data.wordCount) < 300,
      lowKeywordDensity: false,
      poorReadability: safeNumber(data.readabilityScore) < 60,
    }),

    // Heading structure with safe numbers
    headingStructure: {
      h1Count: safeNumber(data.headingStructure?.h1Count),
      h2Count: safeNumber(data.headingStructure?.h2Count),
      h3Count: safeNumber(data.headingStructure?.h3Count),
      h4Count: safeNumber(data.headingStructure?.h4Count),
      h5Count: safeNumber(data.headingStructure?.h5Count),
      h6Count: safeNumber(data.headingStructure?.h6Count),
      h1Missing: safeNumber(data.headingStructure?.h1Count) === 0,
      h1Multiple: safeNumber(data.headingStructure?.h1Count) > 1,
      h2H3AtTop: false,
      headingScore: safeNumber(data.headingStructure?.headingScore),
    },

    urlIssues: safeObject(data.urlIssues, {
      tooLong: pageUrl.length > 100,
      containsSpecialChars: /[^a-zA-Z0-9\-_\/\.]/.test(pageUrl),
      containsParams: pageUrl.includes("?"),
      nonDescriptive: false,
    }),

    technicalSeo: {
      canonicalTagExists: Boolean(data.technicalSeo?.canonicalTagExists),
      canonicalUrl: safeString(data.technicalSeo?.canonicalUrl),
      robotsDirectives: safeString(data.technicalSeo?.robotsDirectives),
      hreflangTags: safeArray(data.technicalSeo?.hreflangTags),
      structuredData: Boolean(data.technicalSeo?.structuredData),
      structuredDataTypes: safeArray(data.technicalSeo?.structuredDataTypes),
      technicalScore: safeNumber(data.technicalSeo?.technicalScore),
    },

    images: {
      count: safeNumber(data.images?.count),
      optimizedCount: safeNumber(data.images?.optimizedCount),
      altTextMissing: safeArray(data.images?.altTextMissing),
      largeImages: safeArray(data.images?.largeImages),
      imageScore: safeNumber(data.images?.imageScore),
    },

    links: {
      internalCount: safeNumber(data.links?.internalCount),
      externalCount: safeNumber(data.links?.externalCount),
      brokenLinks: safeArray(data.links?.brokenLinks),
      httpLinks: safeArray(data.links?.httpLinks),
      redirectLinks: safeArray(data.links?.redirectLinks),
      linkScore: safeNumber(data.links?.linkScore),
    },

    performance: {
      mobileResponsive: Boolean(data.performance?.mobileResponsive),
      mobileIssues: safeArray(data.performance?.mobileIssues),
      pageSpeedScore: safeNumber(data.performance?.pageSpeedScore),
      coreWebVitals: {
        LCP: safeNumber(data.performance?.coreWebVitals?.LCP),
        FID: safeNumber(data.performance?.coreWebVitals?.FID),
        CLS: safeNumber(data.performance?.coreWebVitals?.CLS),
      },
      performanceScore: safeNumber(data.performance?.performanceScore),
    },

    contentQuality: {
      spellingErrors: safeArray(data.contentQuality?.spellingErrors),
      grammarErrors: safeArray(data.contentQuality?.grammarErrors),
      duplicateContent: safeArray(data.contentQuality?.duplicateContent),
      contentQualityScore: safeNumber(data.contentQuality?.contentQualityScore),
    },

    duplicates: {
      titleDuplicates: safeArray(data.duplicates?.titleDuplicates),
      descriptionDuplicates: safeArray(data.duplicates?.descriptionDuplicates),
      duplicateScore: safeNumber(data.duplicates?.duplicateScore) || 10,
    },

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

    // Initialize scores to prevent undefined errors
    seoScore: safeNumber(data.seoScore),
    seoGrade: safeString(data.seoGrade) || "F",
    individualScores: safeObject(data.individualScores, {}),
  };
}

/**
 * Validates that all required fields for calculateSeoScore() exist and are properly typed
 */
function validateSeoScoreData(webpage) {
  const errors = [];

  // Check critical string fields that need .length
  if (typeof webpage.title !== "string") {
    errors.push(`title is ${typeof webpage.title}, expected string`);
    webpage.title = String(webpage.title || "");
  }

  if (typeof webpage.metaDescription !== "string") {
    errors.push(
      `metaDescription is ${typeof webpage.metaDescription}, expected string`
    );
    webpage.metaDescription = String(webpage.metaDescription || "");
  }

  // Check number fields
  const numberFields = [
    "titleLength",
    "metaDescriptionLength",
    "wordCount",
    "titleScore",
    "metaDescriptionScore",
    "contentScore",
    "readabilityScore",
    "urlScore",
  ];

  numberFields.forEach((field) => {
    if (typeof webpage[field] !== "number" || isNaN(webpage[field])) {
      errors.push(`${field} is ${typeof webpage[field]}, expected number`);
      webpage[field] = 0;
    }
  });

  // Ensure lengths match actual string lengths
  webpage.titleLength = webpage.title.length;
  webpage.metaDescriptionLength = webpage.metaDescription.length;

  // Check nested objects exist
  if (
    !webpage.headingStructure ||
    typeof webpage.headingStructure !== "object"
  ) {
    errors.push("headingStructure missing or invalid");
    webpage.headingStructure = {
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
    };
  }

  if (!webpage.images || typeof webpage.images !== "object") {
    errors.push("images missing or invalid");
    webpage.images = {
      count: 0,
      optimizedCount: 0,
      altTextMissing: [],
      largeImages: [],
      imageScore: 0,
    };
  }

  if (!webpage.links || typeof webpage.links !== "object") {
    errors.push("links missing or invalid");
    webpage.links = {
      internalCount: 0,
      externalCount: 0,
      brokenLinks: [],
      httpLinks: [],
      redirectLinks: [],
      linkScore: 0,
    };
  }

  if (!webpage.technicalSeo || typeof webpage.technicalSeo !== "object") {
    errors.push("technicalSeo missing or invalid");
    webpage.technicalSeo = {
      canonicalTagExists: false,
      canonicalUrl: "",
      robotsDirectives: "",
      hreflangTags: [],
      structuredData: false,
      structuredDataTypes: [],
      technicalScore: 0,
    };
  }

  if (!webpage.performance || typeof webpage.performance !== "object") {
    errors.push("performance missing or invalid");
    webpage.performance = {
      mobileResponsive: false,
      mobileIssues: [],
      pageSpeedScore: 0,
      coreWebVitals: { LCP: 0, FID: 0, CLS: 0 },
      performanceScore: 0,
    };
  }

  if (!webpage.contentQuality || typeof webpage.contentQuality !== "object") {
    errors.push("contentQuality missing or invalid");
    webpage.contentQuality = {
      spellingErrors: [],
      grammarErrors: [],
      duplicateContent: [],
      contentQualityScore: 0,
    };
  }

  if (!webpage.duplicates || typeof webpage.duplicates !== "object") {
    errors.push("duplicates missing or invalid");
    webpage.duplicates = {
      titleDuplicates: [],
      descriptionDuplicates: [],
      duplicateScore: 10,
    };
  }

  return errors;
}

/**
 * Enhanced webpage data saving with comprehensive error handling
 */
async function saveWebpageData(scrapedData, userId, userActivityId) {
  const startTime = Date.now();

  try {
    if (!scrapedData || scrapedData.error) {
      logger.error(
        `Error in scraped data for ${
          scrapedData?.pageUrl || scrapedData?.url || "unknown URL"
        }: ${scrapedData?.error}`,
        null,
        userId
      );
      return null;
    }

    // Ensure websiteUrl is present
    if (!scrapedData.websiteUrl && (scrapedData.pageUrl || scrapedData.url)) {
      try {
        const pageUrl = new URL(scrapedData.pageUrl || scrapedData.url);
        scrapedData.websiteUrl = `${pageUrl.protocol}//${pageUrl.hostname}`;
      } catch (urlError) {
        logger.error(
          `Invalid pageUrl: ${scrapedData.pageUrl || scrapedData.url}`,
          urlError,
          userId
        );
        return null;
      }
    }

    // Ensure pageUrl is present
    if (!scrapedData.pageUrl && scrapedData.url) {
      scrapedData.pageUrl = scrapedData.url;
    }

    // Debug log to understand the data structure
    logger.debug(
      `saveWebpageData received - websiteUrl: ${scrapedData.websiteUrl}, pageUrl: ${scrapedData.pageUrl}, url: ${scrapedData.url}`,
      userId
    );

    await MemoryManager.waitForMemoryAvailable();

    let webpage = await Webpage.findOne({ pageUrl: scrapedData.pageUrl });
    let mappedData;

    try {
      mappedData = mapScrapedDataToWebpageModel(
        scrapedData,
        userId,
        userActivityId
      );

      // Force convert any object title/metaDescription to strings early
      if (typeof mappedData.title === "object") {
        mappedData.title = mappedData.title?.text || "";
      }
      if (typeof mappedData.metaDescription === "object") {
        mappedData.metaDescription = mappedData.metaDescription?.text || "";
      }

      mappedData = sanitizeWebpageData(mappedData);
    } catch (mappingError) {
      logger.error(
        `Error mapping data for ${scrapedData.pageUrl || scrapedData.url}:`,
        mappingError,
        userId
      );
      return null;
    }

    if (webpage) {
      Object.assign(webpage, mappedData);
    } else {
      // Ensure required fields are strings before creating new instance
      if (typeof mappedData.title === "object") {
        mappedData.title = String(mappedData.title?.text || "");
      }
      if (typeof mappedData.metaDescription === "object") {
        mappedData.metaDescription = String(
          mappedData.metaDescription?.text || ""
        );
      }
      if (!mappedData.websiteUrl && mappedData.pageUrl) {
        try {
          const url = new URL(mappedData.pageUrl);
          mappedData.websiteUrl = `${url.protocol}//${url.hostname}`;
        } catch (e) {
          mappedData.websiteUrl = mappedData.pageUrl;
        }
      }
      webpage = new Webpage(mappedData);
    }

    // Comprehensive validation before calculateSeoScore
    try {
      const validationErrors = validateSeoScoreData(webpage);
      if (validationErrors.length > 0) {
        logger.debug(
          `Fixed validation issues for ${
            scrapedData.pageUrl
          }: ${validationErrors.join(", ")}`,
          userId
        );
      }

      // Double-check critical fields right before calculation
      if (!webpage.title || typeof webpage.title !== "string") {
        webpage.title = "";
        webpage.titleLength = 0;
      } else {
        webpage.titleLength = webpage.title.length;
      }

      if (
        !webpage.metaDescription ||
        typeof webpage.metaDescription !== "string"
      ) {
        webpage.metaDescription = "";
        webpage.metaDescriptionLength = 0;
      } else {
        webpage.metaDescriptionLength = webpage.metaDescription.length;
      }

      // Ensure arrays exist for methods that might call .length
      if (!Array.isArray(webpage.contentQuality?.spellingErrors)) {
        webpage.contentQuality.spellingErrors = [];
      }
      if (!Array.isArray(webpage.contentQuality?.grammarErrors)) {
        webpage.contentQuality.grammarErrors = [];
      }
      if (!Array.isArray(webpage.links?.brokenLinks)) {
        webpage.links.brokenLinks = [];
      }

      webpage.calculateSeoScore();
    } catch (scoreError) {
      logger.warn(
        `⚠️ Error calculating SEO score for ${
          scrapedData.pageUrl || scrapedData.url
        }, using default score: ${scoreError.message}`,
        userId
      );

      // Set safe defaults
      webpage.seoScore = 0;
      webpage.seoGrade = "F";
      webpage.individualScores = {
        titleScore: webpage.titleScore || 0,
        metaDescriptionScore: webpage.metaDescriptionScore || 0,
        contentScore: webpage.contentScore || 0,
        headingScore: webpage.headingStructure?.headingScore || 0,
        urlScore: webpage.urlScore || 0,
        technicalScore: webpage.technicalSeo?.technicalScore || 0,
        imageScore: webpage.images?.imageScore || 0,
        linkScore: webpage.links?.linkScore || 0,
        performanceScore: webpage.performance?.performanceScore || 0,
        contentQualityScore: webpage.contentQuality?.contentQualityScore || 0,
      };
    }

    try {
      await webpage.save();
      const processingTime = Date.now() - startTime;
      logger.debug(
        `Successfully saved webpage: ${
          scrapedData.pageUrl || scrapedData.url
        } (${processingTime}ms)`,
        userId
      );
      return webpage;
    } catch (saveError) {
      if (saveError.name === "ValidationError") {
        for (const field in saveError.errors) {
          const error = saveError.errors[field];
          logger.warn(
            `Validation error for field ${field}: ${error.message}. Auto-fixing...`,
            userId
          );

          if (field === "title") {
            webpage.title = String(webpage.title?.text || webpage.title || "");
            webpage.titleLength = webpage.title.length;
          } else if (field === "metaDescription") {
            webpage.metaDescription = String(
              webpage.metaDescription?.text || webpage.metaDescription || ""
            );
            webpage.metaDescriptionLength = webpage.metaDescription.length;
          } else if (field === "websiteUrl" && !webpage.websiteUrl) {
            if (webpage.pageUrl) {
              try {
                const url = new URL(webpage.pageUrl);
                webpage.websiteUrl = `${url.protocol}//${url.hostname}`;
              } catch (e) {
                webpage.websiteUrl = webpage.pageUrl;
              }
            } else {
              webpage.websiteUrl = "unknown";
            }
          } else if (field === "pageUrl" && !webpage.pageUrl) {
            webpage.pageUrl =
              scrapedData?.url ||
              scrapedData?.pageUrl ||
              webpage.url ||
              "unknown";
          } else if (
            field.includes("Length") &&
            typeof webpage[field] !== "number"
          ) {
            webpage[field] = 0;
          } else if (
            field.includes("Score") &&
            typeof webpage[field] !== "number"
          ) {
            webpage[field] = 0;
          } else if (
            field.includes("Count") &&
            typeof webpage[field] !== "number"
          ) {
            webpage[field] = 0;
          }
        }

        const sanitized = sanitizeWebpageData(webpage.toObject());
        Object.assign(webpage, sanitized);

        await webpage.save();
        const processingTime = Date.now() - startTime;
        logger.info(
          `Fixed and saved webpage after validation error: ${
            scrapedData.pageUrl || scrapedData.url
          } (${processingTime}ms)`,
          userId
        );
        return webpage;
      } else {
        throw saveError;
      }
    }
  } catch (error) {
    const processingTime = Date.now() - startTime;
    logger.error(
      `Error saving webpage data for ${
        scrapedData?.pageUrl || scrapedData?.url || "unknown URL"
      } (${processingTime}ms)`,
      error,
      userId
    );
    return null;
  }
}

/**
 * Enhanced data mapping with comprehensive error handling and safe defaults
 */
function mapScrapedDataToWebpageModel(scrapedData, userId, userActivityId) {
  const safeString = (value) => {
    if (typeof value === "string") return value;
    if (typeof value === "object" && value !== null) {
      return String(value.text || value.content || "");
    }
    return String(value || "");
  };

  const safeNumber = (value) => {
    if (typeof value === "number" && !isNaN(value)) return value;
    const parsed = parseInt(value) || 0;
    return isNaN(parsed) ? 0 : parsed;
  };

  const safeArray = (value) => {
    return Array.isArray(value) ? value : [];
  };

  const safeObject = (value, defaultObj = {}) => {
    return typeof value === "object" && value !== null ? value : defaultObj;
  };

  // Ensure we have at least pageUrl
  if (!scrapedData.pageUrl && !scrapedData.url) {
    throw new Error("No URL found in scraped data");
  }

  const title = safeString(scrapedData.title);
  const metaDescription = safeString(scrapedData.metaDescription);

  // Ensure websiteUrl is available
  let websiteUrl = safeString(scrapedData.websiteUrl);
  const pageUrl = safeString(scrapedData.pageUrl || scrapedData.url);

  if (!websiteUrl && pageUrl) {
    try {
      const url = new URL(pageUrl);
      websiteUrl = `${url.protocol}//${url.hostname}`;
    } catch (e) {
      logger.warn(
        `Could not extract websiteUrl from pageUrl: ${pageUrl}`,
        userId
      );
      websiteUrl = pageUrl; // Fallback to pageUrl
    }
  }

  return {
    userId,
    userActivityId,
    websiteUrl: websiteUrl,
    pageUrl: pageUrl,
    lastCrawled: scrapedData.lastCrawled || new Date(),

    title: title,
    titleLength: title.length,
    titleScore: safeNumber(scrapedData.titleScore),
    titleIssues: safeObject(scrapedData.titleIssues, {
      tooShort: false,
      tooLong: false,
      missing: !title,
      duplicate: false,
      multiple: false,
    }),

    metaDescription: metaDescription,
    metaDescriptionLength: metaDescription.length,
    metaDescriptionScore: safeNumber(scrapedData.metaDescriptionScore),
    metaDescriptionIssues: safeObject(scrapedData.metaDescriptionIssues, {
      tooShort: false,
      tooLong: false,
      missing: !metaDescription,
      duplicate: false,
      multiple: false,
    }),

    contentScore: safeNumber(scrapedData.contentScore),
    contentIssues: safeObject(scrapedData.contentIssues, {
      tooShort: true,
      lowKeywordDensity: false,
      poorReadability: false,
    }),
    wordCount: safeNumber(scrapedData.wordCount),
    readabilityScore: safeNumber(scrapedData.readabilityScore),

    headingStructure: safeObject(scrapedData.headingStructure, {
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
    }),

    urlScore: safeNumber(scrapedData.urlScore),
    urlIssues: safeObject(scrapedData.urlIssues, {
      tooLong: false,
      containsSpecialChars: false,
      containsParams: false,
      nonDescriptive: false,
    }),

    technicalSeo: safeObject(scrapedData.technicalSeo, {
      canonicalTagExists: false,
      canonicalUrl: "",
      robotsDirectives: "",
      hreflangTags: safeArray(scrapedData.technicalSeo?.hreflangTags),
      structuredData: false,
      structuredDataTypes: safeArray(
        scrapedData.technicalSeo?.structuredDataTypes
      ),
      technicalScore: 0,
    }),

    images: safeObject(scrapedData.images, {
      count: 0,
      optimizedCount: 0,
      altTextMissing: safeArray(scrapedData.images?.altTextMissing),
      largeImages: safeArray(scrapedData.images?.largeImages),
      imageScore: 0,
    }),

    links: safeObject(scrapedData.links, {
      internalCount: 0,
      externalCount: 0,
      brokenLinks: safeArray(scrapedData.links?.brokenLinks),
      httpLinks: safeArray(scrapedData.links?.httpLinks),
      redirectLinks: safeArray(scrapedData.links?.redirectLinks),
      linkScore: 0,
    }),

    performance: safeObject(scrapedData.performance, {
      mobileResponsive: false,
      mobileIssues: safeArray(scrapedData.performance?.mobileIssues),
      pageSpeedScore: 0,
      coreWebVitals: safeObject(scrapedData.performance?.coreWebVitals, {
        LCP: 0,
        FID: 0,
        CLS: 0,
      }),
      performanceScore: 0,
    }),

    contentQuality: safeObject(scrapedData.contentQuality, {
      spellingErrors: safeArray(scrapedData.contentQuality?.spellingErrors),
      grammarErrors: safeArray(scrapedData.contentQuality?.grammarErrors),
      duplicateContent: safeArray(scrapedData.contentQuality?.duplicateContent),
      contentQualityScore: 0,
    }),

    duplicates: safeObject(scrapedData.duplicates, {
      titleDuplicates: safeArray(scrapedData.duplicates?.titleDuplicates),
      descriptionDuplicates: safeArray(
        scrapedData.duplicates?.descriptionDuplicates
      ),
      duplicateScore: 10,
    }),

    seoScoreComponents: safeObject(scrapedData.seoScoreComponents, {
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
    }),
  };
}

/**
 * Enhanced sitemap processing with rate limiting
 */
async function processSitemap(
  sitemapUrl,
  userActivityId,
  parentSitemapId = null,
  userId = null
) {
  try {
    const parsedUrl = new URL(sitemapUrl);
    const domain = parsedUrl.hostname;

    // Check if domain is blocked
    if (globalRateLimiter.isDomainBlocked(domain)) {
      logger.warn(`Skipping sitemap from blocked domain: ${domain}`, userId);
      return {
        sitemapUrl,
        success: false,
        error: `Domain ${domain} is temporarily blocked`,
        childSitemaps: [],
        webpages: [],
      };
    }

    const sitemapData = {
      url: sitemapUrl,
      urlType: 0,
      userActivityId,
      status: 1,
    };

    if (parentSitemapId) {
      sitemapData.parentSitemaps = [parentSitemapId];
    }

    let sitemap = await Sitemap.findOneAndUpdate(
      { url: sitemapUrl, urlType: 0 },
      sitemapData,
      { upsert: true, new: true }
    );

    await globalRateLimiter.acquire(domain);

    const sitemapFetcher = new Sitemapper({
      url: sitemapUrl,
      timeout: CONFIG.TIMEOUTS.SITEMAP_FETCH,
    });

    const result = await sitemapFetcher.fetch();
    const sites = result.sites || [];

    // Report success
    globalRateLimiter.reportSuccess(domain);

    const childSitemaps = [];
    const webpages = [];

    sites.forEach((site) => {
      try {
        const url = new URL(site);
        if (
          site.includes("sitemap") &&
          (site.endsWith(".xml") || site.endsWith(".xml.gz"))
        ) {
          childSitemaps.push(site);
        } else {
          webpages.push(site);
        }
      } catch (urlError) {
        logger.warn(`Invalid URL found in sitemap: ${site}`, userId);
      }
    });

    return {
      sitemapId: sitemap._id,
      childSitemaps,
      webpages,
      success: true,
    };
  } catch (error) {
    logger.error(`Error processing sitemap ${sitemapUrl}`, error, userId);

    // Report error to rate limiter
    try {
      const parsedUrl = new URL(sitemapUrl);
      globalRateLimiter.reportError(parsedUrl.hostname, "sitemap");
    } catch (e) {
      // Ignore URL parsing errors
    }

    return {
      sitemapUrl,
      success: false,
      error: error.message,
      childSitemaps: [],
      webpages: [],
    };
  }
}

/**
 * Enhanced webpage processing with better error recovery and rate limiting
 */
async function processAndSaveWebpages(
  webpageUrls,
  userId,
  userActivityId,
  websiteUrl,
  concurrency = CONFIG.CONCURRENCY.WEBPAGE
) {
  if (!Array.isArray(webpageUrls)) {
    throw new Error("webpageUrls must be an array");
  }

  if (webpageUrls.length === 0) {
    return {
      success: true,
      message: "No webpages to process",
      processed: 0,
      failed: 0,
      saved: 0,
      total: 0,
    };
  }

  const adjustedConcurrency = Math.max(
    1,
    Math.min(concurrency, sessionManager.calculateWebpageConcurrency())
  );
  const limit = createLimiter(adjustedConcurrency);

  const stats = {
    processed: 0,
    failed: 0,
    saved: 0,
    total: webpageUrls.length,
  };

  logger.info(
    `Starting to process ${webpageUrls.length} webpages with adjusted concurrency of ${adjustedConcurrency} using AWS proxy for website: ${websiteUrl}`,
    userId
  );

  const batchSize = Math.min(
    CONFIG.BATCH_SIZE,
    Math.ceil(webpageUrls.length / 10)
  );

  for (let i = 0; i < webpageUrls.length; i += batchSize) {
    const batch = webpageUrls.slice(i, i + batchSize);

    await MemoryManager.waitForMemoryAvailable();

    const promises = batch.map((url, index) => {
      return limit(async () => {
        try {
          const memoryUsage = MemoryManager.checkMemoryUsage();
          if (memoryUsage.heapUsedMB > CONFIG.MEMORY.HEAP_LIMIT_MB * 0.9) {
            await MemoryManager.forceCleanup();
          }

          if (
            (i + index) % 25 === 0 ||
            index === 0 ||
            index === batch.length - 1
          ) {
            logger.info(
              `Processing webpage ${i + index + 1}/${
                webpageUrls.length
              } via AWS proxy: ${url}`,
              userId
            );

            await updateActivityProgress(
              userActivityId,
              {
                progress:
                  50 + Math.round(((i + index) / webpageUrls.length) * 50),
                webpagesSuccessful: stats.saved,
                webpagesFailed: stats.failed,
                isWebpageCrawling: 1,
              },
              userId
            );
          }

          let result;
          try {
            result = await scrapeWebpage(url, userId);

            // Ensure websiteUrl is set in the result
            if (result && !result.error) {
              result.websiteUrl = result.websiteUrl || websiteUrl;
              result.pageUrl = result.pageUrl || result.url || url;
            }
          } catch (scrapeError) {
            logger.warn(
              `AWS proxy scraping failed for ${url}: ${scrapeError.message}`,
              userId
            );
            result = { url, error: scrapeError.message };
          }

          if (!result.error) {
            try {
              const savedWebpage = await saveWebpageData(
                result,
                userId,
                userActivityId
              );
              if (savedWebpage) {
                stats.saved++;
                if (stats.saved % 10 === 0) {
                  await updateActivityProgress(
                    userActivityId,
                    {
                      webpagesSuccessful: stats.saved,
                      isWebpageCrawling: 1,
                    },
                    userId
                  );
                }
              } else {
                stats.failed++;
                logger.warn(`Failed to save webpage data for ${url}`, userId);
              }
            } catch (saveError) {
              stats.failed++;
              logger.error(
                `Database save error for ${url}: ${saveError.message}`,
                null,
                userId
              );
            }
          } else {
            stats.failed++;
          }

          if (result.error) {
            logger.warn(`Failed to process ${url}: ${result.error}`, userId);
          } else {
            stats.processed++;
          }

          return result;
        } catch (error) {
          stats.failed++;
          logger.error(`Exception while processing ${url}`, error, userId);
          return { url, error: error.message };
        }
      });
    });

    const results = await Promise.allSettled(promises);

    results.forEach((result, index) => {
      if (result.status === "rejected") {
        logger.error(
          `Promise rejected for batch item ${index}: ${result.reason}`,
          null,
          userId
        );
        stats.failed++;
      }
    });

    // Add a delay between batches to avoid overwhelming the server
    if (i + batchSize < webpageUrls.length) {
      const delay = Math.min(5000, batchSize * 100); // Max 5 second delay
      logger.debug(`Waiting ${delay}ms before next batch...`, userId);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    if (i % (batchSize * 3) === 0) {
      await MemoryManager.forceCleanup();
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  logger.info(
    `Completed processing ${stats.processed} webpages successfully, ${stats.failed} failed, ${stats.saved} saved to database via AWS proxy`,
    userId
  );

  return {
    success: true,
    message: "Webpage processing and saving completed via AWS proxy",
    ...stats,
  };
}

async function processAllSitemapsAndWebpages(
  sitemapUrls,
  userId,
  websiteUrl,
  options = {}
) {
  const {
    sitemapConcurrency = CONFIG.CONCURRENCY.SITEMAP,
    webpageConcurrency = CONFIG.CONCURRENCY.WEBPAGE,
  } = options;

  if (
    sessionManager.globalStats.activeUsers >=
    CONFIG.CONCURRENCY.MAX_CONCURRENT_USERS
  ) {
    return {
      error: "Maximum concurrent users limit reached. Please try again later.",
      waitTime: 300,
    };
  }

  const session = sessionManager.createSession(userId, websiteUrl);

  try {
    // Find existing or create new UserActivity using the updated method
    let userActivity = await UserActivity.findOne({ userId, websiteUrl });

    if (!userActivity) {
      userActivity = await UserActivity.create({
        userId,
        websiteUrl,
        isSitemapCrawling: 1,
        isWebpageCrawling: 0,
        isBacklinkFetching: 0,
        startTime: new Date(),
        lastCrawlStarted: new Date(),
        status: "processing",
        sitemapCount: 0,
        webpageCount: 0,
        webpagesSuccessful: 0,
        webpagesFailed: 0,
        progress: 0,
        crawlCount: 1,
        backlinkSummaryStatus: "pending",
      });
    } else {
      // Update existing activity for new crawl
      userActivity.isSitemapCrawling = 1;
      userActivity.isWebpageCrawling = 0;
      userActivity.isBacklinkFetching = 0;
      userActivity.startTime = new Date();
      userActivity.lastCrawlStarted = new Date();
      userActivity.endTime = undefined;
      userActivity.status = "processing";
      userActivity.progress = 0;
      userActivity.sitemapCount = 0;
      userActivity.webpageCount = 0;
      userActivity.webpagesSuccessful = 0;
      userActivity.webpagesFailed = 0;
      userActivity.estimatedTimeRemaining = 0;
      userActivity.estimatedTotalUrls = 0;
      userActivity.errorMessages = [];
      userActivity.crawlCount = (userActivity.crawlCount || 0) + 1;

      if (!userActivity.backlinkSummaryId) {
        userActivity.backlinkSummaryStatus = "pending";
        userActivity.backlinkError = undefined;
      }

      await userActivity.save();
    }

    const userActivityId = userActivity._id;

    // Emit crawl started event
    socketService.emitCrawlStarted(userId, {
      activityId: userActivityId,
      websiteUrl,
      status: "processing",
      sitemapCount: sitemapUrls.length,
      crawlCount: userActivity.crawlCount,
    });

    let totalSitemapCount = 0;
    let totalWebpageCount = 0;
    let errors = [];
    let allWebpageUrls = new Set();

    const estimatedTotalUrls = sitemapUrls.length * 500;

    await updateActivityProgress(
      userActivityId,
      {
        sitemapCount: sitemapUrls.length,
        estimatedTotalUrls,
        status: "processing",
      },
      userId
    );

    logger.info(`Starting to process ${sitemapUrls.length} sitemaps`, userId);

    // ========== START BACKLINK SUMMARY FETCHING (EARLY IN PROCESS) ==========
    logger.info(`Starting backlink summary fetch for ${websiteUrl}`, userId);

    try {
      fetchAndSaveBacklinkSummary(websiteUrl, userId, userActivityId)
        .then((result) => {
          if (result.success) {
            logger.info(
              `✅ Backlink summary ${
                result.cached ? "retrieved from cache" : "fetched and saved"
              } for ${websiteUrl}`,
              userId
            );
          } else {
            logger.warn(
              `⚠️ Failed to fetch backlink summary for ${websiteUrl}: ${result.error}`,
              userId
            );
          }
        })
        .catch((error) => {
          logger.error(
            `❌ Backlink summary fetch error for ${websiteUrl}:`,
            error,
            userId
          );
        });
    } catch (error) {
      logger.error(
        `❌ Error starting backlink summary fetch for ${websiteUrl}:`,
        error,
        userId
      );
    }

    const limit = createLimiter(sitemapConcurrency);
    const sitemapPromises = [];

    for (let i = 0; i < sitemapUrls.length; i++) {
      const url = sitemapUrls[i];
      sitemapPromises.push(
        limit(async () => {
          try {
            const result = await processSitemap(
              url,
              userActivityId,
              null,
              userId
            );

            totalSitemapCount++;
            session.stats.sitemapsProcessed++;

            await updateActivityProgress(
              userActivityId,
              {
                sitemapCount: totalSitemapCount,
                progress: Math.round(
                  (i / sitemapUrls.length) * CONFIG.PROGRESS.SITEMAP_PHASE
                ),
                status: "processing",
              },
              userId
            );

            result.webpages.forEach((page) => allWebpageUrls.add(page));

            return {
              success: true,
              sitemapId: result.sitemapId,
              childSitemaps: result.childSitemaps,
            };
          } catch (error) {
            logger.error(`Failed to process sitemap ${url}`, error, userId);
            errors.push(`Failed to process sitemap: ${url}`);
            session.stats.errors++;
            return { success: false, childSitemaps: [] };
          }
        })
      );
    }

    const parentResults = await Promise.all(sitemapPromises);

    const childSitemapLimit = createLimiter(sitemapConcurrency);
    const allChildSitemaps = parentResults
      .filter((r) => r.success)
      .flatMap((r) =>
        r.childSitemaps.map((url) => ({ url, parentId: r.sitemapId }))
      );

    logger.info(`Processing ${allChildSitemaps.length} child sitemaps`, userId);

    if (allChildSitemaps.length > 0) {
      const childPromises = [];

      for (let i = 0; i < allChildSitemaps.length; i++) {
        const sitemap = allChildSitemaps[i];
        childPromises.push(
          childSitemapLimit(async () => {
            try {
              const result = await processSitemap(
                sitemap.url,
                userActivityId,
                sitemap.parentId,
                userId
              );

              totalSitemapCount++;
              session.stats.sitemapsProcessed++;

              await updateActivityProgress(
                userActivityId,
                {
                  sitemapCount: totalSitemapCount,
                  progress:
                    CONFIG.PROGRESS.SITEMAP_PHASE +
                    Math.round(
                      (i / allChildSitemaps.length) *
                        CONFIG.PROGRESS.CHILD_SITEMAP_PHASE
                    ),
                  status: "processing",
                },
                userId
              );

              result.webpages.forEach((page) => allWebpageUrls.add(page));
              return { success: true };
            } catch (error) {
              logger.error(
                `Failed to process child sitemap ${sitemap.url}`,
                error,
                userId
              );
              errors.push(`Failed to process child sitemap: ${sitemap.url}`);
              session.stats.errors++;
              return { success: false };
            }
          })
        );
      }

      await Promise.all(childPromises);
    }

    const webpageUrlsArray = Array.from(allWebpageUrls);

    logger.info(
      `Saving ${webpageUrlsArray.length} webpage URLs to database in batches`,
      userId
    );

    for (let i = 0; i < webpageUrlsArray.length; i += CONFIG.BATCH_SIZE) {
      const batch = webpageUrlsArray.slice(i, i + CONFIG.BATCH_SIZE);
      const operations = batch.map((url) => ({
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
        await Sitemap.bulkWrite(operations, { ordered: false });

        totalWebpageCount += batch.length;
        await updateActivityProgress(
          userActivityId,
          {
            webpageCount: totalWebpageCount,
            progress:
              CONFIG.PROGRESS.SITEMAP_PHASE +
              CONFIG.PROGRESS.CHILD_SITEMAP_PHASE +
              Math.round((totalWebpageCount / webpageUrlsArray.length) * 20),
            status: "processing",
          },
          userId
        );
      }
    }

    await updateActivityProgress(
      userActivityId,
      {
        isWebpageCrawling: 1,
        isSitemapCrawling: 0,
        status: "processing",
      },
      userId
    );

    logger.info(
      `Processing and saving ${webpageUrlsArray.length} webpages with SEO data via AWS proxy...`,
      userId
    );

    const scrapingResults = await processAndSaveWebpages(
      webpageUrlsArray,
      userId,
      userActivityId,
      websiteUrl,
      webpageConcurrency
    );

    session.stats.webpagesProcessed = scrapingResults.processed;
    session.stats.webpagesSuccessful = scrapingResults.saved;

    let errorMessages = [];
    if (errors.length > 0) {
      errorMessages = errors;
    }

    const finalStatus =
      errorMessages.length > 0 ? "completed_with_errors" : "completed";

    const userActivityUpdated = await UserActivity.findById(userActivityId);
    await userActivityUpdated.completeCrawl(
      finalStatus !== "failed",
      errorMessages.join("; ")
    );

    await updateActivityProgress(
      userActivityId,
      {
        sitemapCount: totalSitemapCount,
        webpageCount: totalWebpageCount,
        webpagesSuccessful: scrapingResults.saved,
        webpagesFailed: scrapingResults.failed,
        progress: 100,
        timeRemaining: 0,
      },
      userId
    );

    // Emit crawl completion event
    socketService.emitCrawlComplete(userId, {
      activityId: userActivityId,
      websiteUrl,
      status: finalStatus,
      totalSitemaps: totalSitemapCount,
      totalWebpages: totalWebpageCount,
      savedPages: scrapingResults.saved,
      failedPages: scrapingResults.failed,
      progress: 100,
      endTime: new Date(),
    });

    logger.info(
      `Completed processing for user ${userId}: ${scrapingResults.saved}/${webpageUrlsArray.length} pages saved via AWS proxy`,
      userId
    );

    return {
      success: true,
      message: "Sitemap and webpage processing completed via AWS proxy",
      totalSitemaps: totalSitemapCount,
      totalWebpages: totalWebpageCount,
      scrapedPages: scrapingResults.processed,
      savedPages: scrapingResults.saved,
      failedPages: scrapingResults.failed,
      errors: errors.length > 0 ? errors : [],
      sessionStats: session.stats,
      awsProxyStats: awsProxyManager.getStats(),
    };
  } catch (error) {
    logger.error("Error in processAllSitemapsAndWebpages", error, userId);

    try {
      const userActivityUpdated = await UserActivity.findById(userActivityId);
      if (userActivityUpdated) {
        await userActivityUpdated.completeCrawl(false, error.message);
      }
    } catch (updateError) {
      logger.error(
        "Failed to update user activity status",
        updateError,
        userId
      );
    }

    // Emit error event
    socketService.emitError(userId, {
      websiteUrl,
      message: error.message,
      activityId: userActivityId,
    });

    return { error: error.message };
  } finally {
    sessionManager.endSession(session.sessionId);
  }
}

/**
 * Enhanced health check and monitoring endpoint
 */
function getSystemHealth() {
  const memoryUsage = MemoryManager.checkMemoryUsage();
  const globalStats = sessionManager.getGlobalStats();
  const awsProxyStats = awsProxyManager.getStats();
  const pythonStats = pythonProcessPool.getStats();

  return {
    status:
      memoryUsage.heapUsedMB > CONFIG.MEMORY.HEAP_LIMIT_MB
        ? "degraded"
        : "healthy",
    timestamp: new Date().toISOString(),
    memory: memoryUsage,
    users: globalStats,
    awsProxy: awsProxyStats, // NEW: AWS proxy statistics
    pythonProcesses: pythonStats,
    redis: redisClient ? "connected" : "disconnected",
    queues: queueManager
      ? {
          sitemap: queueManager.sitemapQueue.waiting(),
          webpage: queueManager.webpageQueue.waiting(),
        }
      : null,
    config: {
      maxConcurrentUsers: CONFIG.CONCURRENCY.MAX_CONCURRENT_USERS,
      maxGlobalWorkers: CONFIG.CONCURRENCY.MAX_GLOBAL_WORKERS,
      awsProxyEndpoint: CONFIG.AWS_PROXY.ENDPOINT,
      awsProxyMaxConcurrent: CONFIG.AWS_PROXY.MAX_CONCURRENT_REQUESTS,
    },
  };
}

/**
 * Enhanced graceful shutdown handler
 */
async function gracefulShutdown() {
  logger.info("Starting graceful shutdown...");

  CONFIG.CONCURRENCY.MAX_CONCURRENT_USERS = 0;

  // Cleanup AWS proxy manager
  awsProxyManager.cleanup();

  // Cleanup Python processes
  pythonProcessPool.cleanup();

  const activeUsers = sessionManager.globalStats.activeUsers;
  if (activeUsers > 0) {
    logger.info(`Waiting for ${activeUsers} active users to complete...`);

    const maxWaitTime = 5 * 60 * 1000;
    const startTime = Date.now();

    while (
      sessionManager.globalStats.activeUsers > 0 &&
      Date.now() - startTime < maxWaitTime
    ) {
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }

  if (redisClient) {
    await redisClient.quit();
  }

  if (queueManager) {
    await queueManager.sitemapQueue.close();
    await queueManager.webpageQueue.close();
  }

  logger.info("Graceful shutdown completed");
}

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

setInterval(() => {
  sessionManager.cleanupStaleSessions();
}, 10 * 60 * 1000);

module.exports = {
  validateWebsite,
  processAllSitemapsAndWebpages,
  scrapeWebpage,
  findSitemap,
  processSitemap,
  getSystemHealth,
  gracefulShutdown,
  handleProxyRotateRequest,
  SessionManager: sessionManager,
  MemoryManager,
  RateLimiter,
  PriorityLimiter,
  AWSProxyManager: awsProxyManager, // NEW: Export AWS proxy manager
  PythonProcessPool: pythonProcessPool,
};
