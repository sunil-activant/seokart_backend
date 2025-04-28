const jwt = require('jsonwebtoken');
const Webpage = require('../models/Webpages');
const UserActivity = require('../models/UserActivity');
const mongoose = require('mongoose');

// Cache for active subscriptions
const activeSubscriptions = new Map();
// Timer for polling database
let pollingInterval = null;

/**
 * Socket controller for real-time webpage updates
 * @param {Object} io - Socket.IO server instance
 */
const initializeSocketController = (io) => {
  // Authentication middleware
  io.use((socket, next) => {
    const token = socket.handshake.auth.token;
    
    if (!token) {
      return next(new Error('Authentication token is required'));
    }
    
    try {
      // Verify JWT token (use the same secret as your API authentication)
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      socket.user = decoded;
      next();
    } catch (error) {
      console.error('Socket authentication error:', error);
      next(new Error('Authentication failed'));
    }
  });

  io.on('connection', (socket) => {
    console.log(`Socket connected: ${socket.id}`);
    
    // Handle subscription to webpage updates for a specific website
    socket.on('subscribe-website', async (websiteUrl) => {
      try {
        const userId = socket.user ? socket.user._id : new mongoose.Types.ObjectId('6618dde65a25055d0ff67579');
        
        // Check if the user has activity for this website
        const userActivity = await UserActivity.findOne({
          userId,
          websiteUrl
        });
        
        if (!userActivity) {
          socket.emit('error', { message: 'No crawl activity found for this website' });
          return;
        }
        
        // Create a unique room name for this user+website combination
        const roomName = `${userId}-${encodeURIComponent(websiteUrl)}`;
        
        // Join the room
        socket.join(roomName);
        
        // Track subscription
        if (!activeSubscriptions.has(roomName)) {
          activeSubscriptions.set(roomName, {
            userId,
            websiteUrl,
            subscribers: 0,
            lastCheck: Date.now(),
            lastWebpageId: null, // Track the latest webpage ID we've seen
            lastStatsUpdate: Date.now()
          });
        }
        
        const subscription = activeSubscriptions.get(roomName);
        subscription.subscribers++;
        
        // Send initial stats
        const stats = await getWebsiteStats(userId, websiteUrl);
        socket.emit('webpage-stats', stats);
        
        // Send latest webpages (last 10)
        const latestWebpages = await Webpage.find({ userId, websiteUrl })
          .sort({ crawledAt: -1 })
          .limit(10);
        
        // Update the last webpage ID if we have results
        if (latestWebpages.length > 0) {
          subscription.lastWebpageId = latestWebpages[0]._id;
        }
        
        socket.emit('initial-webpages', latestWebpages);
        
        // Start polling if not already running
        if (!pollingInterval) {
          startPolling(io);
        }
        
        console.log(`User ${userId} subscribed to updates for ${websiteUrl}`);
      } catch (error) {
        console.error('Error in subscribe-website:', error);
        socket.emit('error', { message: 'Failed to subscribe to website updates' });
      }
    });
    
    // Handle unsubscribe
    socket.on('unsubscribe-website', (websiteUrl) => {
      try {
        const userId = socket.user ? socket.user._id : new mongoose.Types.ObjectId('6618dde65a25055d0ff67579');
        const roomName = `${userId}-${encodeURIComponent(websiteUrl)}`;
        
        socket.leave(roomName);
        
        if (activeSubscriptions.has(roomName)) {
          const subscription = activeSubscriptions.get(roomName);
          subscription.subscribers--;
          
          if (subscription.subscribers <= 0) {
            activeSubscriptions.delete(roomName);
          }
        }
        
        // Stop polling if no more active subscriptions
        if (activeSubscriptions.size === 0 && pollingInterval) {
          clearInterval(pollingInterval);
          pollingInterval = null;
        }
        
        console.log(`User ${userId} unsubscribed from updates for ${websiteUrl}`);
      } catch (error) {
        console.error('Error in unsubscribe-website:', error);
      }
    });
    
    // Handle disconnection
    socket.on('disconnect', () => {
      console.log(`Socket disconnected: ${socket.id}`);
      // Note: rooms are automatically left on disconnect,
      // but we may want to clean up subscriptions here too
    });
  });
};

/**
 * Start polling for database changes
 * @param {Object} io - Socket.IO server instance
 */
const startPolling = (io) => {
  // Clear any existing interval
  if (pollingInterval) {
    clearInterval(pollingInterval);
  }
  
  // Set up new polling interval (every 2 seconds)
  pollingInterval = setInterval(async () => {
    try {
      // Skip if no active subscriptions
      if (activeSubscriptions.size === 0) return;
      
      for (const [roomName, subscription] of activeSubscriptions.entries()) {
        const { userId, websiteUrl, lastWebpageId, lastStatsUpdate } = subscription;
        
        // Check for new webpages since the last check
        const query = { userId, websiteUrl };
        
        // If we have a last webpage ID, only get newer webpages
        if (lastWebpageId) {
          query._id = { $gt: lastWebpageId };
        }
        
        const newWebpages = await Webpage.find(query)
          .sort({ _id: 1 })
          .limit(20);
        
        // Send new webpages to subscribers if found
        if (newWebpages.length > 0) {
          // Update the last webpage ID
          subscription.lastWebpageId = newWebpages[newWebpages.length - 1]._id;
          
          // Send each new webpage as an update
          newWebpages.forEach(webpage => {
            io.to(roomName).emit('webpage-update', webpage);
          });
          
          // Update webpage stats every 5 seconds
          if (Date.now() - lastStatsUpdate > 5000) {
            const stats = await getWebsiteStats(userId, websiteUrl);
            io.to(roomName).emit('webpage-stats', stats);
            subscription.lastStatsUpdate = Date.now();
          }
        }
        
        // Check activity status for completion updates
        const activity = await UserActivity.findOne({ userId, websiteUrl });
        if (activity && 
           (activity.status === 'completed' || 
            activity.status === 'failed' || 
            activity.status === 'cancelled')) {
          
          // Send final stats update
          const stats = await getWebsiteStats(userId, websiteUrl);
          io.to(roomName).emit('webpage-stats', stats);
          io.to(roomName).emit('crawl-complete', { 
            status: activity.status,
            completedAt: activity.endTime
          });
        }
        
        // Update timestamp for last check
        subscription.lastCheck = Date.now();
      }
    } catch (error) {
      console.error('Error in polling interval:', error);
    }
  }, 2000); // Poll every 2 seconds
};

/**
 * Get website stats for socket updates
 * @param {String} userId - User ID
 * @param {String} websiteUrl - Website URL
 * @returns {Object} Website stats
 */
const getWebsiteStats = async (userId, websiteUrl) => {
  // Get total count
  const totalCount = await Webpage.countDocuments({ userId, websiteUrl });
  
  // Get error count
  const errorCount = await Webpage.countDocuments({ 
    userId, 
    websiteUrl,
    hasError: true
  });
  
  // Get status code distribution
  const statusCodes = await Webpage.aggregate([
    { $match: { userId: mongoose.Types.ObjectId(userId), websiteUrl } },
    { $group: {
        _id: "$statusCode",
        count: { $sum: 1 }
      }
    },
    { $sort: { count: -1 } },
    { $limit: 5 }
  ]);
  
  // Get crawl progress from user activity
  const activity = await UserActivity.findOne({ userId, websiteUrl });
  
  return {
    totalCount,
    errorCount,
    statusCodes,
    progress: activity ? activity.progress : 100,
    status: activity ? activity.status : 'completed'
  };
};

module.exports = {
  initializeSocketController
};