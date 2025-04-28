// socketNotifications.js
let io = null;

/**
 * Initialize the notification system with Socket.IO instance
 * @param {Object} socketIO - Socket.IO instance
 */
const initialize = (socketIO) => {
  io = socketIO;
};

/**
 * Notify clients about a new webpage
 * @param {Object} webpage - The webpage document
 */
const notifyNewWebpage = (webpage) => {
  if (!io) return;
  
  try {
    const { userId, websiteUrl } = webpage;
    const roomName = `${userId}-${encodeURIComponent(websiteUrl)}`;
    
    io.to(roomName).emit('webpage-update', webpage);
  } catch (error) {
    console.error('Error in notifyNewWebpage:', error);
  }
};

/**
 * Update clients about crawl progress
 * @param {Object} activity - UserActivity object
 */
const updateCrawlProgress = (activity) => {
  if (!io) return;
  
  try {
    const { userId, websiteUrl, progress, status } = activity;
    const roomName = `${userId}-${encodeURIComponent(websiteUrl)}`;
    
    io.to(roomName).emit('crawl-progress', { progress, status });
    
    // Notify about completion if finished
    if (status === 'completed' || status === 'failed' || status === 'cancelled') {
      io.to(roomName).emit('crawl-complete', { 
        status, 
        completedAt: activity.endTime 
      });
    }
  } catch (error) {
    console.error('Error in updateCrawlProgress:', error);
  }
};

module.exports = {
  initialize,
  notifyNewWebpage,
  updateCrawlProgress
};