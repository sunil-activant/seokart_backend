// services/socketService.js

class SocketService {
  constructor() {
    this.io = null;
  }

  // Initialize socket service
  init(io) {
    this.io = io;
    console.log('✅ Socket service initialized');
  }

  // Emit activity progress update to specific user
  emitActivityUpdate(userId, activityData) {
    if (!this.io) {
      console.warn('Socket.io not initialized');
      return;
    }

    try {
      this.io.to(`user_${userId}`).emit('activity_progress_update', {
        timestamp: new Date().toISOString(),
        ...activityData
      });
      
      console.log(`📡 Emitted activity update to user ${userId}:`, {
        progress: activityData.progress,
        status: activityData.status,
        websiteUrl: activityData.websiteUrl
      });
    } catch (error) {
      console.error('Error emitting activity update:', error);
    }
  }

  // Emit crawl completion notification
  emitCrawlComplete(userId, activityData) {
    if (!this.io) {
      console.warn('Socket.io not initialized');
      return;
    }

    try {
      this.io.to(`user_${userId}`).emit('crawl_completed', {
        timestamp: new Date().toISOString(),
        ...activityData
      });
      
      console.log(`🎉 Emitted crawl completion to user ${userId} for ${activityData.websiteUrl}`);
    } catch (error) {
      console.error('Error emitting crawl completion:', error);
    }
  }

  // Emit crawl started notification
  emitCrawlStarted(userId, activityData) {
    if (!this.io) {
      console.warn('Socket.io not initialized');
      return;
    }

    try {
      this.io.to(`user_${userId}`).emit('crawl_started', {
        timestamp: new Date().toISOString(),
        ...activityData
      });
      
      console.log(`🚀 Emitted crawl started to user ${userId} for ${activityData.websiteUrl}`);
    } catch (error) {
      console.error('Error emitting crawl started:', error);
    }
  }

  // Emit real-time user activities list update
  emitUserActivitiesUpdate(userId, activities) {
    if (!this.io) {
      console.warn('Socket.io not initialized');
      return;
    }

    try {
      this.io.to(`user_${userId}`).emit('user_activities_update', {
        timestamp: new Date().toISOString(),
        success: true,
        count: activities.length,
        data: activities
      });
      
      console.log(`📋 Emitted activities list update to user ${userId} (${activities.length} activities)`);
    } catch (error) {
      console.error('Error emitting activities update:', error);
    }
  }

  // Emit error notification
  emitError(userId, errorData) {
    if (!this.io) {
      console.warn('Socket.io not initialized');
      return;
    }

    try {
      this.io.to(`user_${userId}`).emit('crawl_error', {
        timestamp: new Date().toISOString(),
        ...errorData
      });
      
      console.log(`❌ Emitted error to user ${userId}:`, errorData.message);
    } catch (error) {
      console.error('Error emitting error notification:', error);
    }
  }

  // Get connected users count for a specific user
  getConnectedClientsCount(userId) {
    if (!this.io) return 0;
    
    try {
      const room = this.io.sockets.adapter.rooms.get(`user_${userId}`);
      return room ? room.size : 0;
    } catch (error) {
      console.error('Error getting connected clients count:', error);
      return 0;
    }
  }

  // Check if user is connected
  isUserConnected(userId) {
    return this.getConnectedClientsCount(userId) > 0;
  }
}

// Create singleton instance
const socketService = new SocketService();

module.exports = socketService;