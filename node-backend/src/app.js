const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const authRoutes = require("./routes/authRoutes");
const webpageRoutes = require('./routes/webpageRoutes');
const scraperRoutes = require('./routes/scraperRoutes');
const proxyRoute = require('./routes/proxyRoute');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');
const jwt = require('jsonwebtoken');
const { handleSitemapCrawl, checkCrawlStatus } = require("./controllers/scraperController");
const axios = require('axios');

const app = express();
const server = http.createServer(app);

// Socket.io setup with CORS
const io = socketIo(server, {
  cors: {
    origin: ['http://localhost:3000', 'http://52.27.43.67'],
    credentials: true,
    methods: ["GET", "POST"]
  }
});

const corsOpts = {
  origin: ['http://localhost:3000','http://52.27.43.67'],
  credentials: true, 
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"], 
};

// Security middleware
app.use(helmet());
app.use(cors(corsOpts));
app.use(express.json({ limit: '1mb' }));

// Enable CORS if needed
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
  next();
});

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, 
  max: 200, 
});
app.use(limiter);

// Socket.io authentication middleware
io.use(async (socket, next) => {
  try {
    const token = socket.handshake.auth.token || socket.handshake.headers.authorization?.replace('Bearer ', '');
    
    if (!token) {
      return next(new Error('Authentication error: No token provided'));
    }

    // Verify JWT token (adjust according to your JWT secret)
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'your-secret-key');
    
    // You might want to fetch user from database here
    socket.userId = decoded.id || decoded.userId;
    socket.user = decoded;
    
    console.log(`✅ Socket authenticated for user: ${socket.userId}`);
    next();
  } catch (error) {
    console.error('Socket authentication failed:', error.message);
    next(new Error('Authentication error: Invalid token'));
  }
});

// Socket.io connection handling
io.on('connection', (socket) => {
  const userId = socket.userId;
  console.log(`🔌 User ${userId} connected via socket`);

  // Join user to their personal room
  socket.join(`user_${userId}`);

  // Handle user requesting their activities
  socket.on('get_user_activities', async () => {
    try {
      const UserActivity = require('./models/UserActivity');
      const userActivities = await UserActivity.find({ userId }).sort({ lastCrawlStarted: -1 });
      
      socket.emit('user_activities_update', {
        success: true,
        count: userActivities.length,
        data: userActivities
      });
    } catch (error) {
      console.error('Error fetching user activities via socket:', error);
      socket.emit('user_activities_update', {
        success: false,
        message: 'Failed to fetch activities',
        error: error.message
      });
    }
  });

  // Handle user requesting specific activity status
  socket.on('get_activity_status', async (activityId) => {
    try {
      const UserActivity = require('./models/UserActivity');
      const activity = await UserActivity.findOne({
        _id: activityId,
        userId
      });

      if (!activity) {
        socket.emit('activity_status_update', {
          success: false,
          message: 'Activity not found'
        });
        return;
      }

      const { calculateTimeRemaining } = require('./services/scraperService');
      
      socket.emit('activity_status_update', {
        success: true,
        activityId,
        status: activity.status,
        progress: activity.progress || 0,
        isSitemapCrawling: activity.isSitemapCrawling,
        isWebpageCrawling: activity.isWebpageCrawling,
        isBacklinkFetching: activity.isBacklinkFetching,
        sitemapCount: activity.sitemapCount || 0,
        webpageCount: activity.webpageCount || 0,
        webpagesSuccessful: activity.webpagesSuccessful || 0,
        webpagesFailed: activity.webpagesFailed || 0,
        startTime: activity.startTime,
        endTime: activity.endTime,
        errorMessages: activity.errorMessages || [],
        estimatedTimeRemaining: activity.progress < 100 ? calculateTimeRemaining(activity) : 0,
        websiteUrl: activity.websiteUrl
      });
    } catch (error) {
      console.error('Error fetching activity status via socket:', error);
      socket.emit('activity_status_update', {
        success: false,
        message: 'Failed to fetch activity status',
        error: error.message
      });
    }
  });

  // Handle disconnect
  socket.on('disconnect', () => {
    console.log(`🔌 User ${userId} disconnected from socket`);
  });
});

// Make io available globally for other modules
global.io = io;

// Initialize socket service in controllers
const { initializeSocket } = require('./controllers/scraperController');
initializeSocket(io);

app.get('/', (req, res) => {
  res.send('Node.js app with Socket.io is running successfully');
});

// Routes
app.use("/api/auth", authRoutes);
app.use("/api/scraper", scraperRoutes);
app.use('/api/webpage', webpageRoutes);
app.use('/api/proxy_rotate', proxyRoute);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    message: "Something went wrong!",
    error: process.env.NODE_ENV === 'production' ? null : err.message
  });
});

app.use((req, res, next) => {
  const allowedPublicRoutes = ['/api/proxy_rotate'];
  const isPublic = allowedPublicRoutes.includes(req.path);

  const clientIP = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

  if (!isPublic && !clientIP.includes('127.0.0.1') && !clientIP.includes('localhost')) {
    return res.status(403).json({ message: 'Access denied' });
  }

  next();
});

module.exports = { server, io };