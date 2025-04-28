// socketSetup.js
const socketIO = require('socket.io');
const { initializeSocketController } = require('../controllers/socketController');
const socketNotifications = require('../helper/socketNotifications');

/**
 * Initialize Socket.IO with the Express server
 * @param {Object} server - HTTP server instance
 */
const setupSocketIO = (server) => {
  const io = socketIO(server, {
    cors: {
      origin: process.env.FRONTEND_URL || '*',
      methods: ['GET', 'POST'],
      credentials: true
    }
  });
  
  // Initialize the socket controller
  initializeSocketController(io);
  
  // Initialize notification system
  socketNotifications.initialize(io);
  
  return io;
};

module.exports = setupSocketIO;