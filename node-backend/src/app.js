const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const authRoutes = require("./routes/authRoutes");
const webpageRoutes = require('./routes/webpageRoutes');
const scraperRoutes = require('./routes/scraperRoutes');
const setupSocketIO = require('./utils/socketSetup');
const http = require('http');

const { handleSitemapCrawl, checkCrawlStatus } = require("./controllers/scraperController");


const app = express();
const server = http.createServer(app);


const io = setupSocketIO(server);

const corsOpts = {
  origin: 'http://localhost:3000',
  credentials: true, 
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"], 
};

// Security middleware
app.use(helmet());
app.use(cors(corsOpts));
app.use(express.json({ limit: '1mb' })); // Add size limit for security

// Enable CORS if needed
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');
  next();
});

// Rate limiter (prevents brute-force attacks)
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per window
});
app.use(limiter);

// Routes
app.use("/api/auth", authRoutes);
app.use("/api/scraper", scraperRoutes); // Pass io to the scraper routes
// Register routes
app.use('/api/webpage', webpageRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    message: "Something went wrong!",
    error: process.env.NODE_ENV === 'production' ? null : err.message
  });
});

module.exports = server;