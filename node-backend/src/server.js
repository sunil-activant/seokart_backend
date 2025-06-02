const {server} = require("./app");
const dotenv = require('dotenv')
dotenv.config()
const connectDB = require("./config/database");

const PORT = process.env.PORT || 5000;

// Connect to MongoDB
connectDB();

// Start Server
server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
