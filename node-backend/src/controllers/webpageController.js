const mongoose = require("mongoose");
const Webpage = require("../models/Webpages");
const UserActivity = require("../models/UserActivity");


const getPaginatedWebpages = async (req, res) => {
  try {
    const { activityId } = req.params;
    const { page = 1, limit = 10, sort = "crawledAt", order = "desc", filter } = req.query;
    const userId = req.user.id;

    // Check if user has activity for this website
    const userActivity = await UserActivity.findOne({ 
      userId, 
      _id:activityId,
    });

    if (!userActivity) {
      return res.status(404).json({
        success: false,
        message: "No crawl activity found for this website"
      });
    }

    // Build query object
    const query = { 
      userId, 
      userActivityId:activityId
    };

    // Apply filters if provided
    if (filter) {
      try {
        const filterObj = JSON.parse(filter);

        // Handle specific filters
        if (filterObj.statusCode) {
          query.statusCode = filterObj.statusCode;
        }
        
        if (filterObj.contentType) {
          query.contentType = { $regex: filterObj.contentType, $options: 'i' };
        }
        
        if (filterObj.hasError !== undefined) {
          query.hasError = filterObj.hasError;
        }

        if (filterObj.url) {
          query.url = { $regex: filterObj.url, $options: 'i' };
        }

        // Add more filters as needed
      } catch (e) {
        console.error("Filter parsing error:", e);
      }
    }

    // Calculate skip value for pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);
    
    // Define sort object
    const sortObj = {};
    sortObj[sort] = order === 'desc' ? -1 : 1;

    // Execute query with pagination
    const webpages = await Webpage.find(query)
      .sort(sortObj)
      .skip(skip)
      .limit(parseInt(limit));

    // Get total count for pagination
    const total = await Webpage.countDocuments(query);

    return res.status(200).json({
      success: true,
      data: {
        webpages,
        pagination: {
          total,
          page: parseInt(page),
          limit: parseInt(limit),
          pages: Math.ceil(total / parseInt(limit))
        }
      }
    });
  } catch (error) {
    console.error("Error in getPaginatedWebpages:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching webpages",
      error: error.message
    });
  }
};


const getWebpageById = async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user ? req.user._id : new mongoose.Types.ObjectId('6618dde65a25055d0ff67579');

    const webpage = await Webpage.findOne({
      _id: id,
      userId
    });

    if (!webpage) {
      return res.status(404).json({
        success: false,
        message: "Webpage not found"
      });
    }

    return res.status(200).json({
      success: true,
      data: webpage
    });
  } catch (error) {
    console.error("Error in getWebpageById:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching the webpage",
      error: error.message
    });
  }
};


const getWebpageStats = async (req, res) => {
  try {
    const { websiteUrl } = req.params;
    const userId = req.user ? req.user._id : new mongoose.Types.ObjectId('6618dde65a25055d0ff67579');

    // Get total count
    const totalCount = await Webpage.countDocuments({ userId, websiteUrl });
    
    // Get counts by status code group
    const statusCodeStats = await Webpage.aggregate([
      { $match: { userId: mongoose.Types.ObjectId(userId), websiteUrl } },
      { $group: {
          _id: { $substr: ["$statusCode", 0, 1] },
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]);

    // Get content type distribution
    const contentTypeStats = await Webpage.aggregate([
      { $match: { userId: mongoose.Types.ObjectId(userId), websiteUrl } },
      { $group: {
          _id: "$contentType",
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1 } },
      { $limit: 10 }
    ]);

    // Get error count
    const errorCount = await Webpage.countDocuments({ 
      userId, 
      websiteUrl,
      hasError: true
    });

    return res.status(200).json({
      success: true,
      data: {
        totalCount,
        statusCodeStats,
        contentTypeStats,
        errorCount
      }
    });
  } catch (error) {
    console.error("Error in getWebpageStats:", error);
    return res.status(500).json({
      success: false,
      message: "An error occurred while fetching webpage statistics",
      error: error.message
    });
  }
};

module.exports = {
  getPaginatedWebpages,
  getWebpageById,
  getWebpageStats
};