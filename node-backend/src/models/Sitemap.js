const mongoose = require("mongoose");

const SitemapSchema = new mongoose.Schema(
  {
    userActivityId: { type: mongoose.Schema.Types.ObjectId, ref: "UserActivity", required: true }, 
    url: { type: String, required: true },
    status: { type: Number, enum: [0, 1], default: 0 }, // 0 = Pending, 1 = Success
    urlType: { type: Number, enum: [0, 1], default: 0 }, // 0 = Sitemap, 1 = URL
    parentSitemaps: [{ type: mongoose.Schema.Types.ObjectId, ref: "Sitemap" }], // Array of parent sitemap IDs
  },
  { timestamps: true }
);

module.exports = mongoose.model("Sitemap", SitemapSchema);
