const mongoose = require("mongoose");

const WebpageSchema = new mongoose.Schema(
  {
    // Core relationships
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
    userActivityId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "UserActivity",
      required: true,
    },

    // Page identifiers
    websiteUrl: { type: String, required: true, trim: true },
    // Remove the 'unique: true' from here since we'll define it in the explicit index below
    pageUrl: { type: String, required: true, trim: true },
    lastCrawled: { type: Date, default: Date.now },

    // Title metrics
    title: { type: String, trim: true },
    titleLength: { type: Number, default: 0 },
    titleScore: { type: Number, min: 0, max: 10, default: 0 },
    titleIssues: {
      tooShort: { type: Boolean, default: false },
      tooLong: { type: Boolean, default: false },
      missing: { type: Boolean, default: false },
      duplicate: { type: Boolean, default: false },
      multiple: { type: Boolean, default: false },
    },

    // Meta description metrics
    metaDescription: { type: String, trim: true },
    metaDescriptionLength: { type: Number, default: 0 },
    metaDescriptionScore: { type: Number, min: 0, max: 10, default: 0 },
    metaDescriptionIssues: {
      tooShort: { type: Boolean, default: false },
      tooLong: { type: Boolean, default: false },
      missing: { type: Boolean, default: false },
      duplicate: { type: Boolean, default: false },
      multiple: { type: Boolean, default: false },
    },

    // Content quality metrics
    contentScore: { type: Number, min: 0, max: 10, default: 0 },
    contentIssues: {
      tooShort: { type: Boolean, default: false },
      lowKeywordDensity: { type: Boolean, default: false },
      poorReadability: { type: Boolean, default: false },
    },
    wordCount: { type: Number, default: 0 },

    // Heading structure metrics
    headingStructure: {
      h1Count: { type: Number, default: 0 },
      h2Count: { type: Number, default: 0 },
      h3Count: { type: Number, default: 0 },
      h4Count: { type: Number, default: 0 },
      h5Count: { type: Number, default: 0 },
      h6Count: { type: Number, default: 0 },
      h1Missing: { type: Boolean, default: true },
      h1Multiple: { type: Boolean, default: false },
      h2H3AtTop: { type: Boolean, default: false },
      headingScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // URL quality metrics
    urlScore: { type: Number, min: 0, max: 10, default: 0 },
    urlIssues: {
      tooLong: { type: Boolean, default: false },
      containsSpecialChars: { type: Boolean, default: false },
      containsParams: { type: Boolean, default: false },
      nonDescriptive: { type: Boolean, default: false },
    },

    // Technical SEO metrics
    technicalSeo: {
      canonicalTagExists: { type: Boolean, default: false },
      canonicalUrl: { type: String, trim: true },
      robotsDirectives: { type: String, trim: true },
      hreflangTags: [{ lang: String, url: String }],
      technicalScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // Image optimization
    images: {
      count: { type: Number, default: 0 },
      optimizedCount: { type: Number, default: 0 },
      altTextMissing: [{ src: String, context: String }],
      largeImages: [{ src: String, size: Number }],
      imageScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // Link metrics
    links: {
      internalCount: { type: Number, default: 0 },
      externalCount: { type: Number, default: 0 },
      brokenLinks: [{ url: String, text: String, status: Number }],
      httpLinks: [{ url: String, text: String }],
      redirectLinks: [{ url: String, text: String, redirectTo: String }],
      linkScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // Performance metrics
    performance: {
      mobileResponsive: { type: Boolean, default: false },
      mobileIssues: [{ type: String }],
      pageSpeedScore: { type: Number, min: 0, max: 100, default: 0 },
      coreWebVitals: {
        LCP: { type: Number, default: 0 },
        FID: { type: Number, default: 0 },
        CLS: { type: Number, default: 0 },
      },
      performanceScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // Content quality metrics
    contentQuality: {
      spellingErrors: [
        {
          word: { type: String },
          context: { type: String },
          suggestion: { type: String },
          isIgnored: { type: Boolean, default: false },
        },
      ],
      grammarErrors: [
        {
          text: { type: String },
          context: { type: String },
          suggestion: { type: String },
          isIgnored: { type: Boolean, default: false },
        },
      ],
      duplicateContent: [
        {
          duplicateUrl: { type: String },
          similarityScore: { type: Number, min: 0, max: 100 },
          isIgnored: { type: Boolean, default: false },
        },
      ],
      contentQualityScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // Duplicate detection
    duplicates: {
      titleDuplicates: [{ url: String, title: String }],
      descriptionDuplicates: [{ url: String, description: String }],
      duplicateScore: { type: Number, min: 0, max: 10, default: 0 },
    },

    // Overall scores
    seoScoreComponents: {
      titleWeight: { type: Number, default: 0.1 },
      metaDescriptionWeight: { type: Number, default: 0.1 },
      contentWeight: { type: Number, default: 0.15 },
      headingsWeight: { type: Number, default: 0.1 },
      urlWeight: { type: Number, default: 0.05 },
      technicalWeight: { type: Number, default: 0.1 },
      imagesWeight: { type: Number, default: 0.05 },
      linksWeight: { type: Number, default: 0.1 },
      performanceWeight: { type: Number, default: 0.15 },
      contentQualityWeight: { type: Number, default: 0.1 },
    },
    seoScore: { type: Number, min: 0, max: 100, default: 0 },
    seoGrade: {
      type: String,
      enum: ["A+", "A", "B+", "B", "C", "D", "F"],
      default: "F",
    },
  },
  {
    timestamps: true,
    toJSON: { virtuals: true },
    toObject: { virtuals: true },
  }
);

WebpageSchema.methods.calculateTitleScore = function () {
  let score = 10; // Start with perfect score
  const minLength = 30;
  const maxLength = 60;

  if (!this.title) {
    this.titleIssues.missing = true;
    return 0; // Critical error
  }

  // Length checks
  if (this.title.length < minLength) {
    score -= 3;
    this.titleIssues.tooShort = true;
  } else if (this.title.length > maxLength) {
    score -= 2;
    this.titleIssues.tooLong = true;
  }

  // Multiple title tags
  if (this.titleIssues.multiple) {
    score -= 4;
  }

  // Duplicate titles
  if (this.titleIssues.duplicate) {
    score -= 3;
  }

  this.titleScore = Math.max(0, Math.min(10, score));
  return this.titleScore;
};

WebpageSchema.methods.calculateMetaDescriptionScore = function () {
  let score = 10; // Start with perfect score
  const minLength = 80;
  const maxLength = 160;

  if (!this.metaDescription) {
    this.metaDescriptionIssues.missing = true;
    return 0; // Critical error
  }

  // Length checks
  if (this.metaDescription.length < minLength) {
    score -= 3;
    this.metaDescriptionIssues.tooShort = true;
  } else if (this.metaDescription.length > maxLength) {
    score -= 2;
    this.metaDescriptionIssues.tooLong = true;
  }

  // Multiple meta descriptions
  if (this.metaDescriptionIssues.multiple) {
    score -= 4;
  }

  // Duplicate meta descriptions
  if (this.metaDescriptionIssues.duplicate) {
    score -= 3;
  }

  this.metaDescriptionScore = Math.max(0, Math.min(10, score));
  return this.metaDescriptionScore;
};

WebpageSchema.methods.calculateHeadingScore = function () {
  let score = 10; // Start with perfect score

  // H1 checks
  if (this.headingStructure.h1Count === 0) {
    score -= 5;
    this.headingStructure.h1Missing = true;
  } else if (this.headingStructure.h1Count > 1) {
    score -= 3;
    this.headingStructure.h1Multiple = true;
  }

  // Heading hierarchy check
  if (this.headingStructure.h2H3AtTop) {
    score -= 2;
  }

  this.headingStructure.headingScore = Math.max(0, Math.min(10, score));
  return this.headingStructure.headingScore;
};

WebpageSchema.methods.calculateContentScore = function () {
  let score = 10; // Start with perfect score
  const minWordCount = 300;

  // Content length check
  if (this.wordCount < minWordCount) {
    score -= 4;
    this.contentIssues.tooShort = true;
  }

  if (this.contentIssues.lowKeywordDensity) {
    score -= 2;
  }

  this.contentScore = Math.max(0, Math.min(10, score));
  return this.contentScore;
};

WebpageSchema.methods.calculateUrlScore = function () {
  let score = 10; // Start with perfect score
  const maxLength = 75;

  // URL length check
  if (this.pageUrl.length > maxLength) {
    score -= 2;
    this.urlIssues.tooLong = true;
  }

  // Check for special characters
  if (/[^a-zA-Z0-9\-\/\.]/.test(this.pageUrl)) {
    score -= 3;
    this.urlIssues.containsSpecialChars = true;
  }

  // Check for URL parameters
  if (this.pageUrl.includes("?")) {
    score -= 2;
    this.urlIssues.containsParams = true;
  }

  // Check for descriptive URL
  if (this.pageUrl.split("/").pop().length < 3) {
    score -= 2;
    this.urlIssues.nonDescriptive = true;
  }

  this.urlScore = Math.max(0, Math.min(10, score));
  return this.urlScore;
};

WebpageSchema.methods.calculateTechnicalScore = function () {
  let score = 10; // Start with perfect score

  // Canonical tag check
  if (!this.technicalSeo.canonicalTagExists) {
    score -= 3;
  }

  // Robots directives check
  if (!this.technicalSeo.robotsDirectives) {
    score -= 1;
  }

  this.technicalSeo.technicalScore = Math.max(0, Math.min(10, score));
  return this.technicalSeo.technicalScore;
};

WebpageSchema.methods.calculateImageScore = function () {
  let score = 10; // Start with perfect score

  if (this.images.count === 0) {
    return 10; // No images to optimize
  }

  // Missing alt text check
  const missingAltTextRatio =
    this.images.altTextMissing.length / this.images.count;
  if (missingAltTextRatio > 0) {
    score -= Math.min(6, Math.round(missingAltTextRatio * 10));
  }

  // Large images check
  const largeImagesRatio = this.images.largeImages.length / this.images.count;
  if (largeImagesRatio > 0) {
    score -= Math.min(4, Math.round(largeImagesRatio * 8));
  }

  this.images.imageScore = Math.max(0, Math.min(10, score));
  return this.images.imageScore;
};

WebpageSchema.methods.calculateLinkScore = function () {
  let score = 10; // Start with perfect score
  const totalLinks = this.links.internalCount + this.links.externalCount;

  if (totalLinks === 0) {
    return 5; // No links is not ideal
  }

  // Broken links check
  const brokenLinksRatio = this.links.brokenLinks.length / totalLinks;
  if (brokenLinksRatio > 0) {
    score -= Math.min(10, Math.round(brokenLinksRatio * 20)); // Severe penalty
  }

  // HTTP links check
  const httpLinksRatio = this.links.httpLinks.length / totalLinks;
  if (httpLinksRatio > 0) {
    score -= Math.min(4, Math.round(httpLinksRatio * 8));
  }

  // Redirect links check
  const redirectLinksRatio = this.links.redirectLinks.length / totalLinks;
  if (redirectLinksRatio > 0) {
    score -= Math.min(3, Math.round(redirectLinksRatio * 6));
  }

  this.links.linkScore = Math.max(0, Math.min(10, score));
  return this.links.linkScore;
};

WebpageSchema.methods.calculatePerformanceScore = function () {
  let score = 10; // Start with perfect score

  // Mobile responsiveness check
  if (!this.performance.mobileResponsive) {
    score -= 5; // Critical factor
  }

  // Page speed score
  if (this.performance.pageSpeedScore < 50) {
    score -= 4;
  } else if (this.performance.pageSpeedScore < 80) {
    score -= 2;
  }

  // Core Web Vitals check
  // LCP (Largest Contentful Paint) should be <= 2.5s
  if (this.performance.coreWebVitals.LCP > 2.5) {
    score -= 1;
  }

  // CLS (Cumulative Layout Shift) should be <= 0.1
  if (this.performance.coreWebVitals.CLS > 0.1) {
    score -= 1;
  }

  this.performance.performanceScore = Math.max(0, Math.min(10, score));
  return this.performance.performanceScore;
};

WebpageSchema.methods.calculateContentQualityScore = function () {
  let score = 10; // Start with perfect score

  // Calculate active spelling errors (not ignored)
  const activeSpellingErrors = this.contentQuality.spellingErrors.filter(
    (error) => !error.isIgnored
  );

  // Calculate active grammar errors (not ignored)
  const activeGrammarErrors = this.contentQuality.grammarErrors.filter(
    (error) => !error.isIgnored
  );

  // Check for spelling errors
  if (activeSpellingErrors.length > 0) {
    // Penalty based on number of errors relative to content length
    const errorRatio = Math.min(
      1,
      activeSpellingErrors.length / (this.wordCount / 100)
    );
    score -= Math.round(errorRatio * 5);
  }

  // Check for grammar errors
  if (activeGrammarErrors.length > 0) {
    // Penalty based on number of errors relative to content length
    const errorRatio = Math.min(
      1,
      activeGrammarErrors.length / (this.wordCount / 100)
    );
    score -= Math.round(errorRatio * 4);
  }

  // Check for duplicate content
  const activeDuplicates = this.contentQuality.duplicateContent.filter(
    (dup) => !dup.isIgnored
  );
  if (activeDuplicates.length > 0) {
    // Find highest similarity score among duplicates
    const highestSimilarity = Math.max(
      ...activeDuplicates.map((dup) => dup.similarityScore)
    );
    if (highestSimilarity > 85) {
      score -= 5; // Severe penalty for high similarity
    } else if (highestSimilarity > 70) {
      score -= 3;
    } else if (highestSimilarity > 50) {
      score -= 1;
    }
  }

  this.contentQuality.contentQualityScore = Math.max(0, Math.min(10, score));
  return this.contentQuality.contentQualityScore;
};

WebpageSchema.methods.calculateDuplicateScore = function () {
  let score = 10; // Start with perfect score

  // Title duplicates check
  if (this.duplicates.titleDuplicates.length > 0) {
    score -= Math.min(5, this.duplicates.titleDuplicates.length);
  }

  // Meta description duplicates check
  if (this.duplicates.descriptionDuplicates.length > 0) {
    score -= Math.min(5, this.duplicates.descriptionDuplicates.length);
  }

  this.duplicates.duplicateScore = Math.max(0, Math.min(10, score));
  return this.duplicates.duplicateScore;
};

WebpageSchema.methods.calculateSeoScore = function () {
  // Calculate individual component scores
  this.calculateTitleScore();
  this.calculateMetaDescriptionScore();
  this.calculateContentScore();
  this.calculateHeadingScore();
  this.calculateUrlScore();
  this.calculateTechnicalScore();
  this.calculateImageScore();
  this.calculateLinkScore();
  this.calculatePerformanceScore();
  this.calculateContentQualityScore();
  this.calculateDuplicateScore();

  // Calculate weighted average
  const weightedScore =
    (this.titleScore * this.seoScoreComponents.titleWeight +
      this.metaDescriptionScore *
        this.seoScoreComponents.metaDescriptionWeight +
      this.contentScore * this.seoScoreComponents.contentWeight +
      this.headingStructure.headingScore *
        this.seoScoreComponents.headingsWeight +
      this.urlScore * this.seoScoreComponents.urlWeight +
      this.technicalSeo.technicalScore *
        this.seoScoreComponents.technicalWeight +
      this.images.imageScore * this.seoScoreComponents.imagesWeight +
      this.links.linkScore * this.seoScoreComponents.linksWeight +
      this.performance.performanceScore *
        this.seoScoreComponents.performanceWeight +
      this.contentQuality.contentQualityScore *
        this.seoScoreComponents.contentQualityWeight) *
    10; // Convert from 0-10 to 0-100

  // Round to nearest integer
  this.seoScore = Math.round(weightedScore);

  // Assign letter grade
  if (this.seoScore >= 95) {
    this.seoGrade = "A+";
  } else if (this.seoScore >= 90) {
    this.seoGrade = "A";
  } else if (this.seoScore >= 85) {
    this.seoGrade = "B+";
  } else if (this.seoScore >= 80) {
    this.seoGrade = "B";
  } else if (this.seoScore >= 70) {
    this.seoGrade = "C";
  } else if (this.seoScore >= 60) {
    this.seoGrade = "D";
  } else {
    this.seoGrade = "F";
  }

  return this.seoScore;
};

WebpageSchema.virtual("criticalIssues").get(function () {
  const issues = [];

  if (this.titleIssues.missing) issues.push("Missing title tag");
  if (this.metaDescriptionIssues.missing)
    issues.push("Missing meta description");
  if (this.headingStructure.h1Missing) issues.push("Missing H1 tag");
  if (this.links.brokenLinks.length > 0)
    issues.push(`${this.links.brokenLinks.length} broken links`);
  if (!this.performance.mobileResponsive) issues.push("Not mobile responsive");
  if (this.performance.pageSpeedScore < 50) issues.push("Very slow page speed");

  return issues;
});

// Keep only these explicit index definitions
WebpageSchema.index({ userId: 1, websiteUrl: 1 });
WebpageSchema.index({ pageUrl: 1 }, { unique: true });

module.exports = mongoose.model("Webpage", WebpageSchema);
