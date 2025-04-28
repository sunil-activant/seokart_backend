import sys
import json
import time
import re
import traceback
import asyncio
import aiohttp
from urllib.parse import urljoin, urlparse
from collections import Counter
import textstat
from bs4 import BeautifulSoup
import concurrent.futures
import multiprocessing
from functools import partial
from typing import Dict, List, Any, Optional, Set, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("seo_scraper")

# Constants for SEO analysis
class SEOConstants:
    # Title related constants
    TITLE_MIN_LENGTH = 30
    TITLE_MAX_LENGTH = 60
    
    # Meta description constants
    META_DESC_MIN_LENGTH = 70
    META_DESC_MAX_LENGTH = 160
    
    # Content related constants
    CONTENT_MIN_WORDS = 300
    
    # URL related constants
    URL_MAX_LENGTH = 75
    
    # Network related constants
    MAX_CONNECTIONS = 100
    MAX_RETRY_ATTEMPTS = 3
    REQUEST_TIMEOUT = 15
    LINK_CHECK_TIMEOUT = 3
    LINK_CHECK_MAX = 15  # Increased from 10 to check more links
    
    # Performance related constants
    MAX_WORKERS = max(2, multiprocessing.cpu_count() - 1)  # Leave one core free
    
    # Common file extensions to ignore in link checking
    IGNORED_EXTENSIONS = {
        '.pdf', '.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx',
        '.zip', '.rar', '.tar', '.gz', '.jpg', '.jpeg', '.png', 
        '.gif', '.svg', '.mp4', '.mp3', '.avi', '.mov'
    }
    
    # Common typos for spell checking
    COMMON_TYPOS = {
        "teh": "the",
        "recieve": "receive",
        "wierd": "weird",
        "alot": "a lot",
        "seperate": "separate",
        "definately": "definitely",
        "accomodate": "accommodate",
        "occurence": "occurrence",
        "neccessary": "necessary",
        "concensus": "consensus",
        "maintainance": "maintenance",
        "developement": "development",
        "commited": "committed",
        "apropriate": "appropriate",
        "accross": "across",
        "occured": "occurred",
        "begining": "beginning",
        "beleive": "believe",
        "buisness": "business",
        "calender": "calendar",
        "cemetary": "cemetery",
        "collegue": "colleague",
    }

# Helper utility classes
class URLUtils:
    @staticmethod
    def get_website_url(page_url: str) -> str:
        """Extract the base website URL from a page URL."""
        parsed = urlparse(page_url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    @staticmethod
    def normalize_url(url: str, base_url: str) -> str:
        """Normalize a URL (make relative URLs absolute, etc.)"""
        if not url:
            return ""
            
        # Handle anchor links and javascript
        if url.startswith('#') or url.startswith('javascript:'):
            return ""
            
        # Make relative URLs absolute
        if not url.startswith(("http://", "https://")):
            return urljoin(base_url, url)
            
        return url
    
    @staticmethod
    def should_check_link(url: str) -> bool:
        """Determine if a link should be checked (filters out PDFs, etc.)"""
        if not url:
            return False
            
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        # Check for excluded file extensions
        for ext in SEOConstants.IGNORED_EXTENSIONS:
            if path.endswith(ext):
                return False
                
        return True
    
    @staticmethod
    def is_internal_link(url: str, base_domain: str) -> bool:
        """Check if a URL is internal to the given domain."""
        try:
            link_domain = urlparse(url).netloc
            return link_domain == base_domain or not link_domain
        except Exception:
            return False

class HTMLParser:
    @staticmethod
    def extract_text_content(soup: BeautifulSoup) -> str:
        """Extract main content text from HTML."""
        # Try to find main content using common selectors
        main_selectors = [
            'main', 'article', '#content', '.content', '.main-content',
            '[role="main"]', '.post-content', '.entry-content'
        ]
        
        for selector in main_selectors:
            content_element = soup.select_one(selector)
            if content_element:
                return content_element.get_text(strip=True)
        
        # If no main content found, fallback to body text
        if soup.body:
            return soup.body.get_text(strip=True)
            
        return ""
    
    @staticmethod
    def get_element_context(element, context_length: int = 50) -> str:
        """Get text context around an element."""
        if element.parent:
            context = element.parent.get_text(strip=True)
            if len(context) > context_length:
                context = context[:context_length] + '...'
            return context
        return ""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean text by removing extra whitespace."""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()
    
    @staticmethod
    def get_meta_content(soup: BeautifulSoup, name: str) -> str:
        """Get content from a meta tag."""
        meta = soup.find('meta', attrs={'name': name})
        if not meta:
            meta = soup.find('meta', attrs={'property': name})
        return meta.get('content', '') if meta else ""

# SEO Analyzer components
class SEOComponent:
    """Base class for SEO analysis components."""
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze the component and return results."""
        raise NotImplementedError

class TitleAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze the page title."""
        title = ""
        if soup.title:
            title = HTMLParser.clean_text(soup.title.string)
        
        title_length = len(title) if title else 0
        
        # Check for multiple title tags
        title_tags = soup.find_all('title')
        multiple_titles = len(title_tags) > 1
        
        # Check if title contains branding
        has_branding = False
        if title:
            # Common brand dividers
            dividers = [' | ', ' - ', ' — ', ' – ', ' :: ', ' » ']
            for divider in dividers:
                if divider in title:
                    has_branding = True
                    break
        
        title_issues = {
            "tooShort": title_length < SEOConstants.TITLE_MIN_LENGTH and title_length > 0,
            "tooLong": title_length > SEOConstants.TITLE_MAX_LENGTH,
            "missing": title_length == 0,
            "multiple": multiple_titles,
            "noBranding": not has_branding and title_length > 0
        }
        
        # Calculate score (0-10)
        score = 10
        for issue, has_issue in title_issues.items():
            if has_issue:
                if issue in ["missing", "tooShort", "tooLong"]:
                    score -= 3  # Major issues
                else:
                    score -= 1  # Minor issues
        
        score = max(0, score)
        
        # Check if title matches H1
        h1_tags = soup.find_all('h1')
        title_matches_h1 = False
        if h1_tags and title:
            h1_text = HTMLParser.clean_text(h1_tags[0].get_text())
            # Compare normalized versions (case insensitive)
            title_matches_h1 = h1_text.lower() == title.lower()
        
        return {
            "title": title,
            "titleLength": title_length,
            "titleScore": score,
            "titleIssues": title_issues,
            "titleMatchesH1": title_matches_h1
        }

class MetaDescriptionAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze the meta description."""
        meta_description = HTMLParser.get_meta_content(soup, 'description')
        meta_length = len(meta_description) if meta_description else 0
        
        # Count meta description tags
        meta_tags = soup.find_all('meta', attrs={'name': 'description'}) + \
                   soup.find_all('meta', attrs={'property': 'description'})
        multiple_meta = len(meta_tags) > 1
        
        meta_issues = {
            "tooShort": meta_length < SEOConstants.META_DESC_MIN_LENGTH and meta_length > 0,
            "tooLong": meta_length > SEOConstants.META_DESC_MAX_LENGTH,
            "missing": meta_length == 0,
            "multiple": multiple_meta,
            "noKeywords": False  # Will be filled in after keyword analysis
        }
        
        # Calculate score (0-10)
        score = 10
        for issue, has_issue in meta_issues.items():
            if has_issue:
                if issue == "missing":
                    score -= 4
                elif issue in ["tooShort", "tooLong"]:
                    score -= 2
                else:
                    score -= 1
        
        score = max(0, score)
        
        return {
            "metaDescription": meta_description,
            "metaDescriptionLength": meta_length,
            "metaDescriptionScore": score,
            "metaDescriptionIssues": meta_issues
        }

class ContentAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze content quality and return metrics."""
        content = HTMLParser.extract_text_content(soup)
        
        # Split into words and count
        words = re.findall(r'\b\w+\b', content.lower()) if content else []
        word_count = len(words)
        
        # Get paragraphs
        paragraphs = soup.find_all('p')
        paragraph_count = len(paragraphs)
        
        # Calculate readability
        readability_score = 0
        gunning_fog_score = 0
        
        if content and word_count > 50:
            readability_score = textstat.flesch_reading_ease(content)
            gunning_fog_score = textstat.gunning_fog(content)
        
        # Calculate keyword density (would use TF-IDF ideally)
        word_freq = Counter(words)
        # Filter out common stop words
        stop_words = {'the', 'and', 'is', 'of', 'to', 'a', 'in', 'for', 'that', 'with', 'as', 'on'}
        for stop_word in stop_words:
            if stop_word in word_freq:
                del word_freq[stop_word]
        
        # Get top keywords
        top_keywords = word_freq.most_common(10) if word_freq else []
        
        content_issues = {
            "tooShort": word_count < SEOConstants.CONTENT_MIN_WORDS,
            "poorReadability": readability_score < 50,
            "highReadingLevel": gunning_fog_score > 14,
            "fewParagraphs": paragraph_count < 3 and word_count > 200
        }
        
        # Calculate score (0-10)
        content_score = 10
        for issue, has_issue in content_issues.items():
            if has_issue:
                content_score -= 2
        
        content_score = max(0, content_score)
        
        return {
            "contentScore": content_score,
            "contentIssues": content_issues,
            "wordCount": word_count,
            "paragraphCount": paragraph_count,
            "readabilityScore": readability_score,
            "gunningFogScore": gunning_fog_score,
            "topKeywords": [{"word": word, "count": count} for word, count in top_keywords]
        }

class HeadingStructureAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze heading structure."""
        headings = {
            "h1Count": len(soup.find_all('h1')),
            "h2Count": len(soup.find_all('h2')),
            "h3Count": len(soup.find_all('h3')),
            "h4Count": len(soup.find_all('h4')),
            "h5Count": len(soup.find_all('h5')),
            "h6Count": len(soup.find_all('h6')),
        }
        
        # Get actual heading text
        h1_text = ""
        if soup.find('h1'):
            h1_text = HTMLParser.clean_text(soup.find('h1').get_text())
        
        h2_texts = [HTMLParser.clean_text(h.get_text()) for h in soup.find_all('h2')]
        
        # Check for issues
        headings["h1Missing"] = headings["h1Count"] == 0
        headings["h1Multiple"] = headings["h1Count"] > 1
        headings["h2Missing"] = headings["h2Count"] == 0
        headings["noHeadings"] = sum(headings[f"h{i}Count"] for i in range(1, 7)) == 0
        
        # Check heading order (e.g., no h2 before h1)
        all_headings = soup.select('h1, h2, h3, h4, h5, h6')
        ordered_headings = []
        for h in all_headings:
            ordered_headings.append(int(h.name[1]))
        
        order_issues = []
        if ordered_headings:
            for i in range(1, len(ordered_headings)):
                # Check if heading level jumps by more than 1
                if ordered_headings[i] > ordered_headings[i-1] + 1:
                    order_issues.append(f"Jump from h{ordered_headings[i-1]} to h{ordered_headings[i]}")
        
        headings["orderIssues"] = order_issues
        headings["hasOrderIssues"] = len(order_issues) > 0
        
        # Calculate heading score
        heading_score = 10
        if headings["h1Missing"]:
            heading_score -= 4
        if headings["h1Multiple"]:
            heading_score -= 2
        if headings["h2Missing"] and not headings["noHeadings"]:
            heading_score -= 1
        if headings["noHeadings"]:
            heading_score -= 5
        if headings["hasOrderIssues"]:
            heading_score -= len(order_issues)
        
        heading_score = max(0, heading_score)
        
        return {
            "headingStructure": {
                **headings,
                "headingScore": heading_score,
                "h1Text": h1_text,
                "h2Texts": h2_texts[:5]  # Limit to first 5 h2s
            }
        }

class URLAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze URL quality."""
        parsed_url = urlparse(url)
        path = parsed_url.path
        
        # Clean the path for analysis
        path_segments = [seg for seg in path.split('/') if seg]
        
        # Check for specific URL patterns
        contains_dates = any(re.match(r'\d{4}/\d{2}', seg) for seg in path_segments)
        contains_numbers = any(seg.isdigit() for seg in path_segments)
        contains_underscores = '_' in path
        
        url_issues = {
            "tooLong": len(url) > SEOConstants.URL_MAX_LENGTH,
            "containsSpecialChars": bool(re.search(r'[^\w\-/]', path)),
            "containsParams": bool(parsed_url.query),
            "containsFragment": bool(parsed_url.fragment),
            "nonDescriptive": len(path_segments) == 0 or path == "/",
            "containsUnderscores": contains_underscores,
            "containsDatePatterns": contains_dates
        }
        
        # Calculate URL score
        url_score = 10
        for issue, has_issue in url_issues.items():
            if has_issue:
                if issue in ["tooLong", "containsSpecialChars", "nonDescriptive"]:
                    url_score -= 2
                else:
                    url_score -= 1
        
        url_score = max(0, url_score)
        
        return {
            "urlScore": url_score,
            "urlStructure": {
                "domain": parsed_url.netloc,
                "path": parsed_url.path,
                "pathSegments": path_segments,
                "params": parsed_url.query,
                "fragment": parsed_url.fragment,
            },
            "urlIssues": url_issues
        }

class TechnicalSEOAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze technical SEO elements."""
        technical_seo = {
            "hasCanonical": False,
            "canonicalUrl": "",
            "robotsDirectives": "",
            "hasHreflang": False,
            "hreflangTags": [],
            "hasStructuredData": False,
            "structuredDataTypes": [],
            "hasSitemap": False,
            "openGraphTags": {},
            "twitterCards": {}
        }
        
        # Check canonical tag
        canonical_tags = soup.find_all('link', rel='canonical')
        technical_seo["hasCanonical"] = len(canonical_tags) > 0
        if technical_seo["hasCanonical"] and canonical_tags[0].get('href'):
            technical_seo["canonicalUrl"] = canonical_tags[0].get('href')
        
        # Check robots directives
        robots_tags = soup.find_all('meta', attrs={'name': 'robots'})
        if robots_tags:
            technical_seo["robotsDirectives"] = robots_tags[0].get('content', '')
        
        # Check hreflang tags
        hreflang_tags = soup.find_all('link', rel='alternate', attrs={'hreflang': True})
        technical_seo["hasHreflang"] = len(hreflang_tags) > 0
        technical_seo["hreflangTags"] = [
            {"lang": tag.get('hreflang', ''), "url": tag.get('href', '')}
            for tag in hreflang_tags
        ]
        
        # Check structured data
        structured_data_scripts = soup.find_all('script', attrs={'type': 'application/ld+json'})
        technical_seo["hasStructuredData"] = len(structured_data_scripts) > 0
        
        for script in structured_data_scripts:
            try:
                script_content = script.string
                if script_content:
                    data = json.loads(script_content)
                    if "@type" in data:
                        technical_seo["structuredDataTypes"].append({"type": data["@type"]})
            except Exception:
                pass
        
        # Check Open Graph tags
        og_tags = soup.find_all('meta', attrs={'property': lambda x: x and x.startswith('og:')})
        for tag in og_tags:
            prop = tag.get('property', '')[3:]  # Remove 'og:' prefix
            content = tag.get('content', '')
            if prop and content:
                technical_seo["openGraphTags"][prop] = content
        
        # Check Twitter Card tags
        twitter_tags = soup.find_all('meta', attrs={'name': lambda x: x and x.startswith('twitter:')})
        for tag in twitter_tags:
            prop = tag.get('name', '')[8:]  # Remove 'twitter:' prefix
            content = tag.get('content', '')
            if prop and content:
                technical_seo["twitterCards"][prop] = content
        
        # Check for sitemap link in HTML
        sitemap_links = soup.find_all('a', href=lambda x: x and ('sitemap' in x.lower()))
        technical_seo["hasSitemap"] = len(sitemap_links) > 0
        
        # Calculate technical SEO score
        technical_score = 10
        if not technical_seo["hasCanonical"]:
            technical_score -= 2
        if not technical_seo["robotsDirectives"]:
            technical_score -= 1
        if not technical_seo["hasStructuredData"]:
            technical_score -= 2
        if not technical_seo["openGraphTags"]:
            technical_score -= 1
        if not technical_seo["twitterCards"]:
            technical_score -= 1
        
        technical_seo["technicalScore"] = max(0, technical_score)
        
        return {"technicalSeo": technical_seo}

class ImageAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze images."""
        images = soup.find_all('img')
        image_data = {
            "count": len(images),
            "withAlt": 0,
            "withDimensions": 0,
            "altTextMissing": [],
            "largeImages": [],
            "smallImages": [],
            "lazyLoaded": 0
        }
        
        for img in images:
            # Check for alt text
            alt_text = img.get('alt', '')
            if alt_text:
                image_data["withAlt"] += 1
            else:
                src = img.get('src', '')
                if src:
                    context = HTMLParser.get_element_context(img)
                    image_data["altTextMissing"].append({"src": src, "context": context})
            
            # Check dimensions
            width = img.get('width', '')
            height = img.get('height', '')
            if width and height:
                image_data["withDimensions"] += 1
                
                try:
                    width = int(width)
                    height = int(height)
                    if width > 1000 and height > 1000:
                        image_data["largeImages"].append({
                            "src": img.get('src', ''),
                            "width": width,
                            "height": height
                        })
                    elif width < 100 and height < 100 and not (img.get('src', '').endswith('.svg')):
                        image_data["smallImages"].append({
                            "src": img.get('src', ''),
                            "width": width,
                            "height": height
                        })
                except ValueError:
                    pass
            
            # Check for lazy loading
            if (img.get('loading') == 'lazy' or 
                img.get('data-src') or 
                img.get('data-lazy') or
                img.get('data-lazy-src')):
                image_data["lazyLoaded"] += 1
        
        # Calculate image score
        image_score = 10
        if image_data["count"] > 0:
            # Calculate percentage of images with alt text
            alt_ratio = image_data["withAlt"] / image_data["count"]
            if alt_ratio < 0.5:
                image_score -= 4
            elif alt_ratio < 0.8:
                image_score -= 2
            
            # Calculate percentage of images with dimensions
            dim_ratio = image_data["withDimensions"] / image_data["count"]
            if dim_ratio < 0.5:
                image_score -= 2
            
            # Check for large images
            if len(image_data["largeImages"]) > 3:
                image_score -= 2
            elif len(image_data["largeImages"]) > 0:
                image_score -= 1
            
            # Bonus for lazy loading
            if image_data["count"] > 3 and image_data["lazyLoaded"] > 0:
                ratio = image_data["lazyLoaded"] / image_data["count"]
                if ratio > 0.8:
                    image_score += 1
        
        image_data["imageScore"] = max(0, image_score)
        
        return {"images": image_data}

class PerformanceAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str, html_content: str) -> Dict[str, Any]:
        """Analyze page performance."""
        performance = {
            "mobileResponsive": False,
            "mobileIssues": [],
            "pageSize": len(html_content) / 1024,  # Size in KB
            "estimatedLCP": 0,
            "estimatedFID": 0,
            "estimatedCLS": 0,
            "externalScripts": [],
            "externalStyles": [],
            "performanceScore": 0
        }
        
        # Check for viewport meta tag (basic mobile responsiveness check)
        viewport_tags = soup.find_all('meta', attrs={'name': 'viewport'})
        if viewport_tags:
            viewport_content = viewport_tags[0].get('content', '')
            if viewport_content and "width=device-width" in viewport_content:
                performance["mobileResponsive"] = True
            else:
                performance["mobileIssues"].append({
                    "type": "Invalid viewport setting",
                    "details": f"Found: {viewport_content}"
                })
        else:
            performance["mobileIssues"].append({"type": "No viewport meta tag"})
        
        # Count external scripts
        scripts = soup.find_all('script', src=True)
        for script in scripts:
            src = script.get('src', '')
            if src and not src.startswith('data:'):
                performance["externalScripts"].append({
                    "src": src,
                    "async": script.get('async') is not None,
                    "defer": script.get('defer') is not None
                })
        
        # Count external stylesheets
        styles = soup.find_all('link', rel='stylesheet')
        for style in styles:
            href = style.get('href', '')
            if href and not href.startswith('data:'):
                performance["externalStyles"].append({
                    "href": href
                })
        
        # Rough LCP estimation
        html_size = len(html_content) / 1024  # Size in KB
        estimated_lcp = min(10, html_size / 80)  # Very rough approximation
        performance["estimatedLCP"] = estimated_lcp
        
        # Rough FID estimation based on script count
        external_script_count = len(performance["externalScripts"])
        async_scripts = sum(1 for s in performance["externalScripts"] if s.get("async") or s.get("defer"))
        
        blocking_scripts = external_script_count - async_scripts
        estimated_fid = min(500, blocking_scripts * 50)  # Very rough approximation
        performance["estimatedFID"] = estimated_fid
        
        # Calculate performance score
        perf_score = 6  # Start in the middle
        
        # Mobile responsive bonus
        if performance["mobileResponsive"]:
            perf_score += 1
        else:
            perf_score -= 2
        
        # Page size adjustments
        if html_size < 100:
            perf_score += 1
        elif html_size > 500:
            perf_score -= 1
        
        # Script optimizations
        if external_script_count > 0:
            async_ratio = async_scripts / external_script_count
            if async_ratio > 0.8:
                perf_score += 1
            elif async_ratio < 0.3:
                perf_score -= 1
        
        # Estimated Core Web Vitals impact
        if estimated_lcp < 2.5:
            perf_score += 1
        elif estimated_lcp > 4:
            perf_score -= 1
        
        if estimated_fid < 100:
            perf_score += 1
        elif estimated_fid > 300:
            perf_score -= 1
        
        performance["performanceScore"] = max(0, min(10, perf_score))
        
        return {"performance": performance}

class ContentQualityAnalyzer(SEOComponent):
    def analyze(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Analyze content quality (spelling, grammar, etc.)"""
        content = HTMLParser.extract_text_content(soup)
        
        quality_data = {
            "spellingErrors": [],
            "contentQualityScore": 0,
            "keywordDensity": {},
            "readingTime": 0,
            "wordCount": 0
        }
        
        if content:
            # Calculate word count
            words = re.findall(r'\b\w+\b', content.lower())
            quality_data["wordCount"] = len(words)
            
            # Estimate reading time (average reading speed: 200 words per minute)
            quality_data["readingTime"] = round(len(words) / 200, 1)
            
            # Calculate keyword density
            if len(words) > 0:
                word_counts = Counter(words)
                total_words = len(words)
                
                # Remove common stop words
                stop_words = {'the', 'and', 'is', 'of', 'to', 'a', 'in', 'for', 'that', 'with'}
                for stop_word in stop_words:
                    if stop_word in word_counts:
                        del word_counts[stop_word]
                
                # Get keyword density for top words
                for word, count in word_counts.most_common(10):
                    if len(word) > 3:  # Only include words with more than 3 characters
                        density = round((count / total_words) * 100, 1)
                        quality_data["keywordDensity"][word] = density
            
            # Check for common spelling errors
            for idx, word in enumerate(words):
                if word in SEOConstants.COMMON_TYPOS:
                    # Get some context
                    start_idx = max(0, idx - 3)
                    end_idx = min(len(words), idx + 4)
                    context = " ".join(words[start_idx:end_idx])

                    quality_data["spellingErrors"].append({
                        "word": word,
                        "context": context,
                        "suggestion": SEOConstants.COMMON_TYPOS[word],
                        "isIgnored": False
                    })
        
        # Calculate content quality score
        quality_score = 10
        if quality_data["spellingErrors"]:
            quality_score -= min(4, len(quality_data["spellingErrors"]))
        
        # Check for short paragraphs (good for readability)
        paragraphs = soup.find_all('p')
        short_paragraph_count = 0
        
        for p in paragraphs:
            p_text = p.get_text(strip=True)
            p_words = len(re.findall(r'\b\w+\b', p_text))
            if 10 <= p_words <= 50:  # Good paragraph length
                short_paragraph_count += 1
        
        # If at least half paragraphs are concise, add points
        if paragraphs and short_paragraph_count / len(paragraphs) >= 0.5:
            quality_score += 1
        
        quality_data["contentQualityScore"] = max(0, min(10, quality_score))
        
        return {"contentQuality": quality_data}

class LinkAnalyzer:
    """Separated as this requires async operations."""
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
    
    async def check_link_status(self, link_info: Dict[str, str]) -> Dict[str, Any]:
        """Check if a link is broken or redirecting."""
        url = link_info["url"]
        result = {
            "url": url,
            "text": link_info["text"],
            "status": 0,
            "redirectTo": ""
        }
        
        # Skip checking if the URL should not be checked
        if not URLUtils.should_check_link(url):
            result["status"] = -1  # Special code for skipped
            return result
        
        try:
            async with self.session.head(
                url, 
                allow_redirects=False, 
                timeout=SEOConstants.LINK_CHECK_TIMEOUT
            ) as response:
                result["status"] = response.status
                
                # Check for redirects
                if 300 <= response.status < 400 and "Location" in response.headers:
                    result["redirectTo"] = response.headers["Location"]
        except asyncio.TimeoutError:
            result["status"] = 408  # Timeout
        except Exception:
            result["status"] = 404  # Assume broken
        
        return result
    
    async def analyze(self, soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
        """Analyze links and return metrics."""
        links = soup.find_all('a')
        
        link_data = {
            "internalCount": 0,
            "externalCount": 0,
            "brokenLinks": [],
            "httpLinks": [],
            "redirectLinks": [],
            "socialMediaLinks": [],
            "noFollowLinks": []
        }
        
        all_links = []
        base_domain = urlparse(base_url).netloc
        
        # Social media domains
        social_domains = {
            'facebook.com', 'twitter.com', 'instagram.com', 
            'linkedin.com', 'youtube.com', 'pinterest.com'
        }
        
        for link in links:
            href = link.get('href', '')
            if not href:
                continue
                
            # Normalize URL
            href = URLUtils.normalize_url(href, base_url)
            if not href:
                continue
            
            # Store link info
            link_text = HTMLParser.clean_text(link.get_text())
            link_text = link_text or "No text"
            
            link_info = {
                "url": href,
                "text": link_text[:50]  # Limit text length
            }
            
            # Check if internal or external
            try:
                if URLUtils.is_internal_link(href, base_domain):
                    link_data["internalCount"] += 1
                else:
                    link_data["externalCount"] += 1
                    
                    # Check for social media links
                    parsed_link = urlparse(href)
                    link_domain = parsed_link.netloc.lower()
                    
                    if any(social in link_domain for social in social_domains):
                        link_data["socialMediaLinks"].append(link_info)
                
                # Check for HTTP links
                if href.startswith("http://"):
                    link_data["httpLinks"].append(link_info)
                
                # Check for nofollow
                rel = link.get('rel', '')
                if rel and 'nofollow' in rel:
                    link_data["noFollowLinks"].append(link_info)
                
                all_links.append(link_info)
            except Exception:
                # Skip invalid URLs
                continue
        
        # Check link status in parallel (limit the number of links to check)
        sample_links = all_links[:SEOConstants.LINK_CHECK_MAX]
        
        # Use asyncio.gather to check links concurrently
        link_statuses = []
        try:
            tasks = [self.check_link_status(link) for link in sample_links]
            if tasks:
                link_statuses = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass
        
        for result in link_statuses:
            if isinstance(result, Exception):
                continue
                
            if result["status"] >= 400:
                link_data["brokenLinks"].append({
                    "url": result["url"],
                    "text": result["text"],
                    "status": result["status"]
                })
            elif result["redirectTo"]:
                link_data["redirectLinks"].append({
                    "url": result["url"],
                    "text": result["text"],
                    "redirectTo": result["redirectTo"]
                })
        
        # Calculate link score
        link_score = 10
        
        # Deduct points for broken links
        broken_link_count = len(link_data["brokenLinks"])
        if broken_link_count > 0:
            link_score -= min(5, broken_link_count)
            
        # Deduct points for HTTP links
        http_link_count = len(link_data["httpLinks"])
        if http_link_count > 0:
            link_score -= min(3, http_link_count)
        
        # Give bonus for social media presence
        if link_data["socialMediaLinks"]:
            link_score += 1
        
        link_data["linkScore"] = max(0, min(10, link_score))
        
        return link_data

class SEOScanner:
    """Main class for SEO scanning."""
    
    def __init__(self):
        self.session = None
        self.analyzers = [
            TitleAnalyzer(),
            MetaDescriptionAnalyzer(),
            ContentAnalyzer(),
            HeadingStructureAnalyzer(),
            URLAnalyzer(),
            TechnicalSEOAnalyzer(),
            ImageAnalyzer(),
            ContentQualityAnalyzer()
        ]
    
    async def create_session(self):
        """Create an aiohttp session with optimized settings."""
        if self.session is None:
            try:
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                connector = aiohttp.TCPConnector(
                    limit=SEOConstants.MAX_CONNECTIONS,
                    ssl=False,
                    use_dns_cache=True,
                    ttl_dns_cache=300
                )
                self.session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers={
                        'User-Agent': 'SEOScanner/1.0',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                        'Accept-Language': 'en-US,en;q=0.5'
                    }
                )
            except Exception as e:
                logger.error(f"Failed to create session: {str(e)}")
                return None
        return self.session
    
    async def close_session(self):
        """Close the aiohttp session."""
        if self.session is not None:
            try:
                await self.session.close()
                self.session = None
            except Exception as e:
                logger.error(f"Error closing session: {str(e)}")
    
    async def fetch_page(self, url: str, max_retries: int = SEOConstants.MAX_RETRY_ATTEMPTS) -> Tuple[str, Dict[str, Any]]:
        """Fetch page HTML with retry logic."""
        session = await self.create_session()
        if not session:
            return "", {"error": "Failed to create HTTP session"}
        
        error = None
        html_content = ""
        response_info = {}
        
        for attempt in range(max_retries):
            try:
                async with session.get(
                    url, 
                    timeout=SEOConstants.REQUEST_TIMEOUT,
                    allow_redirects=True
                ) as response:
                    response_info = {
                        "status": response.status,
                        "headers": dict(response.headers),
                        "url": str(response.url)  # Final URL after redirects
                    }
                    
                    if response.status >= 400:
                        error = f"HTTP error: {response.status}"
                        # Don't retry on client errors, only server errors
                        if response.status < 500:
                            break
                    else:
                        html_content = await response.text()
                        error = None
                        break
            except asyncio.TimeoutError:
                error = f"Timeout on attempt {attempt+1}"
            except aiohttp.ClientError as e:
                error = f"Connection error: {str(e)}"
            except Exception as e:
                error = f"Unexpected error: {str(e)}"
            
            # Only retry on server errors and network issues
            if attempt < max_retries - 1:
                await asyncio.sleep(1)  # Wait before retry
        
        return html_content, {"error": error, "response": response_info}
    
    async def scan_url(self, url: str) -> Dict[str, Any]:
        """Scan a URL and get SEO analysis."""
        website_url = URLUtils.get_website_url(url)
        
        result = {
            "websiteUrl": website_url,
            "pageUrl": url,
            "lastCrawled": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "error": None,
            "overallScore": 0
        }
        
        try:
            # Fetch the page HTML
            html_content, fetch_result = await self.fetch_page(url)
            
            if fetch_result["error"]:
                result["error"] = fetch_result["error"]
                result["responseInfo"] = fetch_result.get("response", {})
                return result
            
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Run all component analyzers
            component_scores = []
            
            for analyzer in self.analyzers:
                try:
                    if isinstance(analyzer, PerformanceAnalyzer):
                        component_data = analyzer.analyze(soup, url, html_content)
                    else:
                        component_data = analyzer.analyze(soup, url)
                    
                    result.update(component_data)
                    
                    # Extract component score if available
                    for key, value in component_data.items():
                        if isinstance(value, dict) and key + "Score" in value:
                            component_scores.append(value[key + "Score"])
                except Exception as e:
                    logger.error(f"Error in {analyzer.__class__.__name__}: {str(e)}")
                    result[f"{analyzer.__class__.__name__}Error"] = str(e)
            
            # Run async link analyzer
            try:
                link_analyzer = LinkAnalyzer(self.session)
                link_data = await link_analyzer.analyze(soup, url)
                result.update({"links": link_data})
                if "linkScore" in link_data:
                    component_scores.append(link_data["linkScore"])
            except Exception as e:
                logger.error(f"Error in LinkAnalyzer: {str(e)}")
                result["LinkAnalyzerError"] = str(e)
            
            # Performance analysis
            try:
                perf_analyzer = PerformanceAnalyzer()
                perf_data = perf_analyzer.analyze(soup, url, html_content)
                result.update(perf_data)
                if "performanceScore" in perf_data.get("performance", {}):
                    component_scores.append(perf_data["performance"]["performanceScore"])
            except Exception as e:
                logger.error(f"Error in PerformanceAnalyzer: {str(e)}")
                result["PerformanceAnalyzerError"] = str(e)
            
            # Calculate overall score based on component scores
            if component_scores:
                result["overallScore"] = round(sum(component_scores) / len(component_scores), 1)
                
                # Add score categorization
                if result["overallScore"] >= 8:
                    result["scoreCategory"] = "Good"
                elif result["overallScore"] >= 6:
                    result["scoreCategory"] = "Average"
                else:
                    result["scoreCategory"] = "Poor"
            
            # Get page load time
            result["analysisTime"] = time.time() - time.mktime(time.strptime(result["lastCrawled"], "%Y-%m-%dT%H:%M:%SZ"))
            
        except Exception as e:
            logger.error(f"Error scanning URL {url}: {str(e)}")
            result["error"] = f"Scan error: {str(e)}"
        
        return result

async def process_urls(urls: List[str]) -> List[Dict[str, Any]]:
    """Process multiple URLs concurrently."""
    scanner = SEOScanner()
    
    try:
        # Create session first
        await scanner.create_session()
        
        # Create tasks for all URLs
        tasks = [scanner.scan_url(url) for url in urls]
        
        # Process URLs concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "pageUrl": urls[i] if i < len(urls) else "unknown",
                    "error": f"Processing error: {str(result)}"
                })
            else:
                processed_results.append(result)
        
        # Close the session
        await scanner.close_session()
        
        return processed_results
    
    except Exception as e:
        logger.error(f"Error processing URLs: {str(e)}")
        # Ensure session is closed even on error
        await scanner.close_session()
        return [{"error": f"Failed to process URLs: {str(e)}"}]

async def main():
    """Main entry point for the script."""
    try:
        # Configure logging based on verbosity
        if '--verbose' in sys.argv:
            logger.setLevel(logging.DEBUG)
        
        if len(sys.argv) < 2 or sys.argv[1].startswith('--'):
            print(json.dumps({
                "error": "URL is required. Usage: python script.py URL [URL2,URL3,...] [--verbose]"
            }))
            sys.exit(1)
        
        url = sys.argv[1]
        
        start_time = time.time()
        
        # Check if multiple URLs are provided (comma-separated)
        if ',' in url:
            urls = [u.strip() for u in url.split(',') if u.strip()]
            logger.info(f"Processing {len(urls)} URLs")
            results = await process_urls(urls)
            print(json.dumps(results, ensure_ascii=False, indent=2))
        else:
            # Single URL
            logger.info(f"Processing URL: {url}")
            scanner = SEOScanner()
            result = await scanner.scan_url(url)
            await scanner.close_session()
            print(json.dumps(result, ensure_ascii=False, indent=2))
        
        end_time = time.time()
        logger.info(f"Total processing time: {end_time - start_time:.2f} seconds")
    
    except Exception as e:
        # Catch-all error handler
        error_result = {
            "error": f"Script execution error: {str(e)}",
            "traceback": traceback.format_exc()
        }
        print(json.dumps(error_result, ensure_ascii=False))

if __name__ == "__main__":
    # Ensure we always output valid JSON even on uncaught exceptions
    try:
        # Set up asyncio event loop policy for Windows
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        # Run the main function
        asyncio.run(main())
    except KeyboardInterrupt:
        print(json.dumps({"error": "Process interrupted by user"}))
    except Exception as e:
        # Last resort error handling
        error_result = {
            "error": f"Fatal error: {str(e)}",
            "traceback": traceback.format_exc()
        }
        print(json.dumps(error_result, ensure_ascii=False))