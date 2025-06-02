import sys
import json
import time
import re
import traceback
import asyncio
from urllib.parse import urljoin, urlparse, parse_qs
from collections import Counter, defaultdict
import textstat
from bs4 import BeautifulSoup, Comment
from typing import Dict, List, Any, Optional, Set, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import multiprocessing
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import difflib
from datetime import datetime
import hashlib
import socket
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os
import random

# Disable SSL warnings
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

# Enhanced spell checking with pyspellchecker
try:
    from spellchecker import SpellChecker

    PYSPELLCHECKER_AVAILABLE = True
except ImportError:
    PYSPELLCHECKER_AVAILABLE = False

# Enhanced duplicate detection libraries
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np

    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from fuzzywuzzy import fuzz, process

    FUZZYWUZZY_AVAILABLE = True
except ImportError:
    FUZZYWUZZY_AVAILABLE = False

# Playwright imports
try:
    from playwright.async_api import async_playwright
    from playwright.sync_api import sync_playwright

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# Enhanced logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("enhanced_seo_scraper")

# Log library availability after logger is initialized
if PYSPELLCHECKER_AVAILABLE:
    logger.debug("✅ PySpellChecker available")
else:
    logger.debug("❌ PySpellChecker not available. Install: pip install pyspellchecker")

if SKLEARN_AVAILABLE:
    logger.debug("✅ Scikit-learn available")
else:
    logger.debug("❌ Scikit-learn not available. Install: pip install scikit-learn")

if FUZZYWUZZY_AVAILABLE:
    logger.debug("✅ FuzzyWuzzy available")
else:
    logger.debug(
        "❌ FuzzyWuzzy not available. Install: pip install fuzzywuzzy python-levenshtein"
    )

if PLAYWRIGHT_AVAILABLE:
    logger.debug("✅ Playwright available")
else:
    logger.debug("❌ Playwright not available. Install: pip install playwright")

# Global storage for cross-page analysis
GLOBAL_CONTENT_STORE = {
    "pages": {},
    "titles": {},
    "descriptions": {},
    "contents": {},
    "tfidf_vectorizer": None,
    "content_vectors": {},
}


class SEOConstants:
    TITLE_MIN_LENGTH = 30
    TITLE_MAX_LENGTH = 60
    META_DESC_MIN_LENGTH = 80
    META_DESC_MAX_LENGTH = 160
    CONTENT_MIN_WORDS = 300
    URL_MAX_LENGTH = 75

    # Enhanced detection settings
    DUPLICATE_SIMILARITY_THRESHOLD = 75  # More strict threshold
    BROKEN_LINK_TIMEOUT = 20
    SPELL_CHECK_MIN_WORD_LENGTH = 3
    MAX_LINKS_TO_CHECK = 150

    # Content extraction improvements
    MIN_TEXT_LENGTH = 10
    MAX_SPELL_ERRORS = 100
    MAX_GRAMMAR_ERRORS = 50

    IGNORED_EXTENSIONS = {
        ".pdf",
        ".doc",
        ".docx",
        ".ppt",
        ".pptx",
        ".xls",
        ".xlsx",
        ".zip",
        ".rar",
        ".tar",
        ".gz",
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".svg",
        ".mp4",
        ".mp3",
        ".avi",
        ".mov",
        ".webp",
        ".ico",
        ".css",
        ".js",
        ".xml",
        ".txt",
        ".woff",
        ".woff2",
        ".ttf",
        ".eot",
        ".json",
        ".rss",
        ".atom",
        ".dmg",
        ".exe",
        ".pkg",
    }

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0",
    ]

    SCORING_WEIGHTS = {
        "titleWeight": 0.1,
        "metaDescriptionWeight": 0.1,
        "contentWeight": 0.15,
        "headingsWeight": 0.1,
        "urlWeight": 0.05,
        "technicalWeight": 0.1,
        "imagesWeight": 0.05,
        "linksWeight": 0.1,
        "performanceWeight": 0.15,
        "contentQualityWeight": 0.1,
    }


class EnhancedSpellChecker:
    def __init__(self):
        self.spell_checker = None
        self.personal_dict = set()

        if PYSPELLCHECKER_AVAILABLE:
            try:
                self.spell_checker = SpellChecker()
                # Add common technical terms to personal dictionary
                self.personal_dict.update(
                    [
                        "seo",
                        "html",
                        "css",
                        "javascript",
                        "api",
                        "url",
                        "http",
                        "https",
                        "webapp",
                        "website",
                        "webpage",
                        "bigcommerce",
                        "chatgpt",
                        "seokart",
                        "analytics",
                        "optimization",
                        "metadata",
                        "schema",
                        "json",
                        "xml",
                        "ecommerce",
                        "cms",
                        "crm",
                        "saas",
                        "ui",
                        "ux",
                        "roi",
                        "ctr",
                        "serp",
                        "wordpress",
                        "shopify",
                        "magento",
                        "woocommerce",
                    ]
                )
                self.spell_checker.word_frequency.load_words(self.personal_dict)
                logger.debug(
                    "✅ Enhanced spell checker initialized with custom dictionary"
                )
            except Exception as e:
                logger.error(f"Failed to initialize spell checker: {e}")
                self.spell_checker = None

    def check_spelling(self, text: str, url: str) -> List[Dict[str, Any]]:
        """Enhanced spell checking with better accuracy."""
        if not self.spell_checker or not text:
            return []

        spelling_errors = []

        try:
            # Clean and tokenize text
            cleaned_text = self._clean_text(text)
            words = self._extract_words(cleaned_text)

            if not words:
                return []

            # Check spelling in batches for better performance
            batch_size = 100
            for i in range(0, len(words), batch_size):
                batch = words[i : i + batch_size]
                misspelled = self.spell_checker.unknown(batch)

                for word in misspelled:
                    if self._should_check_word(word):
                        context = self._get_word_context(text, word)
                        suggestions = list(self.spell_checker.candidates(word))
                        best_suggestion = suggestions[0] if suggestions else ""

                        confidence = self._calculate_confidence(word, best_suggestion)

                        spelling_errors.append(
                            {
                                "word": word,
                                "context": context,
                                "suggestion": best_suggestion,
                                "isIgnored": False,
                                "confidence": confidence,
                                "position": text.lower().find(word.lower()),
                            }
                        )

                        if len(spelling_errors) >= SEOConstants.MAX_SPELL_ERRORS:
                            break

                if len(spelling_errors) >= SEOConstants.MAX_SPELL_ERRORS:
                    break

        except Exception as e:
            logger.error(f"Spell checking error for {url}: {e}")

        logger.debug(f"Found {len(spelling_errors)} spelling errors in {url}")
        return spelling_errors

    def _clean_text(self, text: str) -> str:
        """Clean text for spell checking."""
        # Remove URLs, emails, and technical strings
        text = re.sub(
            r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
            "",
            text,
        )
        text = re.sub(r"\S+@\S+\.\S+", "", text)
        text = re.sub(r"[0-9]+", "", text)  # Remove numbers
        text = re.sub(r"[^\w\s]", " ", text)  # Keep only words and spaces
        return text

    def _extract_words(self, text: str) -> List[str]:
        """Extract words for spell checking."""
        words = re.findall(r"\b[a-zA-Z]{3,}\b", text)  # Words with 3+ characters
        return [
            word.lower()
            for word in words
            if len(word) >= SEOConstants.SPELL_CHECK_MIN_WORD_LENGTH
        ]

    def _should_check_word(self, word: str) -> bool:
        """Determine if word should be spell checked."""
        if len(word) < SEOConstants.SPELL_CHECK_MIN_WORD_LENGTH:
            return False

        # Skip proper nouns (capitalized words)
        if word[0].isupper() and len(word) > 4:
            return False

        # Skip common technical terms
        if word.lower() in self.personal_dict:
            return False

        # Skip words that are mostly numbers
        if re.search(r"\d", word):
            return False

        return True

    def _get_word_context(self, text: str, word: str, context_length: int = 80) -> str:
        """Get context around a word."""
        word_pos = text.lower().find(word.lower())
        if word_pos == -1:
            return word

        start = max(0, word_pos - context_length // 2)
        end = min(len(text), word_pos + len(word) + context_length // 2)
        context = text[start:end].strip()

        if start > 0:
            context = "..." + context
        if end < len(text):
            context = context + "..."

        return context

    def _calculate_confidence(self, word: str, suggestion: str) -> str:
        """Calculate confidence level for suggestion."""
        if not suggestion:
            return "low"

        # Use edit distance to determine confidence
        distance = self._edit_distance(word, suggestion)
        word_length = len(word)

        if distance <= 1:
            return "high"
        elif distance <= 2 and word_length > 4:
            return "medium"
        else:
            return "low"

    def _edit_distance(self, s1: str, s2: str) -> int:
        """Calculate edit distance between two strings."""
        if len(s1) < len(s2):
            return self._edit_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]


class EnhancedLinkChecker:
    def __init__(self):
        self.session = requests.Session()

        # Enhanced retry strategy
        retry_strategy = Retry(
            total=4,
            backoff_factor=0.5,
            status_forcelist=[408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=30, pool_maxsize=30
        )

        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Enhanced headers rotation
        self.user_agents = SEOConstants.USER_AGENTS
        self.current_ua_index = 0

        # Initialize session headers
        self._update_headers()

        # Disable SSL verification for broken link checking
        self.session.verify = False

    def _update_headers(self):
        """Update session headers with rotating user agent."""
        self.current_ua_index = (self.current_ua_index + 1) % len(self.user_agents)
        self.session.headers.update(
            {
                "User-Agent": self.user_agents[self.current_ua_index],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "no-cache",
            }
        )

    def check_links_comprehensively(
        self, links: List[str], base_url: str
    ) -> Dict[str, List[Dict]]:
        """Comprehensively check all links for issues."""
        broken_links = []
        redirect_links = []
        http_links = []

        # Filter and prepare links
        links_to_check = []
        for link in links[: SEOConstants.MAX_LINKS_TO_CHECK]:
            if self._should_check_link(link, base_url):
                absolute_url = self._make_absolute_url(link, base_url)
                if absolute_url:
                    links_to_check.append(absolute_url)

        logger.debug(f"Checking {len(links_to_check)} links for {base_url}")

        # Use ThreadPoolExecutor for concurrent checking
        with ThreadPoolExecutor(max_workers=15) as executor:
            future_to_link = {
                executor.submit(self._check_single_link_comprehensive, link): link
                for link in links_to_check
            }

            for future in as_completed(future_to_link):
                link = future_to_link[future]
                try:
                    result = future.result(
                        timeout=SEOConstants.BROKEN_LINK_TIMEOUT + 10
                    )
                    if result:
                        if result["type"] == "broken":
                            broken_links.append(result["data"])
                        elif result["type"] == "redirect":
                            redirect_links.append(result["data"])
                        elif result["type"] == "http":
                            http_links.append(result["data"])
                except Exception as e:
                    # If checking fails, consider it broken
                    broken_links.append(
                        {
                            "url": link,
                            "text": link[:50],
                            "status": 0,
                            "error": f"Check failed: {str(e)[:100]}",
                            "errorType": "timeout",
                        }
                    )

        logger.debug(
            f"Found {len(broken_links)} broken, {len(redirect_links)} redirect, {len(http_links)} HTTP links"
        )

        return {
            "brokenLinks": broken_links,
            "redirectLinks": redirect_links,
            "httpLinks": http_links,
        }

    def _should_check_link(self, url: str, base_url: str) -> bool:
        """Enhanced link filtering."""
        if not url or len(url) < 4:
            return False

        # Skip certain protocols and fragments
        if url.startswith(("mailto:", "tel:", "javascript:", "ftp:", "#", "data:")):
            return False

        # Skip anchors on the same page
        if url.startswith("#"):
            return False

        # Check for file extensions to skip
        parsed = urlparse(url)
        path = parsed.path.lower()

        for ext in SEOConstants.IGNORED_EXTENSIONS:
            if path.endswith(ext):
                return False

        return True

    def _make_absolute_url(self, url: str, base_url: str) -> Optional[str]:
        """Convert relative URLs to absolute URLs."""
        try:
            if url.startswith(("http://", "https://")):
                return url
            elif url.startswith("//"):
                parsed_base = urlparse(base_url)
                return f"{parsed_base.scheme}:{url}"
            else:
                return urljoin(base_url, url)
        except Exception:
            return None

    def _check_single_link_comprehensive(self, url: str) -> Optional[Dict[str, Any]]:
        """Comprehensively check a single link."""
        try:
            # Rotate user agent for this request
            self._update_headers()

            # Check for HTTP links (security issue)
            if url.startswith("http://"):
                return {"type": "http", "data": {"url": url, "text": url[:50]}}

            # First try HEAD request (faster)
            try:
                response = self.session.head(
                    url, timeout=SEOConstants.BROKEN_LINK_TIMEOUT, allow_redirects=False
                )

                # Check for redirects
                if response.status_code in [301, 302, 303, 307, 308]:
                    redirect_url = response.headers.get("Location", "Unknown")
                    return {
                        "type": "redirect",
                        "data": {
                            "url": url,
                            "text": url[:50],
                            "redirectTo": redirect_url,
                            "status": response.status_code,
                        },
                    }

                # Check for broken links
                if response.status_code >= 400:
                    return {
                        "type": "broken",
                        "data": {
                            "url": url,
                            "text": url[:50],
                            "status": response.status_code,
                            "errorType": (
                                "client_error"
                                if response.status_code < 500
                                else "server_error"
                            ),
                        },
                    }

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                # If HEAD fails due to timeout/connection, try GET with smaller timeout
                try:
                    response = self.session.get(
                        url,
                        timeout=SEOConstants.BROKEN_LINK_TIMEOUT // 2,
                        allow_redirects=True,
                        stream=True,
                    )

                    # Stop downloading after headers
                    response.close()

                    # Check for redirects in GET request
                    if len(response.history) > 0:
                        return {
                            "type": "redirect",
                            "data": {
                                "url": url,
                                "text": url[:50],
                                "redirectTo": response.url,
                                "status": response.history[-1].status_code,
                            },
                        }

                    # Check for broken links
                    if response.status_code >= 400:
                        return {
                            "type": "broken",
                            "data": {
                                "url": url,
                                "text": url[:50],
                                "status": response.status_code,
                                "errorType": (
                                    "client_error"
                                    if response.status_code < 500
                                    else "server_error"
                                ),
                            },
                        }

                except requests.exceptions.RequestException as e:
                    # Link is definitely broken
                    return {
                        "type": "broken",
                        "data": {
                            "url": url,
                            "text": url[:50],
                            "status": 0,
                            "error": str(e)[:100],
                            "errorType": "connection_error",
                        },
                    }

            except requests.exceptions.RequestException as e:
                # Try GET as final attempt
                try:
                    response = self.session.get(
                        url,
                        timeout=SEOConstants.BROKEN_LINK_TIMEOUT,
                        allow_redirects=True,
                        stream=True,
                    )
                    response.close()

                    if response.status_code >= 400:
                        return {
                            "type": "broken",
                            "data": {
                                "url": url,
                                "text": url[:50],
                                "status": response.status_code,
                                "errorType": (
                                    "client_error"
                                    if response.status_code < 500
                                    else "server_error"
                                ),
                            },
                        }

                except requests.exceptions.RequestException:
                    return {
                        "type": "broken",
                        "data": {
                            "url": url,
                            "text": url[:50],
                            "status": 0,
                            "error": str(e)[:100],
                            "errorType": "connection_error",
                        },
                    }

        except Exception as e:
            # Any other error means the link is problematic
            return {
                "type": "broken",
                "data": {
                    "url": url,
                    "text": url[:50],
                    "status": 0,
                    "error": f"Unexpected error: {str(e)[:100]}",
                    "errorType": "unknown_error",
                },
            }

        return None


class EnhancedDuplicateDetector:
    def __init__(self):
        self.tfidf_vectorizer = None
        if SKLEARN_AVAILABLE:
            try:
                self.tfidf_vectorizer = TfidfVectorizer(
                    max_features=1000,
                    stop_words="english",
                    ngram_range=(1, 2),
                    min_df=1,
                    max_df=0.95,
                )
                logger.debug("✅ TF-IDF vectorizer initialized")
            except Exception as e:
                logger.error(f"Failed to initialize TF-IDF: {e}")
                self.tfidf_vectorizer = None

    def check_duplicates_advanced(
        self, content: str, title: str, description: str, url: str
    ) -> Dict[str, Any]:
        """Advanced duplicate detection using multiple algorithms."""
        global GLOBAL_CONTENT_STORE

        duplicates = {
            "titleDuplicates": [],
            "descriptionDuplicates": [],
            "contentDuplicates": [],
        }

        # Store current page data
        GLOBAL_CONTENT_STORE["pages"][url] = {
            "title": title,
            "description": description,
            "content": content,
            "content_hash": (
                hashlib.md5(content.encode()).hexdigest() if content else ""
            ),
            "content_normalized": self._normalize_content(content),
        }

        # Check title duplicates
        if title:
            duplicates["titleDuplicates"] = self._find_title_duplicates(title, url)

        # Check description duplicates
        if description:
            duplicates["descriptionDuplicates"] = self._find_description_duplicates(
                description, url
            )

        # Check content duplicates with multiple methods
        if content and len(content) > 100:
            duplicates["contentDuplicates"] = self._find_content_duplicates_advanced(
                content, url
            )

        return duplicates

    def _find_title_duplicates(
        self, title: str, current_url: str
    ) -> List[Dict[str, Any]]:
        """Find title duplicates with fuzzy matching."""
        global GLOBAL_CONTENT_STORE
        duplicates = []

        title_normalized = title.lower().strip()

        for other_url, other_data in GLOBAL_CONTENT_STORE["pages"].items():
            if other_url == current_url:
                continue

            other_title = other_data.get("title", "")
            if not other_title:
                continue

            other_title_normalized = other_title.lower().strip()

            # Exact match
            if title_normalized == other_title_normalized:
                duplicates.append(
                    {
                        "url": other_url,
                        "title": other_title,
                        "similarity": 100.0,
                        "method": "exact",
                    }
                )
            # Fuzzy match using fuzzywuzzy if available
            elif FUZZYWUZZY_AVAILABLE:
                similarity = fuzz.ratio(title_normalized, other_title_normalized)
                if similarity >= 85:  # High similarity threshold for titles
                    duplicates.append(
                        {
                            "url": other_url,
                            "title": other_title,
                            "similarity": similarity,
                            "method": "fuzzy",
                        }
                    )

        return duplicates

    def _find_description_duplicates(
        self, description: str, current_url: str
    ) -> List[Dict[str, Any]]:
        """Find description duplicates with fuzzy matching."""
        global GLOBAL_CONTENT_STORE
        duplicates = []

        desc_normalized = description.lower().strip()

        for other_url, other_data in GLOBAL_CONTENT_STORE["pages"].items():
            if other_url == current_url:
                continue

            other_desc = other_data.get("description", "")
            if not other_desc:
                continue

            other_desc_normalized = other_desc.lower().strip()

            # Exact match
            if desc_normalized == other_desc_normalized:
                duplicates.append(
                    {
                        "url": other_url,
                        "description": other_desc,
                        "similarity": 100.0,
                        "method": "exact",
                    }
                )
            # Fuzzy match
            elif FUZZYWUZZY_AVAILABLE:
                similarity = fuzz.ratio(desc_normalized, other_desc_normalized)
                if similarity >= 80:  # High similarity threshold for descriptions
                    duplicates.append(
                        {
                            "url": other_url,
                            "description": other_desc,
                            "similarity": similarity,
                            "method": "fuzzy",
                        }
                    )

        return duplicates

    def _find_content_duplicates_advanced(
        self, content: str, current_url: str
    ) -> List[Dict[str, Any]]:
        """Find content duplicates using multiple advanced methods."""
        global GLOBAL_CONTENT_STORE
        duplicates = []

        current_normalized = self._normalize_content(content)
        current_words = set(current_normalized.split())

        for other_url, other_data in GLOBAL_CONTENT_STORE["pages"].items():
            if other_url == current_url:
                continue

            other_content = other_data.get("content", "")
            if not other_content or len(other_content) < 100:
                continue

            other_normalized = other_data.get(
                "content_normalized"
            ) or self._normalize_content(other_content)
            other_words = set(other_normalized.split())

            # Method 1: TF-IDF cosine similarity (most accurate for content)
            if SKLEARN_AVAILABLE and self.tfidf_vectorizer:
                try:
                    similarity_tfidf = self._calculate_tfidf_similarity(
                        current_normalized, other_normalized
                    )
                except Exception as e:
                    logger.debug(f"TF-IDF similarity calculation failed: {e}")
                    similarity_tfidf = 0
            else:
                similarity_tfidf = 0

            # Method 2: Jaccard similarity (word overlap)
            similarity_jaccard = self._jaccard_similarity(current_words, other_words)

            # Method 3: Fuzzy sequence similarity
            if FUZZYWUZZY_AVAILABLE:
                similarity_fuzzy = fuzz.ratio(current_normalized, other_normalized)
            else:
                similarity_fuzzy = (
                    difflib.SequenceMatcher(
                        None, current_normalized, other_normalized
                    ).ratio()
                    * 100
                )

            # Method 4: Hash-based similarity (for exact matches)
            similarity_hash = (
                100
                if other_data.get("content_hash")
                == hashlib.md5(content.encode()).hexdigest()
                else 0
            )

            # Use the highest similarity score
            max_similarity = max(
                similarity_tfidf, similarity_jaccard, similarity_fuzzy, similarity_hash
            )
            best_method = (
                "tfidf"
                if similarity_tfidf >= max_similarity
                else (
                    "jaccard"
                    if similarity_jaccard >= max_similarity
                    else "fuzzy" if similarity_fuzzy >= max_similarity else "hash"
                )
            )

            # Use dynamic threshold based on content length
            content_length = len(current_normalized.split())
            threshold = SEOConstants.DUPLICATE_SIMILARITY_THRESHOLD
            if content_length < 100:  # Short content needs higher similarity
                threshold = 85
            elif content_length > 500:  # Long content can have lower threshold
                threshold = 70

            if max_similarity >= threshold:
                duplicates.append(
                    {
                        "duplicateUrl": other_url,
                        "similarityScore": round(max_similarity, 2),
                        "isIgnored": False,
                        "method": best_method,
                        "details": {
                            "tfidf": (
                                round(similarity_tfidf, 2)
                                if similarity_tfidf > 0
                                else 0
                            ),
                            "jaccard": round(similarity_jaccard, 2),
                            "fuzzy": round(similarity_fuzzy, 2),
                            "hash": similarity_hash,
                        },
                    }
                )

        return duplicates

    def _calculate_tfidf_similarity(self, text1: str, text2: str) -> float:
        """Calculate TF-IDF cosine similarity between two texts."""
        if not SKLEARN_AVAILABLE or not self.tfidf_vectorizer:
            return 0

        try:
            # Fit and transform both texts
            tfidf_matrix = self.tfidf_vectorizer.fit_transform([text1, text2])

            # Calculate cosine similarity
            similarity_matrix = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])

            return similarity_matrix[0][0] * 100
        except Exception as e:
            logger.debug(f"TF-IDF calculation error: {e}")
            return 0

    def _normalize_content(self, content: str) -> str:
        """Normalize content for comparison."""
        if not content:
            return ""

        # Remove HTML tags
        content = re.sub(r"<[^>]+>", "", content)
        # Normalize whitespace
        content = re.sub(r"\s+", " ", content)
        # Remove special characters but keep basic punctuation
        content = re.sub(r"[^\w\s.,!?;:-]", "", content)
        # Convert to lowercase
        content = content.lower().strip()

        return content

    def _jaccard_similarity(self, set1: set, set2: set) -> float:
        """Calculate Jaccard similarity between two sets."""
        if not set1 and not set2:
            return 100.0
        if not set1 or not set2:
            return 0.0

        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))

        return (intersection / union) * 100 if union > 0 else 0.0


class PlaywrightAnalyzer:
    @staticmethod
    async def get_page_content_advanced(url: str) -> Dict[str, Any]:
        """Get page content using Playwright with enhanced settings."""
        if not PLAYWRIGHT_AVAILABLE:
            return {"error": "Playwright not available"}

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--no-first-run",
                        "--disable-background-timer-throttling",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-renderer-backgrounding",
                    ],
                )

                context = await browser.new_context(
                    user_agent=random.choice(SEOConstants.USER_AGENTS),
                    viewport={"width": 1920, "height": 1080},
                    ignore_https_errors=True,
                )

                page = await context.new_page()

                # Navigate with longer timeout and better error handling
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    # Wait for additional dynamic content
                    await page.wait_for_timeout(5000)

                    # Try to wait for network idle as well
                    try:
                        await page.wait_for_load_state("networkidle", timeout=15000)
                    except:
                        pass  # Continue if networkidle fails

                except Exception as e:
                    logger.warning(f"Navigation issue for {url}: {e}")
                    # Try to continue with partial content

                # Get comprehensive page data
                html_content = await page.content()
                title = await page.title()
                final_url = page.url

                # Get additional performance metrics
                try:
                    performance_timing = await page.evaluate(
                        """
                        () => {
                            const timing = performance.timing;
                            return {
                                domContentLoaded: timing.domContentLoadedEventEnd - timing.navigationStart,
                                loadComplete: timing.loadEventEnd - timing.navigationStart,
                                domInteractive: timing.domInteractive - timing.navigationStart
                            };
                        }
                    """
                    )
                except:
                    performance_timing = {}

                await browser.close()

                return {
                    "html": html_content,
                    "title": title,
                    "finalUrl": final_url,
                    "performance": performance_timing,
                }

        except Exception as e:
            logger.error(f"Playwright error for {url}: {str(e)}")
            return {"error": str(e)}


class EnhancedSEOAnalyzer:
    def __init__(self):
        self.spell_checker = EnhancedSpellChecker()
        self.link_checker = EnhancedLinkChecker()
        self.duplicate_detector = EnhancedDuplicateDetector()

    async def analyze_page_comprehensive(
        self, url: str, use_playwright: bool = True
    ) -> Dict[str, Any]:
        """Perform comprehensive SEO analysis with maximum accuracy."""
        start_time = time.time()
        logger.debug(f"🚀 Starting comprehensive analysis of: {url}")

        # Get page content with multiple fallback methods
        html_content, final_url, performance_data = await self._get_page_content_robust(
            url, use_playwright
        )

        if not html_content:
            return {
                "pageUrl": url,
                "error": "Failed to retrieve page content after multiple attempts",
                "lastCrawled": datetime.now().isoformat(),
            }

        # Parse HTML with better error handling
        try:
            soup = BeautifulSoup(html_content, "html.parser")
        except Exception as e:
            logger.error(f"HTML parsing failed for {url}: {e}")
            return {
                "pageUrl": url,
                "error": f"HTML parsing failed: {str(e)}",
                "lastCrawled": datetime.now().isoformat(),
            }

        website_url = self._get_website_url(final_url)

        # Extract all basic elements with enhanced methods
        title = self._extract_title_enhanced(soup)
        meta_description = self._extract_meta_description_enhanced(soup)
        content = self._extract_main_content_enhanced(soup)

        # Initialize comprehensive result
        result = {
            "websiteUrl": website_url,
            "pageUrl": final_url,
            "lastCrawled": datetime.now().isoformat(),
            "responseTime": time.time() - start_time,
            "responseSize": len(html_content),
            "performance": performance_data or {},
        }

        # Perform all comprehensive analyses
        try:
            result.update(self._analyze_title_comprehensive(title, final_url))
            result.update(
                self._analyze_meta_description_comprehensive(
                    meta_description, final_url
                )
            )
            result.update(self._analyze_content_comprehensive(content, final_url))
            result.update(self._analyze_headings_comprehensive(soup))
            result.update(self._analyze_url_comprehensive(final_url))
            result.update(
                self._analyze_technical_seo_comprehensive(soup, final_url, html_content)
            )
            result.update(self._analyze_images_comprehensive(soup, final_url))
            result.update(self._analyze_links_comprehensive(soup, final_url))
            result.update(
                self._analyze_performance_comprehensive(
                    soup, html_content, performance_data
                )
            )

            # Enhanced duplicate detection
            duplicates = self.duplicate_detector.check_duplicates_advanced(
                content, title, meta_description, final_url
            )
            result["duplicates"] = duplicates

            # Calculate comprehensive scores
            result.update(self._calculate_comprehensive_seo_score(result))

        except Exception as e:
            logger.error(f"Analysis error for {url}: {e}")
            result["error"] = f"Analysis failed: {str(e)}"

        processing_time = time.time() - start_time
        result["processingTime"] = round(processing_time, 2)

        logger.debug(
            f"✅ Comprehensive analysis completed for {url} in {processing_time:.2f}s"
        )
        return result

    async def _get_page_content_robust(
        self, url: str, use_playwright: bool
    ) -> Tuple[str, str, Optional[Dict]]:
        """Get page content with multiple fallback methods."""
        html_content = ""
        final_url = url
        performance_data = None

        # Method 1: Playwright (most comprehensive)
        if use_playwright and PLAYWRIGHT_AVAILABLE:
            try:
                playwright_result = await PlaywrightAnalyzer.get_page_content_advanced(
                    url
                )
                if "error" not in playwright_result:
                    html_content = playwright_result["html"]
                    final_url = playwright_result.get("finalUrl", url)
                    performance_data = playwright_result.get("performance")
                    logger.debug(f"✅ Content retrieved using Playwright for {url}")
                else:
                    logger.warning(
                        f"Playwright failed for {url}: {playwright_result['error']}"
                    )
            except Exception as e:
                logger.warning(f"Playwright exception for {url}: {e}")

        # Method 2: Enhanced requests fallback
        if not html_content:
            try:
                html_content, final_url = self._get_content_with_enhanced_requests(url)
                if html_content:
                    logger.debug(
                        f"✅ Content retrieved using enhanced requests for {url}"
                    )
            except Exception as e:
                logger.error(f"Enhanced requests failed for {url}: {e}")

        # Method 3: Basic requests as last resort
        if not html_content:
            try:
                html_content, final_url = self._get_content_basic_requests(url)
                if html_content:
                    logger.debug(f"✅ Content retrieved using basic requests for {url}")
            except Exception as e:
                logger.error(f"Basic requests failed for {url}: {e}")

        return html_content, final_url, performance_data

    def _get_content_with_enhanced_requests(self, url: str) -> Tuple[str, str]:
        """Enhanced requests method with better headers and error handling."""
        session = requests.Session()

        # Enhanced retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        headers = {
            "User-Agent": random.choice(SEOConstants.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
        }

        response = session.get(
            url,
            headers=headers,
            timeout=45,
            allow_redirects=True,
            verify=False,
        )
        response.raise_for_status()
        return response.text, response.url

    def _get_content_basic_requests(self, url: str) -> Tuple[str, str]:
        """Basic requests method as final fallback."""
        response = requests.get(
            url,
            headers={"User-Agent": SEOConstants.USER_AGENTS[0]},
            timeout=30,
            allow_redirects=True,
            verify=False,
        )
        response.raise_for_status()
        return response.text, response.url

    def _get_website_url(self, page_url: str) -> str:
        """Extract website URL."""
        parsed = urlparse(page_url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _extract_title_enhanced(self, soup: BeautifulSoup) -> str:
        """Extract page title with enhanced methods."""
        # Try multiple methods to get title
        title_candidates = []

        # Method 1: Standard title tag
        title_tag = soup.find("title")
        if title_tag and title_tag.get_text().strip():
            title_candidates.append(title_tag.get_text().strip())

        # Method 2: OG title
        og_title = soup.find("meta", property="og:title")
        if og_title and og_title.get("content", "").strip():
            title_candidates.append(og_title.get("content", "").strip())

        # Method 3: Twitter title
        twitter_title = soup.find("meta", attrs={"name": "twitter:title"})
        if twitter_title and twitter_title.get("content", "").strip():
            title_candidates.append(twitter_title.get("content", "").strip())

        # Method 4: H1 as fallback
        h1_tag = soup.find("h1")
        if h1_tag and h1_tag.get_text().strip():
            title_candidates.append(h1_tag.get_text().strip())

        # Return the first non-empty title, preferring the standard title tag
        for title in title_candidates:
            if title and len(title.strip()) > 0:
                return title.strip()

        return ""

    def _extract_meta_description_enhanced(self, soup: BeautifulSoup) -> str:
        """Extract meta description with enhanced methods."""
        # Try multiple methods to get description
        desc_candidates = []

        # Method 1: Standard meta description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content", "").strip():
            desc_candidates.append(meta_desc.get("content", "").strip())

        # Method 2: OG description
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content", "").strip():
            desc_candidates.append(og_desc.get("content", "").strip())

        # Method 3: Twitter description
        twitter_desc = soup.find("meta", attrs={"name": "twitter:description"})
        if twitter_desc and twitter_desc.get("content", "").strip():
            desc_candidates.append(twitter_desc.get("content", "").strip())

        # Return the first non-empty description
        for desc in desc_candidates:
            if desc and len(desc.strip()) > 0:
                return desc.strip()

        return ""

    def _extract_main_content_enhanced(self, soup: BeautifulSoup) -> str:
        """Extract main content with enhanced content detection."""
        # Remove unwanted elements
        for element in soup(
            ["script", "style", "nav", "header", "footer", "aside", "form"]
        ):
            element.decompose()

        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Try multiple content selectors in order of preference
        content_selectors = [
            "main",
            "article",
            '[role="main"]',
            ".main-content",
            ".content",
            ".post-content",
            ".entry-content",
            ".article-content",
            ".page-content",
            ".blog-post",
            ".post-body",
            "#content",
            "#main",
            ".container .content",
            ".wrapper .content",
        ]

        for selector in content_selectors:
            try:
                content_elements = soup.select(selector)
                if content_elements:
                    # Get the largest content element
                    largest_element = max(
                        content_elements, key=lambda x: len(x.get_text())
                    )
                    content_text = largest_element.get_text(separator=" ", strip=True)
                    if len(content_text) > SEOConstants.MIN_TEXT_LENGTH:
                        return content_text
            except:
                continue

        # Fallback to body content
        if soup.body:
            body_text = soup.body.get_text(separator=" ", strip=True)
            if len(body_text) > SEOConstants.MIN_TEXT_LENGTH:
                return body_text

        # Final fallback to all text
        all_text = soup.get_text(separator=" ", strip=True)
        return all_text if len(all_text) > SEOConstants.MIN_TEXT_LENGTH else ""

    def _analyze_title_comprehensive(self, title: str, url: str) -> Dict[str, Any]:
        """Comprehensive title analysis."""
        title_length = len(title)

        title_issues = {
            "missing": not title,
            "tooShort": 0 < title_length < SEOConstants.TITLE_MIN_LENGTH,
            "tooLong": title_length > SEOConstants.TITLE_MAX_LENGTH,
            "multiple": False,  # Would need soup context
            "duplicate": False,  # Checked in duplicates
        }

        # Enhanced scoring
        score = 10
        if title_issues["missing"]:
            score = 0
        else:
            if title_issues["tooShort"]:
                score -= 4
            elif title_issues["tooLong"]:
                score -= 3

            # Additional checks
            if title.isupper():  # All caps penalty
                score -= 1
            if title.count("|") > 2 or title.count("-") > 3:  # Too many separators
                score -= 1
            if not any(char.isalpha() for char in title):  # No letters
                score -= 2

        return {
            "title": title,
            "titleLength": title_length,
            "titleScore": max(0, min(10, score)),
            "titleIssues": title_issues,
        }

    def _analyze_meta_description_comprehensive(
        self, meta_description: str, url: str
    ) -> Dict[str, Any]:
        """Comprehensive meta description analysis."""
        meta_length = len(meta_description)

        meta_issues = {
            "missing": not meta_description,
            "tooShort": 0 < meta_length < SEOConstants.META_DESC_MIN_LENGTH,
            "tooLong": meta_length > SEOConstants.META_DESC_MAX_LENGTH,
            "multiple": False,
            "duplicate": False,
        }

        # Enhanced scoring
        score = 10
        if meta_issues["missing"]:
            score = 0
        else:
            if meta_issues["tooShort"]:
                score -= 4
            elif meta_issues["tooLong"]:
                score -= 3

            # Additional checks
            if meta_description == meta_description.upper():  # All caps
                score -= 1
            if not meta_description.endswith((".", "!", "?")):  # No proper ending
                score -= 0.5

        return {
            "metaDescription": meta_description,
            "metaDescriptionLength": meta_length,
            "metaDescriptionScore": max(0, min(10, score)),
            "metaDescriptionIssues": meta_issues,
        }

    def _analyze_content_comprehensive(self, content: str, url: str) -> Dict[str, Any]:
        """Comprehensive content analysis with enhanced spell checking."""
        if not content:
            return {
                "wordCount": 0,
                "contentScore": 0,
                "readabilityScore": 0,
                "contentIssues": {
                    "tooShort": True,
                    "lowKeywordDensity": True,
                    "poorReadability": True,
                },
                "contentQuality": {
                    "spellingErrors": [],
                    "grammarErrors": [],
                    "duplicateContent": [],
                    "contentQualityScore": 0,
                },
            }

        # Enhanced word counting
        words = re.findall(r"\b\w+\b", content.lower())
        word_count = len(words)

        # Enhanced readability score
        try:
            readability_score = textstat.flesch_reading_ease(content)
            readability_score = max(0, min(100, readability_score))
        except:
            readability_score = 50  # Default middle score

        # Comprehensive spell checking
        spelling_errors = self.spell_checker.check_spelling(content, url)

        # Enhanced grammar checking
        grammar_errors = self._check_grammar_comprehensive(content)

        # Calculate content quality score
        content_quality_score = self._calculate_content_quality_score_enhanced(
            spelling_errors, grammar_errors, word_count, readability_score
        )

        content_issues = {
            "tooShort": word_count < SEOConstants.CONTENT_MIN_WORDS,
            "lowKeywordDensity": self._check_keyword_density(content),
            "poorReadability": readability_score < 30,
        }

        # Enhanced content scoring
        score = 10
        if content_issues["tooShort"]:
            score -= 5
        if content_issues["poorReadability"]:
            score -= 2
        if content_issues["lowKeywordDensity"]:
            score -= 1

        return {
            "wordCount": word_count,
            "contentScore": max(0, min(10, score)),
            "readabilityScore": readability_score,
            "contentIssues": content_issues,
            "contentQuality": {
                "spellingErrors": spelling_errors,
                "grammarErrors": grammar_errors,
                "duplicateContent": [],  # Will be filled by duplicate detector
                "contentQualityScore": content_quality_score,
            },
        }

    def _check_grammar_comprehensive(self, content: str) -> List[Dict[str, Any]]:
        """Enhanced grammar checking with more patterns."""
        grammar_errors = []

        # Enhanced grammar patterns
        patterns = [
            (r"\b(your)\s+(going|coming|doing)\b", "you're", "Contraction needed"),
            (r"\b(there)\s+(going|coming|doing)\b", "they're", "Contraction needed"),
            (r"\b(its)\s+a\b", "it's a", "Contraction needed"),
            (
                r"\b(could|should|would)\s+of\b",
                "could/should/would have",
                "Incorrect preposition",
            ),
            (r"\.\s*[a-z]", ". [Capital]", "Sentence should start with capital"),
            (r"\bthen\b(?=\s+[a-zA-Z]+er\b)", "than", "Comparison word needed"),
            (r"\baffect\b(?=\s+on\b)", "effect", "Noun needed"),
            (r"\bloose\b(?=\s+(weight|money|game)\b)", "lose", "Verb needed"),
            (r"\bi\s", "I ", "Capital I needed"),
            (r"\s{2,}", " ", "Extra spaces"),
            (r"[.!?]{2,}", ".", "Multiple punctuation"),
        ]

        for pattern, suggestion, description in patterns:
            matches = re.finditer(pattern, content, re.IGNORECASE)
            for match in matches:
                context_start = max(0, match.start() - 50)
                context_end = min(len(content), match.end() + 50)
                context = content[context_start:context_end].strip()

                grammar_errors.append(
                    {
                        "text": match.group(0),
                        "context": context,
                        "suggestion": suggestion,
                        "description": description,
                        "isIgnored": False,
                        "position": match.start(),
                    }
                )

                if len(grammar_errors) >= SEOConstants.MAX_GRAMMAR_ERRORS:
                    break

        return grammar_errors

    def _check_keyword_density(self, content: str) -> bool:
        """Check if content has poor keyword density."""
        if not content or len(content) < 100:
            return True

        words = re.findall(r"\b\w+\b", content.lower())
        if len(words) < 50:
            return True

        # Calculate keyword density for top words
        word_freq = Counter(words)
        most_common = word_freq.most_common(10)

        # Check if any word appears too frequently (keyword stuffing)
        total_words = len(words)
        for word, count in most_common:
            if len(word) > 3:  # Ignore short words
                density = (count / total_words) * 100
                if density > 5:  # More than 5% is likely keyword stuffing
                    return True

        return False

    def _analyze_headings_comprehensive(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Comprehensive heading structure analysis."""
        heading_counts = {}
        heading_texts = {}

        for i in range(1, 7):
            headings = soup.find_all(f"h{i}")
            heading_counts[f"h{i}Count"] = len(headings)
            heading_texts[f"h{i}Texts"] = [
                h.get_text().strip() for h in headings[:5]
            ]  # First 5 only

        heading_structure = {
            **heading_counts,
            "h1Missing": heading_counts["h1Count"] == 0,
            "h1Multiple": heading_counts["h1Count"] > 1,
            "h2H3AtTop": False,  # Could be enhanced with position checking
            "headingTexts": heading_texts,
        }

        # Enhanced scoring
        score = 10
        if heading_structure["h1Missing"]:
            score -= 6
        elif heading_structure["h1Multiple"]:
            score -= 4

        # Check for logical heading hierarchy
        has_h2_without_h1 = (
            heading_counts["h2Count"] > 0 and heading_counts["h1Count"] == 0
        )
        has_h3_without_h2 = (
            heading_counts["h3Count"] > 0 and heading_counts["h2Count"] == 0
        )

        if has_h2_without_h1 or has_h3_without_h2:
            score -= 2

        heading_structure["headingScore"] = max(0, min(10, score))
        return {"headingStructure": heading_structure}

    def _analyze_url_comprehensive(self, url: str) -> Dict[str, Any]:
        """Comprehensive URL analysis."""
        url_length = len(url)
        parsed_url = urlparse(url)

        url_issues = {
            "tooLong": url_length > SEOConstants.URL_MAX_LENGTH,
            "containsSpecialChars": bool(
                re.search(r"[^a-zA-Z0-9\-\/\._~:?#\[\]@!$&\'()*+,;=]", url)
            ),
            "containsParams": "?" in url,
            "nonDescriptive": (
                len(parsed_url.path.split("/")[-1]) < 3
                if parsed_url.path.split("/")[-1]
                else True
            ),
            "hasSpaces": " " in url,
            "hasUnderscores": "_" in parsed_url.path,
            "tooManySubdirectories": len([p for p in parsed_url.path.split("/") if p])
            > 5,
        }

        # Enhanced scoring
        score = 10
        if url_issues["tooLong"]:
            score -= 3
        if url_issues["containsSpecialChars"]:
            score -= 3
        if url_issues["containsParams"]:
            score -= 1
        if url_issues["nonDescriptive"]:
            score -= 2
        if url_issues["hasSpaces"]:
            score -= 2
        if url_issues["hasUnderscores"]:
            score -= 1
        if url_issues["tooManySubdirectories"]:
            score -= 1

        return {"urlScore": max(0, min(10, score)), "urlIssues": url_issues}

    def _analyze_technical_seo_comprehensive(
        self, soup: BeautifulSoup, url: str, html_content: str
    ) -> Dict[str, Any]:
        """Comprehensive technical SEO analysis."""
        # Canonical analysis
        canonical_tag = soup.find("link", rel="canonical")
        canonical_url = canonical_tag.get("href", "") if canonical_tag else ""

        # Robots analysis
        robots_tag = soup.find("meta", attrs={"name": "robots"})
        robots_content = robots_tag.get("content", "") if robots_tag else ""

        # Hreflang analysis
        hreflang_tags = soup.find_all("link", rel="alternate", attrs={"hreflang": True})
        hreflang_data = []
        for tag in hreflang_tags:
            hreflang_data.append(
                {"lang": tag.get("hreflang", ""), "url": tag.get("href", "")}
            )

        # Structured data analysis
        structured_data_scripts = soup.find_all("script", type="application/ld+json")
        structured_data_types = []
        for script in structured_data_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and "@type" in data:
                    structured_data_types.append(data["@type"])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "@type" in item:
                            structured_data_types.append(item["@type"])
            except:
                pass

        # Additional technical checks
        has_viewport = bool(soup.find("meta", attrs={"name": "viewport"}))
        has_charset = bool(soup.find("meta", attrs={"charset": True})) or bool(
            soup.find("meta", attrs={"http-equiv": "Content-Type"})
        )

        technical_seo = {
            "canonicalTagExists": bool(canonical_tag),
            "canonicalUrl": canonical_url,
            "robotsDirectives": robots_content,
            "hreflangTags": hreflang_data,
            "structuredData": len(structured_data_types) > 0,
            "structuredDataTypes": structured_data_types,
            "hasViewport": has_viewport,
            "hasCharset": has_charset,
            "htmlSize": len(html_content),
        }

        # Enhanced scoring
        score = 10
        if not technical_seo["canonicalTagExists"]:
            score -= 3
        if not technical_seo["robotsDirectives"]:
            score -= 1
        if not technical_seo["hasViewport"]:
            score -= 2
        if not technical_seo["hasCharset"]:
            score -= 1
        if not technical_seo["structuredData"]:
            score -= 2

        technical_seo["technicalScore"] = max(0, min(10, score))
        return {"technicalSeo": technical_seo}

    def _analyze_images_comprehensive(
        self, soup: BeautifulSoup, base_url: str
    ) -> Dict[str, Any]:
        """Comprehensive image analysis."""
        images_tags = soup.find_all("img")

        alt_text_missing = []
        title_missing = []
        dimension_missing = []
        lazy_loaded = 0
        responsive_images = 0
        next_gen_formats = 0
        oversized_images = []
        unoptimized_images = []

        for img in images_tags:
            src = (
                img.get("src", "")
                or img.get("data-src", "")
                or img.get("data-lazy-src", "")
            )
            alt_text = img.get("alt", "")
            title_text = img.get("title", "")
            width = img.get("width", "")
            height = img.get("height", "")

            # Alt text check
            if not alt_text or not alt_text.strip():
                alt_text_missing.append(
                    {
                        "src": src[:100] if src else "No src",
                        "context": str(img.parent)[:100] if img.parent else "",
                    }
                )

            # Title check
            if not title_text or not title_text.strip():
                title_missing.append({"src": src[:100] if src else "No src"})

            # Dimensions check
            if not width or not height:
                dimension_missing.append({"src": src[:100] if src else "No src"})
            else:
                try:
                    w, h = int(width), int(height)
                    if w > 2000 or h > 2000:
                        oversized_images.append(
                            {
                                "src": src[:100] if src else "No src",
                                "width": w,
                                "height": h,
                            }
                        )
                except:
                    pass

            # Lazy loading check
            if img.get("loading") == "lazy" or "lazy" in img.get("class", []):
                lazy_loaded += 1

            # Responsive check
            if img.get("srcset") or img.get("sizes"):
                responsive_images += 1

            # Next-gen format check
            if src and any(fmt in src.lower() for fmt in [".webp", ".avif"]):
                next_gen_formats += 1

            # Optimization check (basic heuristic)
            if src and not any(
                opt in src.lower()
                for opt in ["optimized", "compressed", "webp", "avif"]
            ):
                unoptimized_images.append({"src": src[:100] if src else "No src"})

        total_images = len(images_tags)
        images_with_alt = total_images - len(alt_text_missing)
        images_with_title = total_images - len(title_missing)
        images_with_dimensions = total_images - len(dimension_missing)

        # Enhanced scoring
        score = 10
        if total_images > 0:
            alt_ratio = len(alt_text_missing) / total_images
            if alt_ratio > 0.7:
                score -= 6
            elif alt_ratio > 0.5:
                score -= 4
            elif alt_ratio > 0.3:
                score -= 2

            # Additional penalties
            if len(oversized_images) / total_images > 0.3:
                score -= 2
            if responsive_images / total_images < 0.3:
                score -= 1

        return {
            "images": {
                "totalCount": total_images,
                "withAlt": images_with_alt,
                "withTitle": images_with_title,
                "withDimensions": images_with_dimensions,
                "lazyLoaded": lazy_loaded,
                "responsive": responsive_images,
                "altTextMissing": alt_text_missing[:20],  # Limit for response size
                "oversizedImages": oversized_images[:10],
                "unoptimizedImages": unoptimized_images[:10],
                "nextGenFormats": next_gen_formats,
                "score": max(0, min(10, score)),
            }
        }

    def _analyze_links_comprehensive(
        self, soup: BeautifulSoup, url: str
    ) -> Dict[str, Any]:
        """Comprehensive link analysis with enhanced broken link detection."""
        link_tags = soup.find_all("a", href=True)
        base_domain = urlparse(url).netloc.lower()

        # Initialize counters
        total_count = 0
        internal_count = 0
        external_count = 0
        social_media_count = 0
        no_follow_count = 0
        email_links = 0
        phone_links = 0
        download_links = 0
        empty_links = []
        long_urls = []
        social_media_links = []

        all_links = []

        # Social media domains
        social_domains = {
            "facebook.com",
            "twitter.com",
            "instagram.com",
            "linkedin.com",
            "youtube.com",
            "pinterest.com",
            "snapchat.com",
            "tiktok.com",
        }

        for link in link_tags:
            href = link.get("href", "").strip()
            link_text = link.get_text().strip()
            rel = link.get("rel", [])

            if not href:
                empty_links.append({"text": link_text[:50]})
                continue

            # Skip special links
            if href.startswith(("javascript:", "#")):
                continue

            total_count += 1

            # Email and phone links
            if href.startswith("mailto:"):
                email_links += 1
                continue
            elif href.startswith("tel:"):
                phone_links += 1
                continue

            # Make absolute URL for analysis
            if not href.startswith(("http://", "https://")):
                href = urljoin(url, href)

            all_links.append(href)

            # Check URL length
            if len(href) > 100:
                long_urls.append({"url": href[:100], "text": link_text[:50]})

            # Parse link domain
            try:
                link_domain = urlparse(href).netloc.lower()
            except:
                continue

            # Categorize links
            if link_domain == base_domain or not link_domain:
                internal_count += 1
            else:
                external_count += 1

            # Check for social media links
            if any(social in link_domain for social in social_domains):
                social_media_count += 1
                social_media_links.append(
                    {
                        "platform": next(
                            social for social in social_domains if social in link_domain
                        ),
                        "url": href[:100],
                    }
                )

            # Check for nofollow
            if isinstance(rel, list) and "nofollow" in rel:
                no_follow_count += 1
            elif isinstance(rel, str) and "nofollow" in rel:
                no_follow_count += 1

            # Check for download links
            if any(ext in href.lower() for ext in SEOConstants.IGNORED_EXTENSIONS):
                download_links += 1

        # Comprehensive link checking
        logger.debug(f"Starting comprehensive link check for {len(all_links)} links")
        link_results = self.link_checker.check_links_comprehensively(all_links, url)

        # Enhanced scoring
        score = 10
        if total_count > 0:
            broken_ratio = len(link_results["brokenLinks"]) / total_count
            if broken_ratio > 0.15:
                score -= 8
            elif broken_ratio > 0.1:
                score -= 6
            elif broken_ratio > 0.05:
                score -= 3
            elif broken_ratio > 0:
                score -= 1

            # Additional scoring factors
            if internal_count == 0 and external_count > 0:
                score -= 2  # No internal links
            if external_count > internal_count * 3:
                score -= 1  # Too many external links

        return {
            "links": {
                "totalCount": total_count,
                "internalCount": internal_count,
                "externalCount": external_count,
                "socialMediaCount": social_media_count,
                "noFollowCount": no_follow_count,
                "emailLinks": email_links,
                "phoneLinks": phone_links,
                "downloadLinks": download_links,
                "brokenLinks": link_results["brokenLinks"],
                "redirectLinks": link_results["redirectLinks"],
                "httpLinks": link_results["httpLinks"],
                "socialMediaLinks": social_media_links[:10],
                "longUrls": long_urls[:10],
                "emptyLinks": empty_links[:10],
                "score": max(0, min(10, score)),
            }
        }

    def _analyze_performance_comprehensive(
        self, soup: BeautifulSoup, html_content: str, performance_data: Optional[Dict]
    ) -> Dict[str, Any]:
        """Comprehensive performance analysis."""
        # Mobile optimization analysis
        viewport_tag = soup.find("meta", attrs={"name": "viewport"})
        viewport_content = viewport_tag.get("content", "") if viewport_tag else ""

        mobile_optimized = {
            "hasViewport": bool(viewport_tag),
            "viewportContent": viewport_content,
            "isResponsive": "width=device-width" in viewport_content,
            "hasMediaQueries": "@media" in html_content.lower(),
        }

        # Page size analysis
        page_size_kb = len(html_content.encode("utf-8")) / 1024

        # Compression analysis
        compression_enabled = (
            "gzip" in html_content.lower() or "deflate" in html_content.lower()
        )

        # Cache headers analysis (would need response headers in real implementation)
        cache_headers = {
            "hasCacheHeaders": False,
            "cacheControl": "",
            "expires": "",
            "etag": "",
            "lastModified": "",
        }

        # Resource analysis
        external_scripts = len(soup.find_all("script", src=True))
        external_styles = len(soup.find_all("link", rel="stylesheet"))
        inline_scripts = len(soup.find_all("script", src=False)) - len(
            soup.find_all("script", type="application/ld+json")
        )
        inline_styles = len(soup.find_all("style"))

        # FIXED: Use attrs dictionary to handle async attribute
        async_scripts = len(soup.find_all("script", attrs={"async": True}))

        resources = {
            "externalScriptsCount": external_scripts,
            "externalStylesCount": external_styles,
            "asyncScriptsCount": async_scripts,
            "inlineScriptsCount": inline_scripts,
            "inlineStylesCount": inline_styles,
        }

        # Critical rendering path analysis
        blocking_stylesheets = len(
            soup.find_all(
                "link",
                rel="stylesheet",
                media=lambda x: not x or "print" not in x.lower(),
            )
        )

        # FIXED: Use attrs dictionary for multiple attributes with boolean values
        blocking_scripts = len(
            soup.find_all("script", src=True, attrs={"async": False, "defer": False})
        )
        critical_css_found = bool(
            soup.find("style", string=lambda x: x and "critical" in x.lower())
        )

        critical_rendering_path = {
            "blockingStylesheets": blocking_stylesheets,
            "blockingScripts": blocking_scripts,
            "criticalCSSFound": critical_css_found,
            "totalRenderBlockingResources": blocking_stylesheets + blocking_scripts,
        }

        # Estimated Core Web Vitals (rough estimates)
        estimated_lcp = min(5.0, max(1.0, page_size_kb / 100))  # Very rough estimate
        estimated_fid = (
            0 if async_scripts > blocking_scripts else min(300, blocking_scripts * 50)
        )
        estimated_cls = min(0.25, max(0, (inline_styles + blocking_stylesheets) * 0.02))

        # Rate the vitals
        lcp_rating = (
            "good"
            if estimated_lcp <= 2.5
            else "needs_improvement" if estimated_lcp <= 4.0 else "poor"
        )
        fid_rating = (
            "good"
            if estimated_fid <= 100
            else "needs_improvement" if estimated_fid <= 300 else "poor"
        )
        cls_rating = (
            "good"
            if estimated_cls <= 0.1
            else "needs_improvement" if estimated_cls <= 0.25 else "poor"
        )

        web_vitals = {
            "estimatedLCP": round(estimated_lcp, 2),
            "estimatedFID": int(estimated_fid),
            "estimatedCLS": round(estimated_cls, 2),
            "lcpRating": lcp_rating,
            "fidRating": fid_rating,
            "clsRating": cls_rating,
        }

        # Enhanced performance scoring
        score = 10
        if not mobile_optimized["hasViewport"]:
            score -= 3
        if not mobile_optimized["isResponsive"]:
            score -= 2
        if page_size_kb > 1000:  # Large page
            score -= 2
        elif page_size_kb > 500:
            score -= 1
        if blocking_scripts > 3:
            score -= 2
        if blocking_stylesheets > 5:
            score -= 1
        if not compression_enabled:
            score -= 1

        return {
            "performance": {
                "pageSize": round(page_size_kb, 2),
                "mobileOptimized": mobile_optimized,
                "compressionEnabled": compression_enabled,
                "cacheHeaders": cache_headers,
                "resources": resources,
                "criticalRenderingPath": critical_rendering_path,
                "webVitals": web_vitals,
                "score": max(0, min(10, score)),
            }
        }

    def _calculate_content_quality_score_enhanced(
        self,
        spelling_errors: List,
        grammar_errors: List,
        word_count: int,
        readability_score: float,
    ) -> float:
        """Calculate enhanced content quality score."""
        score = 10

        # Spelling errors penalty
        spell_error_ratio = len(spelling_errors) / max(word_count, 1) * 100
        if spell_error_ratio > 2:
            score -= 5
        elif spell_error_ratio > 1:
            score -= 3
        elif spell_error_ratio > 0.5:
            score -= 2
        elif len(spelling_errors) > 0:
            score -= 1

        # Grammar errors penalty
        grammar_error_ratio = len(grammar_errors) / max(word_count, 1) * 100
        if grammar_error_ratio > 1:
            score -= 4
        elif grammar_error_ratio > 0.5:
            score -= 2
        elif len(grammar_errors) > 0:
            score -= 1

        # Readability bonus/penalty
        if readability_score >= 60:
            score += 1
        elif readability_score < 30:
            score -= 2

        # Word count factor
        if word_count < 100:
            score -= 3
        elif word_count < 300:
            score -= 1

        return max(0, min(10, score))

    def _calculate_comprehensive_seo_score(
        self, result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate comprehensive SEO score with all factors."""
        weights = SEOConstants.SCORING_WEIGHTS

        scores = {
            "titleScore": result.get("titleScore", 0),
            "metaDescriptionScore": result.get("metaDescriptionScore", 0),
            "contentScore": result.get("contentScore", 0),
            "headingScore": result.get("headingStructure", {}).get("headingScore", 0),
            "urlScore": result.get("urlScore", 0),
            "technicalScore": result.get("technicalSeo", {}).get("technicalScore", 0),
            "imageScore": result.get("images", {}).get("score", 0),
            "linkScore": result.get("links", {}).get("score", 0),
            "performanceScore": result.get("performance", {}).get("score", 0),
            "contentQualityScore": result.get("contentQuality", {}).get(
                "contentQualityScore", 0
            ),
        }

        # Calculate weighted score
        weighted_score = (
            scores["titleScore"] * weights["titleWeight"]
            + scores["metaDescriptionScore"] * weights["metaDescriptionWeight"]
            + scores["contentScore"] * weights["contentWeight"]
            + scores["headingScore"] * weights["headingsWeight"]
            + scores["urlScore"] * weights["urlWeight"]
            + scores["technicalScore"] * weights["technicalWeight"]
            + scores["imageScore"] * weights["imagesWeight"]
            + scores["linkScore"] * weights["linksWeight"]
            + scores["performanceScore"] * weights["performanceWeight"]
            + scores["contentQualityScore"] * weights["contentQualityWeight"]
        ) * 10

        seo_score = max(0, min(100, round(weighted_score)))

        # Assign grade
        if seo_score >= 95:
            seo_grade = "A+"
        elif seo_score >= 90:
            seo_grade = "A"
        elif seo_score >= 85:
            seo_grade = "B+"
        elif seo_score >= 80:
            seo_grade = "B"
        elif seo_score >= 70:
            seo_grade = "C"
        elif seo_score >= 60:
            seo_grade = "D"
        else:
            seo_grade = "F"

        return {
            "seoScore": seo_score,
            "seoGrade": seo_grade,
            "seoScoreComponents": weights,
            "individualScores": scores,
        }


async def process_urls_comprehensive(urls: List[str]) -> List[Dict[str, Any]]:
    """Process URLs with comprehensive analysis."""
    analyzer = EnhancedSEOAnalyzer()
    results = []

    logger.debug(f"🚀 Starting comprehensive analysis of {len(urls)} URLs")

    for i, url in enumerate(urls, 1):
        try:
            logger.debug(f"📊 Analyzing {i}/{len(urls)}: {url}")
            result = await analyzer.analyze_page_comprehensive(url)
            results.append(result)

            # Log comprehensive findings
            if "error" not in result:
                broken_links = len(result.get("links", {}).get("brokenLinks", []))
                spelling_errors = len(
                    result.get("contentQuality", {}).get("spellingErrors", [])
                )
                grammar_errors = len(
                    result.get("contentQuality", {}).get("grammarErrors", [])
                )
                duplicates = len(
                    result.get("duplicates", {}).get("contentDuplicates", [])
                )
                seo_score = result.get("seoScore", 0)

                logger.debug(
                    f"✅ Analysis complete - SEO Score: {seo_score}, "
                    f"Broken Links: {broken_links}, Spelling Errors: {spelling_errors}, "
                    f"Grammar Errors: {grammar_errors}, Duplicates: {duplicates}"
                )
            else:
                logger.error(
                    f"❌ Analysis failed for {url}: {result.get('error', 'Unknown error')}"
                )

        except Exception as e:
            logger.error(f"💥 Exception analyzing {url}: {str(e)}")
            results.append(
                {
                    "pageUrl": url,
                    "error": str(e),
                    "lastCrawled": datetime.now().isoformat(),
                }
            )

    return results


def process_urls_sync(urls: List[str]) -> List[Dict[str, Any]]:
    """Synchronous wrapper for comprehensive processing."""
    return asyncio.run(process_urls_comprehensive(urls))


def main():
    """Enhanced main entry point with better error handling."""
    try:
        if len(sys.argv) < 2:
            print(
                json.dumps(
                    {
                        "error": "URL is required. Usage: python script.py URL [URL2,URL3,...] [--verbose] [--playwright]"
                    }
                )
            )
            sys.exit(1)

        # Parse arguments
        verbose = "--verbose" in sys.argv
        use_playwright = "--playwright" in sys.argv or True  # Default to True

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.ERROR)  # Only show errors to stderr

        url_input = sys.argv[1]

        # Parse URLs
        if "," in url_input:
            urls = [u.strip() for u in url_input.split(",") if u.strip()]
        else:
            urls = [url_input.strip()]

        # Validate and normalize URLs
        valid_urls = []
        for url in urls:
            if not url.startswith(("http://", "https://")):
                url = "https://" + url

            try:
                parsed = urlparse(url)
                if parsed.netloc:
                    valid_urls.append(url)
                else:
                    logger.warning(f"Invalid URL: {url}")
            except Exception as e:
                logger.warning(f"URL parsing failed for {url}: {e}")

        if not valid_urls:
            print(json.dumps({"error": "No valid URLs provided"}))
            sys.exit(1)

        logger.debug(f"🎯 Processing {len(valid_urls)} valid URLs")
        logger.debug(
            f"🔧 Playwright: {'Enabled' if use_playwright and PLAYWRIGHT_AVAILABLE else 'Disabled'}"
        )
        logger.debug(
            f"🔤 Spell Checker: {'Enabled' if PYSPELLCHECKER_AVAILABLE else 'Disabled'}"
        )
        logger.debug(
            f"🔍 Advanced Duplicates: {'Enabled' if SKLEARN_AVAILABLE and FUZZYWUZZY_AVAILABLE else 'Partial'}"
        )

        # Process URLs
        results = process_urls_sync(valid_urls)

        # Output results
        if len(results) == 1:
            print(json.dumps(results[0], ensure_ascii=False, indent=2))
        else:
            print(json.dumps(results, ensure_ascii=False, indent=2))

        logger.debug(f"🏁 Processing completed for {len(results)} URLs")

    except KeyboardInterrupt:
        logger.debug("⛔ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        error_response = {
            "error": f"Script error: {str(e)}",
            "traceback": traceback.format_exc() if "--verbose" in sys.argv else None,
        }
        print(json.dumps(error_response))
        sys.exit(1)


if __name__ == "__main__":
    main()
