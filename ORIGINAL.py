import threading
import time
import os
import sys
import logging
import queue
import re
import signal
import json
from collections import OrderedDict, deque
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from typing import Optional, Tuple, Dict, List, Any, Protocol
from dataclasses import dataclass, field
from contextlib import contextmanager
from abc import ABC, abstractmethod
from enum import Enum

# ------------------ DEPENDENCY CHECK ------------------
missing_deps = []

try:
    import requests
except ImportError:
    missing_deps.append("requests")

try:
    from dotenv import load_dotenv
except ImportError:
    missing_deps.append("python-dotenv")

if missing_deps:
    print(f"ERROR: Missing required dependencies: {', '.join(missing_deps)}")
    print(f"Install with: pip install {' '.join(missing_deps)}")
    sys.exit(1)

# ------------------ CONFIGURATION ------------------
load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

class LogLevel(Enum):
    """Logging level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

@dataclass
class Config:
    """Centralized configuration for the giveaway monitor."""
    # Subreddit
    SUBREDDIT: str = "plsdonategame"
    
    # Reddit Search API URLs - one per flair for better tracking
    SEARCH_URLS: List[str] = field(default_factory=lambda: [
        'https://www.reddit.com/search.json?q=subreddit:plsdonategame+flair:"Free Giveaway"&sort=new&limit=50&t=week',
        'https://www.reddit.com/search.json?q=subreddit:plsdonategame+flair:"Requirement Giveaway"&sort=new&limit=50&t=week'
    ])
    
    # File paths
    SEEN_FILE: str = os.path.join(BASE_DIR, "seen_posts.txt")
    FAILED_NOTIFICATIONS_FILE: str = os.path.join(BASE_DIR, "failed_notifications.txt")
    LOG_FILE: str = os.path.join(BASE_DIR, "giveaway_monitor.log")
    SHUTDOWN_TIME_FILE: str = os.path.join(BASE_DIR, "last_shutdown.txt")
    METRICS_FILE: str = os.path.join(BASE_DIR, "metrics.json")
    
    # Timing (seconds)
    BASE_INTERVAL: int = 20  # Search API is more targeted, can check more frequently
    FAST_INTERVAL: int = 20
    SLOW_INTERVAL: int = 45
    BACKOFF_INTERVAL: int = 300
    SAVE_INTERVAL: int = 300
    SHUTDOWN_TIMEOUT: int = 30
    THREAD_JOIN_TIMEOUT: float = 10.0
    WORKER_HEALTH_CHECK_INTERVAL: int = 30
    RATE_LIMIT_SLEEP_CAP: float = 0.1
    METRICS_SAVE_INTERVAL: int = 600
    CACHE_CLEANUP_INTERVAL: int = 3600
    
    # Reddit API rate limiting (100 QPM for Data API)
    REDDIT_RATE_LIMIT_QPM: int = 100
    REDDIT_RATE_LIMIT_WINDOW: int = 600  # 10 minute window
    REDDIT_RATE_WARNING_THRESHOLD: float = 0.8  # Warn at 80% usage
    REDDIT_RATE_SAFETY_BUFFER: int = 10  # Stay 10 requests under limit
    
    # Discord rate limiting (5 messages per 2 seconds)
    DISCORD_RATE_LIMIT_MESSAGES: int = 5
    DISCORD_RATE_LIMIT_WINDOW: float = 2.0
    
    # Retry configuration
    MAX_RETRY_ATTEMPTS: int = 3
    MAX_BACKOFF_TIME: int = 60
    BASE_BACKOFF: int = 2
    REQUEST_TIMEOUT: int = 15
    
    # Circuit breaker
    CIRCUIT_BREAKER_THRESHOLD: int = 5
    CIRCUIT_BREAKER_TIMEOUT: int = 300
    CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS: int = 3
    
    # Limits
    MAX_SEEN_POSTS: int = 2000
    MAX_TITLE_LENGTH: int = 250
    MAX_DESC_LENGTH: int = 1000
    MAX_JSON_SIZE_MB: int = 10
    SEEN_POSTS_CLEANUP_THRESHOLD: int = 2500
    
    # Post filtering
    POST_MAX_AGE_HOURS: int = 36
    TARGET_FLAIRS: List[str] = field(default_factory=lambda: ["Free Giveaway", "Requirement Giveaway"])
    
    # Startup catchup configuration
    ENABLE_STARTUP_CATCHUP: bool = True
    STARTUP_CATCHUP_MIN_DOWNTIME_HOURS: float = 0.5  # Catchup if down for 30+ minutes
    
    # Discord
    WEBHOOK_URL: Optional[str] = os.getenv("DISCORD_WEBHOOK_URL")
    ROLE_ID: Optional[str] = os.getenv("DISCORD_ROLE_ID")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_MAX_BYTES: int = 5 * 1024 * 1024
    LOG_BACKUP_COUNT: int = 3
    
    # User agent for Reddit requests
    USER_AGENT: str = "python:giveaway-monitor:v2.4 by u/Cautious-Secret9975"
    
    # Required environment variables
    REQUIRED_ENV_VARS: List[str] = field(default_factory=lambda: [
        "DISCORD_WEBHOOK_URL"
    ])
    
    # Health monitoring
    ENABLE_HEALTH_ALERTS: bool = os.getenv("ENABLE_HEALTH_ALERTS", "false").lower() == "true"
    HEALTH_CHECK_WEBHOOK_URL: Optional[str] = os.getenv("HEALTH_CHECK_WEBHOOK_URL")
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        if self.MAX_SEEN_POSTS < 100:
            raise ValueError("MAX_SEEN_POSTS must be at least 100")
        
        if self.WEBHOOK_URL and not self._is_valid_webhook_url(self.WEBHOOK_URL):
            raise ValueError("Invalid Discord webhook URL format")
        
        if self.ROLE_ID:
            if not self._is_valid_snowflake(self.ROLE_ID):
                raise ValueError(f"ROLE_ID '{self.ROLE_ID}' is not a valid Discord snowflake")
        
        if self.LOG_LEVEL not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            raise ValueError(f"Invalid LOG_LEVEL: {self.LOG_LEVEL}")
    
    @staticmethod
    def _is_valid_webhook_url(url: str) -> bool:
        """Validate Discord webhook URL format."""
        pattern = r'^https://discord\.com/api/webhooks/\d+/[\w-]+$'
        return bool(re.match(pattern, url))
    
    @staticmethod
    def _is_valid_snowflake(snowflake: str) -> bool:
        """Validate Discord snowflake ID format."""
        return snowflake.isdigit() and 17 <= len(snowflake) <= 20

config = Config()

# ------------------ LOGGING ------------------
def setup_logging() -> None:
    """Configure logging with rotating file handler and console output."""
    log_handler = RotatingFileHandler(
        config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    
    console_handler = logging.StreamHandler()
    
    log_level = getattr(logging, config.LOG_LEVEL)
    
    logging.basicConfig(
        level=log_level,
        format='[%(asctime)s] %(levelname)s [%(threadName)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[console_handler, log_handler]
    )

setup_logging()

# ------------------ PROTOCOLS ------------------
class NotificationSender(Protocol):
    """Protocol for notification senders."""
    def send(self, post_data: Dict[str, str]) -> bool:
        """Send a notification."""
        ...

# ------------------ METRICS TRACKING ------------------
class MetricsTracker:
    """Track and persist application metrics."""
    
    def __init__(self, metrics_file: str):
        self.metrics_file = metrics_file
        self.lock = threading.Lock()
        self.metrics = {
            "total_uptime": 0.0,
            "total_posts_checked": 0,
            "total_giveaways_found": 0,
            "total_notifications_sent": 0,
            "total_notifications_failed": 0,
            "total_api_errors": 0,
            "total_circuit_breaker_opens": 0,
            "total_rate_limit_hits": 0,
            "total_catchup_runs": 0,
            "total_catchup_posts_found": 0,
            "total_search_queries": 0,
            "session_start": time.time(),
            "last_update": time.time()
        }
        self._load_metrics()
    
    def _load_metrics(self) -> None:
        """Load metrics from file."""
        if os.path.exists(self.metrics_file):
            try:
                with open(self.metrics_file, 'r', encoding='utf-8') as f:
                    saved = json.load(f)
                    for key in ["total_uptime", "total_posts_checked", "total_giveaways_found",
                               "total_notifications_sent", "total_notifications_failed",
                               "total_api_errors", "total_circuit_breaker_opens", "total_rate_limit_hits",
                               "total_catchup_runs", "total_catchup_posts_found", "total_search_queries"]:
                        if key in saved:
                            self.metrics[key] = saved[key]
                logging.info(f"[OK] Loaded metrics: {self.metrics['total_giveaways_found']} total giveaways found")
            except Exception as e:
                logging.warning(f"[WARN] Failed to load metrics: {e}")
    
    def save_metrics(self) -> None:
        """Save metrics to file."""
        with self.lock:
            try:
                self.metrics["total_uptime"] += time.time() - self.metrics["last_update"]
                self.metrics["last_update"] = time.time()
                
                temp_file = self.metrics_file + ".tmp"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.metrics, f, indent=2)
                os.replace(temp_file, self.metrics_file)
                logging.debug("[OK] Metrics saved")
            except Exception as e:
                logging.error(f"[FAIL] Failed to save metrics: {e}")
    
    def increment(self, key: str, value: int = 1) -> None:
        """Increment a metric."""
        with self.lock:
            if key in self.metrics:
                self.metrics[key] += value
    
    def get(self, key: str) -> Any:
        """Get a metric value."""
        with self.lock:
            return self.metrics.get(key, 0)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        with self.lock:
            return self.metrics.copy()

# ------------------ CIRCUIT BREAKER ------------------
class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class CircuitBreaker:
    """Enhanced circuit breaker with half-open state support."""
    
    def __init__(self, threshold: int, timeout: int, half_open_max: int = 3):
        self.threshold = threshold
        self.timeout = timeout
        self.half_open_max = half_open_max
        self.failures = 0
        self.successes = 0
        self.half_open_attempts = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitBreakerState.CLOSED
        self.lock = threading.Lock()
        self.state_change_callbacks: List[callable] = []
    
    def register_callback(self, callback: callable) -> None:
        """Register callback for state changes."""
        self.state_change_callbacks.append(callback)
    
    def _notify_state_change(self, old_state: CircuitBreakerState, new_state: CircuitBreakerState):
        """Notify callbacks of state change."""
        for callback in self.state_change_callbacks:
            try:
                callback(old_state, new_state)
            except Exception as e:
                logging.error(f"Circuit breaker callback error: {e}")
    
    def call(self, func, *args, **kwargs):
        """Execute function through circuit breaker."""
        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time < self.timeout:
                    raise Exception("Circuit breaker is OPEN")
                else:
                    old_state = self.state
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_attempts = 0
                    logging.info("[CB] Circuit breaker entering HALF_OPEN state")
                    self._notify_state_change(old_state, self.state)
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_attempts >= self.half_open_max:
                    raise Exception("Circuit breaker HALF_OPEN limit reached")
                self.half_open_attempts += 1
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call."""
        with self.lock:
            old_state = self.state
            self.failures = 0
            self.successes += 1
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.successes >= self.half_open_max:
                    self.state = CircuitBreakerState.CLOSED
                    self.successes = 0
                    logging.info("[CB] Circuit breaker CLOSED after recovery")
                    self._notify_state_change(old_state, self.state)
    
    def _on_failure(self):
        """Handle failed call."""
        with self.lock:
            old_state = self.state
            self.failures += 1
            self.successes = 0
            self.last_failure_time = time.time()
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                logging.warning("[CB] Circuit breaker OPEN (failed during HALF_OPEN)")
                self._notify_state_change(old_state, self.state)
            elif self.failures >= self.threshold:
                self.state = CircuitBreakerState.OPEN
                logging.warning(f"[CB] Circuit breaker OPEN after {self.failures} failures")
                self._notify_state_change(old_state, self.state)
    
    def get_state(self) -> str:
        """Get current state."""
        with self.lock:
            return self.state.value

# ------------------ REDDIT API RATE LIMITER WITH HEADER TRACKING ------------------
@dataclass
class RateLimitInfo:
    """Store rate limit information from Reddit headers."""
    used: int = 0
    remaining: int = 100
    reset: int = 0
    last_update: float = 0.0

class RedditRateLimiter:
    """
    Rate limiter for Reddit API that monitors response headers.
    
    Tracks:
    - X-Ratelimit-Used: Requests used in current period
    - X-Ratelimit-Remaining: Requests remaining
    - X-Ratelimit-Reset: Seconds until reset
    """
    
    def __init__(self):
        self.lock = threading.Lock()
        self.rate_limit_info = RateLimitInfo()
        self.local_requests = deque()  # Fallback tracking
        self.warning_threshold = int(config.REDDIT_RATE_LIMIT_QPM * config.REDDIT_RATE_WARNING_THRESHOLD)
    
    def update_from_headers(self, headers: Dict[str, str]) -> None:
        """Update rate limit info from response headers."""
        with self.lock:
            try:
                # Reddit uses lowercase or mixed case headers
                used = headers.get('x-ratelimit-used') or headers.get('X-Ratelimit-Used')
                remaining = headers.get('x-ratelimit-remaining') or headers.get('X-Ratelimit-Remaining')
                reset = headers.get('x-ratelimit-reset') or headers.get('X-Ratelimit-Reset')
                
                if used is not None:
                    self.rate_limit_info.used = int(float(used))
                
                if remaining is not None:
                    self.rate_limit_info.remaining = int(float(remaining))
                
                if reset is not None:
                    self.rate_limit_info.reset = int(float(reset))
                
                self.rate_limit_info.last_update = time.time()
                
                # Log if we have valid rate limit data
                if used is not None and remaining is not None:
                    total = self.rate_limit_info.used + self.rate_limit_info.remaining
                    percentage = (self.rate_limit_info.used / total * 100) if total > 0 else 0
                    
                    logging.debug(
                        f"[RATE] Reddit limits: {self.rate_limit_info.used}/{total} used "
                        f"({percentage:.1f}%), {self.rate_limit_info.remaining} remaining, "
                        f"resets in {self.rate_limit_info.reset}s"
                    )
                    
                    # Warn if approaching limit
                    if self.rate_limit_info.remaining <= (config.REDDIT_RATE_LIMIT_QPM - self.warning_threshold):
                        logging.warning(
                            f"[RATE] Approaching rate limit! Only {self.rate_limit_info.remaining} requests remaining"
                        )
                        
            except (ValueError, TypeError) as e:
                logging.debug(f"[RATE] Failed to parse rate limit headers: {e}")
    
    def can_make_request(self) -> Tuple[bool, Optional[float]]:
        """
        Check if a request can be made based on rate limit headers.
        
        Returns:
            Tuple of (can_make_request, wait_time_if_not)
        """
        with self.lock:
            # Clean up old local tracking
            self._cleanup_old_requests()
            
            # If we have recent header data, use it
            if time.time() - self.rate_limit_info.last_update < 60:
                # Use safety buffer to avoid hitting exact limit
                if self.rate_limit_info.remaining > config.REDDIT_RATE_SAFETY_BUFFER:
                    return True, None
                
                # If we're at or near limit, wait for reset
                wait_time = max(1, self.rate_limit_info.reset)
                logging.warning(
                    f"[RATE] Rate limit reached. Waiting {wait_time}s for reset "
                    f"(remaining: {self.rate_limit_info.remaining})"
                )
                return False, float(wait_time)
            
            # Fallback to local tracking if no recent header data
            if len(self.local_requests) >= config.REDDIT_RATE_LIMIT_QPM:
                oldest_request = self.local_requests[0]
                wait_time = config.REDDIT_RATE_LIMIT_WINDOW - (time.time() - oldest_request)
                return False, max(0, wait_time)
            
            return True, None
    
    def record_request(self) -> None:
        """Record that a request was made (local tracking)."""
        with self.lock:
            self.local_requests.append(time.time())
            self._cleanup_old_requests()
    
    def _cleanup_old_requests(self) -> None:
        """Remove requests older than tracking window."""
        cutoff = time.time() - config.REDDIT_RATE_LIMIT_WINDOW
        while self.local_requests and self.local_requests[0] < cutoff:
            self.local_requests.popleft()
    
    def get_usage(self) -> Dict[str, Any]:
        """Get current rate limit usage."""
        with self.lock:
            # Prefer header data if available
            if time.time() - self.rate_limit_info.last_update < 60:
                total = self.rate_limit_info.used + self.rate_limit_info.remaining
                return {
                    "used": self.rate_limit_info.used,
                    "remaining": self.rate_limit_info.remaining,
                    "max": total if total > 0 else config.REDDIT_RATE_LIMIT_QPM,
                    "percentage": (self.rate_limit_info.used / total * 100) if total > 0 else 0,
                    "reset_in": self.rate_limit_info.reset,
                    "source": "headers"
                }
            
            # Fallback to local tracking
            self._cleanup_old_requests()
            return {
                "used": len(self.local_requests),
                "remaining": config.REDDIT_RATE_LIMIT_QPM - len(self.local_requests),
                "max": config.REDDIT_RATE_LIMIT_QPM,
                "percentage": (len(self.local_requests) / config.REDDIT_RATE_LIMIT_QPM) * 100,
                "reset_in": None,
                "source": "local"
            }
    
    def get_recommended_interval(self) -> int:
        """Get recommended interval based on current usage."""
        usage = self.get_usage()
        percentage = usage["percentage"]
        
        if percentage >= 90:
            return config.BACKOFF_INTERVAL
        elif percentage >= 70:
            return config.SLOW_INTERVAL
        else:
            return config.FAST_INTERVAL

# ------------------ DISCORD RATE LIMITER ------------------
class RateLimiter:
    """Token bucket rate limiter for Discord API."""
    
    def __init__(self, max_messages: int, time_window: float):
        self.max_messages = max_messages
        self.time_window = time_window
        self.tokens = max_messages
        self.last_update = time.time()
        self.condition = threading.Condition()
    
    def acquire(self) -> None:
        """Wait until a token is available."""
        with self.condition:
            while True:
                self._refill_tokens()
                
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                
                wait_time = (1 - self.tokens) * (self.time_window / self.max_messages)
                self.condition.wait(timeout=min(wait_time, config.RATE_LIMIT_SLEEP_CAP))
    
    def _refill_tokens(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        
        self.tokens = min(
            self.max_messages,
            self.tokens + (elapsed / self.time_window) * self.max_messages
        )
        self.last_update = now

# ------------------ REDDIT JSON PARSER ------------------
@dataclass
class RedditPost:
    """Represents a post parsed from Reddit JSON API."""
    id: str
    title: str
    author: str
    link: str
    selftext: str
    created_utc: float
    link_flair_text: Optional[str] = None

    @property
    def age_hours(self) -> float:
        """Calculate post age in hours."""
        return (time.time() - self.created_utc) / 3600


class RedditJSONParser:
    """Parse Reddit JSON API responses into structured post data."""
    
    @staticmethod
    def parse_response(json_data: dict) -> List[RedditPost]:
        """Parse Reddit JSON response into RedditPost objects."""
        posts = []
        
        try:
            if 'data' not in json_data or 'children' not in json_data['data']:
                logging.warning("Invalid JSON structure from Reddit")
                return []
            
            children = json_data['data']['children']
            
            for child in children:
                try:
                    if child.get('kind') != 't3':
                        continue
                    
                    post_data = child.get('data', {})
                    post = RedditJSONParser._parse_post(post_data)
                    
                    if post:
                        posts.append(post)
                        
                except Exception as e:
                    logging.debug(f"Failed to parse post: {e}")
                    continue
            
            logging.debug(f"Parsed {len(posts)} posts from JSON")
            return posts
            
        except Exception as e:
            logging.error(f"Failed to parse Reddit JSON: {e}")
            return []
    
    @staticmethod
    def _parse_post(post_data: dict) -> Optional[RedditPost]:
        """Parse a single post from Reddit JSON data."""
        try:
            post_id = post_data.get('id')
            if not post_id or not re.match(r'^[a-z0-9]{6,10}$', post_id):
                return None
            
            title = post_data.get('title', 'Untitled')
            title = RedditJSONParser._sanitize_text(title, config.MAX_TITLE_LENGTH)
            
            author = post_data.get('author', 'Unknown')
            author = RedditJSONParser._sanitize_username(author)
            
            permalink = post_data.get('permalink', '')
            link = f"https://www.reddit.com{permalink}" if permalink else ""
            if not RedditJSONParser._is_valid_url(link):
                return None
            
            selftext = post_data.get('selftext', '')
            selftext = RedditJSONParser._sanitize_text(selftext, config.MAX_DESC_LENGTH)
            
            created_utc = float(post_data.get('created_utc', time.time()))
            
            link_flair_text = post_data.get('link_flair_text')
            if link_flair_text:
                link_flair_text = RedditJSONParser._sanitize_text(link_flair_text, 100)
            
            return RedditPost(
                id=post_id,
                title=title,
                author=author,
                link=link,
                selftext=selftext,
                created_utc=created_utc,
                link_flair_text=link_flair_text
            )
            
        except Exception as e:
            logging.debug(f"Error parsing post data: {e}")
            return None
    
    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """Validate URL format and scheme."""
        if not url:
            return False
        pattern = r'^https://(?:www\.)?reddit\.com/.+'
        return bool(re.match(pattern, url))
    
    @staticmethod
    def _sanitize_username(username: str) -> str:
        """Sanitize Reddit username."""
        username = re.sub(r'[^\w-]', '', username)
        return username[:20] if username else "Unknown"
    
    @staticmethod
    def _sanitize_text(text: str, max_length: int) -> str:
        """Sanitize and truncate text."""
        if not text:
            return ""
        
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
        text = text.strip()
        
        if len(text) > max_length:
            return text[:max_length-3] + "..."
        return text


# ------------------ LRU CACHE FOR SEEN POSTS ------------------
class LRUCache:
    """Thread-safe LRU cache for tracking seen posts with age-based cleanup."""
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: Dict[str, float] = {}
        self.lock = threading.Lock()
    
    def add(self, key: str) -> bool:
        """Add key to cache with timestamp."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return False
            
            self.cache[key] = True
            self.timestamps[key] = time.time()
            
            if len(self.cache) > self.capacity:
                oldest = self.cache.popitem(last=False)
                self.timestamps.pop(oldest[0], None)
            
            return True
    
    def cleanup_old_entries(self, max_age_hours: int) -> int:
        """Remove entries older than max_age_hours."""
        with self.lock:
            cutoff_time = time.time() - (max_age_hours * 3600)
            keys_to_remove = [
                key for key, timestamp in self.timestamps.items()
                if timestamp < cutoff_time
            ]
            
            for key in keys_to_remove:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
            
            return len(keys_to_remove)
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists in cache."""
        with self.lock:
            return key in self.cache
    
    def __len__(self) -> int:
        """Get cache size."""
        with self.lock:
            return len(self.cache)
    
    def get_all(self) -> List[str]:
        """Get all keys in cache."""
        with self.lock:
            return list(self.cache.keys())


# ------------------ DISCORD NOTIFICATION ------------------
class DiscordNotifier:
    """Handles Discord webhook notifications with retry logic."""
    
    def __init__(self, webhook_url: str, role_id: Optional[str] = None):
        self.webhook_url = webhook_url
        self.role_id = role_id
        self.session = requests.Session()
        self.circuit_breaker = CircuitBreaker(
            threshold=config.CIRCUIT_BREAKER_THRESHOLD,
            timeout=config.CIRCUIT_BREAKER_TIMEOUT,
            half_open_max=config.CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS
        )
    
    def send(self, post_data: Dict[str, str]) -> bool:
        """Send notification to Discord with circuit breaker protection."""
        try:
            return self.circuit_breaker.call(self._send_with_retry, post_data)
        except Exception as e:
            logging.error(f"[CB] Circuit breaker prevented send: {e}")
            return False
    
    def _send_with_retry(self, post_data: Dict[str, str]) -> bool:
        """Send notification with exponential backoff retry."""
        content = f"<@&{self.role_id}>" if self.role_id else ""
        
        payload = {
            "content": content,
            "embeds": [{
                "title": f"🎁 {post_data['title']}",
                "description": post_data['desc'],
                "url": post_data['url'],
                "color": 0xFF4500,
                "fields": [
                    {"name": "Subreddit", "value": f"r/{post_data['subreddit']}", "inline": True},
                    {"name": "Author", "value": f"u/{post_data['author']}", "inline": True},
                    {"name": "Flair", "value": post_data['flair'], "inline": True}
                ],
                "timestamp": post_data['timestamp'],
                "footer": {"text": "Reddit Giveaway Monitor v2.4 (Search API)"}
            }]
        }

        for attempt in range(config.MAX_RETRY_ATTEMPTS):
            try:
                r = self.session.post(
                    self.webhook_url,
                    json=payload,
                    timeout=config.REQUEST_TIMEOUT
                )
                
                if r.status_code in [200, 204]:
                    logging.info(f"[SENT] {post_data['title'][:40]}...")
                    return True
                    
                elif r.status_code == 429:
                    retry_after = r.json().get('retry_after', 5)
                    logging.warning(f"[WARN] Discord rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    
                else:
                    logging.warning(f"[WARN] Discord returned status {r.status_code}")
                    if attempt < config.MAX_RETRY_ATTEMPTS - 1:
                        backoff = min(config.BASE_BACKOFF ** attempt, config.MAX_BACKOFF_TIME)
                        time.sleep(backoff)
                    
            except requests.exceptions.Timeout:
                logging.warning(f"[WARN] Discord timeout (attempt {attempt + 1}/{config.MAX_RETRY_ATTEMPTS})")
                if attempt < config.MAX_RETRY_ATTEMPTS - 1:
                    backoff = min(config.BASE_BACKOFF ** attempt, config.MAX_BACKOFF_TIME)
                    time.sleep(backoff)
                    
            except Exception as e:
                logging.exception(f"[FAIL] Discord send failed (attempt {attempt + 1}): {e}")
                if attempt < config.MAX_RETRY_ATTEMPTS - 1:
                    backoff = min(config.BASE_BACKOFF ** attempt, config.MAX_BACKOFF_TIME)
                    time.sleep(backoff)
        
        logging.error(f"[FAIL] Failed after {config.MAX_RETRY_ATTEMPTS} attempts")
        return False
    
    def close(self):
        """Close the session."""
        self.session.close()


# ------------------ HEALTH MONITOR ------------------
class HealthMonitor:
    """Monitor system health and send alerts."""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url
        self.session = requests.Session() if webhook_url else None
        self.last_alert_time: Dict[str, float] = {}
        self.alert_cooldown = 300
    
    def should_alert(self, alert_type: str) -> bool:
        """Check if enough time has passed to send another alert."""
        last_time = self.last_alert_time.get(alert_type, 0)
        return time.time() - last_time > self.alert_cooldown
    
    def send_alert(self, title: str, message: str, severity: str = "warning") -> None:
        """Send health alert to Discord."""
        if not self.webhook_url or not self.session:
            logging.warning(f"[HEALTH] {title}: {message}")
            return
        
        if not self.should_alert(title):
            return
        
        color_map = {
            "info": 0x3498db,
            "warning": 0xf39c12,
            "error": 0xe74c3c
        }
        
        payload = {
            "embeds": [{
                "title": f"⚠️ {title}",
                "description": message,
                "color": color_map.get(severity, 0xf39c12),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "footer": {"text": "Health Monitor"}
            }]
        }
        
        try:
            self.session.post(self.webhook_url, json=payload, timeout=10)
            self.last_alert_time[title] = time.time()
            logging.info(f"[HEALTH] Alert sent: {title}")
        except Exception as e:
            logging.error(f"[HEALTH] Failed to send alert: {e}")
    
    def close(self):
        """Close session."""
        if self.session:
            self.session.close()


# ------------------ MAIN MONITOR CLASS ------------------
class GiveawayMonitor:
    """
    Monitors a subreddit via Reddit Search API for giveaway posts and sends notifications to Discord.
    
    Version 2.4 with Reddit Search API for targeted giveaway detection.
    """
    
    def __init__(self):
        """Initialize the monitor with all required components."""
        self._validate_environment()
        
        # Threading primitives
        self._stop_event = threading.Event()
        self._pause_condition = threading.Condition()
        self._is_paused = False
        self.alert_queue: queue.Queue = queue.Queue()
        
        # Worker health tracking
        self.discord_worker_alive = threading.Event()
        self.discord_worker_alive.set()
        self.discord_thread: Optional[threading.Thread] = None
        
        # Rate limiters with header tracking
        self.reddit_rate_limiter = RedditRateLimiter()
        self.discord_rate_limiter = RateLimiter(
            config.DISCORD_RATE_LIMIT_MESSAGES,
            config.DISCORD_RATE_LIMIT_WINDOW
        )
        
        # Notifier and health monitor
        self.notifier = DiscordNotifier(config.WEBHOOK_URL, config.ROLE_ID)
        self.health_monitor = HealthMonitor(
            config.HEALTH_CHECK_WEBHOOK_URL if config.ENABLE_HEALTH_ALERTS else None
        )
        
        # Register circuit breaker callback
        self.notifier.circuit_breaker.register_callback(self._on_circuit_breaker_state_change)
        
        # Metrics tracking
        self.metrics_tracker = MetricsTracker(config.METRICS_FILE)
        
        # Statistics tracking
        self.stats = {
            "total_checked": 0,
            "giveaways_found": 0,
            "failed_notifications": 0,
            "start_time": time.time(),
            "status": "Initializing",
            "last_check": None,
            "api_fetch_errors": 0,
            "worker_restarts": 0,
            "json_parse_errors": 0,
            "current_interval": config.BASE_INTERVAL,
            "rate_limit_hits": 0,
            "catchup_runs": 0,
            "posts_from_catchup": 0,
            "search_queries": 0
        }
        self.stats_lock = threading.Lock()
        
        # Seen posts tracking with LRU cache
        self.seen_posts = LRUCache(config.MAX_SEEN_POSTS)
        
        # HTTP session for Reddit API requests
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': config.USER_AGENT})
        
        # Load historical data
        self._load_seen_posts()
        self._last_shutdown_time = self._load_shutdown_time()
        
        # Background maintenance
        self.last_metrics_save = time.time()
        self.last_cache_cleanup = time.time()

    def _on_circuit_breaker_state_change(self, old_state: CircuitBreakerState, new_state: CircuitBreakerState):
        """Handle circuit breaker state changes."""
        if new_state == CircuitBreakerState.OPEN:
            self.metrics_tracker.increment("total_circuit_breaker_opens")
            self.health_monitor.send_alert(
                "Circuit Breaker Opened",
                f"Discord notifications circuit breaker opened after multiple failures. "
                f"Will retry in {config.CIRCUIT_BREAKER_TIMEOUT} seconds.",
                severity="error"
            )

    @contextmanager
    def _managed_resource(self):
        """Context manager for resource cleanup."""
        try:
            yield self
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Cleanup resources."""
        try:
            self.session.close()
            self.notifier.close()
            self.health_monitor.close()
            self.metrics_tracker.save_metrics()
        except Exception as e:
            logging.warning(f"Cleanup error: {e}")

    def _validate_environment(self) -> None:
        """Validate all required environment variables exist."""
        missing = [var for var in config.REQUIRED_ENV_VARS if not os.getenv(var)]
        
        if missing:
            logging.error("=" * 70)
            logging.error("CONFIGURATION ERROR: Missing environment variables")
            logging.error("=" * 70)
            for var in missing:
                logging.error(f"  [MISSING] {var}")
            logging.error("")
            logging.error("Please create a .env file with:")
            for var in config.REQUIRED_ENV_VARS:
                logging.error(f"  {var}=your_value_here")
            logging.error("=" * 70)
            sys.exit(1)
        
        logging.info("[OK] Environment validated")

    def _get_stat(self, key: str) -> Any:
        """Thread-safe statistics getter."""
        with self.stats_lock:
            return self.stats.get(key)

    def _set_stat(self, key: str, value: Any) -> None:
        """Thread-safe statistics setter."""
        with self.stats_lock:
            self.stats[key] = value

    def _update_stat(self, key: str, func) -> None:
        """Thread-safe statistics updater with function."""
        with self.stats_lock:
            self.stats[key] = func(self.stats.get(key, 0))

    def _load_shutdown_time(self) -> Optional[float]:
        """Load the last shutdown timestamp."""
        if os.path.exists(config.SHUTDOWN_TIME_FILE):
            try:
                with open(config.SHUTDOWN_TIME_FILE, "r", encoding='utf-8') as f:
                    timestamp = float(f.read().strip())
                    dt = datetime.fromtimestamp(timestamp)
                    logging.info(f"[OK] Last shutdown: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
                    return timestamp
            except Exception as e:
                logging.warning(f"[WARN] Could not load shutdown time: {e}")
        return None

    def _save_shutdown_time(self) -> None:
        """Save the current time as shutdown timestamp."""
        try:
            with open(config.SHUTDOWN_TIME_FILE, "w", encoding='utf-8') as f:
                f.write(str(time.time()))
        except Exception as e:
            logging.exception(f"[FAIL] Failed to save shutdown time: {e}")

    def _load_seen_posts(self) -> None:
        """Load previously seen posts from file."""
        if os.path.exists(config.SEEN_FILE):
            try:
                with open(config.SEEN_FILE, "r", encoding='utf-8') as f:
                    for line in f:
                        post_id = line.strip()
                        if post_id:
                            self.seen_posts.add(post_id)
                logging.info(f"[OK] Loaded {len(self.seen_posts)} seen posts")
            except Exception as e:
                logging.exception(f"[WARN] Failed to load history: {e}")

    def save_state(self) -> None:
        """Atomically save seen posts to file."""
        try:
            data = self.seen_posts.get_all()
            temp_file = config.SEEN_FILE + ".tmp"
            
            with open(temp_file, "w", encoding='utf-8') as f:
                f.write("\n".join(data))
            
            os.replace(temp_file, config.SEEN_FILE)
            logging.debug("[OK] State saved")
        except Exception as e:
            logging.exception(f"[FAIL] Save failed: {e}")

    def _log_failed_notification(self, post_data: Dict[str, str]) -> None:
        """Log failed Discord notifications for manual review."""
        try:
            with open(config.FAILED_NOTIFICATIONS_FILE, "a", encoding='utf-8') as f:
                timestamp = datetime.now().isoformat()
                f.write(f"\n{'='*60}\n")
                f.write(f"Failed at: {timestamp}\n")
                f.write(f"Title: {post_data['title']}\n")
                f.write(f"URL: {post_data['url']}\n")
                f.write(f"Flair: {post_data['flair']}\n")
                f.write(f"{'='*60}\n")
        except Exception as e:
            logging.exception(f"Failed to log failed notification: {e}")

    def _is_giveaway(self, post: RedditPost, is_catchup: bool = False) -> Tuple[bool, str]:
        """
        Determine if a post is a valid giveaway.
        
        With Search API, flair filtering is already done by Reddit,
        but we still validate age and other criteria.
        """
        # Search API already filters by flair, but double-check
        if not post.link_flair_text:
            return False, "no_flair"
        
        if post.link_flair_text not in config.TARGET_FLAIRS:
            return False, f"wrong_flair ({post.link_flair_text})"
        
        if post.age_hours > config.POST_MAX_AGE_HOURS:
            return False, "too_old"
            
        return True, "valid"

    def _startup_catchup(self) -> None:
        """
        On startup, fetch recent posts to catch anything missed during downtime.
        """
        if not config.ENABLE_STARTUP_CATCHUP:
            logging.info("[CATCHUP] Startup catchup disabled in config")
            return
        
        if not self._last_shutdown_time:
            logging.info("[CATCHUP] No previous shutdown time, skipping catchup")
            return
        
        downtime_hours = (time.time() - self._last_shutdown_time) / 3600
        
        if downtime_hours < config.STARTUP_CATCHUP_MIN_DOWNTIME_HOURS:
            logging.info(f"[CATCHUP] Downtime ({downtime_hours:.1f}h) below threshold, skipping catchup")
            return
        
        logging.info("=" * 60)
        logging.info(f"[CATCHUP] Bot was down for {downtime_hours:.1f} hours")
        logging.info("[CATCHUP] Checking for missed posts during downtime...")
        logging.info("=" * 60)
        
        self._update_stat("catchup_runs", lambda x: x + 1)
        self.metrics_tracker.increment("total_catchup_runs")
        
        try:
            posts = self._fetch_reddit_search()
            
            if not posts:
                logging.info("[CATCHUP] No posts fetched")
                return
            
            # Filter to posts created during downtime
            downtime_posts = [
                p for p in posts 
                if self._last_shutdown_time <= p.created_utc <= time.time()
            ]
            
            logging.info(f"[CATCHUP] Found {len(downtime_posts)} posts created during downtime")
            
            if downtime_posts:
                found = self._process_posts(downtime_posts, is_catchup=True)
                if found > 0:
                    logging.info(f"[CATCHUP] ✓ Found {found} giveaways from downtime period!")
                    self._update_stat("posts_from_catchup", lambda x: x + found)
                    self.metrics_tracker.increment("total_catchup_posts_found", found)
                else:
                    logging.info("[CATCHUP] No new giveaways found during downtime")
            
        except Exception as e:
            logging.error(f"[CATCHUP] Error during catchup: {e}")
        
        logging.info("=" * 60)

    def _start_discord_worker(self) -> None:
        """Start or restart the Discord worker thread."""
        self.discord_thread = threading.Thread(
            target=self._discord_worker,
            daemon=True,
            name="DiscordWorker"
        )
        self.discord_thread.start()
        logging.info("[OK] Discord worker started")

    def _discord_worker(self) -> None:
        """Worker thread that sends queued Discord notifications."""
        self.discord_worker_alive.set()
        
        try:
            while not self._stop_event.is_set():
                try:
                    post_data = self.alert_queue.get(timeout=2)
                except queue.Empty:
                    continue

                self.discord_rate_limiter.acquire()
                
                success = self.notifier.send(post_data)
                if success:
                    self.metrics_tracker.increment("total_notifications_sent")
                else:
                    self._log_failed_notification(post_data)
                    self._update_stat("failed_notifications", lambda x: x + 1)
                    self.metrics_tracker.increment("total_notifications_failed")
                
                self.alert_queue.task_done()
        except Exception as e:
            logging.exception(f"[FAIL] Discord worker crashed: {e}")
        finally:
            self.discord_worker_alive.clear()
            logging.info("Discord worker shutting down")

    def _check_worker_health(self) -> None:
        """Check if Discord worker is alive and restart if needed."""
        if not self.discord_worker_alive.is_set():
            logging.warning("[WARN] Discord worker died! Restarting...")
            self._update_stat("worker_restarts", lambda x: x + 1)
            self.health_monitor.send_alert(
                "Worker Restarted",
                "Discord worker thread crashed and was automatically restarted.",
                severity="warning"
            )
            self._start_discord_worker()

    def _fetch_reddit_search(self) -> List[RedditPost]:
        """
        Fetch posts using Reddit Search API.
        
        Makes multiple search queries (one per flair) and combines results.
        """
        all_posts = []
        seen_ids = set()  # Deduplicate across searches
        
        for search_url in config.SEARCH_URLS:
            # Check rate limit before making request
            can_request, wait_time = self.reddit_rate_limiter.can_make_request()
            
            if not can_request:
                logging.warning(f"[RATE] Reddit rate limit reached. Waiting {wait_time:.1f}s...")
                self._update_stat("rate_limit_hits", lambda x: x + 1)
                self.metrics_tracker.increment("total_rate_limit_hits")
                time.sleep(wait_time)
                continue
            
            try:
                response = self.session.get(
                    search_url,
                    timeout=config.REQUEST_TIMEOUT
                )
                
                # Update rate limit info from response headers
                self.reddit_rate_limiter.update_from_headers(dict(response.headers))
                
                # Record the request for fallback tracking
                self.reddit_rate_limiter.record_request()
                
                # Track search query
                self._update_stat("search_queries", lambda x: x + 1)
                self.metrics_tracker.increment("total_search_queries")
                
                response.raise_for_status()
                
                # Check response size
                content_length = len(response.content)
                if content_length > config.MAX_JSON_SIZE_MB * 1024 * 1024:
                    logging.error(f"Response too large: {content_length / (1024*1024):.2f}MB")
                    continue
                
                # Parse JSON
                json_data = response.json()
                posts = RedditJSONParser.parse_response(json_data)
                
                # Deduplicate
                for post in posts:
                    if post.id not in seen_ids:
                        all_posts.append(post)
                        seen_ids.add(post.id)
                
                logging.info(f"[SEARCH] Retrieved {len(posts)} posts from search query")
                
            except requests.exceptions.Timeout:
                logging.warning("[WARN] Reddit Search API request timed out")
                self._update_stat("api_fetch_errors", lambda x: x + 1)
                self.metrics_tracker.increment("total_api_errors")
                continue
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"[WARN] Reddit Search API fetch error: {e}")
                self._update_stat("api_fetch_errors", lambda x: x + 1)
                self.metrics_tracker.increment("total_api_errors")
                continue
                
            except ValueError as e:
                logging.error(f"[FAIL] JSON parse error: {e}")
                self._update_stat("json_parse_errors", lambda x: x + 1)
                continue
                
            except Exception as e:
                logging.exception(f"[FAIL] Unexpected API error: {e}")
                self._update_stat("api_fetch_errors", lambda x: x + 1)
                self.metrics_tracker.increment("total_api_errors")
                continue
        
        logging.info(f"[FETCH] Total {len(all_posts)} unique giveaway posts from all searches")
        return all_posts

    def _process_posts(self, posts: List[RedditPost], is_catchup: bool = False) -> int:
        """
        Process a list of posts and queue valid giveaways.
        """
        new_finds = 0
        mode = "CATCHUP" if is_catchup else "PROCESS"
        
        logging.debug(f"[{mode}] Processing {len(posts)} posts...")
        
        for post in posts:
            if self._stop_event.is_set():
                break

            if post.id in self.seen_posts:
                continue
            
            is_new = self.seen_posts.add(post.id)
            if not is_new:
                continue

            self._update_stat("total_checked", lambda x: x + 1)
            self.metrics_tracker.increment("total_posts_checked")

            is_valid, reason = self._is_giveaway(post, is_catchup=is_catchup)
            
            # Only log valid giveaways
            if is_valid:
                log_msg = (
                    f"[FOUND] 🎁 {post.title[:50]}... | "
                    f"Flair: {post.link_flair_text} | "
                    f"Age: {post.age_hours:.1f}h | "
                    f"Author: u/{post.author}"
                )
                logging.info(log_msg)
                
                try:
                    post_data = {
                        "title": post.title or "Untitled Post",
                        "desc": post.selftext or "No content.",
                        "url": post.link,
                        "subreddit": config.SUBREDDIT,
                        "author": post.author,
                        "flair": post.link_flair_text or "Unknown",
                        "timestamp": datetime.fromtimestamp(
                            post.created_utc,
                            timezone.utc
                        ).isoformat()
                    }
                    
                    self.alert_queue.put(post_data)
                    new_finds += 1
                    self._update_stat("giveaways_found", lambda x: x + 1)
                    self.metrics_tracker.increment("total_giveaways_found")
                        
                except Exception as e:
                    logging.exception(f"[FAIL] Error processing post {post.id}: {e}")
        
        if new_finds > 0:
            logging.info(f"[{mode}] ✓ Found {new_finds} new giveaways")
        else:
            logging.debug(f"[{mode}] No new giveaways found")
        
        return new_finds

    def _perform_maintenance(self) -> None:
        """Perform periodic maintenance tasks."""
        now = time.time()
        
        # Save metrics periodically
        if now - self.last_metrics_save > config.METRICS_SAVE_INTERVAL:
            self.metrics_tracker.save_metrics()
            self.last_metrics_save = now
        
        # Cleanup old cache entries
        if now - self.last_cache_cleanup > config.CACHE_CLEANUP_INTERVAL:
            removed = self.seen_posts.cleanup_old_entries(config.POST_MAX_AGE_HOURS)
            if removed > 0:
                logging.info(f"[CLEANUP] Removed {removed} old cache entries")
            self.last_cache_cleanup = now
            
            # Check if cache is approaching limit
            if len(self.seen_posts) > config.SEEN_POSTS_CLEANUP_THRESHOLD:
                logging.warning(f"[WARN] Cache size {len(self.seen_posts)} exceeds threshold")

    def _get_adaptive_interval(self) -> int:
        """Calculate adaptive polling interval based on rate limit usage."""
        recommended = self.reddit_rate_limiter.get_recommended_interval()
        
        # Check circuit breaker state
        cb_state = self.notifier.circuit_breaker.get_state()
        if cb_state == "OPEN":
            logging.info(f"[ADAPTIVE] Using backoff interval due to circuit breaker")
            return config.BACKOFF_INTERVAL
        
        self._set_stat("current_interval", recommended)
        return recommended

    def _fetch_cycle(self) -> int:
        """Fetch posts from Reddit Search API and process them."""
        try:
            posts = self._fetch_reddit_search()
            new_finds = self._process_posts(posts)
            self._set_stat("last_check", datetime.now())
            
            # Log rate limit usage
            usage = self.reddit_rate_limiter.get_usage()
            logging.debug(
                f"[RATE] Reddit API usage ({usage['source']}): "
                f"{usage['used']}/{usage['max']} ({usage['percentage']:.1f}%), "
                f"{usage['remaining']} remaining" +
                (f", resets in {usage['reset_in']}s" if usage['reset_in'] else "")
            )
            
            return new_finds
        except Exception as e:
            logging.exception(f"[FAIL] Unexpected error during fetch: {e}")
            return 0

    def run(self) -> None:
        """Main monitoring loop with adaptive intervals."""
        logging.info("=" * 50)
        logging.info("Monitor Started (Reddit Search API v2.4)")
        logging.info("=" * 50)
        
        self._start_discord_worker()
        
        # Run startup catchup to find posts missed during downtime
        self._startup_catchup()
        
        last_save = time.time()
        last_health_check = time.time()
        
        self._set_stat("status", "Running")

        while not self._stop_event.is_set():
            # Health check
            if time.time() - last_health_check > config.WORKER_HEALTH_CHECK_INTERVAL:
                self._check_worker_health()
                last_health_check = time.time()
            
            # Pause handling
            with self._pause_condition:
                while self._is_paused and not self._stop_event.is_set():
                    self._pause_condition.wait(timeout=1.0)
            
            if not self._stop_event.is_set():
                # Fetch cycle
                self._fetch_cycle()
                
                # Maintenance
                self._perform_maintenance()

                # State persistence
                if time.time() - last_save > config.SAVE_INTERVAL:
                    self.save_state()
                    last_save = time.time()
            
            # Adaptive sleep interval
            sleep_interval = self._get_adaptive_interval()
            self._stop_event.wait(timeout=sleep_interval)

        logging.info("Waiting for Discord queue to empty...")
        
        start_wait = time.time()
        while not self.alert_queue.empty() and (time.time() - start_wait) < config.SHUTDOWN_TIMEOUT:
            time.sleep(0.5)
        
        self.save_state()
        self._save_shutdown_time()
        self._cleanup()
        
        # Print final metrics
        metrics = self.metrics_tracker.get_summary()
        logging.info("=" * 50)
        logging.info("FINAL METRICS")
        logging.info("=" * 50)
        logging.info(f"Total Uptime: {metrics['total_uptime'] / 3600:.2f} hours")
        logging.info(f"Total Posts Checked: {metrics['total_posts_checked']}")
        logging.info(f"Total Giveaways Found: {metrics['total_giveaways_found']}")
        logging.info(f"Total Notifications Sent: {metrics['total_notifications_sent']}")
        logging.info(f"Total Notifications Failed: {metrics['total_notifications_failed']}")
        logging.info(f"Total API Errors: {metrics['total_api_errors']}")
        logging.info(f"Circuit Breaker Opens: {metrics['total_circuit_breaker_opens']}")
        logging.info(f"Rate Limit Hits: {metrics['total_rate_limit_hits']}")
        logging.info(f"Catchup Runs: {metrics['total_catchup_runs']}")
        logging.info(f"Posts from Catchup: {metrics['total_catchup_posts_found']}")
        logging.info(f"Total Search Queries: {metrics['total_search_queries']}")
        logging.info("=" * 50)
        logging.info("Monitor Stopped")
        logging.info("=" * 50)

    def stop(self) -> None:
        """Signal the monitor to stop gracefully."""
        logging.info("Stop signal received")
        self._stop_event.set()
        with self._pause_condition:
            self._pause_condition.notify_all()

    def toggle_pause(self) -> bool:
        """Toggle the pause state of the monitor."""
        with self._pause_condition:
            self._is_paused = not self._is_paused
            is_paused = self._is_paused
            state = "Paused" if is_paused else "Running"
            self._pause_condition.notify_all()
        
        logging.info(f"State changed to: {state}")
        self._set_stat("status", state)
        return is_paused

    def get_info(self) -> str:
        """Get current status information."""
        with self.stats_lock:
            uptime = int(time.time() - self.stats["start_time"])
            q_size = self.alert_queue.qsize()
            
            last_check = self.stats.get("last_check")
            last_check_str = last_check.strftime("%H:%M:%S") if isinstance(last_check, datetime) else "Never"
            
            cb_state = self.notifier.circuit_breaker.get_state()
            usage = self.reddit_rate_limiter.get_usage()
            
            metrics = self.metrics_tracker.get_summary()
            
            return (
                f"Status: {self.stats['status']}\n"
                f"Mode: Reddit Search API v2.4\n"
                f"Circuit Breaker: {cb_state}\n"
                f"Uptime: {uptime // 3600}h {(uptime % 3600) // 60}m\n"
                f"Last Check: {last_check_str}\n"
                f"Current Interval: {self.stats['current_interval']}s\n"
                f"Reddit API Usage ({usage['source']}): {usage['used']}/{usage['max']} "
                f"({usage['percentage']:.1f}%), {usage['remaining']} remaining" +
                (f", resets in {usage['reset_in']}s\n" if usage['reset_in'] else "\n") +
                f"Session Checked: {self.stats['total_checked']}\n"
                f"Session Found: {self.stats['giveaways_found']}\n"
                f"Session Failed: {self.stats['failed_notifications']}\n"
                f"Search Queries: {self.stats['search_queries']}\n"
                f"Catchup Runs: {self.stats['catchup_runs']}\n"
                f"Posts from Catchup: {self.stats['posts_from_catchup']}\n"
                f"API Errors: {self.stats['api_fetch_errors']}\n"
                f"JSON Parse Errors: {self.stats['json_parse_errors']}\n"
                f"Worker Restarts: {self.stats['worker_restarts']}\n"
                f"Rate Limit Hits: {self.stats['rate_limit_hits']}\n"
                f"Queue: {q_size}\n"
                f"Seen Posts: {len(self.seen_posts)}\n"
                f"--- Lifetime Stats ---\n"
                f"Total Uptime: {metrics['total_uptime'] / 3600:.2f}h\n"
                f"Total Checked: {metrics['total_posts_checked']}\n"
                f"Total Found: {metrics['total_giveaways_found']}\n"
                f"Total Sent: {metrics['total_notifications_sent']}\n"
                f"CB Opens: {metrics['total_circuit_breaker_opens']}\n"
                f"Rate Limit Hits: {metrics['total_rate_limit_hits']}\n"
                f"Catchup Runs: {metrics['total_catchup_runs']}\n"
                f"Catchup Posts: {metrics['total_catchup_posts_found']}\n"
                f"Search Queries: {metrics['total_search_queries']}"
            )


# ------------------ CONSOLE MODE ------------------
def _run_console_mode(bot: GiveawayMonitor, monitor_thread: threading.Thread) -> None:
    """Run in console mode with signal handlers."""
    def signal_handler(signum, frame):
        logging.info(f"Received signal {signum}")
        bot.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.info("=" * 60)
    logging.info("Running in CONSOLE MODE (Reddit Search API v2.4)")
    logging.info("=" * 60)
    logging.info("Press Ctrl+C to stop")
    logging.info(f"PID: {os.getpid()}")
    
    def show_status(signum, frame):
        status_info = bot.get_info()
        logging.info(f"\n{'='*40}\nSTATUS:\n{status_info}\n{'='*40}\n")
    
    try:
        signal.signal(signal.SIGUSR1, show_status)
        logging.info(f"Send SIGUSR1 for status: kill -USR1 {os.getpid()}")
    except AttributeError:
        pass
    
    logging.info("=" * 60)
    
    try:
        while monitor_thread.is_alive():
            monitor_thread.join(timeout=1.0)
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received")
        bot.stop()
    finally:
        monitor_thread.join(timeout=config.THREAD_JOIN_TIMEOUT)


def main() -> None:
    """Main entry point."""
    print("=" * 60)
    print("Reddit Giveaway Monitor v2.4 (Search API)")
    print("=" * 60)
    print(f"Mode: Console")
    print(f"Monitoring: r/{config.SUBREDDIT}")
    print(f"API: Reddit Search (Targeted)")
    print(f"Rate Limit: 100 QPM (10-minute window)")
    print(f"Polling Interval: {config.FAST_INTERVAL}s")
    print(f"Startup Catchup: {'Enabled' if config.ENABLE_STARTUP_CATCHUP else 'Disabled'}")
    print(f"Log Level: {config.LOG_LEVEL}")
    print("=" * 60)
    print()
    print("KEY FEATURES:")
    print("  ✓ Search API targets only giveaway flairs")
    print("  ✓ Faster polling (20s intervals)")
    print("  ✓ Searches up to 1 week back")
    print("  ✓ Startup catchup for missed posts")
    print("  ✓ Deduplication across searches")
    print("  ✓ Rate limit monitoring with headers")
    print("=" * 60)
    print()
    
    bot = GiveawayMonitor()
    
    monitor_thread = threading.Thread(
        target=bot.run,
        daemon=True,
        name="MonitorThread"
    )
    monitor_thread.start()
    
    _run_console_mode(bot, monitor_thread)

if __name__ == "__main__":
    main()
