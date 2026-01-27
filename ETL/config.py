"""
ETL Pipeline Configuration
==========================
Centralized configuration for the ETL pipeline.
Edit these settings to customize the pipeline behavior.
"""

# ================================================================
# DATABASE CONFIGURATION
# ================================================================

DATABASE_CONFIG = {
    'host': 'localhost',
    'database': 'ecommerce_etl',
    'user': 'root',
    'password': 'Otrera11!',  # REQUIRED: Update with your MySQL password
    'port': 3306,
}

# ================================================================
# SCRAPING CONFIGURATION
# ================================================================

# Number of pages to scrape per category
# Increase for more products, decrease for faster execution
PAGES_PER_CATEGORY = 5  # ~100 products per category = ~500 total

# Delay between HTTP requests (in seconds)
# Increase to be more respectful to the server
REQUEST_DELAY = 2

# HTTP request timeout (in seconds)
REQUEST_TIMEOUT = 10

# User agent for web scraping
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

# ================================================================
# CATEGORIES TO SCRAPE
# ================================================================

AMAZON_CATEGORIES = [
    ('Electronics', 'https://www.amazon.com/best-sellers-electronics/zgbs/electronics'),
    ('Books', 'https://www.amazon.com/Best-Sellers-Books/zgbs/books'),
    ('Home & Kitchen', 'https://www.amazon.com/best-sellers-home-garden/zgbs/home-garden'),
    ('Toys & Games', 'https://www.amazon.com/Best-Sellers-Toys-Games/zgbs/toys-and-games'),
    ('Sports & Outdoors', 'https://www.amazon.com/Best-Sellers-Sports-Outdoors/zgbs/sporting-goods'),
]

# ================================================================
# LOGGING CONFIGURATION
# ================================================================

# Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = 'INFO'

# Log file path (None for console only)
LOG_FILE = None  # e.g., 'etl_pipeline.log'

# ================================================================
# DATA QUALITY SETTINGS
# ================================================================

# Minimum required fields for a valid product
REQUIRED_FIELDS = ['product_id', 'name', 'url']

# Price validation range (min, max in USD)
PRICE_RANGE = (0.01, 1000000.00)

# Rating validation range
RATING_RANGE = (0.0, 5.0)

# ================================================================
# FEATURE FLAGS
# ================================================================

# Enable/disable specific features
ENABLE_TRANSACTION_ROLLBACK = True
ENABLE_DUPLICATE_PREVENTION = True
ENABLE_DATA_VALIDATION = True

# ================================================================
# PERFORMANCE SETTINGS
# ================================================================

# Batch size for database inserts (not currently used, for future optimization)
BATCH_SIZE = 100

# Maximum retries for failed HTTP requests
MAX_RETRIES = 3
