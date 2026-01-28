"""
E-Commerce Product ETL Pipeline
================================
This script extracts product data from Amazon's Best Sellers page,
transforms/cleans the data, and loads it into a MySQL database.

Functions:
- extract_products(): Scrapes product data from Amazon
- transform_data(): Cleans and normalizes the extracted data
- load_to_database(): Inserts data into MySQL using parameterized queries

Author: ETL Pipeline
Date: 2026-01-27
"""

import re
import time
import logging
from typing import List, Dict, Optional, Tuple
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ================================================================
# DATA MODELS
# ================================================================

@dataclass
class Product:
    """Data class representing a product record"""
    product_id: str
    name: str
    category: str
    company: str
    description: str
    price: Optional[float]
    url: str
    reviews_count: int
    avg_rating: Optional[float]


# ================================================================
# EXTRACT FUNCTIONS
# ================================================================

def extract_products(num_pages: int = 5, delay: int = 2) -> List[Dict]:
    """
    Extract product data from Amazon Best Sellers pages.
    
    This function scrapes multiple categories from Amazon's Best Sellers
    to gather at least 500 product records with relevant attributes.
    
    Args:
        num_pages: Number of pages to scrape per category
        delay: Delay between requests in seconds (be respectful)
    
    Returns:
        List of dictionaries containing raw product data
    
    Raises:
        requests.RequestException: If HTTP request fails
    """
    logger.info("Starting data extraction from Amazon Best Sellers")
    
    # Amazon Best Sellers categories to scrape
    categories = [
        ('Electronics', 'https://www.amazon.com/best-sellers-electronics/zgbs/electronics'),
        ('Books', 'https://www.amazon.com/Best-Sellers-Books/zgbs/books'),
        ('Home & Kitchen', 'https://www.amazon.com/best-sellers-home-garden/zgbs/home-garden'),
        ('Toys & Games', 'https://www.amazon.com/Best-Sellers-Toys-Games/zgbs/toys-and-games'),
        ('Sports & Outdoors', 'https://www.amazon.com/Best-Sellers-Sports-Outdoors/zgbs/sporting-goods'),
    ]
    
    all_products = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    for category_name, base_url in categories:
        logger.info(f"Scraping category: {category_name}")
        
        for page in range(1, num_pages + 1):
            try:
                # Construct URL with page parameter
                url = f"{base_url}?pg={page}" if page > 1 else base_url
                
                # Make HTTP request
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
                
                # Parse HTML
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract products from page
                products = _extract_products_from_page(soup, category_name, base_url)
                all_products.extend(products)
                
                logger.info(f"Extracted {len(products)} products from {category_name} - Page {page}")
                
                # Respectful delay between requests
                time.sleep(delay)
                
            except requests.RequestException as e:
                logger.error(f"Error fetching {category_name} page {page}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error on {category_name} page {page}: {e}")
                continue
    
    logger.info(f"Extraction complete. Total products extracted: {len(all_products)}")
    return all_products


def _extract_products_from_page(soup: BeautifulSoup, category: str, base_url: str) -> List[Dict]:
    """
    Extract individual product data from a BeautifulSoup page object.
    
    Args:
        soup: BeautifulSoup object of the page
        category: Category name for the products
        base_url: Base URL for constructing product URLs
    
    Returns:
        List of product dictionaries
    """
    products = []
    
    # Find all product items on the page
    # Amazon uses various selectors; adjust based on actual page structure
    product_items = soup.find_all('div', {'class': re.compile(r'zg-grid-general-faceout|p13n-sc-uncoverable-faceout')})
    
    for idx, item in enumerate(product_items):
        try:
            # Extract product ID (from ASIN or data attribute)
            asin_elem = item.find('div', {'data-asin': True})
            product_id = asin_elem.get('data-asin', f'UNKNOWN_{category}_{idx}') if asin_elem else f'UNKNOWN_{category}_{idx}'
            
            # Extract product name/title
            name_elem = item.find('div', {'class': re.compile(r'p13n-sc-truncate|_cDEzb_p13n-sc-css-line-clamp-3')})
            if not name_elem:
                name_elem = item.find('img', {'alt': True})
                name = name_elem.get('alt', 'Unknown Product').strip() if name_elem else 'Unknown Product'
            else:
                name = name_elem.get_text(strip=True)
            
            # Extract company/brand (often in title or separate element)
            company = _extract_company_from_name(name)
            
            # Extract description/tags (use truncated title as description)
            description = name[:200] if len(name) > 200 else name
            
            # Extract price
            price_elem = item.find('span', {'class': re.compile(r'p13n-sc-price|a-price-whole')})
            price_text = price_elem.get_text(strip=True) if price_elem else None
            
            # Extract reviews count
            reviews_elem = item.find('a', {'class': re.compile(r'a-size-small|a-link-normal')})
            reviews_text = reviews_elem.get_text(strip=True) if reviews_elem else '0'
            
            # Extract rating
            rating_elem = item.find('span', {'class': re.compile(r'a-icon-alt')})
            rating_text = rating_elem.get_text(strip=True) if rating_elem else None
            
            # Extract product URL
            url_elem = item.find('a', {'class': re.compile(r'a-link-normal')})
            product_url = url_elem.get('href', '') if url_elem else ''
            if product_url and not product_url.startswith('http'):
                product_url = f"https://www.amazon.com{product_url}"
            
            # Create product dictionary
            product = {
                'product_id': product_id,
                'name': name,
                'category': category,
                'company': company,
                'description': description,
                'price': price_text,
                'url': product_url or f"{base_url}#{product_id}",
                'reviews_count': reviews_text,
                'avg_rating': rating_text,
            }
            
            products.append(product)
            
        except Exception as e:
            logger.warning(f"Error extracting product at index {idx}: {e}")
            continue
    
    return products


def _extract_company_from_name(name: str) -> str:
    """
    Extract company/brand name from product title.
    
    Many product titles start with brand name.
    
    Args:
        name: Product name/title
    
    Returns:
        Extracted company name or 'Unknown'
    """
    # Common patterns: "Brand Name - Product Description"
    # Or "Brand Name Product Description"
    parts = name.split('-')[0].split(',')[0].strip()
    
    # Take first 1-3 words as potential brand
    words = parts.split()[:3]
    company = ' '.join(words) if words else 'Unknown'
    
    return company


# ================================================================
# TRANSFORM FUNCTIONS
# ================================================================

def transform_data(raw_products: List[Dict]) -> List[Product]:
    """
    Transform and clean raw product data.
    
    This function performs the following transformations:
    - Strips whitespace from text fields
    - Converts prices to numeric format (Decimal)
    - Handles missing values with appropriate defaults
    - Validates and cleans ratings
    - Normalizes review counts
    
    Args:
        raw_products: List of raw product dictionaries
    
    Returns:
        List of cleaned Product objects
    """
    logger.info("Starting data transformation")
    
    cleaned_products = []
    
    for raw in raw_products:
        try:
            # Clean and validate product data
            product = Product(
                product_id=_clean_text(raw.get('product_id', '')),
                name=_clean_text(raw.get('name', 'Unknown')),
                category=_clean_text(raw.get('category', 'Uncategorized')),
                company=_clean_text(raw.get('company', 'Unknown')),
                description=_clean_text(raw.get('description', '')),
                price=_parse_price(raw.get('price')),
                url=_clean_text(raw.get('url', '')),
                reviews_count=_parse_reviews_count(raw.get('reviews_count')),
                avg_rating=_parse_rating(raw.get('avg_rating')),
            )
            
            # Validate required fields
            if product.product_id and product.name and product.url:
                cleaned_products.append(product)
            else:
                logger.warning(f"Skipping invalid product: {raw.get('product_id', 'NO_ID')}")
                
        except Exception as e:
            logger.error(f"Error transforming product {raw.get('product_id', 'UNKNOWN')}: {e}")
            continue
    
    logger.info(f"Transformation complete. Cleaned products: {len(cleaned_products)}")
    return cleaned_products


def _clean_text(text: Optional[str]) -> str:
    """
    Clean text by stripping whitespace and removing extra spaces.
    
    Args:
        text: Input text string
    
    Returns:
        Cleaned text string
    """
    if not text:
        return ''
    
    # Strip leading/trailing whitespace
    cleaned = text.strip()
    
    # Remove extra spaces
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned


def _parse_price(price_text: Optional[str]) -> Optional[float]:
    """
    Parse price string to numeric float value.
    
    Handles various formats:
    - $99.99
    - $1,234.56
    - 99.99
    - Price range: $50-$100 (takes average)
    
    Args:
        price_text: Raw price string
    
    Returns:
        Numeric price value or None if invalid
    """
    if not price_text:
        return None
    
    try:
        # Remove currency symbols and commas
        cleaned = re.sub(r'[$,€£]', '', price_text)
        
        # Check for price range (e.g., "50-100")
        if '-' in cleaned:
            parts = cleaned.split('-')
            if len(parts) == 2:
                try:
                    low = float(re.findall(r'[\d.]+', parts[0])[0])
                    high = float(re.findall(r'[\d.]+', parts[1])[0])
                    return round((low + high) / 2, 2)
                except (IndexError, ValueError):
                    pass
        
        # Extract first numeric value
        matches = re.findall(r'[\d.]+', cleaned)
        if matches:
            price = float(matches[0])
            # Validate reasonable price range
            if 0 < price < 1000000:
                return round(price, 2)
    
    except (ValueError, InvalidOperation) as e:
        logger.debug(f"Could not parse price '{price_text}': {e}")
    
    return None


def _parse_reviews_count(reviews_text: Optional[str]) -> int:
    """
    Parse reviews count from text to integer.
    
    Handles formats like:
    - "1,234 reviews"
    - "1.2K"
    - "500"
    
    Args:
        reviews_text: Raw reviews count text
    
    Returns:
        Integer count or 0 if invalid
    """
    if not reviews_text:
        return 0
    
    try:
        # Remove commas
        cleaned = reviews_text.replace(',', '')
        
        # Handle K/M notation
        if 'k' in cleaned.lower():
            num = float(re.findall(r'[\d.]+', cleaned)[0])
            return int(num * 1000)
        elif 'm' in cleaned.lower():
            num = float(re.findall(r'[\d.]+', cleaned)[0])
            return int(num * 1000000)
        
        # Extract numeric value
        matches = re.findall(r'\d+', cleaned)
        if matches:
            return int(matches[0])
    
    except (ValueError, IndexError) as e:
        logger.debug(f"Could not parse reviews count '{reviews_text}': {e}")
    
    return 0


def _parse_rating(rating_text: Optional[str]) -> Optional[float]:
    """
    Parse rating from text to float value.
    
    Handles formats like:
    - "4.5 out of 5 stars"
    - "4.5 stars"
    - "4.5"
    
    Args:
        rating_text: Raw rating text
    
    Returns:
        Float rating (0-5) or None if invalid
    """
    if not rating_text:
        return None
    
    try:
        # Extract first numeric value
        matches = re.findall(r'[\d.]+', rating_text)
        if matches:
            rating = float(matches[0])
            # Validate rating range
            if 0 <= rating <= 5:
                return round(rating, 2)
    
    except (ValueError, IndexError) as e:
        logger.debug(f"Could not parse rating '{rating_text}': {e}")
    
    return None


# ================================================================
# LOAD FUNCTIONS
# ================================================================

def load_to_database(
    products: List[Product],
    host: str = 'localhost',
    database: str = 'ecommerce_etl',
    user: str = 'root',
    password: str = ''
) -> None:
    """
    Load transformed product data into MySQL database.
    
    This function:
    1. Establishes database connection
    2. Inserts categories and companies (normalized)
    3. Inserts products with foreign key references
    4. Inserts product metrics
    
    Uses parameterized queries to prevent SQL injection.
    
    Args:
        products: List of Product objects to insert
        host: MySQL host
        database: Database name
        user: Database user
        password: Database password
    
    Raises:
        mysql.connector.Error: If database operation fails
    """
    logger.info("Starting data load to MySQL database")
    
    connection = None
    cursor = None
    
    try:
        # Establish database connection
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            logger.info(f"Connected to MySQL database: {database}")
            
            # Insert data in transaction
            connection.start_transaction()
            
            # Load categories and companies (get ID mappings)
            category_map = _load_categories(cursor, products)
            company_map = _load_companies(cursor, products)
            
            # Load products
            _load_products(cursor, products, category_map, company_map)
            
            # Load product metrics
            _load_product_metrics(cursor, products)
            
            # Commit transaction
            connection.commit()
            logger.info("Data successfully loaded to database")
            
    except Error as e:
        if connection:
            connection.rollback()
        logger.error(f"Database error: {e}")
        raise
    
    finally:
        # Close database connections
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed")


def _load_categories(cursor, products: List[Product]) -> Dict[str, int]:
    """
    Insert unique categories and return category_name -> category_id mapping.
    
    Args:
        cursor: MySQL cursor object
        products: List of Product objects
    
    Returns:
        Dictionary mapping category names to category IDs
    """
    logger.info("Loading categories")
    
    # Get unique categories
    unique_categories = {p.category for p in products if p.category}
    
    category_map = {}
    
    # Insert categories using parameterized query
    insert_query = """
        INSERT INTO categories (category_name)
        VALUES (%s)
        ON DUPLICATE KEY UPDATE category_id=LAST_INSERT_ID(category_id)
    """
    
    for category_name in unique_categories:
        try:
            cursor.execute(insert_query, (category_name,))
            category_id = cursor.lastrowid
            
            # If category already existed, get its ID
            if category_id == 0:
                cursor.execute(
                    "SELECT category_id FROM categories WHERE category_name = %s",
                    (category_name,)
                )
                result = cursor.fetchone()
                if result:
                    category_id = result['category_id']
            
            category_map[category_name] = category_id
            
        except Error as e:
            logger.error(f"Error inserting category '{category_name}': {e}")
    
    logger.info(f"Loaded {len(category_map)} categories")
    return category_map


def _load_companies(cursor, products: List[Product]) -> Dict[str, int]:
    """
    Insert unique companies and return company_name -> company_id mapping.
    
    Args:
        cursor: MySQL cursor object
        products: List of Product objects
    
    Returns:
        Dictionary mapping company names to company IDs
    """
    logger.info("Loading companies")
    
    # Get unique companies
    unique_companies = {p.company for p in products if p.company}
    
    company_map = {}
    
    # Insert companies using parameterized query
    insert_query = """
        INSERT INTO companies (company_name, company_industry)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE company_id=LAST_INSERT_ID(company_id)
    """
    
    for company_name in unique_companies:
        try:
            # For now, company_industry is NULL (could be enhanced)
            cursor.execute(insert_query, (company_name, None))
            company_id = cursor.lastrowid
            
            # If company already existed, get its ID
            if company_id == 0:
                cursor.execute(
                    "SELECT company_id FROM companies WHERE company_name = %s",
                    (company_name,)
                )
                result = cursor.fetchone()
                if result:
                    company_id = result['company_id']
            
            company_map[company_name] = company_id
            
        except Error as e:
            logger.error(f"Error inserting company '{company_name}': {e}")
    
    logger.info(f"Loaded {len(company_map)} companies")
    return company_map


def _load_products(
    cursor,
    products: List[Product],
    category_map: Dict[str, int],
    company_map: Dict[str, int]
) -> None:
    """
    Insert products into database using foreign key references.
    
    Args:
        cursor: MySQL cursor object
        products: List of Product objects
        category_map: Category name to ID mapping
        company_map: Company name to ID mapping
    """
    logger.info("Loading products")
    
    # Parameterized insert query to prevent SQL injection
    insert_query = """
        INSERT INTO products (
            product_id, name, category_id, company_id,
            description, price, url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            category_id = VALUES(category_id),
            company_id = VALUES(company_id),
            description = VALUES(description),
            price = VALUES(price),
            url = VALUES(url),
            updated_at = CURRENT_TIMESTAMP
    """
    
    inserted_count = 0
    
    for product in products:
        try:
            # Get foreign key IDs
            category_id = category_map.get(product.category)
            company_id = company_map.get(product.company)
            
            # Execute parameterized query
            cursor.execute(insert_query, (
                product.product_id,
                product.name,
                category_id,
                company_id,
                product.description,
                product.price,
                product.url
            ))
            
            inserted_count += 1
            
        except Error as e:
            logger.error(f"Error inserting product '{product.product_id}': {e}")
    
    logger.info(f"Loaded {inserted_count} products")


def _load_product_metrics(cursor, products: List[Product]) -> None:
    """
    Insert product metrics (reviews, ratings) into database.
    
    Args:
        cursor: MySQL cursor object
        products: List of Product objects
    """
    logger.info("Loading product metrics")
    
    # Get current date for snapshot
    snapshot_date = date.today()
    
    # Parameterized insert query
    insert_query = """
        INSERT INTO product_metrics (
            product_id, reviews_count, avg_rating,
            is_featured, snapshot_date
        )
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            reviews_count = VALUES(reviews_count),
            avg_rating = VALUES(avg_rating),
            is_featured = VALUES(is_featured)
    """
    
    inserted_count = 0
    
    for product in products:
        try:
            # Determine if product is featured (has high rating and reviews)
            is_featured = (
                product.avg_rating is not None and
                product.avg_rating >= 4.5 and
                product.reviews_count >= 100
            )
            
            # Execute parameterized query
            cursor.execute(insert_query, (
                product.product_id,
                product.reviews_count,
                product.avg_rating,
                is_featured,
                snapshot_date
            ))
            
            inserted_count += 1
            
        except Error as e:
            logger.error(f"Error inserting metrics for '{product.product_id}': {e}")
    
    logger.info(f"Loaded {inserted_count} product metrics")


# ================================================================
# MAIN ETL PIPELINE
# ================================================================

def run_etl_pipeline(
    num_pages: int = 5,
    db_config: Optional[Dict] = None
) -> None:
    """
    Run the complete ETL pipeline.
    
    This orchestrates the three main steps:
    1. Extract: Scrape product data from e-commerce site
    2. Transform: Clean and normalize the data
    3. Load: Insert into MySQL database
    
    Args:
        num_pages: Number of pages to scrape per category
        db_config: Database configuration dictionary
    """
    logger.info("=" * 60)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    try:
        # STEP 1: EXTRACT
        raw_products = extract_products(num_pages=num_pages)
        
        if not raw_products:
            logger.error("No products extracted. Aborting pipeline.")
            return
        
        # STEP 2: TRANSFORM
        cleaned_products = transform_data(raw_products)
        
        if not cleaned_products:
            logger.error("No valid products after transformation. Aborting pipeline.")
            return
        
        # STEP 3: LOAD
        if db_config is None:
            db_config = {
                'host': 'localhost',
                'database': 'ecommerce_etl',
                'user': 'root',
                'password': 'your_password_here'  # UPDATE THIS
            }
        
        load_to_database(cleaned_products, **db_config)
        
        # Pipeline summary
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Total products processed: {len(cleaned_products)}")
        logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise


# ================================================================
# ENTRY POINT
# ================================================================

if __name__ == "__main__":
    # Database configuration
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'ecommerce_etl',
        'user': 'root',
        'password': 'Your_Password_Here',  # UPDATE THIS WITH YOUR PASSWORD
    }
    
    # Run ETL pipeline
    # Adjust num_pages to control how many products to scrape
    # num_pages=5 per category should yield 500+ products
    run_etl_pipeline(num_pages=5, db_config=DB_CONFIG)
