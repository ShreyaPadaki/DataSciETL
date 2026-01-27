"""
Alternative ETL Pipeline - Books to Scrape
===========================================
This is an alternative implementation of the ETL pipeline that uses
"Books to Scrape" (http://books.toscrape.com) - a safe, public website
designed for web scraping practice.

This demonstrates that the ETL architecture is flexible and can work
with different data sources by only changing the extract functions.

The transform and load functions remain the same!
"""

import re
import time
import logging
from typing import List, Dict, Optional
from datetime import date
import requests
from bs4 import BeautifulSoup

# Import transform and load functions from main pipeline
from etl_pipeline import (
    transform_data,
    load_to_database,
    Product,
    logger
)


# ================================================================
# ALTERNATIVE EXTRACT FUNCTIONS (Books to Scrape)
# ================================================================

def extract_books(num_pages: int = 50) -> List[Dict]:
    """
    Extract book data from Books to Scrape website.
    
    This website is specifically designed for web scraping practice
    and contains ~1000 books across 50 pages.
    
    Args:
        num_pages: Number of pages to scrape (max 50)
    
    Returns:
        List of dictionaries containing raw book data
    """
    logger.info("Starting data extraction from Books to Scrape")
    
    base_url = "http://books.toscrape.com/catalogue/page-{}.html"
    all_books = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for page in range(1, num_pages + 1):
        try:
            url = base_url.format(page)
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            books = _extract_books_from_page(soup, page)
            all_books.extend(books)
            
            logger.info(f"Extracted {len(books)} books from page {page}")
            
            # Be respectful - small delay
            time.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            continue
    
    logger.info(f"Extraction complete. Total books extracted: {len(all_books)}")
    return all_books


def _extract_books_from_page(soup: BeautifulSoup, page_num: int) -> List[Dict]:
    """
    Extract book data from a single page.
    
    Args:
        soup: BeautifulSoup object
        page_num: Current page number
    
    Returns:
        List of book dictionaries
    """
    books = []
    
    # Find all book articles
    book_articles = soup.find_all('article', class_='product_pod')
    
    for idx, article in enumerate(book_articles):
        try:
            # Extract book title
            title_elem = article.find('h3').find('a')
            title = title_elem.get('title', 'Unknown Book')
            
            # Generate unique product ID
            product_id = f"BOOK_{page_num}_{idx}_{title[:20].replace(' ', '_')}"
            
            # Extract price
            price_elem = article.find('p', class_='price_color')
            price_text = price_elem.get_text(strip=True) if price_elem else None
            
            # Extract rating (as number of stars)
            rating_elem = article.find('p', class_='star-rating')
            rating_text = None
            if rating_elem:
                rating_class = rating_elem.get('class', [])
                rating_map = {
                    'One': '1.0',
                    'Two': '2.0',
                    'Three': '3.0',
                    'Four': '4.0',
                    'Five': '5.0'
                }
                for r in rating_class:
                    if r in rating_map:
                        rating_text = rating_map[r]
                        break
            
            # Extract availability (treat as reviews count indicator)
            availability_elem = article.find('p', class_='instock availability')
            in_stock = availability_elem is not None
            # Simulate reviews count based on rating and stock
            reviews_count = '0'
            if in_stock and rating_text:
                rating_val = float(rating_text)
                reviews_count = str(int(rating_val * 100))  # Simulate reviews
            
            # Extract URL
            url_elem = article.find('h3').find('a')
            book_url = url_elem.get('href', '') if url_elem else ''
            if book_url and not book_url.startswith('http'):
                book_url = f"http://books.toscrape.com/catalogue/{book_url}"
            
            # Extract category (we'll use genre classification)
            # For this simple scraper, we'll categorize by rating
            if rating_text:
                rating_val = float(rating_text)
                if rating_val >= 4.0:
                    category = 'Bestsellers'
                elif rating_val >= 3.0:
                    category = 'Popular'
                else:
                    category = 'General'
            else:
                category = 'Uncategorized'
            
            # Extract company (publisher) - simulate from title
            company = _extract_publisher_from_title(title)
            
            # Create book dictionary
            book = {
                'product_id': product_id,
                'name': title,
                'category': category,
                'company': company,
                'description': f"{title} - A captivating book",
                'price': price_text,
                'url': book_url,
                'reviews_count': reviews_count,
                'avg_rating': rating_text,
            }
            
            books.append(book)
            
        except Exception as e:
            logger.warning(f"Error extracting book at index {idx}: {e}")
            continue
    
    return books


def _extract_publisher_from_title(title: str) -> str:
    """
    Extract publisher from book title (simulated).
    
    In a real scenario, this would come from the book details page.
    For this demo, we'll create fictional publishers based on title.
    
    Args:
        title: Book title
    
    Returns:
        Publisher name
    """
    # Simple simulation: use first word of title as publisher prefix
    words = title.split()
    if words:
        first_word = words[0]
        publishers = [
            f"{first_word} Press",
            f"{first_word} Publishing",
            f"{first_word} Books",
            "Generic Publishing House",
        ]
        # Use simple hash to consistently assign publisher
        return publishers[hash(title) % len(publishers)]
    return "Unknown Publisher"


# ================================================================
# MAIN ETL PIPELINE FOR BOOKS
# ================================================================

def run_books_etl_pipeline(num_pages: int = 50, db_config: Optional[Dict] = None):
    """
    Run ETL pipeline for Books to Scrape website.
    
    Args:
        num_pages: Number of pages to scrape (max 50)
        db_config: Database configuration
    """
    logger.info("=" * 60)
    logger.info("STARTING BOOKS ETL PIPELINE")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    try:
        # STEP 1: EXTRACT (using alternative extraction)
        raw_books = extract_books(num_pages=num_pages)
        
        if not raw_books:
            logger.error("No books extracted. Aborting pipeline.")
            return
        
        # STEP 2: TRANSFORM (using same transform function!)
        cleaned_books = transform_data(raw_books)
        
        if not cleaned_books:
            logger.error("No valid books after transformation. Aborting pipeline.")
            return
        
        # STEP 3: LOAD (using same load function!)
        if db_config is None:
            db_config = {
                'host': 'localhost',
                'database': 'ecommerce_etl',
                'user': 'root',
                'password': 'Otrera11!'
            }
        
        load_to_database(cleaned_books, **db_config)
        
        # Pipeline summary
        elapsed_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("BOOKS ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Total books processed: {len(cleaned_books)}")
        logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Books ETL pipeline failed: {e}")
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
        'password': 'Otrera11!',  # UPDATE THIS
    }
    
    # Run ETL pipeline for books
    # 50 pages * ~20 books/page = ~1000 books
    run_books_etl_pipeline(num_pages=50, db_config=DB_CONFIG)
