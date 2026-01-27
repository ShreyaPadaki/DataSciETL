# E-Commerce ETL Pipeline

A comprehensive Python-based ETL (Extract, Transform, Load) pipeline that scrapes product data from Amazon Best Sellers, cleans and transforms the data, and loads it into a normalized MySQL database.

## Features

- **Extract**: Scrapes 500+ product records from Amazon Best Sellers across multiple categories
- **Transform**: Comprehensive data cleaning including text normalization, price parsing, and validation
- **Load**: Inserts data into a normalized MySQL database using parameterized queries
- **Logging**: Detailed logging throughout the pipeline for monitoring and debugging
- **Error Handling**: Robust error handling with graceful degradation
- **Analysis**: Analysis Report

## Architecture

### ETL Pipeline Flow

```
┌─────────────────────────────────────────────────────┐
│                   EXTRACT PHASE                     │
│  - Scrapes Amazon Best Sellers (5 categories)      │
│  - Collects: ID, name, price, ratings, reviews     │
│  - Outputs: List of raw product dictionaries        │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│                  TRANSFORM PHASE                    │
│  - Strips whitespace from text fields              │
│  - Parses prices to numeric format                 │
│  - Normalizes review counts and ratings            │
│  - Validates required fields                       │
│  - Outputs: List of cleaned Product objects        │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│                    LOAD PHASE                       │
│  - Creates normalized database entries             │
│  - Inserts categories and companies                │
│  - Links products via foreign keys                 │
│  - Stores metrics with timestamps                  │
│  - Uses parameterized queries (SQL injection safe) │
└─────────────────────────────────────────────────────┘
```

### Database Schema (Normalized)

```
┌──────────────┐         ┌─────────────┐         ┌──────────────────┐
│  categories  │         │  companies  │         │    products      │
├──────────────┤         ├─────────────┤         ├──────────────────┤
│ category_id  │◄────┐   │ company_id  │◄────┐   │ id (auto)        │
│ category_name│     │   │ company_name│     │   │ product_id (UK)  │
│ created_at   │     │   │ company_...│      │   │ name             │
└──────────────┘     │   │ created_at  │     │   │ category_id (FK) │─┐
                     │   └─────────────┘     │   │ company_id (FK)  │─┤
                     │                       │   │ description      │ │
                     │                       │   │ price            │ │
                     └───────────────────────┼───│ url              │ │
                                             └───│ created_at       │ │
                                                 │ updated_at       │ │
                                                 └──────────────────┘ │
                                                            │         │
                                                            ▼         │
                                                 ┌──────────────────┐ │
                                                 │ product_metrics  │ │
                                                 ├──────────────────┤ │
                                                 │ metric_id        │ │
                                                 │ product_id (FK)  │◄┘
                                                 │ reviews_count    │
                                                 │ avg_rating       │
                                                 │ is_featured      │
                                                 │ snapshot_date    │
                                                 │ created_at       │
                                                 └──────────────────┘
```

## Data Collected

For each product, the pipeline collects:

| Attribute | Description | Type | Example |
|-----------|-------------|------|---------|
| Product ID | Unique identifier (ASIN) | String | B08N5WRWNW |
| Product Name | Full product title | String | "Apple AirPods Pro (2nd Gen)" |
| Category | Product category | String | "Electronics" |
| Company/Brand | Manufacturer or brand | String | "Apple" |
| Description | Short description/tags | String | Product title excerpt |
| Price | Product price (USD) | Decimal | 249.99 |
| URL | Product page URL | String | https://amazon.com/... |
| Reviews Count | Number of reviews | Integer | 45,678 |
| Average Rating | Rating out of 5 stars | Decimal | 4.7 |

### Function Reference

#### Extract Functions

- `extract_products(num_pages, delay)` - Main extraction function
  - `num_pages`: Pages to scrape per category (default: 5)
  - `delay`: Delay between requests in seconds (default: 2)
  - Returns: List of raw product dictionaries

#### Transform Functions

- `transform_data(raw_products)` - Main transformation function
  - Cleans all text fields
  - Parses prices, reviews, ratings
  - Validates data integrity
  - Returns: List of Product objects

- `_clean_text(text)` - Strips whitespace and normalizes text
- `_parse_price(price_text)` - Converts price strings to float
- `_parse_reviews_count(reviews_text)` - Extracts review count as integer
- `_parse_rating(rating_text)` - Parses rating value (0-5)

#### Load Functions

- `load_to_database(products, **db_config)` - Main load function
  - Uses parameterized queries for security
  - Handles transactions with rollback on error
  - Returns: None (data is persisted to database)

- `_load_categories(cursor, products)` - Inserts unique categories
- `_load_companies(cursor, products)` - Inserts unique companies
- `_load_products(cursor, products, category_map, company_map)` - Inserts products
- `_load_product_metrics(cursor, products)` - Inserts metrics

## Sample Queries

After loading data, you can analyze it with SQL queries:

### Top-Rated Products
```sql
SELECT 
    p.name,
    c.company_name,
    cat.category_name,
    pm.avg_rating,
    pm.reviews_count
FROM products p
JOIN companies c ON p.company_id = c.company_id
JOIN categories cat ON p.category_id = cat.category_id
JOIN product_metrics pm ON p.product_id = pm.product_id
WHERE pm.avg_rating >= 4.5
ORDER BY pm.avg_rating DESC, pm.reviews_count DESC
LIMIT 20;
```

### Category Performance
```sql
SELECT 
    cat.category_name,
    COUNT(p.id) as product_count,
    AVG(pm.avg_rating) as avg_category_rating,
    SUM(pm.reviews_count) as total_reviews,
    AVG(p.price) as avg_price
FROM categories cat
JOIN products p ON cat.category_id = p.category_id
JOIN product_metrics pm ON p.product_id = pm.product_id
GROUP BY cat.category_id, cat.category_name
ORDER BY product_count DESC;
```

### Company Analysis
```sql
SELECT 
    c.company_name,
    COUNT(p.id) as product_count,
    AVG(p.price) as avg_price,
    AVG(pm.avg_rating) as avg_rating,
    SUM(pm.reviews_count) as total_reviews
FROM companies c
JOIN products p ON c.company_id = p.company_id
JOIN product_metrics pm ON p.product_id = pm.product_id
GROUP BY c.company_id, c.company_name
HAVING product_count >= 3
ORDER BY avg_rating DESC;
```

## Code Quality Features

### Security
- **Parameterized Queries**: All SQL queries use parameterized statements to prevent SQL injection
- **Input Validation**: All user inputs are validated and sanitized
- **Error Handling**: Comprehensive try-catch blocks prevent crashes

### Maintainability
- **Clear Function Boundaries**: Separate functions for extract, transform, and load
- **Comprehensive Documentation**: Docstrings for all functions
- **Type Hints**: Type annotations for better IDE support
- **Logging**: Detailed logging at INFO and ERROR levels

### Data Quality
- **Validation**: Required fields are validated before insertion
- **Normalization**: Prices, ratings, and counts are normalized
- **Missing Value Handling**: Appropriate defaults for missing data
- **Duplicate Prevention**: UNIQUE constraints and ON DUPLICATE KEY UPDATE

## Logging

The pipeline provides detailed logging output:

```
2026-01-27 10:30:15 - INFO - Starting data extraction from Amazon Best Sellers
2026-01-27 10:30:17 - INFO - Scraping category: Electronics
2026-01-27 10:30:20 - INFO - Extracted 50 products from Electronics - Page 1
...
2026-01-27 10:35:42 - INFO - Extraction complete. Total products extracted: 523
2026-01-27 10:35:42 - INFO - Starting data transformation
2026-01-27 10:35:43 - INFO - Transformation complete. Cleaned products: 518
2026-01-27 10:35:43 - INFO - Starting data load to MySQL database
2026-01-27 10:35:44 - INFO - Connected to MySQL database: ecommerce_etl
2026-01-27 10:35:44 - INFO - Loading categories
2026-01-27 10:35:45 - INFO - Loaded 5 categories
2026-01-27 10:35:45 - INFO - Loading companies
2026-01-27 10:35:46 - INFO - Loaded 234 companies
2026-01-27 10:35:46 - INFO - Loading products
2026-01-27 10:35:48 - INFO - Loaded 518 products
2026-01-27 10:35:48 - INFO - Loading product metrics
2026-01-27 10:35:49 - INFO - Loaded 518 product metrics
2026-01-27 10:35:49 - INFO - Data successfully loaded to database
2026-01-27 10:35:49 - INFO - ETL PIPELINE COMPLETED SUCCESSFULLY
2026-01-27 10:35:49 - INFO - Total products processed: 518
2026-01-27 10:35:49 - INFO - Time elapsed: 334.12 seconds
```

## Policy Compliance and Ethical Considerations
This document outlines the policy statement and compliance measures implemented for an Extract, Transform, Load (ETL) data scraping project targeting the website [https://www.amazon.com/best-sellers](https://www.amazon.com/best-sellers) using Python for scraping and MySQL for data storage.

## Website Policy Statement

**Website URL:** [https://www.amazon.com/best-sellers](https://www.amazon.com/best-sellers)

**Policy:** Amazon.com is a commercial website, and web scraping activities are generally governed by its Terms of Service and `robots.txt` file. Unlike a practice site, Amazon is not designed for unrestricted scraping, and automated data extraction without explicit permission can be a violation of their policies. It's imperative to review and adhere to Amazon's specific `robots.txt` directives and Terms of Service.

**Disclaimer:** For any project targeting commercial websites like Amazon, it is absolutely crucial to **rigorously check the target website's `robots.txt` file (e.g., `https://www.amazon.com/robots.txt`) and its Terms of Service (usually found in the footer of the website).** These documents dictate what content can be scraped, at what rate, and for what purpose. Disregarding these can lead to IP blocking, legal action, or a breach of ethical and legal guidelines. Always ensure compliance and consider seeking legal counsel for commercial scraping operations.

## Compliance Assurance

To ensure ethical scraping practices and avoid overloading the target server, the following compliance measures were implemented or considered:

### 1. Rate Limiting (Throttling)

*   **Methodology:** To prevent overwhelming Amazon's servers with requests and to mimic human browsing behavior, a deliberate, randomized delay was introduced between successive requests. This helps in distributing the load and reducing the risk of being identified as a bot or blocked.

### 2. User-Agent Header

*   **Methodology:** A custom `User-Agent` header was set for all HTTP requests to identify the scraping script. This practice allows server administrators to identify the source of requests if issues arise and can help in avoiding bot detection. It should accurately represent a legitimate browser, possibly with an appended identifier.

### 3. Excluded Fields / Data Minimization

*   **Methodology:** Only publicly available information directly relevant to the project's objectives (e.g., product titles, prices, ratings, availability, best-seller rank) was extracted.

### 4. Error Handling and Retry Mechanisms

*   **Methodology:** Robust error handling was implemented to gracefully manage network issues, temporary server unavailability (e.g., HTTP 429 Too Many Requests, 503 Service Unavailable), or connection timeouts. This included retry logic with exponential backoff to reattempt failed requests after increasing delays, further respecting server load.

### 5. Data Storage and Retention

*   **Methodology:** Scraped data was loaded into a local MySQL database. Data retention policies would dictate how long the data is stored and how it is secured. For this project, data is to be stored for analytical purposes, ensuring appropriate security measures for any collected data (even non-PII).

## Conclusion

This ETL project, when targeting commercial sites like Amazon, requires a robust framework for ethical data collection and server-respectful interaction. Thorough legal review and explicit consent (if required by Amazon's policies) are paramount for commercial or large-scale projects.
