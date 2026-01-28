# E-Commerce ETL Pipeline

A comprehensive Python-based ETL (Extract, Transform, Load) pipeline that scrapes product data from Amazon Best Sellers, cleans and transforms the data, and loads it into a normalized MySQL database. Further, a comprehensive data analytics and visualization report is provided.

## Features

- **Extract**: Scrapes 500+ product records from Amazon Best Sellers across multiple categories
- **Transform**: Comprehensive data cleaning including text normalization, price parsing, and validation
- **Load**: Inserts data into a normalized MySQL database using parameterized queries
- **Logging**: Detailed logging throughout the pipeline for monitoring and debugging
- **Error Handling**: Robust error handling with graceful degradation
- **Data Analysis and Visualization**: A comprehensive analytics report with descriptive visualizations

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
## Policy and Compliance

This document outlines the policy statement and compliance measures implemented for an Extract, Transform, Load (ETL) data scraping project targeting the website [https://www.amazon.com/best-sellers](https://www.amazon.com/best-sellers) using Python for scraping and MySQL for data storage.

## Website Policy Statement

**Website URL:** [https://www.amazon.com/best-sellers](https://www.amazon.com/best-sellers)

**Policy:** Amazon.com is a commercial website, and web scraping activities are generally governed by its Terms of Service and `robots.txt` file. Unlike a practice site, Amazon is not designed for unrestricted scraping, and automated data extraction without explicit permission can be a violation of their policies. It's imperative to review and adhere to Amazon's specific `robots.txt` directives and Terms of Service.

**Disclaimer:** For any project targeting commercial websites like Amazon, it is absolutely crucial to **rigorously check the target website's `robots.txt` file (e.g., `https://www.amazon.com/robots.txt`) and its Terms of Service (usually found in the footer of the website).** These documents dictate what content can be scraped, at what rate, and for what purpose. Disregarding these can lead to IP blocking, legal action, or a breach of ethical and legal guidelines. Always ensure compliance and consider seeking legal counsel for commercial scraping operations.

## Compliance Assurance

To ensure ethical scraping practices and avoid overloading the target server, the following compliance measures were implemented:

### 1. Rate Limiting (Throttling)

*   **Methodology:** To prevent overwhelming Amazon's servers with requests and to mimic human browsing behavior, a deliberate, randomized delay was introduced between successive requests. This helps in distributing the load and reducing the risk of being identified as a bot or blocked.

### 2. User-Agent Header

*   **Methodology:** A custom `User-Agent` header was set for all HTTP requests to identify the scraping script. This practice allows server administrators to identify the source of requests if issues arise and can help in avoiding bot detection. It should accurately represent a legitimate browser, possibly with an appended identifier.

### 3. Excluded Fields / Data Minimization

*   **Methodology:** Only publicly available information directly relevant to the project's objectives (e.g., product titles, prices, ratings, availability, best-seller rank) was extracted.

### 4. Error Handling and Retry Mechanisms

*   **Methodology:** Robust error handling would be implemented to gracefully manage network issues, temporary server unavailability (e.g., HTTP 429 Too Many Requests, 503 Service Unavailable), or connection timeouts. This would include retry logic with exponential backoff to reattempt failed requests after increasing delays, further respecting server load.

### 5. Data Storage and Retention

*   **Methodology:** Scraped data was loaded into a local MySQL database. Data retention policies would dictate how long the data is stored and how it is secured. For this project, data was stored for analytical purposes, ensuring appropriate security measures for any collected data (even non-PII).

## Analytics Report
## Overview

This analytics module provides comprehensive data analysis and visualization for the ETL pipeline data. It generates statistical insights, visualizations, and a detailed report covering:

- Basic profiling statistics
- Category distribution analysis
- Price and rating insights
- Price-rating correlation analysis
- Company-level performance metrics

## Features

### 📊 Visualizations Generated

1. **category_distribution.png** - Bar chart and pie chart showing product distribution by category
2. **price_insights.png** - Multi-panel visualization with:
   - Average price by category
   - Price range (min/max) by category
   - Average vs median price comparison
   - Price distribution box plots
3. **rating_insights.png** - Rating and review analysis:
   - Average rating by category
   - Average review count by category
4. **price_rating_correlation.png** - Correlation analysis:
   - Scatter plot of price vs rating
   - Average rating by price segment
   - Correlation coefficients by category
   - Review activity by price segment
5. **company_insights.png** - Company performance:
   - Top 10 companies by product count
   - Average rating by company
   - Total reviews by company
   - Performance matrix (rating vs reviews)

### 📈 Analytics Report

The `analytics_report.txt` file contains:

#### 1. Basic Profiling
- Total products count
- Distinct companies count
- Distinct categories count
- Coverage statistics (products with price/ratings)
- Average reviews per product

#### 2. Product Distribution by Category
- Top 10 categories by product count
- Percentage distribution

#### 3. Price and Rating Insights by Category
- Count, average, median, min, max prices
- Average ratings
- Summary statistics

#### 4. Price-Rating Correlation Analysis
- Overall correlation coefficient
- Interpretation of correlation
- Strongest positive/negative correlations
- Price segment analysis

#### 5. Company-Level Insights
- Top 10 companies by product count
- Average rating per company
- Total and average reviews
- Performance metrics

#### 6. Key Findings & Recommendations
- Actionable insights based on data
- Strategic recommendations

## Analytics Details

### 1. Basic Profiling

Provides high-level statistics:
- **Total Products**: Count of all products in database
- **Distinct Companies**: Number of unique brands/manufacturers
- **Distinct Categories**: Number of product categories
- **Coverage Metrics**: Percentage of products with price/rating data
- **Engagement Metrics**: Average reviews per product

### 2. Category Distribution Analysis

**Answers:**
- Which categories have the most products?
- How are products distributed across categories?
- What percentage of total products does each category represent?

**Visualizations:**
- Horizontal bar chart (top 10 categories)
- Pie chart (percentage distribution)

### 3. Price and Rating Insights

**Price Metrics by Category:**
- Average, median, min, max prices
- Standard deviation (price variability)

**Rating Metrics by Category:**
- Average rating (1-5 scale)
- Average review count

**Visualizations:**
- Average price by category
- Price range comparisons
- Average vs median price scatter plot
- Price distribution box plots
- Average rating by category
- Review count by category

**Key Insights:**
- Which categories are most expensive?
- Which have highest price variability?
- Which categories have best ratings?
- Which have most customer engagement?

### 4. Price-Rating Correlation

**Research Question:** Do higher-priced products have higher ratings?

**Methodology:**
- Calculate Pearson correlation coefficient
- Segment products by price range
- Analyze correlation by category

**Correlation Interpretation:**
- `> 0.3`: Strong positive correlation (higher price → higher rating)
- `0.1 to 0.3`: Moderate positive correlation
- `-0.1 to 0.1`: Weak/no correlation
- `-0.3 to -0.1`: Moderate negative correlation
- `< -0.3`: Strong negative correlation (higher price → lower rating)

**Visualizations:**
- Scatter plot: price vs rating (with trend line)
- Average rating by price segment ($0-20, $20-50, etc.)
- Correlation coefficients by category
- Review activity by price segment

**Business Implications:**
- Positive correlation: Premium pricing justified by quality
- Negative correlation: Value products performing better
- No correlation: Price and quality are independent factors

### 5. Company-Level Insights

**Metrics for Top 10 Companies (by product count):**
- Number of products listed
- Average product rating
- Total reviews across all products
- Average reviews per product
- Average, min, max price points
- Number of highly-rated products (≥4.5 stars)
- Number of featured products

**Visualizations:**
- Product count by company
- Average rating by company
- Total reviews by company
- Performance matrix (rating vs reviews, sized by product count)

**Business Insights:**
- Which companies dominate the marketplace?
- Which have the best customer satisfaction?
- Which have highest engagement?
- Market concentration analysis

## Output

Generated files:
  - category_distribution.png
  - price_insights.png
  - rating_insights.png
  - price_rating_correlation.png
  - company_insights.png
  - analytics_report.txt
```
