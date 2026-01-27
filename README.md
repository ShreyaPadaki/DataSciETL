# E-Commerce ETL Pipeline

A comprehensive Python-based ETL (Extract, Transform, Load) pipeline that scrapes product data from Amazon Best Sellers, cleans and transforms the data, and loads it into a normalized MySQL database.

## Features

- **Extract**: Scrapes 500+ product records from Amazon Best Sellers across multiple categories
- **Transform**: Comprehensive data cleaning including text normalization, price parsing, and validation
- **Load**: Inserts data into a normalized MySQL database using parameterized queries
- **Logging**: Detailed logging throughout the pipeline for monitoring and debugging
- **Error Handling**: Robust error handling with graceful degradation

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

## Setup Instructions

### Prerequisites

- Python 3.8 or higher
- MySQL 5.7 or higher
- pip (Python package manager)

### Installation

1. **Clone or download the project files**

2. **Install Python dependencies**

```bash
pip install -r requirements.txt
```

3. **Set up MySQL database**

```bash
# Log into MySQL
mysql -u root -p

# Create database
CREATE DATABASE ecommerce_etl CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# Create tables using the schema file
mysql -u root -p ecommerce_etl < schema.sql
```

4. **Configure database credentials**

Edit `etl_pipeline.py` and update the database configuration:

```python
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ecommerce_etl',
    'user': 'root',
    'password': 'YOUR_MYSQL_PASSWORD',  # Update this
}
```

## Usage

### Running the ETL Pipeline

```bash
python etl_pipeline.py
```

This will:
1. Scrape ~500 products from Amazon Best Sellers
2. Clean and transform the data
3. Load it into your MySQL database

### Customizing the Pipeline

You can modify the pipeline behavior by adjusting parameters:

```python
# Scrape more/fewer pages per category
run_etl_pipeline(num_pages=10)  # Default is 5

# Use custom database configuration
custom_db = {
    'host': 'remote-server.com',
    'database': 'my_db',
    'user': 'my_user',
    'password': 'my_password'
}
run_etl_pipeline(num_pages=5, db_config=custom_db)
```

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

## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'requests'`
- **Solution**: Install dependencies with `pip install -r requirements.txt`

**Issue**: `mysql.connector.errors.ProgrammingError: Database 'ecommerce_etl' doesn't exist`
- **Solution**: Create the database with `CREATE DATABASE ecommerce_etl;`

**Issue**: `Access denied for user 'root'@'localhost'`
- **Solution**: Update the password in DB_CONFIG to match your MySQL password

**Issue**: Few products extracted (< 100)
- **Solution**: Amazon's page structure may have changed. Update the CSS selectors in `_extract_products_from_page()`

**Issue**: `requests.exceptions.HTTPError: 503`
- **Solution**: Increase the delay between requests (e.g., `delay=5`)

## Best Practices

1. **Respectful Scraping**: The pipeline includes delays between requests to avoid overwhelming servers
2. **Error Recovery**: Failures on individual products don't stop the entire pipeline
3. **Transaction Safety**: Database operations use transactions with rollback on error
4. **Incremental Updates**: ON DUPLICATE KEY UPDATE allows re-running without duplicates

## Future Enhancements

- Add support for multiple e-commerce sites
- Implement incremental updates (only fetch new/changed products)
- Add data quality metrics and validation reports
- Create data visualization dashboard
- Add support for product images
- Implement parallel scraping for faster extraction
- Add email notifications on pipeline completion/failure

## License

This project is for educational purposes. Be sure to comply with the target website's Terms of Service and robots.txt when scraping data.

## Contact

For questions or issues, please check the logging output for detailed error messages.
