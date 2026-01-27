-- ================================================================
-- E-Commerce Product ETL Database Schema
-- ================================================================
-- This schema implements a normalized database structure for storing
-- product information scraped from e-commerce websites.
-- ================================================================

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS product_metrics;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS categories;

-- ================================================================
-- Categories Table
-- ================================================================
-- Stores unique product categories/segments
-- Normalized to avoid redundancy in products table
-- ================================================================
CREATE TABLE categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_category_name (category_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ================================================================
-- Companies Table
-- ================================================================
-- Stores unique company/brand information
-- Normalized to avoid redundancy in products table
-- company_industry can be expanded based on data availability
-- ================================================================
CREATE TABLE companies (
    company_id INT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL UNIQUE,
    company_industry VARCHAR(255) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ================================================================
-- Products Table
-- ================================================================
-- Main products table storing core product information
-- Foreign keys reference categories and companies tables
-- product_id is the scraped unique identifier from the source
-- ================================================================
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(500) NOT NULL,
    category_id INT,
    company_id INT,
    description TEXT,
    price DECIMAL(10, 2) DEFAULT NULL,
    url VARCHAR(1000) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Foreign key constraints for referential integrity
    FOREIGN KEY (category_id) REFERENCES categories(category_id) 
        ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (company_id) REFERENCES companies(company_id) 
        ON DELETE SET NULL ON UPDATE CASCADE,
    
    -- Indexes for query optimization
    INDEX idx_product_id (product_id),
    INDEX idx_name (name(255)),
    INDEX idx_category (category_id),
    INDEX idx_company (company_id),
    INDEX idx_price (price)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ================================================================
-- Product Metrics Table
-- ================================================================
-- Stores time-series metrics for products (reviews, ratings, etc.)
-- Separate table allows tracking metrics changes over time
-- snapshot_date enables historical analysis
-- ================================================================
CREATE TABLE product_metrics (
    metric_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(100) NOT NULL,
    reviews_count INT DEFAULT 0,
    avg_rating DECIMAL(3, 2) DEFAULT NULL,
    is_featured BOOLEAN DEFAULT FALSE,
    snapshot_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key to products table
    FOREIGN KEY (product_id) REFERENCES products(product_id) 
        ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Indexes for query optimization
    INDEX idx_product_metrics (product_id),
    INDEX idx_snapshot_date (snapshot_date),
    INDEX idx_rating (avg_rating),
    
    -- Unique constraint to prevent duplicate snapshots
    UNIQUE KEY unique_product_snapshot (product_id, snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ================================================================
-- Sample Queries for Data Analysis
-- ================================================================

-- Query 1: Get top-rated products with company information
-- SELECT 
--     p.product_id,
--     p.name,
--     c.company_name,
--     cat.category_name,
--     pm.avg_rating,
--     pm.reviews_count
-- FROM products p
-- JOIN companies c ON p.company_id = c.company_id
-- JOIN categories cat ON p.category_id = cat.category_id
-- JOIN product_metrics pm ON p.product_id = pm.product_id
-- WHERE pm.avg_rating >= 4.0
-- ORDER BY pm.avg_rating DESC, pm.reviews_count DESC
-- LIMIT 20;

-- Query 2: Get products by category with metrics
-- SELECT 
--     cat.category_name,
--     COUNT(p.id) as product_count,
--     AVG(pm.avg_rating) as avg_category_rating,
--     SUM(pm.reviews_count) as total_reviews
-- FROM categories cat
-- JOIN products p ON cat.category_id = p.category_id
-- JOIN product_metrics pm ON p.product_id = pm.product_id
-- GROUP BY cat.category_id, cat.category_name
-- ORDER BY product_count DESC;

-- Query 3: Get company performance metrics
-- SELECT 
--     c.company_name,
--     COUNT(p.id) as product_count,
--     AVG(p.price) as avg_price,
--     AVG(pm.avg_rating) as avg_rating
-- FROM companies c
-- JOIN products p ON c.company_id = p.company_id
-- JOIN product_metrics pm ON p.product_id = pm.product_id
-- GROUP BY c.company_id, c.company_name
-- HAVING product_count >= 5
-- ORDER BY avg_rating DESC;
