#!/usr/bin/env python3
"""
ETL Pipeline Validation Script
===============================
Validates the data loaded by the ETL pipeline and generates a summary report.

Usage:
    python validate_etl.py

This script will:
1. Connect to the database
2. Run validation queries
3. Check data quality
4. Generate a summary report
"""

import mysql.connector
from mysql.connector import Error
import sys
import logging
from datetime import datetime
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLValidator:
    """Validator class for ETL pipeline results."""
    
    def __init__(self, db_config: Dict):
        """Initialize validator with database configuration."""
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self.validation_results = {}
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                logger.info("Connected to database successfully")
                return True
        except Error as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def validate_table_counts(self) -> Dict:
        """Validate that tables have data."""
        logger.info("Validating table record counts...")
        
        tables = ['categories', 'companies', 'products', 'product_metrics']
        counts = {}
        
        for table in tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = self.cursor.fetchone()
                count = result['count']
                counts[table] = count
                
                status = "✓" if count > 0 else "✗"
                logger.info(f"{status} {table}: {count:,} records")
                
            except Error as e:
                logger.error(f"Error counting {table}: {e}")
                counts[table] = 0
        
        self.validation_results['table_counts'] = counts
        return counts
    
    def validate_data_quality(self) -> Dict:
        """Check for data quality issues."""
        logger.info("\nValidating data quality...")
        
        quality_checks = {}
        
        # Check 1: Products without prices
        try:
            self.cursor.execute("""
                SELECT COUNT(*) as count 
                FROM products 
                WHERE price IS NULL OR price = 0
            """)
            no_price = self.cursor.fetchone()['count']
            quality_checks['products_without_price'] = no_price
            logger.info(f"Products without price: {no_price:,}")
        except Error as e:
            logger.error(f"Error checking prices: {e}")
        
        # Check 2: Products without ratings
        try:
            self.cursor.execute("""
                SELECT COUNT(*) as count 
                FROM product_metrics 
                WHERE avg_rating IS NULL
            """)
            no_rating = self.cursor.fetchone()['count']
            quality_checks['products_without_rating'] = no_rating
            logger.info(f"Products without rating: {no_rating:,}")
        except Error as e:
            logger.error(f"Error checking ratings: {e}")
        
        # Check 3: Duplicate product IDs
        try:
            self.cursor.execute("""
                SELECT product_id, COUNT(*) as count 
                FROM products 
                GROUP BY product_id 
                HAVING count > 1
            """)
            duplicates = self.cursor.fetchall()
            quality_checks['duplicate_products'] = len(duplicates)
            if duplicates:
                logger.warning(f"Found {len(duplicates)} duplicate product IDs!")
            else:
                logger.info(f"✓ No duplicate product IDs")
        except Error as e:
            logger.error(f"Error checking duplicates: {e}")
        
        # Check 4: Orphaned metrics (metrics without products)
        try:
            self.cursor.execute("""
                SELECT COUNT(*) as count 
                FROM product_metrics pm
                LEFT JOIN products p ON pm.product_id = p.product_id
                WHERE p.id IS NULL
            """)
            orphaned = self.cursor.fetchone()['count']
            quality_checks['orphaned_metrics'] = orphaned
            if orphaned > 0:
                logger.warning(f"Found {orphaned} orphaned metrics!")
            else:
                logger.info(f"✓ No orphaned metrics")
        except Error as e:
            logger.error(f"Error checking orphaned metrics: {e}")
        
        self.validation_results['quality_checks'] = quality_checks
        return quality_checks
    
    def generate_statistics(self) -> Dict:
        """Generate summary statistics."""
        logger.info("\nGenerating statistics...")
        
        stats = {}
        
        # Average price per category
        try:
            self.cursor.execute("""
                SELECT 
                    c.category_name,
                    COUNT(p.id) as product_count,
                    AVG(p.price) as avg_price,
                    MIN(p.price) as min_price,
                    MAX(p.price) as max_price
                FROM categories c
                JOIN products p ON c.category_id = p.category_id
                WHERE p.price IS NOT NULL AND p.price > 0
                GROUP BY c.category_id, c.category_name
                ORDER BY product_count DESC
            """)
            category_stats = self.cursor.fetchall()
            stats['category_statistics'] = category_stats
            
            logger.info("\nCategory Statistics:")
            logger.info(f"{'Category':<20} {'Products':<10} {'Avg Price':<12} {'Min Price':<12} {'Max Price':<12}")
            logger.info("-" * 66)
            for row in category_stats:
                logger.info(
                    f"{row['category_name']:<20} "
                    f"{row['product_count']:<10} "
                    f"${row['avg_price']:>10.2f} "
                    f"${row['min_price']:>10.2f} "
                    f"${row['max_price']:>10.2f}"
                )
        except Error as e:
            logger.error(f"Error generating category stats: {e}")
        
        # Top rated products
        try:
            self.cursor.execute("""
                SELECT 
                    p.name,
                    c.company_name,
                    cat.category_name,
                    p.price,
                    pm.avg_rating,
                    pm.reviews_count
                FROM products p
                JOIN companies c ON p.company_id = c.company_id
                JOIN categories cat ON p.category_id = cat.category_id
                JOIN product_metrics pm ON p.product_id = pm.product_id
                WHERE pm.avg_rating IS NOT NULL
                ORDER BY pm.avg_rating DESC, pm.reviews_count DESC
                LIMIT 10
            """)
            top_products = self.cursor.fetchall()
            stats['top_rated_products'] = top_products
            
            logger.info("\nTop 10 Rated Products:")
            logger.info(f"{'Product':<40} {'Company':<20} {'Rating':<8} {'Reviews':<10}")
            logger.info("-" * 78)
            for row in top_products:
                product_name = row['name'][:37] + '...' if len(row['name']) > 40 else row['name']
                company_name = row['company_name'][:17] + '...' if len(row['company_name']) > 20 else row['company_name']
                logger.info(
                    f"{product_name:<40} "
                    f"{company_name:<20} "
                    f"{row['avg_rating']:<8.2f} "
                    f"{row['reviews_count']:<10}"
                )
        except Error as e:
            logger.error(f"Error fetching top products: {e}")
        
        # Company statistics
        try:
            self.cursor.execute("""
                SELECT 
                    c.company_name,
                    COUNT(p.id) as product_count,
                    AVG(pm.avg_rating) as avg_rating,
                    SUM(pm.reviews_count) as total_reviews
                FROM companies c
                JOIN products p ON c.company_id = p.company_id
                JOIN product_metrics pm ON p.product_id = pm.product_id
                GROUP BY c.company_id, c.company_name
                HAVING product_count >= 3
                ORDER BY avg_rating DESC
                LIMIT 10
            """)
            company_stats = self.cursor.fetchall()
            stats['top_companies'] = company_stats
            
            logger.info("\nTop 10 Companies (with 3+ products):")
            logger.info(f"{'Company':<30} {'Products':<10} {'Avg Rating':<12} {'Total Reviews':<15}")
            logger.info("-" * 67)
            for row in company_stats:
                company_name = row['company_name'][:27] + '...' if len(row['company_name']) > 30 else row['company_name']
                avg_rating = row['avg_rating'] if row['avg_rating'] else 0.0
                logger.info(
                    f"{company_name:<30} "
                    f"{row['product_count']:<10} "
                    f"{avg_rating:<12.2f} "
                    f"{row['total_reviews']:<15}"
                )
        except Error as e:
            logger.error(f"Error generating company stats: {e}")
        
        self.validation_results['statistics'] = stats
        return stats
    
    def generate_report(self) -> str:
        """Generate a comprehensive validation report."""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("ETL PIPELINE VALIDATION REPORT")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 80)
        
        # Table counts
        if 'table_counts' in self.validation_results:
            counts = self.validation_results['table_counts']
            report_lines.append("\nTABLE RECORD COUNTS:")
            report_lines.append("-" * 80)
            for table, count in counts.items():
                status = "PASS" if count > 0 else "FAIL"
                report_lines.append(f"  {table:<25} {count:>10,} records    [{status}]")
        
        # Data quality
        if 'quality_checks' in self.validation_results:
            checks = self.validation_results['quality_checks']
            report_lines.append("\nDATA QUALITY CHECKS:")
            report_lines.append("-" * 80)
            
            if 'products_without_price' in checks:
                count = checks['products_without_price']
                status = "WARNING" if count > 0 else "PASS"
                report_lines.append(f"  Products without price:   {count:>10,}            [{status}]")
            
            if 'products_without_rating' in checks:
                count = checks['products_without_rating']
                status = "WARNING" if count > 0 else "PASS"
                report_lines.append(f"  Products without rating:  {count:>10,}            [{status}]")
            
            if 'duplicate_products' in checks:
                count = checks['duplicate_products']
                status = "FAIL" if count > 0 else "PASS"
                report_lines.append(f"  Duplicate product IDs:    {count:>10,}            [{status}]")
            
            if 'orphaned_metrics' in checks:
                count = checks['orphaned_metrics']
                status = "FAIL" if count > 0 else "PASS"
                report_lines.append(f"  Orphaned metrics:         {count:>10,}            [{status}]")
        
        # Overall status
        report_lines.append("\n" + "=" * 80)
        
        # Determine overall status
        overall_status = "PASS"
        if 'quality_checks' in self.validation_results:
            checks = self.validation_results['quality_checks']
            if checks.get('duplicate_products', 0) > 0 or checks.get('orphaned_metrics', 0) > 0:
                overall_status = "FAIL"
            elif checks.get('products_without_price', 0) > 0 or checks.get('products_without_rating', 0) > 0:
                overall_status = "WARNING"
        
        if 'table_counts' in self.validation_results:
            counts = self.validation_results['table_counts']
            if any(count == 0 for count in counts.values()):
                overall_status = "FAIL"
        
        report_lines.append(f"OVERALL STATUS: {overall_status}")
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def run_validation(self):
        """Run all validation checks."""
        if not self.connect():
            logger.error("Could not connect to database. Validation aborted.")
            return False
        
        try:
            # Run all validations
            self.validate_table_counts()
            self.validate_data_quality()
            self.generate_statistics()
            
            # Generate and display report
            report = self.generate_report()
            print("\n" + report)
            
            # Save report to file
            report_filename = f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_filename, 'w') as f:
                f.write(report)
            logger.info(f"\nValidation report saved to: {report_filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False
        
        finally:
            self.disconnect()


def main():
    """Main entry point."""
    import getpass
    
    print("=" * 80)
    print("ETL Pipeline Validation")
    print("=" * 80)
    
    # Get database credentials
    print("\nPlease provide MySQL credentials:")
    host = input("MySQL Host [localhost]: ").strip() or 'localhost'
    user = input("MySQL User [root]: ").strip() or 'root'
    password = getpass.getpass("MySQL Password: ")
    database = input("Database Name [ecommerce_etl]: ").strip() or 'ecommerce_etl'
    
    db_config = {
        'host': host,
        'database': database,
        'user': user,
        'password': password
    }
    
    print()
    
    # Run validation
    validator = ETLValidator(db_config)
    success = validator.run_validation()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
