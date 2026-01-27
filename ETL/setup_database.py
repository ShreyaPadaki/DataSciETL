#!/usr/bin/env python3
"""
Database Setup Script
=====================
Automates the creation of the MySQL database and tables for the ETL pipeline.

Usage:
    python setup_database.py

This script will:
1. Connect to MySQL server
2. Create the database (if it doesn't exist)
3. Create all required tables
4. Verify the setup
"""

import mysql.connector
from mysql.connector import Error
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_sql_file(filename: str) -> str:
    """Read SQL commands from file."""
    try:
        with open(filename, 'r') as file:
            return file.read()
    except FileNotFoundError:
        logger.error(f"SQL file not found: {filename}")
        sys.exit(1)


def setup_database(host='localhost', user='root', password='', database='ecommerce_etl'):
    """
    Set up the MySQL database and tables.
    
    Args:
        host: MySQL server host
        user: MySQL username
        password: MySQL password
        database: Database name to create
    """
    connection = None
    cursor = None
    
    try:
        # Connect to MySQL server (without specifying database)
        logger.info(f"Connecting to MySQL server at {host}...")
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            logger.info("Connected to MySQL server successfully")
            
            # Create database if it doesn't exist
            logger.info(f"Creating database '{database}'...")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database} "
                         f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            logger.info(f"Database '{database}' created or already exists")
            
            # Switch to the database
            cursor.execute(f"USE {database}")
            logger.info(f"Switched to database '{database}'")
            
            # Read and execute schema SQL
            logger.info("Creating tables from schema.sql...")
            schema_sql = read_sql_file('schema.sql')
            
            # Split SQL commands (simple split by semicolon)
            commands = [cmd.strip() for cmd in schema_sql.split(';') if cmd.strip()]
            
            # Execute each command
            for command in commands:
                # Skip comments and empty commands
                if command.startswith('--') or not command:
                    continue
                
                try:
                    cursor.execute(command)
                    logger.debug(f"Executed: {command[:50]}...")
                except Error as e:
                    # Log error but continue (for DROP IF EXISTS, etc.)
                    logger.debug(f"Command error (may be expected): {e}")
            
            connection.commit()
            logger.info("All tables created successfully")
            
            # Verify tables were created
            logger.info("Verifying table creation...")
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            expected_tables = {'categories', 'companies', 'products', 'product_metrics'}
            created_tables = {table[0] for table in tables}
            
            if expected_tables.issubset(created_tables):
                logger.info("✓ All required tables created successfully:")
                for table in expected_tables:
                    logger.info(f"  - {table}")
            else:
                missing = expected_tables - created_tables
                logger.error(f"✗ Missing tables: {missing}")
                sys.exit(1)
            
            # Display table information
            logger.info("\nDatabase schema summary:")
            for table in expected_tables:
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                logger.info(f"\n{table} ({len(columns)} columns):")
                for col in columns:
                    logger.info(f"  {col[0]:20} {col[1]:20} {col[2]:10}")
            
            logger.info("\n" + "=" * 60)
            logger.info("DATABASE SETUP COMPLETED SUCCESSFULLY!")
            logger.info("=" * 60)
            logger.info(f"\nYou can now run the ETL pipeline:")
            logger.info(f"  python etl_pipeline.py")
            logger.info(f"\nOr connect to the database:")
            logger.info(f"  mysql -u {user} -p {database}")
            
    except Error as e:
        logger.error(f"Database setup failed: {e}")
        sys.exit(1)
    
    finally:
        # Close connections
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            logger.debug("Database connection closed")


def main():
    """Main entry point."""
    import getpass
    
    print("=" * 60)
    print("ETL Pipeline - Database Setup")
    print("=" * 60)
    
    # Get database credentials
    print("\nPlease provide MySQL credentials:")
    host = input("MySQL Host [localhost]: ").strip() or 'localhost'
    user = input("MySQL User [root]: ").strip() or 'root'
    password = getpass.getpass("MySQL Password: ")
    database = input("Database Name [ecommerce_etl]: ").strip() or 'ecommerce_etl'
    
    print()
    
    # Run setup
    setup_database(
        host=host,
        user=user,
        password=password,
        database=database
    )


if __name__ == "__main__":
    main()
