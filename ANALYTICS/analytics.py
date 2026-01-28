"""
E-Commerce ETL Analytics Dashboard
====================================
Comprehensive analytics and visualization for the ETL pipeline data.

This script generates:
1. Basic profiling statistics
2. Category distribution analysis
3. Price and rating insights
4. Price-rating correlation analysis
5. Company-level insights
6. Comprehensive analytics report

Author: ETL Analytics
Date: 2026-01-27
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
import logging
from typing import Dict, Tuple, Optional
import json

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Set style for visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


class EcommerceAnalytics:
    """Analytics class for e-commerce ETL data."""
    
    def __init__(self, db_config: Dict):
        """
        Initialize analytics with database configuration.
        
        Args:
            db_config: Dictionary with database connection parameters
        """
        self.db_config = db_config
        self.connection = None
        self.analytics_results = {}
        
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                logger.info("Connected to database successfully")
                return True
        except Error as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    # ================================================================
    # BASIC PROFILING
    # ================================================================
    
    def basic_profiling(self) -> Dict:
        """
        Generate basic profiling statistics.
        
        Returns:
            Dictionary with profiling metrics
        """
        logger.info("Running basic profiling analysis...")
        
        profiling = {}
        
        # Total products
        query_products = "SELECT COUNT(*) as total FROM products"
        df = pd.read_sql(query_products, self.connection)
        profiling['total_products'] = int(df['total'].iloc[0])
        
        # Distinct companies
        query_companies = "SELECT COUNT(DISTINCT company_id) as total FROM products WHERE company_id IS NOT NULL"
        df = pd.read_sql(query_companies, self.connection)
        profiling['distinct_companies'] = int(df['total'].iloc[0])
        
        # Distinct categories
        query_categories = "SELECT COUNT(DISTINCT category_id) as total FROM products WHERE category_id IS NOT NULL"
        df = pd.read_sql(query_categories, self.connection)
        profiling['distinct_categories'] = int(df['total'].iloc[0])
        
        # Products with prices
        query_priced = "SELECT COUNT(*) as total FROM products WHERE price IS NOT NULL AND price > 0"
        df = pd.read_sql(query_priced, self.connection)
        profiling['products_with_price'] = int(df['total'].iloc[0])
        
        # Products with ratings
        query_rated = "SELECT COUNT(*) as total FROM product_metrics WHERE avg_rating IS NOT NULL"
        df = pd.read_sql(query_rated, self.connection)
        profiling['products_with_rating'] = int(df['total'].iloc[0])
        
        # Average reviews per product
        query_avg_reviews = "SELECT AVG(reviews_count) as avg_reviews FROM product_metrics"
        df = pd.read_sql(query_avg_reviews, self.connection)
        profiling['avg_reviews_per_product'] = float(df['avg_reviews'].iloc[0]) if df['avg_reviews'].iloc[0] else 0.0
        
        logger.info(f"Profiling complete: {profiling['total_products']} products analyzed")
        self.analytics_results['profiling'] = profiling
        
        return profiling
    
    # ================================================================
    # CATEGORY DISTRIBUTION
    # ================================================================
    
    def category_distribution(self) -> pd.DataFrame:
        """
        Analyze product distribution by category.
        
        Returns:
            DataFrame with category distribution
        """
        logger.info("Analyzing category distribution...")
        
        query = """
            SELECT 
                c.category_name,
                COUNT(p.id) as product_count,
                ROUND(COUNT(p.id) * 100.0 / (SELECT COUNT(*) FROM products), 2) as percentage
            FROM categories c
            LEFT JOIN products p ON c.category_id = p.category_id
            GROUP BY c.category_id, c.category_name
            ORDER BY product_count DESC
            LIMIT 10
        """
        
        df = pd.read_sql(query, self.connection)
        
        logger.info(f"Top category: {df.iloc[0]['category_name']} with {df.iloc[0]['product_count']} products")
        self.analytics_results['category_distribution'] = df
        
        return df
    
    def visualize_category_distribution(self, df: pd.DataFrame, save_path: str = 'category_distribution.png'):
        """
        Create visualization for category distribution.
        
        Args:
            df: DataFrame with category distribution data
            save_path: Path to save the plot
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
        
        # Bar chart
        colors = sns.color_palette("viridis", len(df))
        bars = ax1.barh(df['category_name'], df['product_count'], color=colors)
        ax1.set_xlabel('Number of Products', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Category', fontsize=12, fontweight='bold')
        ax1.set_title('Top 10 Categories by Product Count', fontsize=14, fontweight='bold')
        ax1.invert_yaxis()
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax1.text(width, bar.get_y() + bar.get_height()/2, 
                    f'{int(width):,}', 
                    ha='left', va='center', fontweight='bold', fontsize=10)
        
        # Pie chart
        ax2.pie(df['product_count'], labels=df['category_name'], autopct='%1.1f%%',
                startangle=90, colors=colors)
        ax2.set_title('Product Distribution by Category', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Category distribution plot saved to {save_path}")
        plt.close()
    
    # ================================================================
    # PRICE AND RATING INSIGHTS
    # ================================================================
    
    def price_rating_insights(self) -> pd.DataFrame:
        """
        Analyze price and rating statistics by category.
        
        Returns:
            DataFrame with price and rating insights
        """
        logger.info("Analyzing price and rating insights...")
        
        query = """
            SELECT 
                c.category_name,
                COUNT(p.id) as product_count,
                ROUND(AVG(p.price), 2) as avg_price,
                ROUND(STDDEV(p.price), 2) as std_price,
                MIN(p.price) as min_price,
                MAX(p.price) as max_price,
                ROUND(AVG(pm.avg_rating), 2) as avg_rating,
                ROUND(AVG(pm.reviews_count), 0) as avg_reviews
            FROM categories c
            JOIN products p ON c.category_id = p.category_id
            LEFT JOIN product_metrics pm ON p.product_id = pm.product_id
            WHERE p.price IS NOT NULL AND p.price > 0
            GROUP BY c.category_id, c.category_name
            HAVING product_count >= 5
            ORDER BY product_count DESC
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Calculate median (since SQL doesn't have easy median)
        median_query = """
            SELECT 
                c.category_name,
                p.price
            FROM categories c
            JOIN products p ON c.category_id = p.category_id
            WHERE p.price IS NOT NULL AND p.price > 0
            ORDER BY c.category_name, p.price
        """
        
        price_df = pd.read_sql(median_query, self.connection)
        medians = price_df.groupby('category_name')['price'].median().reset_index()
        medians.columns = ['category_name', 'median_price']
        
        df = df.merge(medians, on='category_name', how='left')
        df['median_price'] = df['median_price'].round(2)
        
        logger.info(f"Price/rating analysis complete for {len(df)} categories")
        self.analytics_results['price_rating_insights'] = df
        
        return df
    
    def visualize_price_insights(self, df: pd.DataFrame, save_path: str = 'price_insights.png'):
        """
        Create visualizations for price insights.
        
        Args:
            df: DataFrame with price insights
            save_path: Path to save the plot
        """
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Top categories for plotting
        top_df = df.head(10)
        
        # 1. Average Price by Category
        ax1 = axes[0, 0]
        bars = ax1.barh(top_df['category_name'], top_df['avg_price'], 
                       color=sns.color_palette("coolwarm", len(top_df)))
        ax1.set_xlabel('Average Price ($)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Category', fontsize=12, fontweight='bold')
        ax1.set_title('Average Price by Category', fontsize=14, fontweight='bold')
        ax1.invert_yaxis()
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax1.text(width, bar.get_y() + bar.get_height()/2, 
                    f'${width:.2f}', 
                    ha='left', va='center', fontweight='bold')
        
        # 2. Price Range (Min/Max) by Category
        ax2 = axes[0, 1]
        x = np.arange(len(top_df))
        width = 0.35
        ax2.bar(x - width/2, top_df['min_price'], width, label='Min Price', alpha=0.8)
        ax2.bar(x + width/2, top_df['max_price'], width, label='Max Price', alpha=0.8)
        ax2.set_xlabel('Category', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Price ($)', fontsize=12, fontweight='bold')
        ax2.set_title('Price Range by Category', fontsize=14, fontweight='bold')
        ax2.set_xticks(x)
        ax2.set_xticklabels(top_df['category_name'], rotation=45, ha='right')
        ax2.legend()
        ax2.grid(axis='y', alpha=0.3)
        
        # 3. Average vs Median Price
        ax3 = axes[1, 0]
        ax3.scatter(top_df['avg_price'], top_df['median_price'], 
                   s=top_df['product_count']*10, alpha=0.6, 
                   c=range(len(top_df)), cmap='viridis')
        
        # Add diagonal line (avg = median)
        max_val = max(top_df['avg_price'].max(), top_df['median_price'].max())
        ax3.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Avg = Median')
        
        ax3.set_xlabel('Average Price ($)', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Median Price ($)', fontsize=12, fontweight='bold')
        ax3.set_title('Average vs Median Price Comparison', fontsize=14, fontweight='bold')
        ax3.legend()
        ax3.grid(alpha=0.3)
        
        # 4. Price Distribution Box Plot
        ax4 = axes[1, 1]
        
        # Get detailed price data for box plot
        price_detail_query = """
            SELECT c.category_name, p.price
            FROM categories c
            JOIN products p ON c.category_id = p.category_id
            WHERE p.price IS NOT NULL AND p.price > 0
        """
        price_detail_df = pd.read_sql(price_detail_query, self.connection)
        
        # Filter to top categories
        top_categories = top_df['category_name'].tolist()[:8]  # Top 8 for readability
        filtered_df = price_detail_df[price_detail_df['category_name'].isin(top_categories)]
        
        if not filtered_df.empty:
            filtered_df.boxplot(column='price', by='category_name', ax=ax4, rot=45)
            ax4.set_xlabel('Category', fontsize=12, fontweight='bold')
            ax4.set_ylabel('Price ($)', fontsize=12, fontweight='bold')
            ax4.set_title('Price Distribution by Category (Box Plot)', fontsize=14, fontweight='bold')
            plt.sca(ax4)
            plt.xticks(rotation=45, ha='right')
            ax4.get_figure().suptitle('')  # Remove automatic title
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Price insights plot saved to {save_path}")
        plt.close()
    
    def visualize_rating_insights(self, df: pd.DataFrame, save_path: str = 'rating_insights.png'):
        """
        Create visualizations for rating insights.
        
        Args:
            df: DataFrame with rating insights
            save_path: Path to save the plot
        """
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        
        top_df = df.head(10)
        
        # 1. Average Rating by Category
        ax1 = axes[0]
        colors = sns.color_palette("RdYlGn", len(top_df))
        bars = ax1.barh(top_df['category_name'], top_df['avg_rating'], color=colors)
        ax1.set_xlabel('Average Rating (out of 5)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Category', fontsize=12, fontweight='bold')
        ax1.set_title('Average Product Rating by Category', fontsize=14, fontweight='bold')
        ax1.set_xlim(0, 5)
        ax1.invert_yaxis()
        ax1.axvline(x=4.0, color='green', linestyle='--', alpha=0.5, label='4.0 threshold')
        ax1.legend()
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if pd.notna(width):
                ax1.text(width, bar.get_y() + bar.get_height()/2, 
                        f'{width:.2f}', 
                        ha='left', va='center', fontweight='bold')
        
        # 2. Average Reviews by Category
        ax2 = axes[1]
        bars = ax2.barh(top_df['category_name'], top_df['avg_reviews'], 
                       color=sns.color_palette("plasma", len(top_df)))
        ax2.set_xlabel('Average Number of Reviews', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Category', fontsize=12, fontweight='bold')
        ax2.set_title('Average Review Count by Category', fontsize=14, fontweight='bold')
        ax2.invert_yaxis()
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if pd.notna(width) and width > 0:
                ax2.text(width, bar.get_y() + bar.get_height()/2, 
                        f'{int(width):,}', 
                        ha='left', va='center', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Rating insights plot saved to {save_path}")
        plt.close()
    
    # ================================================================
    # PRICE-RATING CORRELATION
    # ================================================================
    
    def price_rating_correlation(self) -> Tuple[pd.DataFrame, Dict]:
        """
        Analyze correlation between price and rating by category.
        
        Returns:
            Tuple of (DataFrame with correlation data, summary statistics)
        """
        logger.info("Analyzing price-rating correlation...")
        
        query = """
            SELECT 
                c.category_name,
                p.price,
                pm.avg_rating,
                pm.reviews_count
            FROM categories c
            JOIN products p ON c.category_id = p.category_id
            JOIN product_metrics pm ON p.product_id = pm.product_id
            WHERE p.price IS NOT NULL 
                AND p.price > 0 
                AND pm.avg_rating IS NOT NULL
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Overall correlation
        overall_corr = df['price'].corr(df['avg_rating'])
        
        # Correlation by category
        category_corr = df.groupby('category_name').apply(
            lambda x: x['price'].corr(x['avg_rating']) if len(x) > 2 else np.nan
        ).reset_index()
        category_corr.columns = ['category_name', 'correlation']
        category_corr = category_corr.dropna()
        category_corr = category_corr.sort_values('correlation', ascending=False)
        
        # Price segments analysis
        df['price_segment'] = pd.cut(df['price'], 
                                     bins=[0, 20, 50, 100, 200, float('inf')],
                                     labels=['$0-20', '$20-50', '$50-100', '$100-200', '$200+'])
        
        segment_stats = df.groupby('price_segment').agg({
            'avg_rating': ['mean', 'count'],
            'price': 'mean',
            'reviews_count': 'mean'
        }).round(2)
        
        summary = {
            'overall_correlation': round(overall_corr, 3),
            'positive_correlation_categories': len(category_corr[category_corr['correlation'] > 0]),
            'negative_correlation_categories': len(category_corr[category_corr['correlation'] < 0]),
            'strongest_positive': category_corr.iloc[0].to_dict() if len(category_corr) > 0 else None,
            'strongest_negative': category_corr.iloc[-1].to_dict() if len(category_corr) > 0 else None
        }
        
        logger.info(f"Overall price-rating correlation: {overall_corr:.3f}")
        self.analytics_results['correlation'] = {
            'category_correlation': category_corr,
            'segment_stats': segment_stats,
            'summary': summary,
            'raw_data': df
        }
        
        return df, summary
    
    def visualize_price_rating_correlation(self, df: pd.DataFrame, save_path: str = 'price_rating_correlation.png'):
        """
        Create visualizations for price-rating correlation.
        
        Args:
            df: DataFrame with price and rating data
            save_path: Path to save the plot
        """
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Scatter plot: Price vs Rating
        ax1 = axes[0, 0]
        scatter = ax1.scatter(df['price'], df['avg_rating'], 
                            alpha=0.5, s=50, c=df['reviews_count'], 
                            cmap='viridis', edgecolors='black', linewidth=0.5)
        
        # Add trend line
        z = np.polyfit(df['price'], df['avg_rating'], 1)
        p = np.poly1d(z)
        ax1.plot(df['price'].sort_values(), p(df['price'].sort_values()), 
                "r--", alpha=0.8, linewidth=2, label=f'Trend line')
        
        ax1.set_xlabel('Price ($)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax1.set_title('Price vs Rating Correlation', fontsize=14, fontweight='bold')
        ax1.set_ylim(0, 5.5)
        ax1.legend()
        ax1.grid(alpha=0.3)
        
        cbar = plt.colorbar(scatter, ax=ax1)
        cbar.set_label('Review Count', fontsize=10)
        
        # 2. Price segment analysis
        ax2 = axes[0, 1]
        segment_stats = df.groupby('price_segment')['avg_rating'].mean().reset_index()
        bars = ax2.bar(range(len(segment_stats)), segment_stats['avg_rating'], 
                      color=sns.color_palette("coolwarm", len(segment_stats)))
        ax2.set_xlabel('Price Segment', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Average Rating', fontsize=12, fontweight='bold')
        ax2.set_title('Average Rating by Price Segment', fontsize=14, fontweight='bold')
        ax2.set_xticks(range(len(segment_stats)))
        ax2.set_xticklabels(segment_stats['price_segment'], rotation=45)
        ax2.set_ylim(0, 5)
        ax2.axhline(y=4.0, color='green', linestyle='--', alpha=0.5)
        ax2.grid(axis='y', alpha=0.3)
        
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}',
                    ha='center', va='bottom', fontweight='bold')
        
        # 3. Correlation by category
        ax3 = axes[1, 0]
        category_corr = df.groupby('category_name').apply(
            lambda x: x['price'].corr(x['avg_rating']) if len(x) > 2 else np.nan
        ).reset_index()
        category_corr.columns = ['category_name', 'correlation']
        category_corr = category_corr.dropna().sort_values('correlation', ascending=True).tail(10)
        
        colors = ['red' if x < 0 else 'green' for x in category_corr['correlation']]
        bars = ax3.barh(category_corr['category_name'], category_corr['correlation'], color=colors, alpha=0.7)
        ax3.set_xlabel('Correlation Coefficient', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Category', fontsize=12, fontweight='bold')
        ax3.set_title('Price-Rating Correlation by Category', fontsize=14, fontweight='bold')
        ax3.axvline(x=0, color='black', linestyle='-', linewidth=1)
        ax3.grid(axis='x', alpha=0.3)
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax3.text(width, bar.get_y() + bar.get_height()/2, 
                    f'{width:.3f}', 
                    ha='left' if width > 0 else 'right', 
                    va='center', fontweight='bold')
        
        # 4. Reviews count by price segment
        ax4 = axes[1, 1]
        segment_reviews = df.groupby('price_segment')['reviews_count'].mean().reset_index()
        bars = ax4.bar(range(len(segment_reviews)), segment_reviews['reviews_count'], 
                      color=sns.color_palette("mako", len(segment_reviews)))
        ax4.set_xlabel('Price Segment', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Average Review Count', fontsize=12, fontweight='bold')
        ax4.set_title('Review Activity by Price Segment', fontsize=14, fontweight='bold')
        ax4.set_xticks(range(len(segment_reviews)))
        ax4.set_xticklabels(segment_reviews['price_segment'], rotation=45)
        ax4.grid(axis='y', alpha=0.3)
        
        for i, bar in enumerate(bars):
            height = bar.get_height()
            if pd.notna(height):
                ax4.text(bar.get_x() + bar.get_width()/2., height,
                        f'{int(height):,}',
                        ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Price-rating correlation plot saved to {save_path}")
        plt.close()
    
    # ================================================================
    # COMPANY INSIGHTS
    # ================================================================
    
    def company_insights(self) -> pd.DataFrame:
        """
        Analyze top companies by product count and performance.
        
        Returns:
            DataFrame with company insights
        """
        logger.info("Analyzing company-level insights...")
        
        query = """
            SELECT 
                c.company_name,
                COUNT(p.id) as product_count,
                ROUND(AVG(pm.avg_rating), 2) as avg_rating,
                SUM(pm.reviews_count) as total_reviews,
                ROUND(AVG(pm.reviews_count), 0) as avg_reviews_per_product,
                ROUND(AVG(p.price), 2) as avg_price,
                MIN(p.price) as min_price,
                MAX(p.price) as max_price,
                COUNT(CASE WHEN pm.avg_rating >= 4.5 THEN 1 END) as highly_rated_products,
                COUNT(CASE WHEN pm.is_featured = 1 THEN 1 END) as featured_products
            FROM companies c
            JOIN products p ON c.company_id = p.company_id
            LEFT JOIN product_metrics pm ON p.product_id = pm.product_id
            WHERE p.price IS NOT NULL
            GROUP BY c.company_id, c.company_name
            ORDER BY product_count DESC
            LIMIT 20
        """
        
        df = pd.read_sql(query, self.connection)
        
        # Calculate additional metrics
        df['highly_rated_percentage'] = ((df['highly_rated_products'] / df['product_count']) * 100).round(1)
        
        logger.info(f"Company analysis complete: Top company is {df.iloc[0]['company_name']} with {df.iloc[0]['product_count']} products")
        self.analytics_results['company_insights'] = df
        
        return df
    
    def visualize_company_insights(self, df: pd.DataFrame, save_path: str = 'company_insights.png'):
        """
        Create visualizations for company insights.
        
        Args:
            df: DataFrame with company insights
            save_path: Path to save the plot
        """
        fig, axes = plt.subplots(2, 2, figsize=(18, 12))
        
        top_df = df.head(10)
        
        # 1. Top 10 Companies by Product Count
        ax1 = axes[0, 0]
        colors = sns.color_palette("viridis", len(top_df))
        bars = ax1.barh(top_df['company_name'], top_df['product_count'], color=colors)
        ax1.set_xlabel('Number of Products', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Company', fontsize=12, fontweight='bold')
        ax1.set_title('Top 10 Companies by Product Count', fontsize=14, fontweight='bold')
        ax1.invert_yaxis()
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax1.text(width, bar.get_y() + bar.get_height()/2, 
                    f'{int(width):,}', 
                    ha='left', va='center', fontweight='bold')
        
        # 2. Average Rating of Top Companies
        ax2 = axes[0, 1]
        colors = ['green' if x >= 4.0 else 'orange' if x >= 3.5 else 'red' 
                 for x in top_df['avg_rating']]
        bars = ax2.barh(top_df['company_name'], top_df['avg_rating'], color=colors, alpha=0.7)
        ax2.set_xlabel('Average Rating (out of 5)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Company', fontsize=12, fontweight='bold')
        ax2.set_title('Average Product Rating by Company', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 5)
        ax2.axvline(x=4.0, color='darkgreen', linestyle='--', alpha=0.5, label='4.0 threshold')
        ax2.invert_yaxis()
        ax2.legend()
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if pd.notna(width):
                ax2.text(width, bar.get_y() + bar.get_height()/2, 
                        f'{width:.2f}', 
                        ha='left', va='center', fontweight='bold')
        
        # 3. Total Reviews by Company
        ax3 = axes[1, 0]
        bars = ax3.barh(top_df['company_name'], top_df['total_reviews'], 
                       color=sns.color_palette("rocket", len(top_df)))
        ax3.set_xlabel('Total Review Count', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Company', fontsize=12, fontweight='bold')
        ax3.set_title('Total Reviews Across Products by Company', fontsize=14, fontweight='bold')
        ax3.invert_yaxis()
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            if pd.notna(width) and width > 0:
                ax3.text(width, bar.get_y() + bar.get_height()/2, 
                        f'{int(width):,}', 
                        ha='left', va='center', fontweight='bold')
        
        # 4. Company Performance Matrix (Rating vs Review Count)
        ax4 = axes[1, 1]
        scatter = ax4.scatter(top_df['avg_rating'], top_df['total_reviews'], 
                            s=top_df['product_count']*30, 
                            alpha=0.6, 
                            c=range(len(top_df)), 
                            cmap='tab10',
                            edgecolors='black',
                            linewidth=1.5)
        
        # Add company labels
        for idx, row in top_df.iterrows():
            company_short = row['company_name'][:15] + '...' if len(row['company_name']) > 15 else row['company_name']
            ax4.annotate(company_short, 
                        (row['avg_rating'], row['total_reviews']),
                        fontsize=8, alpha=0.7, 
                        xytext=(5, 5), textcoords='offset points')
        
        ax4.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Total Reviews', fontsize=12, fontweight='bold')
        ax4.set_title('Company Performance Matrix\n(Bubble size = Product Count)', 
                     fontsize=14, fontweight='bold')
        ax4.set_xlim(0, 5.5)
        ax4.axvline(x=4.0, color='green', linestyle='--', alpha=0.3)
        ax4.grid(alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Company insights plot saved to {save_path}")
        plt.close()
    
    # ================================================================
    # COMPREHENSIVE REPORT GENERATION
    # ================================================================
    
    def generate_analytics_report(self, output_file: str = 'analytics_report.txt') -> str:
        """
        Generate comprehensive analytics report.
        
        Args:
            output_file: Path to save the report
            
        Returns:
            Report content as string
        """
        logger.info("Generating comprehensive analytics report...")
        
        report_lines = []
        report_lines.append("=" * 100)
        report_lines.append(" " * 30 + "E-COMMERCE ETL ANALYTICS REPORT")
        report_lines.append(" " * 35 + f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 100)
        report_lines.append("")
        
        # ================================================================
        # 1. BASIC PROFILING
        # ================================================================
        
        if 'profiling' in self.analytics_results:
            prof = self.analytics_results['profiling']
            report_lines.append("=" * 100)
            report_lines.append("1. BASIC PROFILING")
            report_lines.append("=" * 100)
            report_lines.append("")
            report_lines.append(f"   Total Products:              {prof['total_products']:>10,}")
            report_lines.append(f"   Distinct Companies:          {prof['distinct_companies']:>10,}")
            report_lines.append(f"   Distinct Categories:         {prof['distinct_categories']:>10,}")
            report_lines.append(f"   Products with Price:         {prof['products_with_price']:>10,} ({prof['products_with_price']/prof['total_products']*100:.1f}%)")
            report_lines.append(f"   Products with Rating:        {prof['products_with_rating']:>10,} ({prof['products_with_rating']/prof['total_products']*100:.1f}%)")
            report_lines.append(f"   Avg Reviews per Product:     {prof['avg_reviews_per_product']:>10,.1f}")
            report_lines.append("")
        
        # ================================================================
        # 2. CATEGORY DISTRIBUTION
        # ================================================================
        
        if 'category_distribution' in self.analytics_results:
            cat_df = self.analytics_results['category_distribution']
            report_lines.append("=" * 100)
            report_lines.append("2. PRODUCT DISTRIBUTION BY CATEGORY (Top 10)")
            report_lines.append("=" * 100)
            report_lines.append("")
            report_lines.append(f"   {'Category':<35} {'Product Count':>15} {'Percentage':>15}")
            report_lines.append("   " + "-" * 65)
            
            for idx, row in cat_df.iterrows():
                report_lines.append(f"   {row['category_name']:<35} {int(row['product_count']):>15,} {row['percentage']:>14.2f}%")
            report_lines.append("")
        
        # ================================================================
        # 3. PRICE AND RATING INSIGHTS
        # ================================================================
        
        if 'price_rating_insights' in self.analytics_results:
            price_df = self.analytics_results['price_rating_insights']
            report_lines.append("=" * 100)
            report_lines.append("3. PRICE AND RATING INSIGHTS BY CATEGORY")
            report_lines.append("=" * 100)
            report_lines.append("")
            report_lines.append(f"   {'Category':<20} {'Count':>8} {'Avg $':>10} {'Med $':>10} {'Min $':>10} {'Max $':>10} {'Avg Rating':>12}")
            report_lines.append("   " + "-" * 90)
            
            for idx, row in price_df.head(10).iterrows():
                report_lines.append(
                    f"   {row['category_name']:<20} "
                    f"{int(row['product_count']):>8,} "
                    f"${row['avg_price']:>9.2f} "
                    f"${row['median_price']:>9.2f} "
                    f"${row['min_price']:>9.2f} "
                    f"${row['max_price']:>9.2f} "
                    f"{row['avg_rating'] if pd.notna(row['avg_rating']) else 'N/A':>12}"
                )
            report_lines.append("")
            
            # Price summary statistics
            report_lines.append("   PRICE SUMMARY STATISTICS:")
            report_lines.append("   " + "-" * 40)
            report_lines.append(f"   Overall Average Price:       ${price_df['avg_price'].mean():>10.2f}")
            report_lines.append(f"   Overall Median Price:        ${price_df['median_price'].median():>10.2f}")
            report_lines.append(f"   Highest Category Avg:        ${price_df['avg_price'].max():>10.2f} ({price_df.loc[price_df['avg_price'].idxmax(), 'category_name']})")
            report_lines.append(f"   Lowest Category Avg:         ${price_df['avg_price'].min():>10.2f} ({price_df.loc[price_df['avg_price'].idxmin(), 'category_name']})")
            report_lines.append("")
            
            # Rating summary statistics
            report_lines.append("   RATING SUMMARY STATISTICS:")
            report_lines.append("   " + "-" * 40)
            valid_ratings = price_df[price_df['avg_rating'].notna()]
            if not valid_ratings.empty:
                report_lines.append(f"   Overall Average Rating:      {valid_ratings['avg_rating'].mean():>10.2f}")
                report_lines.append(f"   Highest Rated Category:      {valid_ratings['avg_rating'].max():>10.2f} ({valid_ratings.loc[valid_ratings['avg_rating'].idxmax(), 'category_name']})")
                report_lines.append(f"   Lowest Rated Category:       {valid_ratings['avg_rating'].min():>10.2f} ({valid_ratings.loc[valid_ratings['avg_rating'].idxmin(), 'category_name']})")
            report_lines.append("")
        
        # ================================================================
        # 4. PRICE-RATING CORRELATION
        # ================================================================
        
        if 'correlation' in self.analytics_results:
            corr_data = self.analytics_results['correlation']
            summary = corr_data['summary']
            
            report_lines.append("=" * 100)
            report_lines.append("4. PRICE-RATING CORRELATION ANALYSIS")
            report_lines.append("=" * 100)
            report_lines.append("")
            report_lines.append("   CORRELATION INSIGHTS:")
            report_lines.append("   " + "-" * 60)
            report_lines.append(f"   Overall Correlation Coefficient:           {summary['overall_correlation']:>8.3f}")
            report_lines.append("")
            
            if summary['overall_correlation'] > 0.3:
                interpretation = "Strong positive correlation"
            elif summary['overall_correlation'] > 0.1:
                interpretation = "Moderate positive correlation"
            elif summary['overall_correlation'] > -0.1:
                interpretation = "Weak/no correlation"
            elif summary['overall_correlation'] > -0.3:
                interpretation = "Moderate negative correlation"
            else:
                interpretation = "Strong negative correlation"
            
            report_lines.append(f"   Interpretation: {interpretation}")
            report_lines.append("")
            
            if summary['overall_correlation'] > 0:
                report_lines.append("   ✓ Higher-priced products tend to have HIGHER ratings")
            else:
                report_lines.append("   ✗ Higher-priced products tend to have LOWER ratings")
            report_lines.append("")
            
            report_lines.append(f"   Categories with Positive Correlation:      {summary['positive_correlation_categories']:>8}")
            report_lines.append(f"   Categories with Negative Correlation:      {summary['negative_correlation_categories']:>8}")
            report_lines.append("")
            
            if summary['strongest_positive']:
                report_lines.append("   Strongest Positive Correlation:")
                report_lines.append(f"      Category: {summary['strongest_positive']['category_name']}")
                report_lines.append(f"      Correlation: {summary['strongest_positive']['correlation']:.3f}")
            
            if summary['strongest_negative']:
                report_lines.append("")
                report_lines.append("   Strongest Negative Correlation:")
                report_lines.append(f"      Category: {summary['strongest_negative']['category_name']}")
                report_lines.append(f"      Correlation: {summary['strongest_negative']['correlation']:.3f}")
            report_lines.append("")
            
            # Price segment analysis
            if 'segment_stats' in corr_data:
                report_lines.append("   PRICE SEGMENT ANALYSIS:")
                report_lines.append("   " + "-" * 60)
                segment_df = corr_data['segment_stats']
                report_lines.append(f"   {'Segment':<15} {'Avg Rating':>12} {'Product Count':>15} {'Avg Price':>12}")
                report_lines.append("   " + "-" * 60)
                
                for segment in segment_df.index:
                    avg_rating = segment_df.loc[segment, ('avg_rating', 'mean')]
                    count = segment_df.loc[segment, ('avg_rating', 'count')]
                    avg_price = segment_df.loc[segment, ('price', 'mean')]
                    report_lines.append(f"   {segment:<15} {avg_rating:>12.2f} {int(count):>15,} ${avg_price:>11.2f}")
            report_lines.append("")
        
        # ================================================================
        # 5. COMPANY INSIGHTS
        # ================================================================
        
        if 'company_insights' in self.analytics_results:
            comp_df = self.analytics_results['company_insights']
            report_lines.append("=" * 100)
            report_lines.append("5. COMPANY-LEVEL INSIGHTS (Top 10 by Product Count)")
            report_lines.append("=" * 100)
            report_lines.append("")
            report_lines.append(f"   {'Company':<25} {'Products':>10} {'Avg Rating':>12} {'Total Reviews':>15} {'Avg Reviews/Product':>20}")
            report_lines.append("   " + "-" * 82)
            
            for idx, row in comp_df.head(10).iterrows():
                report_lines.append(
                    f"   {row['company_name'][:24]:<25} "
                    f"{int(row['product_count']):>10,} "
                    f"{row['avg_rating'] if pd.notna(row['avg_rating']) else 'N/A':>12} "
                    f"{int(row['total_reviews']) if pd.notna(row['total_reviews']) else 0:>15,} "
                    f"{int(row['avg_reviews_per_product']) if pd.notna(row['avg_reviews_per_product']) else 0:>20,}"
                )
            report_lines.append("")
            
            # Company performance metrics
            report_lines.append("   COMPANY PERFORMANCE METRICS:")
            report_lines.append("   " + "-" * 60)
            
            top_company = comp_df.iloc[0]
            report_lines.append(f"   Most Prolific Company:       {top_company['company_name']} ({int(top_company['product_count'])} products)")
            
            if 'avg_rating' in comp_df.columns and comp_df['avg_rating'].notna().any():
                highest_rated = comp_df.loc[comp_df['avg_rating'].idxmax()]
                report_lines.append(f"   Highest Rated Company:       {highest_rated['company_name']} ({highest_rated['avg_rating']:.2f})")
            
            if 'total_reviews' in comp_df.columns and comp_df['total_reviews'].notna().any():
                most_reviewed = comp_df.loc[comp_df['total_reviews'].idxmax()]
                report_lines.append(f"   Most Reviewed Company:       {most_reviewed['company_name']} ({int(most_reviewed['total_reviews']):,} reviews)")
            
            report_lines.append("")
        
        # ================================================================
        # 6. KEY FINDINGS & RECOMMENDATIONS
        # ================================================================
        
        report_lines.append("=" * 100)
        report_lines.append("6. KEY FINDINGS & RECOMMENDATIONS")
        report_lines.append("=" * 100)
        report_lines.append("")
        
        findings = []
        
        # Finding 1: Price-Rating correlation
        if 'correlation' in self.analytics_results:
            corr_val = self.analytics_results['correlation']['summary']['overall_correlation']
            if corr_val > 0.1:
                findings.append(
                    f"   • Price-Quality Signal: Positive correlation ({corr_val:.3f}) suggests customers "
                    "perceive higher-priced\n     products as better quality. Premium pricing strategy may be justified."
                )
            elif corr_val < -0.1:
                findings.append(
                    f"   • Value Perception: Negative correlation ({corr_val:.3f}) indicates customers find "
                    "better value in\n     lower-priced products. Consider value-based pricing strategies."
                )
            else:
                findings.append(
                    f"   • Price Independence: Weak correlation ({corr_val:.3f}) shows price and quality are "
                    "largely independent.\n     Focus on product differentiation beyond price."
                )
        
        # Finding 2: Category concentration
        if 'category_distribution' in self.analytics_results:
            cat_df = self.analytics_results['category_distribution']
            top_cat_pct = cat_df.iloc[0]['percentage']
            if top_cat_pct > 30:
                findings.append(
                    f"   • Category Concentration: {cat_df.iloc[0]['category_name']} dominates with "
                    f"{top_cat_pct:.1f}% of products.\n     Consider diversification or capitalize on this strength."
                )
        
        # Finding 3: Company market presence
        if 'company_insights' in self.analytics_results:
            comp_df = self.analytics_results['company_insights']
            top_companies_products = comp_df.head(10)['product_count'].sum()
            total_products = self.analytics_results['profiling']['total_products']
            concentration = (top_companies_products / total_products) * 100
            
            findings.append(
                f"   • Market Concentration: Top 10 companies account for {concentration:.1f}% of all products.\n"
                f"     {'Market is highly concentrated.' if concentration > 50 else 'Market shows healthy competition.'}"
            )
        
        # Finding 4: Review engagement
        if 'profiling' in self.analytics_results:
            avg_reviews = self.analytics_results['profiling']['avg_reviews_per_product']
            if avg_reviews > 100:
                findings.append(
                    f"   • High Engagement: Average of {avg_reviews:.0f} reviews per product indicates strong "
                    "customer engagement.\n     Leverage reviews for marketing and product improvement."
                )
            elif avg_reviews < 10:
                findings.append(
                    f"   • Low Engagement: Average of {avg_reviews:.0f} reviews per product suggests opportunities "
                    "to increase\n     customer feedback collection."
                )
        
        for finding in findings:
            report_lines.append(finding)
            report_lines.append("")
        
        # ================================================================
        # FOOTER
        # ================================================================
        
        report_lines.append("=" * 100)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 100)
        
        # Join all lines
        report_content = "\n".join(report_lines)
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Analytics report saved to {output_file}")
        
        return report_content
    
    # ================================================================
    # MASTER ANALYSIS FUNCTION
    # ================================================================
    
    def run_complete_analysis(self):
        """Run all analytics and generate visualizations and report."""
        logger.info("=" * 60)
        logger.info("STARTING COMPREHENSIVE ANALYTICS")
        logger.info("=" * 60)
        
        if not self.connect():
            logger.error("Failed to connect to database. Analysis aborted.")
            return False
        
        try:
            # 1. Basic Profiling
            profiling = self.basic_profiling()
            
            # 2. Category Distribution
            cat_df = self.category_distribution()
            self.visualize_category_distribution(cat_df)
            
            # 3. Price and Rating Insights
            price_rating_df = self.price_rating_insights()
            self.visualize_price_insights(price_rating_df)
            self.visualize_rating_insights(price_rating_df)
            
            # 4. Price-Rating Correlation
            corr_df, corr_summary = self.price_rating_correlation()
            self.visualize_price_rating_correlation(corr_df)
            
            # 5. Company Insights
            company_df = self.company_insights()
            self.visualize_company_insights(company_df)
            
            # 6. Generate Report
            report = self.generate_analytics_report()
            
            # Print report to console
            print("\n" + report)
            
            logger.info("=" * 60)
            logger.info("ANALYTICS COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info("\nGenerated files:")
            logger.info("  - category_distribution.png")
            logger.info("  - price_insights.png")
            logger.info("  - rating_insights.png")
            logger.info("  - price_rating_correlation.png")
            logger.info("  - company_insights.png")
            logger.info("  - analytics_report.txt")
            
            return True
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            self.disconnect()


# ================================================================
# MAIN ENTRY POINT
# ================================================================

def main():
    """Main entry point for analytics."""
    
    # Database configuration
    DB_CONFIG = {
        'host': 'localhost',
        'database': 'ecommerce_etl',
        'user': 'root',
        'password': 'Your_Password_Here',  # UPDATE THIS
    }
    
    # Create analytics instance
    analytics = EcommerceAnalytics(DB_CONFIG)
    
    # Run complete analysis
    success = analytics.run_complete_analysis()
    
    if success:
        print("\n✓ Analytics completed successfully!")
        print("  Check the generated PNG files and analytics_report.txt for insights.")
    else:
        print("\n✗ Analytics failed. Check the logs for details.")


if __name__ == "__main__":
    main()
