"""
Integration Tools for Redshift connectivity through GCP Integration Connectors.
"""

from google.adk.tools.application_integration_tool.application_integration_toolset import ApplicationIntegrationToolset
import json
import os


def create_redshift_tool(project_id: str, location: str = "us-central1", 
                        connection: str = "redshift-demo-connection",
                        service_account_json_path: str = "../config/service_account.json"):
    """
    Creates ApplicationIntegrationToolset for Redshift database operations.
    
    Args:
        project_id: GCP project ID
        location: GCP region (default: us-central1)
        connection: Integration connector name
        service_account_json_path: Path to service account JSON file
        
    Returns:
        ApplicationIntegrationToolset configured for Redshift
    """
    
    # Load service account credentials
    if os.path.exists(service_account_json_path):
        with open(service_account_json_path, 'r') as f:
            service_account_json = json.load(f)
    else:
        # Use environment-based authentication if no service account file
        service_account_json = None
    
    # Create the toolset for Redshift operations
    redshift_tool = ApplicationIntegrationToolset(
        project=project_id,
        location=location,
        connection=connection,
        entity_operations={
            "products": ["LIST", "GET"],
            "sales": ["LIST", "GET"], 
            "customers": ["LIST", "GET"]
        },
        actions=["ExecuteCustomQuery"],
        service_account_json=service_account_json,
        tool_name_prefix="redshift",
        tool_instructions="""
        Execute SQL queries on AWS Redshift database through GCP Integration Connector.
        
        Available operations:
        - LIST: Retrieve multiple records from a table
        - GET: Retrieve specific records with filters
        - ExecuteCustomQuery: Execute custom SQL queries
        
        Database Schema:
        - products: product_id, product_name, category, subcategory, brand, price
        - sales: sale_id, product_id, customer_id, quantity, sale_date, sale_amount, region
        - customers: customer_id, customer_name, email, registration_date, region
        
        Use ExecuteCustomQuery for complex analytical queries that require JOINs or aggregations.
        """
    )
    
    return redshift_tool


def create_schema_tool():
    """
    Creates a simple tool for database schema information.
    This helps the agent understand the database structure.
    """
    
    schema_info = {
        "database": "demo_db",
        "schema": "public",
        "tables": {
            "products": {
                "columns": [
                    {"name": "product_id", "type": "INT", "primary_key": True, "description": "Unique product identifier"},
                    {"name": "product_name", "type": "VARCHAR(100)", "description": "Name of the product"},
                    {"name": "category", "type": "VARCHAR(50)", "description": "Product category (Apparel, Electronics, Home)"},
                    {"name": "subcategory", "type": "VARCHAR(50)", "description": "Product subcategory (Shirts, Pants, Shoes, etc.)"},
                    {"name": "brand", "type": "VARCHAR(50)", "description": "Product brand name"},
                    {"name": "price", "type": "DECIMAL(10,2)", "description": "Product price in USD"}
                ],
                "description": "Product catalog with approximately 1000 products"
            },
            "sales": {
                "columns": [
                    {"name": "sale_id", "type": "INT", "primary_key": True, "description": "Unique sale transaction identifier"},
                    {"name": "product_id", "type": "INT", "foreign_key": "products.product_id", "description": "Reference to product"},
                    {"name": "customer_id", "type": "INT", "foreign_key": "customers.customer_id", "description": "Reference to customer"},
                    {"name": "quantity", "type": "INT", "description": "Number of items sold"},
                    {"name": "sale_date", "type": "DATE", "description": "Date of the sale"},
                    {"name": "sale_amount", "type": "DECIMAL(10,2)", "description": "Total sale amount in USD"},
                    {"name": "region", "type": "VARCHAR(50)", "description": "Sales region"}
                ],
                "description": "Sales transactions with approximately 5000 records"
            },
            "customers": {
                "columns": [
                    {"name": "customer_id", "type": "INT", "primary_key": True, "description": "Unique customer identifier"},
                    {"name": "customer_name", "type": "VARCHAR(100)", "description": "Customer full name"},
                    {"name": "email", "type": "VARCHAR(100)", "description": "Customer email address"},
                    {"name": "registration_date", "type": "DATE", "description": "Date customer registered"},
                    {"name": "region", "type": "VARCHAR(50)", "description": "Customer's region"}
                ],
                "description": "Customer information with approximately 500 customers"
            }
        }
    }
    
    return schema_info


def get_table_relationships():
    """
    Returns information about table relationships for JOIN operations.
    """
    
    relationships = {
        "sales_products": {
            "type": "INNER JOIN",
            "condition": "sales.product_id = products.product_id",
            "description": "Links sales to product details"
        },
        "sales_customers": {
            "type": "INNER JOIN", 
            "condition": "sales.customer_id = customers.customer_id",
            "description": "Links sales to customer information"
        }
    }
    
    return relationships


def get_sample_queries():
    """
    Returns sample queries for common business questions.
    These can be used for few-shot prompting.
    """
    
    samples = {
        "apparel_quarterly_sales": {
            "question": "How many apparels were sold in the last quarter?",
            "sql": """
            SELECT COUNT(*) as total_apparel_sold
            FROM sales s
            JOIN products p ON s.product_id = p.product_id
            WHERE p.category = 'Apparel'
            AND s.sale_date >= CURRENT_DATE - INTERVAL '3 months'
            """,
            "explanation": "Counts apparel items sold in the last 3 months by joining sales and products tables"
        },
        "top_apparel_brands": {
            "question": "What are the top 5 selling apparel brands?", 
            "sql": """
            SELECT p.brand, SUM(s.quantity) as total_sold
            FROM sales s
            JOIN products p ON s.product_id = p.product_id  
            WHERE p.category = 'Apparel'
            GROUP BY p.brand
            ORDER BY total_sold DESC
            LIMIT 5
            """,
            "explanation": "Aggregates sales by brand for apparel category, ordered by quantity sold"
        },
        "regional_electronics_sales": {
            "question": "Show sales by region for electronics",
            "sql": """
            SELECT s.region, SUM(s.sale_amount) as total_sales
            FROM sales s
            JOIN products p ON s.product_id = p.product_id
            WHERE p.category = 'Electronics'
            GROUP BY s.region
            ORDER BY total_sales DESC
            """,
            "explanation": "Summarizes electronics sales revenue by region"
        }
    }
    
    return samples