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
    Updated to follow ADK best practices and client's actual schema.

    Args:
        project_id: GCP project ID
        location: GCP region (default: us-central1)
        connection: Integration connector name
        service_account_json_path: Path to service account JSON file

    Returns:
        ApplicationIntegrationToolset configured for Redshift

    Raises:
        ValueError: If required configuration is missing
        FileNotFoundError: If service account file is required but not found
    """

    # Validate required parameters
    if not project_id:
        raise ValueError("project_id is required")
    if not connection:
        raise ValueError("connection name is required")

    # Load service account credentials with better error handling
    service_account_json = None
    if os.path.exists(service_account_json_path):
        try:
            with open(service_account_json_path, 'r') as f:
                service_account_json = json.load(f)
            print(f"✓ Service account loaded from {service_account_json_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in service account file: {e}")
        except Exception as e:
            raise FileNotFoundError(f"Could not load service account file: {e}")
    else:
        # Try to get credentials from environment
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            print("✓ Using GOOGLE_APPLICATION_CREDENTIALS from environment")
        else:
            print("⚠ No service account file or GOOGLE_APPLICATION_CREDENTIALS found")
            print("⚠ Relying on default GCP authentication")

    # Update entity operations to match client's actual schema
    entity_operations = {
        # Primary reporting tables
        "ordernumber": ["LIST", "GET"],
        "shipmentnumber": ["LIST", "GET"],
        # Supporting tables for joins
        "orders": ["LIST", "GET"],
        "product": ["LIST", "GET"],
        "shipment": ["LIST", "GET"],
        "categorynames": ["LIST", "GET"],
        "shipping_pickuptime": ["LIST", "GET"]
    }

    # Enhanced tool instructions with client's business rules
    tool_instructions = f"""
    Execute SQL queries on AWS Redshift database through GCP Integration Connector.
    Specialized for Revolve's e-commerce data analysis.

    <Available Operations>
    - LIST: Retrieve multiple records from a table
    - GET: Retrieve specific records with filters
    - ExecuteCustomQuery: Execute custom SQL queries (preferred for complex analysis)

    <Database Schema - Revolve E-commerce>
    Primary Tables:
    - bi_report.ordernumber_rs: Order-level data (transactions, sales amounts, customers)
    - bi_report.shipmentnumber_rs: Shipment-level data (includes product information)

    Supporting Tables:
    - mars__revolveclothing_com___db.orders: Raw order data (payment tokens)
    - mars__revolveclothing_com___db.product: Product information (brands, codes)
    - mars__revolveclothing_com___db.shipment: Detailed shipment info (carriers, status)
    - mars__id.id_categorynames2: Category mapping
    - mars__id.shipping_pickuptime: Accurate shipping carrier info

    <Critical Business Rules>
    1. REVOLVE ORDERS: Use site <> 'F' to exclude Forward brand orders
    2. PRODUCT ANALYSIS: Use shipmentnumber_rs (has product info), NOT ordernumber_rs
    3. PRODUCT JOINS: Use UPPER(TRIM(p.code)) = productcode for product matching
    4. CATEGORY MAPPING: Use SUBSTRING(SPLIT_PART(p.code, '-', 2), 2, 1) = cn.lettercat
    5. LOST PACKAGES: Use extrastatus = 'lost package' (NOT 'lost')
    6. PAYMENT TOKENS: paymenttokenservice is in mars__revolveclothing_com___db.orders
    7. TOP PERCENTILES: Use PERCENT_RANK() for percentage-based calculations
    8. SHIPPING CARRIERS: Use mars__id.shipping_pickuptime for accurate info
    9. RANDOM SAMPLING: Use RANDOM() function with ORDER BY
    10. DATE FILTERING: Use proper TIMESTAMP comparisons with >= and < operators

    <Usage Recommendations>
    - Use ExecuteCustomQuery for all analytical queries requiring JOINs or aggregations
    - Always use fully qualified table names (schema.table_name)
    - Follow Redshift SQL syntax and data types
    - Include proper error handling for query execution
    """

    try:
        # Create the toolset for Redshift operations
        redshift_tool = ApplicationIntegrationToolset(
            project=project_id,
            location=location,
            connection=connection,
            entity_operations=entity_operations,
            actions=["ExecuteCustomQuery"],
            service_account_json=service_account_json,
            tool_name_prefix="redshift",
            tool_instructions=tool_instructions
        )

        print(f"✓ ApplicationIntegrationToolset created successfully")
        print(f"  Project: {project_id}")
        print(f"  Location: {location}")
        print(f"  Connection: {connection}")

        return redshift_tool

    except Exception as e:
        raise RuntimeError(f"Failed to create ApplicationIntegrationToolset: {e}")


def create_schema_tool():
    """
    Creates schema tool for client's actual Redshift tables.
    Based on bi_report.ordernumber_rs and bi_report.shipmentnumber_rs.
    """

    schema_info = {
        "database": "revolve_redshift",
        "schema": "bi_report",
        "tables": {
            "bi_report.ordernumber_rs": {
                "columns": [
                    {"name": "useremail", "type": "VARCHAR(256)", "description": "Customer email address"},
                    {"name": "transactionid", "type": "VARCHAR(256)", "description": "Unique transaction identifier"},
                    {"name": "site", "type": "VARCHAR(5)", "description": "Site identifier (R=Revolve, F=Forward)"},
                    {"name": "shipcountry", "type": "VARCHAR(256)", "description": "Shipping country"},
                    {"name": "oorderdate", "type": "TIMESTAMP", "description": "Order date"},
                    {"name": "ssales", "type": "DOUBLE PRECISION", "description": "Sales amount"},
                    {"name": "netsales", "type": "DOUBLE PRECISION", "description": "Net sales amount"},
                    {"name": "invoicenum", "type": "VARCHAR(256)", "description": "Invoice number"},
                    {"name": "paymenttype", "type": "VARCHAR(255)", "description": "Payment method (ANET, etc.)"}
                ],
                "description": "Order-level data from Revolve/Forward transactions",
                "business_rules": [
                    "Revolve orders: site <> 'F' (excludes Forward)",
                    "Does NOT contain product-level information",
                    "Contains payment information but not paymenttokenservice"
                ]
            },
            "bi_report.shipmentnumber_rs": {
                "columns": [
                    {"name": "useremail", "type": "VARCHAR(256)", "description": "Customer email address"},
                    {"name": "transactionid", "type": "VARCHAR(256)", "description": "Transaction identifier"},
                    {"name": "shipmentid", "type": "VARCHAR(256)", "description": "Unique shipment identifier"},
                    {"name": "site", "type": "VARCHAR(256)", "description": "Site identifier"},
                    {"name": "oorderdate", "type": "TIMESTAMP", "description": "Original order date"},
                    {"name": "shipmentdate", "type": "TIMESTAMP", "description": "Shipment date"},
                    {"name": "amount", "type": "DOUBLE PRECISION", "description": "Shipment amount"},
                    {"name": "ssales", "type": "REAL", "description": "Shipment sales"},
                    {"name": "netsales", "type": "REAL", "description": "Net sales from shipment"},
                    {"name": "productcode", "type": "VARCHAR(256)", "description": "Product code"},
                    {"name": "projnetsales_shipped", "type": "REAL", "description": "Projected net sales for shipped items"},
                    {"name": "extrastatus", "type": "VARCHAR(256)", "description": "Extra status including 'lost package'"},
                    {"name": "shippingcountry", "type": "VARCHAR(256)", "description": "Shipping country"}
                ],
                "description": "Shipment-level data with product information",
                "business_rules": [
                    "Contains product codes for joining with product tables",
                    "Has projected sales calculations",
                    "extrastatus contains 'lost package' not 'lost'",
                    "Links to mars__revolveclothing_com___db.orders for additional data"
                ]
            },
            "mars__revolveclothing_com___db.orders": {
                "columns": [
                    {"name": "transactionid", "type": "VARCHAR", "description": "Transaction identifier for joining"},
                    {"name": "paymenttokenservice", "type": "VARCHAR", "description": "Payment token service (ApplePay, etc.)"},
                    {"name": "amount", "type": "DECIMAL", "description": "Order amount"}
                ],
                "description": "Raw orders table with payment token information"
            },
            "mars__revolveclothing_com___db.product": {
                "columns": [
                    {"name": "code", "type": "VARCHAR", "description": "Product code (matches shipmentnumber_rs.productcode)"},
                    {"name": "brandname", "type": "VARCHAR", "description": "Brand name"}
                ],
                "description": "Product information table"
            },
            "mars__revolveclothing_com___db.shipment": {
                "columns": [
                    {"name": "shipmentid", "type": "VARCHAR", "description": "Shipment identifier"},
                    {"name": "extrastatus", "type": "VARCHAR", "description": "Extra status with 'lost package'"},
                    {"name": "shippingoption", "type": "VARCHAR", "description": "Shipping carrier"},
                    {"name": "sigrequired", "type": "BOOLEAN", "description": "Signature required flag"}
                ],
                "description": "Detailed shipment information"
            },
            "mars__id.id_categorynames2": {
                "columns": [
                    {"name": "lettercat", "type": "VARCHAR(1)", "description": "Category letter code"},
                    {"name": "catname2", "type": "VARCHAR", "description": "Category name"}
                ],
                "description": "Category mapping table"
            },
            "mars__id.shipping_pickuptime": {
                "columns": [
                    {"name": "shippingoption", "type": "VARCHAR", "description": "More accurate shipping carrier info"}
                ],
                "description": "Shipping carrier information"
            }
        }
    }

    return schema_info


def get_table_relationships():
    """
    Returns information about table relationships for JOIN operations.
    Based on client's actual Redshift schema.
    """

    relationships = {
        "shipment_to_product": {
            "type": "INNER JOIN",
            "condition": "sn.productcode = UPPER(TRIM(p.code))",
            "description": "Links shipments to product details (productcode matching)"
        },
        "shipment_to_orders": {
            "type": "INNER JOIN",
            "condition": "sn.transactionid = o.transactionid",
            "description": "Links shipments to order payment information"
        },
        "shipment_to_detailed_shipment": {
            "type": "INNER JOIN",
            "condition": "sn.shipmentid = s.shipmentid",
            "description": "Links shipment summary to detailed shipment info"
        },
        "product_to_category": {
            "type": "INNER JOIN",
            "condition": "SUBSTRING(SPLIT_PART(p.code, '-', 2), 2, 1) = cn.lettercat",
            "description": "Maps product codes to category names via letter codes"
        },
        "ordernumber_to_orders": {
            "type": "INNER JOIN",
            "condition": "on.transactionid = o.transactionid",
            "description": "Links order summaries to raw order data for payment info"
        }
    }

    return relationships


def get_sample_queries():
    """
    Returns sample queries based on client's actual business questions.
    These correct the issues found in the test document.
    """

    samples = {
        "revolve_orders_aov_by_category": {
            "question": "Show me the number of Revolve orders and AOV for United Kingdom between date ranges, split by Category",
            "sql": """
            SELECT
              CASE
                WHEN t1.oorderdate >= '2022-08-25' AND t1.oorderdate < '2023-08-25' THEN '8/25/22 - 8/24/23'
                WHEN t1.oorderdate >= '2021-08-25' AND t1.oorderdate < '2022-08-25' THEN '8/25/21 - 8/24/22'
              END AS period,
              t4.catname2 AS category,
              COUNT(DISTINCT t1.transactionid) AS nOrders,
              AVG(t1.ssales) AS AOV
            FROM
              bi_report.shipmentnumber_rs t1
              INNER JOIN mars__revolveclothing_com___db.product t2 ON t1.productcode = UPPER(TRIM(t2.code))
              INNER JOIN mars__id.id_categorynames2 t4 ON SUBSTRING(SPLIT_PART(t2.code, '-', 2), 2, 1) = t4.lettercat
            WHERE
              t1.oorderdate >= '2021-08-25'
              AND t1.oorderdate < '2023-08-25'
              AND t1.site <> 'F'
              AND t1.shippingcountry = 'United Kingdom'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            "explanation": "Correct approach: Use shipmentnumber_rs for product-level data, site <> 'F' for Revolve orders"
        },
        "high_value_customers_percentile": {
            "question": "Get top brands and categories based on projected net sales for top 5% high value customers",
            "sql": """
            WITH high_value_customers AS (
              SELECT
                useremail,
                SUM(netsales) as total_netsales,
                PERCENT_RANK() OVER (ORDER BY SUM(netsales) DESC) as percentile_rank
              FROM
                bi_report.ordernumber_rs
              GROUP BY
                useremail
              HAVING PERCENT_RANK() OVER (ORDER BY SUM(netsales) DESC) <= 0.05
            )
            SELECT
              p.brandname,
              cn.catname2,
              SUM(sn.projnetsales_shipped) AS total_projnetsales
            FROM
              bi_report.shipmentnumber_rs sn
              JOIN high_value_customers hvc ON sn.useremail = hvc.useremail
              JOIN mars__revolveclothing_com___db.product p ON sn.productcode = UPPER(TRIM(p.code))
              JOIN mars__id.id_categorynames2 cn ON SUBSTRING(SPLIT_PART(p.code, '-', 2), 2, 1) = cn.lettercat
            GROUP BY p.brandname, cn.catname2
            ORDER BY SUM(sn.projnetsales_shipped) DESC
            LIMIT 10
            """,
            "explanation": "Correct approach: Use PERCENT_RANK for top 5% calculation, proper product code mapping"
        },
        "anet_transactions_exclude_applepay": {
            "question": "Show number of transactions and average monthly gross sales through ANET excluding ApplePay",
            "sql": """
            SELECT
              EXTRACT(YEAR FROM on.oorderdate) as year,
              EXTRACT(MONTH FROM on.oorderdate) as month,
              COUNT(DISTINCT on.transactionid) AS num_transactions,
              AVG(on.ssales) AS avg_gross_sales
            FROM
              bi_report.ordernumber_rs on
              INNER JOIN mars__revolveclothing_com___db.orders o ON on.transactionid = o.transactionid
            WHERE
              on.paymenttype = 'ANET'
              AND (o.paymenttokenservice IS NULL OR o.paymenttokenservice != 'ApplePay')
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            "explanation": "Correct approach: Join with orders table to get paymenttokenservice field"
        },
        "shipping_loss_rates": {
            "question": "Analyze Ontrac and UPS loss rates by order value with signature requirements",
            "sql": """
            SELECT
              sp.shippingoption,
              s.sigrequired,
              CASE
                WHEN o.amount <= 100 THEN '0-100'
                WHEN o.amount <= 200 THEN '101-200'
                WHEN o.amount <= 300 THEN '201-300'
                WHEN o.amount <= 400 THEN '301-400'
                WHEN o.amount <= 500 THEN '401-500'
                ELSE '501+'
              END AS value_range,
              COUNT(*) AS total_shipments,
              SUM(CASE WHEN s.extrastatus = 'lost package' THEN 1 ELSE 0 END) AS lost_shipments,
              SUM(CASE WHEN s.extrastatus = 'lost package' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS loss_rate
            FROM
              bi_report.shipmentnumber_rs sn
              JOIN mars__revolveclothing_com___db.orders o ON sn.transactionid = o.transactionid
              JOIN mars__revolveclothing_com___db.shipment s ON sn.shipmentid = s.shipmentid
              JOIN mars__id.shipping_pickuptime sp ON s.shippingoption = sp.shippingoption
            WHERE
              sp.shippingoption IN ('Ontrac', 'UPS')
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
            "explanation": "Correct approach: Use 'lost package' status, get accurate shipping info from shipping_pickuptime"
        },
        "random_customer_survey": {
            "question": "Get 5K random REVOLVE customers with last transaction in past 12 months",
            "sql": """
            SELECT
              t1.useremail,
              t1.site,
              t1.invoicenum,
              t1.shipcountry,
              t1.ssales
            FROM (
              SELECT
                useremail,
                site,
                invoicenum,
                shipcountry,
                ssales,
                ROW_NUMBER() OVER (PARTITION BY useremail ORDER BY oorderdate DESC) AS rn,
                RANDOM() as rand_num
              FROM
                bi_report.ordernumber_rs
              WHERE
                oorderdate >= (CURRENT_DATE - INTERVAL '12 months')
                AND site = 'R'
            ) t1
            WHERE t1.rn = 1
            ORDER BY t1.rand_num
            LIMIT 5000
            """,
            "explanation": "Correct approach: Use RANDOM() function for random sampling, get most recent transaction per customer"
        }
    }

    return samples