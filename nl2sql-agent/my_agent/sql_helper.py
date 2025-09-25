"""
SQL Helper utilities for NL2SQL conversion, updated for client's Redshift schema.
Based on bi_report.ordernumber_rs and bi_report.shipmentnumber_rs tables.
"""

import json
from typing import Dict, List, Optional, Tuple
from .tools import get_table_relationships, get_sample_queries, create_schema_tool


class RedshiftSQLHelper:
    """
    Helper class for SQL generation and validation for client's Redshift.
    Updated with business rules from the test document.
    """

    def __init__(self):
        self.schema_info = create_schema_tool()
        self.relationships = get_table_relationships()
        self.sample_queries = get_sample_queries()
        self.redshift_data_types = [
            "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC",
            "REAL", "DOUBLE PRECISION", "BOOLEAN", "CHAR", "VARCHAR",
            "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ"
        ]

    def get_buildsql_prompt(self, user_question: str, context: Optional[str] = None) -> str:
        """
        Creates a prompt for SQL generation based on user question.
        Updated with client's business rules and Redshift schema.
        """

        schema_description = self._format_schema_description()
        sample_sql = self._get_relevant_sample_sql(user_question)
        business_rules = self._get_business_rules()

        prompt = f"""
You are a Redshift SQL expert for Revolve's e-commerce data. Write a query that answers the following question.

<Critical Business Rules>
{business_rules}

<Guidelines>
- Use fully qualified table names (bi_report.ordernumber_rs, mars__revolveclothing_com___db.orders, etc.)
- For Revolve orders: Use site <> 'F' (excludes Forward brand)
- Product code joins: Use UPPER(TRIM(p.code)) = sn.productcode
- Category mapping: Use SUBSTRING(SPLIT_PART(p.code, '-', 2), 2, 1) = cn.lettercat
- Lost packages: Use extrastatus = 'lost package' (not 'lost')
- High value customers: Use PERCENT_RANK() for percentile calculations
- Random sampling: Use RANDOM() function with ORDER BY
- Payment token service: Must join with mars__revolveclothing_com___db.orders table
- Date filtering: Use proper TIMESTAMP comparisons
- Don't include any comments in the SQL code
- Generate clean SQL without ```sql or ``` markers

<Database Schema>
{schema_description}

<Table Relationships>
{self._format_relationships()}

<Sample Queries for Reference>
{sample_sql}

<Additional Context>
{context or 'No additional context provided'}

<User Question>
{user_question}

Generate a syntactically and semantically correct Redshift SQL query following the business rules:
"""
        return prompt

    def _get_business_rules(self) -> str:
        """Get client-specific business rules."""

        rules = [
            "1. REVOLVE ORDERS: Use site <> 'F' to exclude Forward brand orders",
            "2. PRODUCT JOINS: ordernumber_rs has NO product info, use shipmentnumber_rs for product-level analysis",
            "3. PRODUCT CODE MAPPING: Use UPPER(TRIM(p.code)) = productcode when joining products",
            "4. CATEGORY MAPPING: Extract category from product code: SUBSTRING(SPLIT_PART(p.code, '-', 2), 2, 1) = cn.lettercat",
            "5. LOST PACKAGES: Use extrastatus = 'lost package' (NOT 'lost')",
            "6. PAYMENT TOKEN: paymenttokenservice is in mars__revolveclothing_com___db.orders, NOT in ordernumber_rs",
            "7. TOP PERCENTILES: Use PERCENT_RANK() OVER (ORDER BY metric DESC) <= 0.05 for top 5%",
            "8. SHIPPING CARRIERS: Use mars__id.shipping_pickuptime for accurate carrier info, not shipment.shippingoption",
            "9. RANDOM SAMPLING: Use RANDOM() function with ORDER BY for random results",
            "10. DATE RANGES: Use proper TIMESTAMP filtering with >= and < operators"
        ]

        return "\n".join(rules)

    def _format_schema_description(self) -> str:
        """Format schema information for prompts."""

        description = []
        for table_name, table_info in self.schema_info["tables"].items():
            columns = []
            for col in table_info["columns"]:
                col_desc = f"{col['name']} ({col['type']})"
                if col.get("primary_key"):
                    col_desc += " [PRIMARY KEY]"
                elif col.get("foreign_key"):
                    col_desc += f" [FOREIGN KEY -> {col['foreign_key']}]"
                if col.get("description"):
                    col_desc += f" - {col['description']}"
                columns.append(col_desc)

            table_desc = f"Table: {table_name}\n"
            table_desc += f"Description: {table_info['description']}\n"
            if table_info.get("business_rules"):
                table_desc += f"Business Rules: {'; '.join(table_info['business_rules'])}\n"
            table_desc += "Columns:\n" + "\n".join(f"  - {col}" for col in columns)
            description.append(table_desc)

        return "\n\n".join(description)

    def _format_relationships(self) -> str:
        """Format table relationships for prompts."""

        relationships = []
        for rel_name, rel_info in self.relationships.items():
            relationships.append(f"- {rel_name}: {rel_info['condition']} ({rel_info['description']})")

        return "\n".join(relationships)

    def _get_relevant_sample_sql(self, user_question: str) -> str:
        """Get relevant sample SQL based on user question keywords."""

        question_lower = user_question.lower()
        relevant_samples = []

        # Enhanced keyword matching for client's business context
        for sample_name, sample_info in self.sample_queries.items():
            sample_keywords = sample_info["question"].lower()

            # Check for various business question patterns
            if any(keyword in question_lower for keyword in ["aov", "average order", "order value"]):
                if "aov" in sample_keywords or "order" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["high value", "top customer", "percentile"]):
                if "high value" in sample_keywords or "percentile" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["payment", "anet", "applepay"]):
                if "anet" in sample_keywords or "payment" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["loss", "shipping", "carrier", "ups", "ontrac"]):
                if "loss" in sample_keywords or "shipping" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["random", "sample", "survey"]):
                if "random" in sample_keywords or "sample" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["brand", "brands"]):
                if "brand" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["category", "categories"]):
                if "category" in sample_keywords:
                    relevant_samples.append(sample_info)

        if not relevant_samples:
            # Return first two samples as examples
            relevant_samples = list(self.sample_queries.values())[:2]

        formatted_samples = []
        for sample in relevant_samples:
            formatted_samples.append(f"Question: {sample['question']}\nSQL: {sample['sql']}\nExplanation: {sample['explanation']}")

        return "\n\n".join(formatted_samples)