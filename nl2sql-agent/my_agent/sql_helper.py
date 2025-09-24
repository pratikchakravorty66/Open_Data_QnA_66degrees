"""
SQL Helper utilities for NL2SQL conversion, adapted from Open Data QnA agents.
Simplified for Redshift and ADK integration.
"""

import json
from typing import Dict, List, Optional, Tuple
from .tools import get_table_relationships, get_sample_queries, create_schema_tool


class RedshiftSQLHelper:
    """
    Helper class for SQL generation and validation for Redshift.
    Adapted from Open Data QnA BuildSQLAgent and ValidateSQLAgent.
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
        Adapted from Open Data QnA buildsql prompts.
        """
        
        schema_description = self._format_schema_description()
        sample_sql = self._get_relevant_sample_sql(user_question)
        
        prompt = f"""
You are a Redshift SQL expert. Your task is to write a Redshift SQL query that answers the following question.

<Guidelines>
- Join only necessary tables to answer the question
- When joining tables ensure all join columns are the same data type
- Use proper Redshift syntax and data types: {', '.join(self.redshift_data_types)}
- Don't include any comments in the SQL code
- Generate clean SQL without ```sql or ``` markers
- Use fully qualified table names: schema.table_name
- For aggregations, include all non-aggregated columns in GROUP BY
- Handle date comparisons appropriately for Redshift

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

Generate a syntactically and semantically correct Redshift SQL query:
"""
        return prompt
    
    def get_validation_prompt(self, user_question: str, generated_sql: str) -> str:
        """
        Creates a prompt for SQL validation.
        Adapted from Open Data QnA ValidateSQLAgent.
        """
        
        schema_description = self._format_schema_description()
        
        prompt = f"""
You are a Redshift SQL validator. Analyze the following SQL query and determine if it's valid.

<Validation Guidelines>
- Check syntax correctness for Redshift
- Verify table and column names exist in the schema
- Ensure JOIN conditions are proper
- Check data type compatibility
- Validate aggregate functions usage
- Ensure GROUP BY includes all non-aggregated SELECT columns

<Database Schema>
{schema_description}

<User Question>
{user_question}

<Generated SQL>
{generated_sql}

Respond with a JSON object:
{{
    "valid": true/false,
    "errors": "description of any errors found (empty string if valid)",
    "suggestions": "suggestions for improvement (optional)"
}}
"""
        return prompt
    
    def generate_response_prompt(self, user_question: str, sql_result: str) -> str:
        """
        Creates a prompt for generating natural language response from SQL results.
        Adapted from Open Data QnA ResponseAgent.
        """
        
        prompt = f"""
You are a data analyst assistant. Generate a clear, informative natural language response based on the SQL query results.

<Guidelines>
- Provide insights and key findings from the data
- Use business-friendly language
- Include specific numbers and metrics where relevant
- If the result is empty or null, explain what this means
- Structure the response in a conversational manner

<User Question>
{user_question}

<SQL Results>
{sql_result}

Generate a helpful natural language response:
"""
        return prompt
    
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
        
        # Simple keyword matching for sample selection
        for sample_name, sample_info in self.sample_queries.items():
            sample_keywords = sample_info["question"].lower()
            if any(keyword in question_lower for keyword in ["apparel", "clothes", "fashion"]):
                if "apparel" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["electronics", "electronic"]):
                if "electronics" in sample_keywords:
                    relevant_samples.append(sample_info)
            elif any(keyword in question_lower for keyword in ["brand", "brands"]):
                if "brand" in sample_keywords:
                    relevant_samples.append(sample_info)
        
        if not relevant_samples:
            # Return first few samples as examples
            relevant_samples = list(self.sample_queries.values())[:2]
        
        formatted_samples = []
        for sample in relevant_samples:
            formatted_samples.append(f"Question: {sample['question']}\nSQL: {sample['sql']}\nExplanation: {sample['explanation']}")
        
        return "\n\n".join(formatted_samples)
    
    def validate_sql_syntax(self, sql: str) -> Tuple[bool, str]:
        """
        Basic SQL syntax validation.
        Returns (is_valid, error_message).
        """
        
        sql = sql.strip()
        
        # Basic checks
        if not sql:
            return False, "Empty SQL query"
        
        if not sql.upper().startswith("SELECT"):
            return False, "Query must start with SELECT"
        
        # Check for required tables
        required_tables = ["products", "sales", "customers"]
        sql_upper = sql.upper()
        
        for table in required_tables:
            if table.upper() in sql_upper:
                # Check if table is properly referenced
                if f"FROM {table}" not in sql_upper and f"JOIN {table}" not in sql_upper:
                    continue
                break
        else:
            return False, "Query must reference at least one of the available tables"
        
        # Check for common syntax issues
        if sql.count("(") != sql.count(")"):
            return False, "Mismatched parentheses"
        
        # Check for proper table references (should include schema)
        if "public." not in sql.lower() and "demo_db." not in sql.lower():
            return False, "Tables should be fully qualified (e.g., public.table_name)"
        
        return True, ""
    
    def extract_table_names(self, sql: str) -> List[str]:
        """Extract table names referenced in the SQL query."""
        
        tables = []
        sql_upper = sql.upper()
        
        # Look for FROM and JOIN clauses
        keywords = ["FROM", "JOIN"]
        for keyword in keywords:
            if keyword in sql_upper:
                # Simple extraction - this could be more sophisticated
                for table in self.schema_info["tables"].keys():
                    if table.upper() in sql_upper:
                        if table not in tables:
                            tables.append(table)
        
        return tables
    
    def get_demo_questions(self) -> List[str]:
        """Get list of demo questions for testing."""
        
        return [
            "How many apparels were sold in the last quarter?",
            "What are the top 5 selling apparel brands?",
            "Show sales by region for electronics",
            "Which customers bought the most items?",
            "What is the average price of products by category?",
            "How many customers registered this year?",
            "Which region has the highest total sales?"
        ]