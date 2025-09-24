"""
Main ADK Agent for NL2SQL Redshift integration.
"""

from google.adk.agents import Agent
from .tools import create_redshift_tool, create_schema_tool
from .sql_helper import RedshiftSQLHelper
import os
import json


class NL2SQLRedshiftAgent:
    """
    ADK Agent that converts natural language to SQL and executes against Redshift.
    """
    
    def __init__(self, 
                 project_id: str,
                 location: str = "us-central1",
                 connection: str = "redshift-demo-connection",
                 model: str = "gemini-2.0-flash"):
        
        self.project_id = project_id
        self.location = location
        self.connection = connection
        self.model = model
        
        # Initialize SQL helper
        self.sql_helper = RedshiftSQLHelper()
        
        # Create tools
        self.redshift_tool = create_redshift_tool(
            project_id=project_id,
            location=location,
            connection=connection
        )
        
        # Create the ADK agent
        self.agent = Agent(
            name="nl2sql-redshift-agent",
            description="Convert natural language to SQL and execute on AWS Redshift via GCP Integration Connector",
            model=model,
            instructions=self._get_agent_instructions(),
            tools=[self.redshift_tool]
        )
    
    def _get_agent_instructions(self) -> str:
        """
        Get detailed instructions for the ADK agent.
        """
        
        schema_description = self.sql_helper._format_schema_description()
        relationships = self.sql_helper._format_relationships()
        
        instructions = f"""
You are an expert SQL analyst specializing in retail data analysis on AWS Redshift databases.

Your primary task is to:
1. Convert natural language questions to correct Redshift SQL queries
2. Execute queries using the Integration Connector tools
3. Provide clear, insightful responses based on the results

<Database Schema>
{schema_description}

<Table Relationships>
{relationships}

<Key Guidelines>
- Always use fully qualified table names (public.table_name)
- Use proper Redshift SQL syntax and data types
- Join only necessary tables to answer the question
- For date queries, assume "last quarter" means last 3 months from today
- When showing "top N" results, always include ORDER BY and LIMIT
- For aggregations, include all non-aggregated columns in GROUP BY
- Provide both the SQL query and natural language explanation of results
- If a query fails, analyze the error and suggest corrections

<Available Tools>
- redshift_list_[table]: List records from specific tables
- redshift_get_[table]: Get specific records with filters  
- redshift_execute_custom_query: Execute custom SQL queries (use this for complex analytical queries)

<Response Format>
When answering questions:
1. First, analyze what data is needed
2. Generate and explain the SQL query
3. Execute the query using appropriate tools
4. Interpret and explain the results in business terms
5. Provide insights and recommendations when relevant

<Sample Query Patterns>
- Count queries: SELECT COUNT(*) FROM table WHERE conditions
- Top N queries: SELECT columns FROM table ORDER BY metric DESC LIMIT N
- Aggregation: SELECT group_col, SUM(metric) FROM table GROUP BY group_col
- Joins: SELECT cols FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id

Always double-check your SQL syntax before execution and provide helpful explanations of what the query does and what the results mean.
"""
        return instructions
    
    def process_question(self, user_question: str, context: str = None) -> dict:
        """
        Process a natural language question and return SQL + results.
        
        Args:
            user_question: User's natural language question
            context: Optional additional context
            
        Returns:
            Dictionary with query, results, and response
        """
        
        try:
            # Use the agent to process the question
            response = self.agent.run(user_question)
            
            return {
                "success": True,
                "user_question": user_question,
                "agent_response": response,
                "context": context
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "user_question": user_question,
                "context": context
            }
    
    def validate_setup(self) -> dict:
        """
        Validate that the agent setup is working correctly.
        """
        
        validation_results = {
            "agent_initialized": False,
            "tools_available": False,
            "schema_loaded": False,
            "sample_queries_loaded": False,
            "errors": []
        }
        
        try:
            # Check agent initialization
            if self.agent:
                validation_results["agent_initialized"] = True
            
            # Check tools
            if self.redshift_tool:
                validation_results["tools_available"] = True
            
            # Check schema
            if self.sql_helper.schema_info:
                validation_results["schema_loaded"] = True
            
            # Check sample queries
            if self.sql_helper.sample_queries:
                validation_results["sample_queries_loaded"] = True
                
        except Exception as e:
            validation_results["errors"].append(str(e))
        
        validation_results["overall_status"] = all([
            validation_results["agent_initialized"],
            validation_results["tools_available"], 
            validation_results["schema_loaded"],
            validation_results["sample_queries_loaded"]
        ]) and len(validation_results["errors"]) == 0
        
        return validation_results
    
    def get_demo_questions(self) -> list:
        """
        Get list of demo questions for testing.
        """
        return self.sql_helper.get_demo_questions()
    
    def get_schema_info(self) -> dict:
        """
        Get database schema information.
        """
        return self.sql_helper.schema_info


def create_agent_from_config(config_path: str = "../config/agent_config.json") -> NL2SQLRedshiftAgent:
    """
    Create agent instance from configuration file.
    
    Args:
        config_path: Path to configuration JSON file
        
    Returns:
        Configured NL2SQLRedshiftAgent instance
    """
    
    # Default configuration
    default_config = {
        "project_id": os.getenv("GCP_PROJECT_ID", "your-project-id"),
        "location": "us-central1",
        "connection": "redshift-demo-connection", 
        "model": "gemini-2.0-flash"
    }
    
    # Try to load configuration file
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                default_config.update(file_config)
        except Exception as e:
            print(f"Warning: Could not load config file {config_path}: {e}")
            print("Using default configuration")
    
    # Create and return agent
    return NL2SQLRedshiftAgent(
        project_id=default_config["project_id"],
        location=default_config["location"],
        connection=default_config["connection"],
        model=default_config["model"]
    )