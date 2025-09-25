"""
Main ADK Agent for NL2SQL Redshift integration.
Updated to follow ADK best practices and handle client's business requirements.
"""

from google.adk.agents import Agent
from .tools import create_redshift_tool, create_schema_tool
from .sql_helper import RedshiftSQLHelper
import os
import json
import logging
from typing import Dict, Optional, Any


class NL2SQLRedshiftAgent:
    """
    ADK Agent that converts natural language to SQL and executes against Redshift.
    Updated with enhanced error handling, validation, and ADK best practices.
    """

    def __init__(self,
                 project_id: str,
                 location: str = "us-central1",
                 connection: str = "redshift-demo-connection",
                 model: str = "gemini-2.0-flash",
                 service_account_path: Optional[str] = None,
                 debug: bool = False):

        # Configure logging
        if debug:
            logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

        # Validate required parameters
        if not project_id:
            raise ValueError("project_id is required and cannot be empty")

        self.project_id = project_id
        self.location = location
        self.connection = connection
        self.model = model
        self.service_account_path = service_account_path or "../config/service_account.json"

        self.logger.info(f"Initializing NL2SQLRedshiftAgent for project: {project_id}")

        try:
            # Initialize SQL helper
            self.sql_helper = RedshiftSQLHelper()
            self.logger.info("✓ SQL helper initialized")

            # Create tools with enhanced error handling
            self.redshift_tool = self._create_tools()
            self.logger.info("✓ Redshift tools created")

            # Create the ADK agent
            self.agent = self._create_agent()
            self.logger.info("✓ ADK agent created successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize NL2SQLRedshiftAgent: {e}")
            raise

    def _create_tools(self) -> Any:
        """Create and validate Redshift tools."""
        try:
            redshift_tool = create_redshift_tool(
                project_id=self.project_id,
                location=self.location,
                connection=self.connection,
                service_account_json_path=self.service_account_path
            )

            # Validate tool creation
            if not redshift_tool:
                raise RuntimeError("Failed to create ApplicationIntegrationToolset")

            return redshift_tool

        except Exception as e:
            self.logger.error(f"Tool creation failed: {e}")
            raise RuntimeError(f"Could not create Redshift tools: {e}")

    def _create_agent(self) -> Agent:
        """Create ADK agent with proper configuration."""
        try:
            agent = Agent(
                name="nl2sql-redshift-agent",
                description="Expert NL2SQL agent for Revolve's e-commerce data analysis on AWS Redshift via GCP Integration Connectors",
                model=self.model,
                instructions=self._get_agent_instructions(),
                tools=[self.redshift_tool]
            )

            return agent

        except Exception as e:
            self.logger.error(f"Agent creation failed: {e}")
            raise RuntimeError(f"Could not create ADK agent: {e}")
    
    def _get_agent_instructions(self) -> str:
        """
        Get detailed instructions for the ADK agent.
        Updated with client's business context and requirements.
        """

        schema_description = self.sql_helper._format_schema_description()
        relationships = self.sql_helper._format_relationships()
        business_rules = self.sql_helper._get_business_rules()

        instructions = f"""
You are an expert SQL analyst specializing in Revolve's e-commerce data analysis on AWS Redshift.

Your primary mission is to:
1. Convert natural language questions to CORRECT Redshift SQL queries following Revolve's business rules
2. Execute queries using Integration Connector tools
3. Provide clear, actionable business insights based on the results

<Critical Business Rules - MUST FOLLOW>
{business_rules}

<Database Schema - Revolve E-commerce>
{schema_description}

<Table Relationships>
{relationships}

<Key Guidelines>
- ALWAYS use fully qualified table names (bi_report.ordernumber_rs, mars__revolveclothing_com___db.orders, etc.)
- ALWAYS apply business rules (e.g., site <> 'F' for Revolve orders)
- Use proper Redshift SQL syntax and data types
- For product analysis, use bi_report.shipmentnumber_rs (has product info), NOT ordernumber_rs
- For date queries, use proper TIMESTAMP comparisons with >= and < operators
- When showing "top N" results, always include ORDER BY and LIMIT
- For aggregations, include all non-aggregated columns in GROUP BY
- Provide both the SQL query and natural language explanation of results
- If a query fails, analyze the error and suggest corrections

<Available Tools>
- redshift_execute_custom_query: Execute custom SQL queries (USE THIS for complex analytical queries)
- redshift_list_[table]: List records from specific tables (for simple data browsing)
- redshift_get_[table]: Get specific records with filters (for targeted lookups)

<Response Format>
When answering questions:
1. First, analyze what data is needed and which business rules apply
2. Determine the correct tables to use (ordernumber_rs vs shipmentnumber_rs)
3. Generate SQL query following business rules
4. Execute the query using redshift_execute_custom_query
5. Interpret results in Revolve's business context
6. Provide actionable insights and recommendations

<Common Query Patterns for Revolve>
- Revolve Orders: SELECT ... FROM bi_report.ordernumber_rs WHERE site <> 'F'
- Product Analysis: SELECT ... FROM bi_report.shipmentnumber_rs sn JOIN mars__revolveclothing_com___db.product p ON sn.productcode = UPPER(TRIM(p.code))
- Category Analysis: Include mars__id.id_categorynames2 with SUBSTRING(SPLIT_PART(p.code, '-', 2), 2, 1) = cn.lettercat
- Payment Analysis: Join with mars__revolveclothing_com___db.orders for paymenttokenservice
- High Value Customers: Use PERCENT_RANK() OVER (ORDER BY metric DESC) <= 0.05
- Random Sampling: Use RANDOM() function with ORDER BY

<Error Handling>
- If a query fails, check business rule compliance first
- Verify table names are fully qualified
- Ensure proper JOINs and field mappings
- Double-check date formats and filtering logic

Always prioritize business rule compliance and provide insights that help Revolve make data-driven decisions.
"""
        return instructions
    
    def process_question(self, user_question: str, context: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a natural language question and return SQL + results.
        Enhanced with better error handling and business rule validation.

        Args:
            user_question: User's natural language question
            context: Optional additional context

        Returns:
            Dictionary with query, results, and response
        """

        if not user_question or not user_question.strip():
            return {
                "success": False,
                "error": "Question cannot be empty",
                "user_question": user_question,
                "context": context
            }

        self.logger.info(f"Processing question: {user_question[:100]}...")

        try:
            # Pre-validate question for business context
            validation_result = self._pre_validate_question(user_question)
            if not validation_result["valid"]:
                self.logger.warning(f"Question validation failed: {validation_result['message']}")

            # Use the agent to process the question
            response = self.agent.run(user_question)

            # Post-validate response for business rules compliance
            compliance_check = self._check_business_rule_compliance(user_question, str(response))

            result = {
                "success": True,
                "user_question": user_question,
                "agent_response": response,
                "context": context,
                "validation": validation_result,
                "compliance_check": compliance_check
            }

            self.logger.info("✓ Question processed successfully")
            return result

        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error processing question: {error_msg}")

            return {
                "success": False,
                "error": error_msg,
                "error_type": type(e).__name__,
                "user_question": user_question,
                "context": context,
                "suggestions": self._get_error_suggestions(error_msg)
            }

    def _pre_validate_question(self, question: str) -> Dict[str, Any]:
        """Validate question for common patterns and business context."""

        question_lower = question.lower()
        warnings = []
        suggestions = []

        # Check for business context keywords
        if any(word in question_lower for word in ["revolve", "forward"]):
            if "revolve" in question_lower and "site" not in question_lower:
                suggestions.append("Consider specifying site <> 'F' to exclude Forward orders")

        # Check for product-related queries
        if any(word in question_lower for word in ["product", "brand", "category"]):
            if "shipment" not in question_lower:
                suggestions.append("Product analysis requires shipmentnumber_rs table for accurate results")

        # Check for payment-related queries
        if any(word in question_lower for word in ["payment", "applepay", "token"]):
            suggestions.append("Payment token analysis requires joining with mars__revolveclothing_com___db.orders")

        return {
            "valid": True,
            "warnings": warnings,
            "suggestions": suggestions,
            "message": "Pre-validation completed"
        }

    def _check_business_rule_compliance(self, question: str, response: str) -> Dict[str, Any]:
        """Check if the response follows business rules."""

        violations = []
        question_lower = question.lower()
        response_lower = response.lower()

        # Check common business rule compliance
        if "revolve" in question_lower and "site" in response_lower:
            if "site <> 'f'" not in response_lower and "site != 'f'" not in response_lower:
                violations.append("Missing Revolve filter: should use site <> 'F'")

        if any(word in question_lower for word in ["lost", "package"]):
            if "'lost'" in response_lower and "'lost package'" not in response_lower:
                violations.append("Should use 'lost package' not 'lost' for package status")

        return {
            "compliant": len(violations) == 0,
            "violations": violations,
            "checked_rules": ["revolve_filter", "lost_package_status"]
        }

    def _get_error_suggestions(self, error_msg: str) -> List[str]:
        """Provide suggestions based on error message."""

        suggestions = []
        error_lower = error_msg.lower()

        if "authentication" in error_lower or "permission" in error_lower:
            suggestions.append("Check service account credentials and IAM roles")
            suggestions.append("Verify GOOGLE_APPLICATION_CREDENTIALS environment variable")

        if "connection" in error_lower or "network" in error_lower:
            suggestions.append("Verify Integration Connector is active and accessible")
            suggestions.append("Check VPC and firewall settings for Redshift connection")

        if "table" in error_lower or "column" in error_lower:
            suggestions.append("Verify table names are fully qualified (schema.table)")
            suggestions.append("Check if the required tables exist in Redshift")

        if not suggestions:
            suggestions.append("Check agent logs for more detailed error information")
            suggestions.append("Validate configuration in agent_config.json")

        return suggestions
    
    def validate_setup(self) -> Dict[str, Any]:
        """
        Validate that the agent setup is working correctly.
        Enhanced with comprehensive checks following ADK best practices.
        """

        validation_results = {
            "agent_initialized": False,
            "tools_available": False,
            "schema_loaded": False,
            "sample_queries_loaded": False,
            "business_rules_loaded": False,
            "authentication_configured": False,
            "integration_connector_accessible": False,
            "errors": [],
            "warnings": [],
            "configuration": {}
        }

        self.logger.info("Starting comprehensive setup validation...")

        try:
            # Check agent initialization
            if self.agent:
                validation_results["agent_initialized"] = True
                self.logger.info("✓ Agent initialized")
            else:
                validation_results["errors"].append("ADK Agent not initialized")

            # Check tools
            if self.redshift_tool:
                validation_results["tools_available"] = True
                self.logger.info("✓ Redshift tools available")
            else:
                validation_results["errors"].append("ApplicationIntegrationToolset not available")

            # Check schema
            if self.sql_helper.schema_info and self.sql_helper.schema_info.get("tables"):
                validation_results["schema_loaded"] = True
                table_count = len(self.sql_helper.schema_info["tables"])
                self.logger.info(f"✓ Schema loaded with {table_count} tables")
            else:
                validation_results["errors"].append("Database schema not loaded")

            # Check sample queries
            if self.sql_helper.sample_queries:
                validation_results["sample_queries_loaded"] = True
                query_count = len(self.sql_helper.sample_queries)
                self.logger.info(f"✓ Sample queries loaded ({query_count} queries)")
            else:
                validation_results["errors"].append("Sample queries not loaded")

            # Check business rules
            try:
                business_rules = self.sql_helper._get_business_rules()
                if business_rules and len(business_rules) > 100:  # Should have substantial rules
                    validation_results["business_rules_loaded"] = True
                    self.logger.info("✓ Business rules loaded")
                else:
                    validation_results["warnings"].append("Business rules may be incomplete")
            except Exception as e:
                validation_results["errors"].append(f"Business rules validation failed: {e}")

            # Check authentication
            auth_configured = self._validate_authentication()
            validation_results["authentication_configured"] = auth_configured["valid"]
            if auth_configured["valid"]:
                self.logger.info("✓ Authentication configured")
            else:
                validation_results["errors"].extend(auth_configured["errors"])

            # Store configuration details
            validation_results["configuration"] = {
                "project_id": self.project_id,
                "location": self.location,
                "connection": self.connection,
                "model": self.model,
                "service_account_path": self.service_account_path
            }

        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            validation_results["errors"].append(error_msg)
            self.logger.error(error_msg)

        # Calculate overall status
        validation_results["overall_status"] = all([
            validation_results["agent_initialized"],
            validation_results["tools_available"],
            validation_results["schema_loaded"],
            validation_results["sample_queries_loaded"],
            validation_results["business_rules_loaded"],
            validation_results["authentication_configured"]
        ]) and len(validation_results["errors"]) == 0

        # Log validation summary
        if validation_results["overall_status"]:
            self.logger.info("✅ All validation checks passed")
        else:
            self.logger.warning(f"❌ Validation failed with {len(validation_results['errors'])} errors")

        return validation_results

    def _validate_authentication(self) -> Dict[str, Any]:
        """Validate authentication configuration."""

        auth_result = {"valid": False, "errors": [], "method": "unknown"}

        try:
            # Check for service account file
            if os.path.exists(self.service_account_path):
                auth_result["method"] = "service_account_file"
                auth_result["valid"] = True
                return auth_result

            # Check for environment credentials
            if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
                creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if os.path.exists(creds_path):
                    auth_result["method"] = "environment_credentials"
                    auth_result["valid"] = True
                    return auth_result
                else:
                    auth_result["errors"].append(f"GOOGLE_APPLICATION_CREDENTIALS points to non-existent file: {creds_path}")

            # Check for default credentials (gcloud)
            try:
                # This is a basic check - in production you might want to test actual authentication
                home_dir = os.path.expanduser("~")
                gcloud_config = os.path.join(home_dir, ".config", "gcloud")
                if os.path.exists(gcloud_config):
                    auth_result["method"] = "gcloud_default"
                    auth_result["valid"] = True
                    return auth_result
            except Exception as e:
                auth_result["errors"].append(f"Error checking gcloud credentials: {e}")

            if not auth_result["valid"]:
                auth_result["errors"].append("No valid authentication method found")

        except Exception as e:
            auth_result["errors"].append(f"Authentication validation failed: {e}")

        return auth_result
    
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


def create_agent_from_config(config_path: str = "../config/agent_config.json",
                            debug: bool = False) -> NL2SQLRedshiftAgent:
    """
    Create agent instance from configuration file following ADK best practices.

    Args:
        config_path: Path to configuration JSON file
        debug: Enable debug logging

    Returns:
        Configured NL2SQLRedshiftAgent instance

    Raises:
        ValueError: If required configuration is missing or invalid
        FileNotFoundError: If config file is required but not found
    """

    # Configure logging for configuration loading
    logger = logging.getLogger(__name__)
    if debug:
        logging.basicConfig(level=logging.DEBUG)

    logger.info(f"Loading agent configuration from {config_path}")

    # Default configuration with enhanced validation
    default_config = {
        "project_id": os.getenv("GCP_PROJECT_ID"),
        "location": "us-central1",
        "connection": "redshift-demo-connection",
        "model": "gemini-2.0-flash",
        "service_account_path": "../config/service_account.json",
        "debug": debug
    }

    # Validate environment variables
    if not default_config["project_id"]:
        logger.warning("GCP_PROJECT_ID environment variable not set")

    # Try to load configuration file with better error handling
    config_loaded = False
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)

            # Validate configuration structure
            _validate_config_structure(file_config)

            default_config.update(file_config)
            config_loaded = True
            logger.info(f"✓ Configuration loaded from {config_path}")

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in config file {config_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading config file {config_path}: {e}")
            logger.info("Falling back to default configuration")
    else:
        logger.warning(f"Config file {config_path} not found, using default configuration")

    # Final validation of required parameters
    if not default_config["project_id"]:
        raise ValueError("project_id is required. Set GCP_PROJECT_ID environment variable or provide in config file.")

    if not default_config["connection"]:
        raise ValueError("connection name is required in configuration")

    logger.info("Configuration validation completed:")
    logger.info(f"  Project ID: {default_config['project_id']}")
    logger.info(f"  Location: {default_config['location']}")
    logger.info(f"  Connection: {default_config['connection']}")
    logger.info(f"  Model: {default_config['model']}")

    # Create and return agent
    try:
        agent = NL2SQLRedshiftAgent(
            project_id=default_config["project_id"],
            location=default_config["location"],
            connection=default_config["connection"],
            model=default_config["model"],
            service_account_path=default_config.get("service_account_path"),
            debug=default_config.get("debug", debug)
        )

        logger.info("✅ Agent created successfully from configuration")
        return agent

    except Exception as e:
        logger.error(f"Failed to create agent from configuration: {e}")
        raise


def _validate_config_structure(config: Dict[str, Any]) -> None:
    """
    Validate configuration file structure and values.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ValueError: If configuration is invalid
    """

    # Required fields
    required_fields = ["project_id"]
    for field in required_fields:
        if field not in config or not config[field]:
            raise ValueError(f"Required configuration field '{field}' is missing or empty")

    # Optional fields with validation
    if "location" in config:
        valid_locations = ["us-central1", "us-east1", "us-west1", "europe-west1", "asia-northeast1"]
        if config["location"] not in valid_locations:
            logging.warning(f"Unusual location: {config['location']}. Common locations are: {valid_locations}")

    if "model" in config:
        if not config["model"].startswith("gemini-"):
            logging.warning(f"Unusual model: {config['model']}. Consider using a gemini-* model for best results")

    # Service account path validation
    if "service_account_path" in config:
        if config["service_account_path"] and not os.path.exists(config["service_account_path"]):
            logging.warning(f"Service account file not found: {config['service_account_path']}")


def create_agent_with_validation(project_id: str,
                                connection: str,
                                location: str = "us-central1",
                                model: str = "gemini-2.0-flash",
                                validate_connectivity: bool = True,
                                debug: bool = False) -> NL2SQLRedshiftAgent:
    """
    Create agent with full validation and connectivity testing.
    Recommended for production deployments.

    Args:
        project_id: GCP project ID
        connection: Integration connector name
        location: GCP region
        model: Gemini model to use
        validate_connectivity: Test Integration Connector connectivity
        debug: Enable debug logging

    Returns:
        Validated NL2SQLRedshiftAgent instance

    Raises:
        RuntimeError: If validation fails
    """

    logger = logging.getLogger(__name__)
    if debug:
        logging.basicConfig(level=logging.DEBUG)

    logger.info("Creating agent with full validation...")

    # Create agent
    agent = NL2SQLRedshiftAgent(
        project_id=project_id,
        location=location,
        connection=connection,
        model=model,
        debug=debug
    )

    # Validate setup
    validation_result = agent.validate_setup()

    if not validation_result["overall_status"]:
        error_msg = f"Agent validation failed: {validation_result['errors']}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info("✅ Agent created and validated successfully")
    return agent