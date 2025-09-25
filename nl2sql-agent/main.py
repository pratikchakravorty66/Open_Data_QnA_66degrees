#!/usr/bin/env python3
"""
Main entry point for the NL2SQL Redshift ADK Agent.
Updated to follow ADK best practices with enhanced error handling.
"""

import os
import sys
import argparse
import logging
from typing import Optional
from my_agent.agent import create_agent_from_config, create_agent_with_validation, NL2SQLRedshiftAgent
from my_agent.demo_queries import run_quick_demo, DemoQueryRunner


def main():
    """Main entry point with enhanced error handling and validation."""

    parser = argparse.ArgumentParser(
        description="NL2SQL Redshift ADK Agent - Revolve E-commerce Data Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --validate                    # Validate setup
  python main.py --quick-demo                  # Run core demo queries
  python main.py --demo                        # Run full demo suite
  python main.py --query "Show top brands"     # Run single query
  python main.py --config my_config.json       # Use custom config

Business Context:
  This agent specializes in Revolve's e-commerce data analysis on AWS Redshift,
  handling orders, customers, products, and shipping data with business rules.
        """
    )

    parser.add_argument("--config", default="config/agent_config.json",
                       help="Path to agent configuration file")
    parser.add_argument("--project-id", help="GCP Project ID (overrides config)")
    parser.add_argument("--connection", help="Integration Connector name (overrides config)")
    parser.add_argument("--location", help="GCP region (overrides config)")
    parser.add_argument("--model", help="Gemini model to use (overrides config)")

    # Action arguments
    parser.add_argument("--demo", action="store_true", help="Run full demo query suite")
    parser.add_argument("--quick-demo", action="store_true", help="Run core demo queries")
    parser.add_argument("--validate", action="store_true", help="Validate agent setup and connectivity")
    parser.add_argument("--query", help="Run a single natural language query")

    # Configuration arguments
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--no-validation", action="store_true", help="Skip setup validation")
    parser.add_argument("--interactive", action="store_true", help="Interactive query mode")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    try:
        # Create agent
        print("🚀 Initializing NL2SQL Redshift Agent for Revolve E-commerce Analysis...")
        logger.info("Starting NL2SQL Redshift Agent")
    
        # Create agent with enhanced configuration handling
        if args.project_id and args.connection and not args.no_validation:
            # Create agent directly with validation for production use
            agent = create_agent_with_validation(
                project_id=args.project_id,
                connection=args.connection,
                location=args.location or "us-central1",
                model=args.model or "gemini-2.0-flash",
                debug=args.debug
            )
        else:
            # Create agent from config file
            agent = create_agent_from_config(args.config, debug=args.debug)

            # Override config with command line arguments if provided
            if args.project_id:
                logger.info(f"Overriding project_id with: {args.project_id}")
            if args.connection:
                logger.info(f"Overriding connection with: {args.connection}")

        print(f"✅ Agent initialized for project: {agent.project_id}")
        print(f"🔗 Using connection: {agent.connection}")
        print(f"📍 Region: {agent.location}")
        print(f"🧠 Model: {agent.model}")

    except Exception as e:
        logger.error(f"Agent initialization failed: {e}")
        print(f"❌ Failed to initialize agent: {e}")
        print("\n💡 Troubleshooting tips:")
        print("  - Verify your GCP_PROJECT_ID environment variable")
        print("  - Check that Integration Connector is deployed and active")
        print("  - Ensure service account has required IAM roles")
        print("  - Use --debug for detailed error information")
        return 1
    
        # Validate setup if requested or not skipped
        if args.validate or not args.no_validation:
            print("\n🔍 Validating agent setup...")
            validation = agent.validate_setup()

            _print_validation_results(validation)

            if not validation["overall_status"]:
                if args.validate:  # If explicit validation was requested, fail
                    return 1
                else:  # If automatic validation, warn but continue
                    print("⚠️  Continuing despite validation warnings...")

        # Run demos
        if args.quick_demo:
            print("\n🚀 Running Quick Demo - Core Revolve Business Queries...")
            run_quick_demo(agent)
            return 0

        if args.demo:
            print("\n🚀 Running Full Demo Suite - All Revolve Business Scenarios...")
            demo_runner = DemoQueryRunner(agent)
            results = demo_runner.run_all_demos()
            return 0 if all(r["success"] for r in results) else 1

        # Run single query
        if args.query:
            print(f"\n🤔 Processing query: {args.query}")
            result = _process_single_query(agent, args.query)
            return 0 if result else 1

        # Interactive mode
        if args.interactive:
            return _run_interactive_mode(agent)

        # If no action specified, show usage
        if not any([args.validate, args.quick_demo, args.demo, args.query, args.interactive]):
            print("\n💡 No action specified. Use --help to see available options.")
            print("Quick start: python main.py --validate --quick-demo")
            return 0

    except KeyboardInterrupt:
        print("\n\n👋 Agent interrupted by user")
        logger.info("Agent interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"\n❌ Unexpected error: {e}")
        print("Use --debug for detailed error information")
        return 1


def _print_validation_results(validation: dict) -> None:
    """Print validation results in a user-friendly format."""

    if validation["overall_status"]:
        print("✅ Agent setup validation passed!")
        print(f"   📊 Schema tables loaded: {len(validation.get('configuration', {}).get('schema_tables', []))}")
        print(f"   🔧 Authentication: {validation.get('authentication_method', 'configured')}")
    else:
        print("❌ Agent setup validation failed:")
        for error in validation.get("errors", []):
            print(f"  ❗ {error}")

        if validation.get("warnings"):
            print("\n⚠️  Warnings:")
            for warning in validation["warnings"]:
                print(f"  ⚠️  {warning}")

        print(f"\n📋 Configuration:")
        config = validation.get("configuration", {})
        for key, value in config.items():
            if key != "service_account_path":  # Don't print sensitive paths
                print(f"   {key}: {value}")


def _process_single_query(agent: NL2SQLRedshiftAgent, query: str) -> bool:
    """Process a single query and display results."""

    result = agent.process_question(query)

    if result.get("success"):
        print("✅ Query executed successfully!")
        print(f"📊 Response: {result.get('agent_response')}")

        # Show validation info if available
        if result.get("validation", {}).get("suggestions"):
            print("\n💡 Query suggestions:")
            for suggestion in result["validation"]["suggestions"]:
                print(f"  - {suggestion}")

        # Show compliance check if available
        if result.get("compliance_check", {}).get("violations"):
            print("\n⚠️  Business rule violations detected:")
            for violation in result["compliance_check"]["violations"]:
                print(f"  ⚠️  {violation}")

        return True
    else:
        print("❌ Query execution failed!")
        print(f"🚨 Error: {result.get('error')}")

        # Show suggestions if available
        if result.get("suggestions"):
            print("\n💡 Suggestions:")
            for suggestion in result["suggestions"]:
                print(f"  - {suggestion}")

        return False


def _run_interactive_mode(agent: NL2SQLRedshiftAgent) -> int:
    """Run the agent in interactive mode."""

    print("\n🎯 Interactive Mode - Revolve E-commerce Data Analysis")
    print("=" * 60)
    print("Ask natural language questions about Revolve's business data.")
    print("Examples:")
    print("  - 'Show me top selling brands this quarter'")
    print("  - 'What is the AOV for UK customers?'")
    print("  - 'How many orders were shipped via UPS?'")
    print("\nType 'exit', 'quit', or Ctrl+C to stop.")
    print("Type 'demo' to see example questions.")
    print("=" * 60)

    try:
        while True:
            query = input("\n🤔 Your question: ").strip()

            if not query:
                continue

            if query.lower() in ['exit', 'quit', 'q']:
                break

            if query.lower() in ['demo', 'examples']:
                demo_questions = agent.get_demo_questions()
                print("\n📝 Example questions:")
                for i, q in enumerate(demo_questions[:5], 1):
                    print(f"  {i}. {q}")
                continue

            print(f"\n🔄 Processing: {query}...")
            success = _process_single_query(agent, query)

    except KeyboardInterrupt:
        pass

    print("\n👋 Exiting interactive mode")
    return 0


if __name__ == "__main__":
    sys.exit(main())