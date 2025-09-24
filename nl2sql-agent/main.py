#!/usr/bin/env python3
"""
Main entry point for the NL2SQL Redshift ADK Agent.
"""

import os
import sys
import argparse
from my_agent.agent import create_agent_from_config
from my_agent.demo_queries import run_quick_demo, DemoQueryRunner


def main():
    """Main entry point."""
    
    parser = argparse.ArgumentParser(description="NL2SQL Redshift ADK Agent")
    parser.add_argument("--config", default="config/agent_config.json", 
                       help="Path to agent configuration file")
    parser.add_argument("--project-id", help="GCP Project ID (overrides config)")
    parser.add_argument("--connection", help="Integration Connector name (overrides config)")
    parser.add_argument("--demo", action="store_true", help="Run demo queries")
    parser.add_argument("--quick-demo", action="store_true", help="Run quick demo")
    parser.add_argument("--validate", action="store_true", help="Validate agent setup")
    parser.add_argument("--query", help="Run a single natural language query")
    
    args = parser.parse_args()
    
    # Create agent
    print("🚀 Initializing NL2SQL Redshift Agent...")
    
    try:
        agent = create_agent_from_config(args.config)
        
        # Override config with command line arguments if provided
        if args.project_id:
            agent.project_id = args.project_id
        if args.connection:
            agent.connection = args.connection
            
        print(f"✅ Agent initialized for project: {agent.project_id}")
        print(f"🔗 Using connection: {agent.connection}")
        
    except Exception as e:
        print(f"❌ Failed to initialize agent: {e}")
        return 1
    
    # Validate setup if requested
    if args.validate:
        print("\n🔍 Validating agent setup...")
        validation = agent.validate_setup()
        
        if validation["overall_status"]:
            print("✅ Agent setup validation passed!")
        else:
            print("❌ Agent setup validation failed:")
            for error in validation["errors"]:
                print(f"  - {error}")
            return 1
    
    # Run demos
    if args.quick_demo:
        run_quick_demo(agent)
        return 0
    
    if args.demo:
        demo_runner = DemoQueryRunner(agent)
        results = demo_runner.run_all_demos()
        return 0 if all(r["success"] for r in results) else 1
    
    # Run single query
    if args.query:
        print(f"\n🤔 Processing query: {args.query}")
        result = agent.process_question(args.query)
        
        if result.get("success"):
            print("✅ Query executed successfully!")
            print(f"📊 Response: {result.get('agent_response')}")
            return 0
        else:
            print("❌ Query execution failed!")
            print(f"🚨 Error: {result.get('error')}")
            return 1
    
    # Interactive mode
    print("\n🎯 Entering interactive mode. Type 'quit' to exit.")
    print("Ask any question about the retail data...")
    
    while True:
        try:
            question = input("\n❓ Your question: ").strip()
            
            if question.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
                
            if not question:
                continue
                
            print(f"🤔 Processing: {question}")
            result = agent.process_question(question)
            
            if result.get("success"):
                print(f"✅ {result.get('agent_response')}")
            else:
                print(f"❌ Error: {result.get('error')}")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())