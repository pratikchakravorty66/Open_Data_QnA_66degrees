"""
Demo queries and test cases for the NL2SQL Redshift Agent.
"""

from typing import List, Dict
from .agent import NL2SQLRedshiftAgent


class DemoQueryRunner:
    """
    Runs demo queries to test and validate the NL2SQL agent.
    """
    
    def __init__(self, agent: NL2SQLRedshiftAgent):
        self.agent = agent
    
    def get_demo_queries(self) -> List[Dict]:
        """
        Get comprehensive demo queries for testing the agent.
        """
        
        queries = [
            {
                "category": "Revolve Orders Analysis",
                "question": "Show me the number of Revolve orders and AOV for the United Kingdom between 8/25/22 - 8/24/23 vs. 8/25/21 - 8/24/22, split by Category",
                "expected_tables": ["bi_report.shipmentnumber_rs", "mars__revolveclothing_com___db.product", "mars__id.id_categorynames2"],
                "expected_operations": ["JOIN", "COUNT", "AVG", "CASE", "GROUP BY", "date filtering"],
                "business_context": "Primary client test case - year-over-year Revolve AOV analysis by category for UK market",
                "business_rules": ["Use site <> 'F' for Revolve orders", "Use shipmentnumber_rs for product analysis", "Proper category mapping"]
            },
            {
                "category": "High Value Customer Analysis",
                "question": "Get top 10 brands and categories based on projected net sales for top 5% high value customers",
                "expected_tables": ["bi_report.ordernumber_rs", "bi_report.shipmentnumber_rs", "mars__revolveclothing_com___db.product", "mars__id.id_categorynames2"],
                "expected_operations": ["CTE", "PERCENT_RANK", "JOIN", "GROUP BY", "ORDER BY", "LIMIT"],
                "business_context": "High-value customer segmentation and brand performance analysis",
                "business_rules": ["Use PERCENT_RANK for top 5%", "Use projnetsales_shipped metric", "Proper product code mapping"]
            },
            {
                "category": "Payment Analysis",
                "question": "Show number of transactions and average monthly gross sales through ANET excluding ApplePay orders",
                "expected_tables": ["bi_report.ordernumber_rs", "mars__revolveclothing_com___db.orders"],
                "expected_operations": ["JOIN", "COUNT", "AVG", "GROUP BY", "EXTRACT", "WHERE"],
                "business_context": "Payment method analysis excluding specific token services",
                "business_rules": ["Join with orders table for paymenttokenservice", "Filter paymenttype = 'ANET'", "Exclude ApplePay"]
            },
            {
                "category": "Shipping Loss Analysis",
                "question": "Analyze Ontrac and UPS loss rates by order value, include signature required filter",
                "expected_tables": ["bi_report.shipmentnumber_rs", "mars__revolveclothing_com___db.orders", "mars__revolveclothing_com___db.shipment", "mars__id.shipping_pickuptime"],
                "expected_operations": ["JOIN", "CASE", "COUNT", "SUM", "GROUP BY", "calculation"],
                "business_context": "Shipping carrier performance analysis with loss rate calculations",
                "business_rules": ["Use 'lost package' not 'lost'", "Use shipping_pickuptime for accurate carrier info", "Include sigrequired"]
            },
            {
                "category": "Customer Survey Sampling",
                "question": "Get 5K random REVOLVE customers who made purchases in the last 12 months with their last transaction details",
                "expected_tables": ["bi_report.ordernumber_rs"],
                "expected_operations": ["ROW_NUMBER", "RANDOM", "ORDER BY", "LIMIT", "date filtering"],
                "business_context": "Random customer sampling for survey purposes",
                "business_rules": ["Use RANDOM() for sampling", "Get last transaction per customer", "Site = 'R' for Revolve"]
            },
            {
                "category": "Brand Performance",
                "question": "What are the top selling brands this quarter by projected net sales?",
                "expected_tables": ["bi_report.shipmentnumber_rs", "mars__revolveclothing_com___db.product"],
                "expected_operations": ["JOIN", "GROUP BY", "SUM", "ORDER BY", "date filtering"],
                "business_context": "Quarterly brand performance tracking",
                "business_rules": ["Use shipmentnumber_rs for product data", "Use projnetsales_shipped metric"]
            },
            {
                "category": "Regional Analysis",
                "question": "Which shipping countries have the highest net sales for the current quarter?",
                "expected_tables": ["bi_report.shipmentnumber_rs"],
                "expected_operations": ["GROUP BY", "SUM", "ORDER BY", "date filtering"],
                "business_context": "Geographic performance analysis",
                "business_rules": ["Use shippingcountry field", "Current quarter filtering"]
            },
            {
                "category": "Time Series Analysis",
                "question": "Show monthly sales trends for the last 6 months using net sales",
                "expected_tables": ["bi_report.ordernumber_rs"],
                "expected_operations": ["GROUP BY", "SUM", "EXTRACT", "ORDER BY", "date filtering"],
                "business_context": "Monthly sales trending analysis",
                "business_rules": ["Use netsales from ordernumber_rs", "6 months date filtering"]
            },
            {
                "category": "Category Performance",
                "question": "Compare average order value by product category for Revolve orders",
                "expected_tables": ["bi_report.shipmentnumber_rs", "mars__revolveclothing_com___db.product", "mars__id.id_categorynames2"],
                "expected_operations": ["JOIN", "GROUP BY", "AVG", "category mapping"],
                "business_context": "Category-wise AOV comparison for business insights",
                "business_rules": ["Use site <> 'F'", "Category mapping via product code", "Use shipmentnumber_rs for product analysis"]
            },
            {
                "category": "Customer Retention",
                "question": "Find customers who made multiple purchases in the last year",
                "expected_tables": ["bi_report.ordernumber_rs"],
                "expected_operations": ["GROUP BY", "COUNT", "HAVING", "date filtering"],
                "business_context": "Customer loyalty and retention analysis",
                "business_rules": ["Count distinct transactions per customer", "12 month date filtering"]
            }
        ]
        
        return queries
    
    def run_single_demo(self, query_info: Dict, verbose: bool = True) -> Dict:
        """
        Run a single demo query and return results.
        
        Args:
            query_info: Dictionary containing query information
            verbose: Whether to print detailed output
            
        Returns:
            Dictionary with query results and analysis
        """
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"Demo Query: {query_info['category']}")
            print(f"Question: {query_info['question']}")
            print(f"Context: {query_info['business_context']}")
            print(f"{'='*60}")
        
        # Execute the query using the agent
        result = self.agent.process_question(query_info["question"])
        
        # Analyze the results
        analysis = {
            "query_info": query_info,
            "execution_result": result,
            "success": result.get("success", False),
            "analysis": self._analyze_result(result, query_info)
        }
        
        if verbose:
            self._print_result_analysis(analysis)
        
        return analysis
    
    def run_all_demos(self, verbose: bool = True) -> List[Dict]:
        """
        Run all demo queries and return comprehensive results.
        """
        
        demo_queries = self.get_demo_queries()
        results = []
        
        if verbose:
            print(f"\nRunning {len(demo_queries)} demo queries...")
        
        for i, query_info in enumerate(demo_queries, 1):
            if verbose:
                print(f"\n[{i}/{len(demo_queries)}] Running demo query...")
            
            result = self.run_single_demo(query_info, verbose=verbose)
            results.append(result)
        
        # Generate summary
        if verbose:
            self._print_summary(results)
        
        return results
    
    def _analyze_result(self, result: Dict, query_info: Dict) -> Dict:
        """
        Analyze the execution result against expected outcomes.
        """
        
        analysis = {
            "executed_successfully": result.get("success", False),
            "error_message": result.get("error", ""),
            "expected_tables_check": "pending",
            "expected_operations_check": "pending",
            "response_quality": "pending"
        }
        
        if result.get("success"):
            # Check if expected tables were likely used
            response_text = str(result.get("agent_response", "")).lower()
            expected_tables = query_info.get("expected_tables", [])
            
            tables_found = 0
            for table in expected_tables:
                if table.lower() in response_text:
                    tables_found += 1
            
            analysis["expected_tables_check"] = f"{tables_found}/{len(expected_tables)} tables referenced"
            
            # Check for expected operations
            expected_ops = query_info.get("expected_operations", [])
            ops_found = 0
            for op in expected_ops:
                if op.lower() in response_text:
                    ops_found += 1
            
            analysis["expected_operations_check"] = f"{ops_found}/{len(expected_ops)} operations detected"
            
            # Basic response quality check
            if len(response_text) > 50:  # Reasonable response length
                analysis["response_quality"] = "adequate"
            else:
                analysis["response_quality"] = "too_short"
        
        return analysis
    
    def _print_result_analysis(self, analysis: Dict):
        """Print detailed analysis of a single query result."""
        
        result = analysis["execution_result"]
        query_analysis = analysis["analysis"]
        
        if result.get("success"):
            print("✅ Query executed successfully")
            print(f"📊 Agent Response: {result.get('agent_response', 'No response')}")
            print(f"🔍 Expected Tables: {query_analysis['expected_tables_check']}")
            print(f"⚙️  Expected Operations: {query_analysis['expected_operations_check']}")
            print(f"📝 Response Quality: {query_analysis['response_quality']}")
        else:
            print("❌ Query execution failed")
            print(f"🚨 Error: {result.get('error', 'Unknown error')}")
    
    def _print_summary(self, results: List[Dict]):
        """Print summary of all demo query results."""
        
        total_queries = len(results)
        successful_queries = sum(1 for r in results if r["success"])
        
        print(f"\n{'='*60}")
        print("DEMO SUMMARY")
        print(f"{'='*60}")
        print(f"Total Queries: {total_queries}")
        print(f"Successful: {successful_queries}")
        print(f"Failed: {total_queries - successful_queries}")
        print(f"Success Rate: {(successful_queries/total_queries)*100:.1f}%")
        
        # Category breakdown
        categories = {}
        for result in results:
            category = result["query_info"]["category"]
            if category not in categories:
                categories[category] = {"total": 0, "success": 0}
            categories[category]["total"] += 1
            if result["success"]:
                categories[category]["success"] += 1
        
        print(f"\nCategory Breakdown:")
        for category, stats in categories.items():
            success_rate = (stats["success"]/stats["total"])*100
            print(f"  {category}: {stats['success']}/{stats['total']} ({success_rate:.1f}%)")
        
        # Failed queries
        failed_queries = [r for r in results if not r["success"]]
        if failed_queries:
            print(f"\nFailed Queries:")
            for result in failed_queries:
                query = result["query_info"]["question"]
                error = result["execution_result"].get("error", "Unknown error")
                print(f"  ❌ '{query}' - {error}")
    
    def get_test_report(self, results: List[Dict]) -> Dict:
        """
        Generate a structured test report from demo results.
        """
        
        total_queries = len(results)
        successful_queries = sum(1 for r in results if r["success"])
        
        report = {
            "timestamp": str(pd.Timestamp.now()),
            "summary": {
                "total_queries": total_queries,
                "successful_queries": successful_queries,
                "failed_queries": total_queries - successful_queries,
                "success_rate": (successful_queries/total_queries)*100 if total_queries > 0 else 0
            },
            "detailed_results": results,
            "recommendations": []
        }
        
        # Generate recommendations
        if report["summary"]["success_rate"] < 80:
            report["recommendations"].append("Success rate below 80% - review agent configuration and prompts")
        
        if any(not r["success"] for r in results):
            report["recommendations"].append("Some queries failed - check Integration Connector connectivity")
        
        return report


def run_quick_demo(agent: NL2SQLRedshiftAgent) -> None:
    """
    Run a quick demo with the most important queries.
    """
    
    demo_runner = DemoQueryRunner(agent)
    
    # Core demo queries from the implementation plan
    core_queries = [
        "How many apparels were sold in the last quarter?",
        "What are the top 5 selling apparel brands?", 
        "Show sales by region for electronics",
        "Which customers bought the most items?"
    ]
    
    print("🚀 Running Quick Demo with Core Queries")
    print("="*60)
    
    for i, question in enumerate(core_queries, 1):
        print(f"\n[{i}/{len(core_queries)}] {question}")
        print("-" * 40)
        
        result = agent.process_question(question)
        
        if result.get("success"):
            print("✅ Success!")
            print(f"Response: {result.get('agent_response', 'No response')}")
        else:
            print("❌ Failed!")
            print(f"Error: {result.get('error', 'Unknown error')}")
    
    print("\n🎉 Quick demo completed!")