# NL2SQL ADK Agent for Redshift

This agent converts natural language questions to SQL queries and executes them against AWS Redshift databases through GCP Integration Connectors.

## Architecture

```
User (Natural Language) → Agent Space → ADK Agent → Integration Connector → AWS Redshift → Results
```

## Prerequisites

1. **GCP Project Setup**
   - Enable required APIs: `connectors.googleapis.com`, `integrations.googleapis.com`, `aiplatform.googleapis.com`
   - Create service account with proper roles (see CLAUDE.md for details)

2. **AWS Redshift Setup** 
   - Create Redshift cluster in us-east-1
   - Configure VPC for Private Service Connect
   - Load demo data (products, sales, customers tables)

3. **Integration Connector Setup**
   - Create Redshift Integration Connector in us-central1
   - Configure connection parameters in GCP console
   - Test connectivity

## Installation & Setup

1. **Install Dependencies**
   ```bash
   cd nl2sql-agent
   pip install -r requirements.txt
   ```

2. **Configure Settings**
   ```bash
   # Edit config/agent_config.json with your settings
   {
     "project_id": "your-gcp-project-id",
     "location": "us-central1", 
     "connection": "redshift-demo-connection",
     "model": "gemini-2.0-flash"
   }
   ```

3. **Set up Service Account** (Optional)
   ```bash
   # Place your service account JSON in config/service_account.json
   # Or use environment-based authentication
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
   ```

## Execution Steps

### 1. Validate Setup
```bash
python main.py --validate
```

### 2. Run Quick Demo (Core Queries)
```bash
python main.py --quick-demo
```

### 3. Run Full Demo Suite
```bash
python main.py --demo
```

### 4. Execute Single Query
```bash
python main.py --query "How many apparels were sold in the last quarter?"
```

### 5. Interactive Mode
```bash
python main.py
# Then type questions interactively
```

### 6. Deploy to Agent Space
```bash
# Follow GCP Agent Space deployment guide
# Use the agent.py implementation with ADK
```

## Demo Queries

**Primary Demo Questions:**
- "How many apparels were sold in the last quarter?"
- "What are the top 5 selling apparel brands?"
- "Show sales by region for electronics"
- "Which customers bought the most items?"

**Additional Test Queries:**
- "What is the average price of products by category?"
- "How many customers registered this year?"
- "Which region has the highest total sales?"
- "Show monthly sales trends for the last 6 months"

## Expected Data Schema

```sql
-- Products table (~1000 records)
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50), -- 'Apparel', 'Electronics', 'Home'
    subcategory VARCHAR(50), -- 'Shirts', 'Pants', 'Shoes'
    brand VARCHAR(50),
    price DECIMAL(10,2)
);

-- Sales table (~5000 records)
CREATE TABLE sales (
    sale_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    quantity INT,
    sale_date DATE,
    sale_amount DECIMAL(10,2),
    region VARCHAR(50)
);

-- Customers table (~500 records)  
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE,
    region VARCHAR(50)
);
```

## Troubleshooting

**Common Issues:**
1. **Authentication Error**: Ensure service account has proper roles
2. **Connection Failed**: Verify Integration Connector is active and configured
3. **Query Failures**: Check Redshift cluster is running and accessible
4. **No Results**: Verify demo data is loaded in the database

**Debug Commands:**
```bash
# Test with validation
python main.py --validate --query "SELECT 1"

# Check agent configuration
python -c "from my_agent.agent import create_agent_from_config; agent = create_agent_from_config(); print(agent.validate_setup())"
```

## Project Structure

- `my_agent/agent.py` - Main ADK agent implementation
- `my_agent/tools.py` - Integration connector tools  
- `my_agent/sql_helper.py` - SQL generation and schema utilities
- `my_agent/demo_queries.py` - Sample queries for testing
- `config/agent_config.json` - Agent configuration
- `main.py` - Command-line interface
- `requirements.txt` - Python dependencies

## Integration with Original Open Data QnA

This agent is built using components extracted from the main Open Data QnA codebase but simplified for ADK integration. The original codebase (`agents/`, `dbconnectors/`, etc.) remains functional for other use cases.

Key differences:
- Uses ADK ApplicationIntegrationToolset instead of direct database connections
- Simplified for Redshift-specific use case
- Focused on retail/apparel demo scenario
- Integrated with GCP Agent Space deployment