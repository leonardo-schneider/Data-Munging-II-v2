"""
Simple DuckDB Client for Remote Access - For Classmates/Colleagues

This is a simplified version that can be configured at the top of the file.
No .env file needed!
"""

import requests
import pandas as pd
import time

# ============================================
# CONFIGURATION - EDIT THESE VALUES
# ============================================

BASE_URL = "https://duckdb.straddlyze.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ"  # Replace with your assigned token

# ============================================


class DuckDBClient:
    def __init__(self, base_url: str, token: str):
        """
        Initialize client

        Args:
            base_url: The public URL of the DuckDB server
            token: Your API token
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    def health_check(self) -> dict:
        """Check if server is healthy"""
        time.sleep(3.0)  # Delay to prevent connection conflicts

        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{self.base_url}/health")
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                # Check if it's a retryable error
                should_retry = False
                if e.response.status_code in [404, 502, 503]:
                    should_retry = True
                elif e.response.status_code == 400:
                    # Retry if it's a connection error (transient issue)
                    try:
                        error_details = e.response.json()
                        if 'Connection Error' in str(error_details.get('detail', '')):
                            should_retry = True
                    except:
                        pass

                if attempt < max_retries - 1 and should_retry:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"⚠ Request failed, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    # Print the actual error message from the API
                    try:
                        error_details = e.response.json()
                        print(f"\n❌ Error {e.response.status_code}: {error_details}")
                    except:
                        print(f"\n❌ Error {e.response.status_code}: {e.response.text}")
                    raise

    def list_tables(self) -> pd.DataFrame:
        """List all available tables"""
        time.sleep(3.0)  # Delay to prevent connection conflicts

        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/tables",
                    headers=self.headers
                )
                response.raise_for_status()
                data = response.json()
                return pd.DataFrame(data["tables"])
            except requests.exceptions.HTTPError as e:
                # Check if it's a retryable error
                should_retry = False
                if e.response.status_code in [404, 502, 503]:
                    should_retry = True
                elif e.response.status_code == 400:
                    # Retry if it's a connection error (transient issue)
                    try:
                        error_details = e.response.json()
                        if 'Connection Error' in str(error_details.get('detail', '')):
                            should_retry = True
                    except:
                        pass

                if attempt < max_retries - 1 and should_retry:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"⚠ Request failed, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    # Print the actual error message from the API
                    try:
                        error_details = e.response.json()
                        print(f"\n❌ Error {e.response.status_code}: {error_details}")
                    except:
                        print(f"\n❌ Error {e.response.status_code}: {e.response.text}")
                    raise

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SELECT query

        Args:
            sql: SQL query string

        Returns:
            pandas DataFrame
        """
        time.sleep(3.0)  # Delay to prevent connection conflicts

        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/query",
                    json={"query": sql},
                    headers=self.headers
                )
                response.raise_for_status()
                data = response.json()

                if data['rows'] > 0:
                    print(f"✓ Query returned {data['rows']} rows")
                    return pd.DataFrame(data["data"])
                else:
                    print("✓ Query executed successfully (0 rows)")
                    return pd.DataFrame()
            except requests.exceptions.HTTPError as e:
                # Check if it's a retryable error
                should_retry = False
                if e.response.status_code in [404, 502, 503]:
                    should_retry = True
                elif e.response.status_code == 400:
                    # Retry if it's a connection error (transient issue)
                    try:
                        error_details = e.response.json()
                        if 'Connection Error' in str(error_details.get('detail', '')):
                            should_retry = True
                    except:
                        pass

                if attempt < max_retries - 1 and should_retry:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"⚠ Request failed, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    # Print the actual error message from the API
                    try:
                        error_details = e.response.json()
                        print(f"\n❌ Error {e.response.status_code}: {error_details}")
                    except:
                        print(f"\n❌ Error {e.response.status_code}: {e.response.text}")
                    raise

    def execute(self, sql: str) -> dict:
        """
        Execute INSERT, UPDATE, DELETE, CREATE, DROP (WRITE operations)

        Args:
            sql: SQL statement string

        Returns:
            dict with execution result
        """
        time.sleep(3.0)  # Delay to prevent connection conflicts

        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"{self.base_url}/execute",
                    json={"query": sql},
                    headers=self.headers
                )
                response.raise_for_status()
                result = response.json()
                print(f"✓ {result['message']}")
                return result
            except requests.exceptions.HTTPError as e:
                # Check if it's a retryable error
                should_retry = False
                if e.response.status_code in [404, 502, 503]:
                    should_retry = True
                elif e.response.status_code == 400:
                    # Retry if it's a connection error (transient issue)
                    try:
                        error_details = e.response.json()
                        if 'Connection Error' in str(error_details.get('detail', '')):
                            should_retry = True
                    except:
                        pass

                if attempt < max_retries - 1 and should_retry:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"⚠ Request failed, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    # Print the actual error message from the API
                    try:
                        error_details = e.response.json()
                        print(f"\n❌ Error {e.response.status_code}: {error_details}")
                    except:
                        print(f"\n❌ Error {e.response.status_code}: {e.response.text}")
                    raise

    def get_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema for a specific table"""
        time.sleep(3.0)  # Delay to prevent connection conflicts

        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/schema/{table_name}",
                    headers=self.headers
                )
                response.raise_for_status()
                data = response.json()

                print(f"Table: {data['table']}")
                print(f"Rows: {data['row_count']:,}")
                print("\nSchema:")
                return pd.DataFrame(data["schema"])
            except requests.exceptions.HTTPError as e:
                # Check if it's a retryable error
                should_retry = False
                if e.response.status_code in [404, 502, 503]:
                    should_retry = True
                elif e.response.status_code == 400:
                    # Retry if it's a connection error (transient issue)
                    try:
                        error_details = e.response.json()
                        if 'Connection Error' in str(error_details.get('detail', '')):
                            should_retry = True
                    except:
                        pass

                if attempt < max_retries - 1 and should_retry:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"⚠ Request failed, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    # Print the actual error message from the API
                    try:
                        error_details = e.response.json()
                        print(f"\n❌ Error {e.response.status_code}: {error_details}")
                    except:
                        print(f"\n❌ Error {e.response.status_code}: {e.response.text}")
                    raise

    def get_info(self) -> dict:
        """Get overall database information"""
        time.sleep(3.0)  # Delay to prevent connection conflicts

        max_retries = 10
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/info",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                # Check if it's a retryable error
                should_retry = False
                if e.response.status_code in [404, 502, 503]:
                    should_retry = True
                elif e.response.status_code == 400:
                    # Retry if it's a connection error (transient issue)
                    try:
                        error_details = e.response.json()
                        if 'Connection Error' in str(error_details.get('detail', '')):
                            should_retry = True
                    except:
                        pass

                if attempt < max_retries - 1 and should_retry:
                    wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                    print(f"⚠ Request failed, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    # Print the actual error message from the API
                    try:
                        error_details = e.response.json()
                        print(f"\n❌ Error {e.response.status_code}: {error_details}")
                    except:
                        print(f"\n❌ Error {e.response.status_code}: {e.response.text}")
                    raise


# ============================================
# EXAMPLE USAGE
# ============================================

if __name__ == "__main__":
    # Initialize client with configured values
    client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)

    print("=" * 60)
    print("DuckDB Remote Database Client")
    print("=" * 60)
    print()

    # Test 1: Health check
    print("1. Health Check")
    print("-" * 60)
    health = client.health_check()
    print(f"Status: {health}")
    print()

    # Test 2: Database info
    print("2. Database Info")
    print("-" * 60)
    info = client.get_info()
    print(f"Total tables: {info['table_count']}")
    print("\nAvailable tables:")
    for table in info['tables']:
        print(f"  - {table['table']:20s} {table['rows']:>10,} rows")
    print()

    # Test 3: List tables
    print("3. Available Tables (DataFrame)")
    print("-" * 60)
    tables = client.list_tables()
    print(tables)
    print()

    # Test 4: Query data summary
    print("4. Data Summary by Zone and Year")
    print("-" * 60)
    summary = client.query("SELECT * FROM data_summary ORDER BY zone, year")
    print(summary)
    print()

    # Test 5: Query hourly data for a specific zone with complete data
    print("5. Sample Hourly Data for Florida (US-FLA-FPL) - Complete Data!")
    print("-" * 60)
    fl_data = client.query("""
        SELECT
            datetime_utc,
            zone,
            carbon_direct,
            carbon_lifecycle,
            cfe_pct,
            re_pct
        FROM hourly_data
        WHERE zone = 'US-FLA-FPL' AND year = 2024
        LIMIT 10
    """)
    print(fl_data)
    print()

    # Test 6: Aggregation query with complete data
    print("6. Average Carbon Metrics by Zone (2024)")
    print("-" * 60)
    avg_carbon = client.query("""
        SELECT
            zone,
            COUNT(*) as record_count,
            ROUND(AVG(carbon_direct), 2) as avg_carbon_direct,
            ROUND(AVG(carbon_lifecycle), 2) as avg_carbon_lifecycle,
            ROUND(AVG(cfe_pct), 2) as avg_cfe_pct,
            ROUND(AVG(re_pct), 2) as avg_re_pct
        FROM hourly_data
        WHERE year = 2024
        GROUP BY zone
        ORDER BY zone
    """)
    print(avg_carbon)
    print()

    # Test 7: Compare zones with complete vs partial data
    print("7. Data Completeness Comparison")
    print("-" * 60)
    print("Zones with COMPLETE data (all columns):")
    complete = client.query("""
        SELECT zone, COUNT(*) as records
        FROM hourly_data
        WHERE cfe_pct IS NOT NULL
        GROUP BY zone
        ORDER BY zone
    """)
    print(complete)
    print()

    print("Zones with PARTIAL data (carbon_direct only):")
    partial = client.query("""
        SELECT zone, COUNT(*) as records
        FROM hourly_data
        WHERE cfe_pct IS NULL
        GROUP BY zone
        ORDER BY zone
    """)
    print(partial)
    print()

    print("=" * 60)
    print("✅ All tests completed successfully!")
    print("=" * 60)
