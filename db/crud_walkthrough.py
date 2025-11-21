"""
DuckDB Client - Full CRUD Operations Demo

This script demonstrates all CRUD (Create, Read, Update, Delete) operations
that you can perform on the remote DuckDB database.

CRUD Operations:
- CREATE: Create tables, insert data
- READ: Query data
- UPDATE: Modify existing data
- DELETE: Remove data or tables
"""

from duckdb_client import DuckDBClient

# ============================================
# CONFIGURATION - EDIT THESE VALUES
# ============================================

BASE_URL = "https://duckdb.straddlyze.com"
API_TOKEN = "token-vGG2vbS8IyEVYct5g6jFqQ"  # Replace with your assigned token

# ============================================
# CRUD DEMO
# ============================================

def main():
    client = DuckDBClient(base_url=BASE_URL, token=API_TOKEN)

    print("=" * 70)
    print("DuckDB CRUD Operations - Complete Walkthrough")
    print("=" * 70)
    print()

    # ========================================
    # PART 1: CREATE Operations
    # ========================================

    print("█" * 70)
    print("PART 1: CREATE Operations")
    print("█" * 70)
    print()

    # CREATE - Step 1: Create a new table
    print("1.1 Creating a new table: 'my_test_table'")
    print("-" * 70)
    client.execute("""
        CREATE OR REPLACE TABLE my_test_table (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            salary DOUBLE,
            created_at TIMESTAMP
        )
    """)
    print()

    # CREATE - Step 2: Insert single row
    print("1.2 Inserting a single row")
    print("-" * 70)
    client.execute("""
        INSERT INTO my_test_table VALUES
        (1, 'Alice Johnson', 'alice@email.com', 28, 75000.50, CURRENT_TIMESTAMP)
    """)
    print()

    # CREATE - Step 3: Insert multiple rows
    print("1.3 Inserting multiple rows at once")
    print("-" * 70)
    client.execute("""
        INSERT INTO my_test_table VALUES
        (2, 'Bob Smith', 'bob@email.com', 35, 85000.00, CURRENT_TIMESTAMP),
        (3, 'Charlie Brown', 'charlie@email.com', 42, 95000.75, CURRENT_TIMESTAMP),
        (4, 'Diana Prince', 'diana@email.com', 31, 88000.25, CURRENT_TIMESTAMP),
        (5, 'Eve Anderson', 'eve@email.com', 29, 72000.00, CURRENT_TIMESTAMP)
    """)
    print()

    # ========================================
    # PART 2: READ Operations
    # ========================================

    print("█" * 70)
    print("PART 2: READ Operations")
    print("█" * 70)
    print()

    # READ - Step 1: Read all data
    print("2.1 Reading all data from the table")
    print("-" * 70)
    df = client.query("SELECT * FROM my_test_table")
    print(df)
    print()

    # READ - Step 2: Read specific columns
    print("2.2 Reading specific columns (name and email)")
    print("-" * 70)
    df = client.query("SELECT id, name, email FROM my_test_table")
    print(df)
    print()

    # READ - Step 3: Read with WHERE clause
    print("2.3 Reading with filtering (age > 30)")
    print("-" * 70)
    df = client.query("""
        SELECT name, age, salary
        FROM my_test_table
        WHERE age > 30
        ORDER BY age DESC
    """)
    print(df)
    print()

    # READ - Step 4: Aggregation queries
    print("2.4 Aggregation: Average salary and count")
    print("-" * 70)
    df = client.query("""
        SELECT
            COUNT(*) as total_employees,
            ROUND(AVG(age), 2) as avg_age,
            ROUND(AVG(salary), 2) as avg_salary,
            ROUND(MIN(salary), 2) as min_salary,
            ROUND(MAX(salary), 2) as max_salary
        FROM my_test_table
    """)
    print(df)
    print()

    # READ - Step 5: Conditional queries
    print("2.5 Reading with CASE statements (salary categories)")
    print("-" * 70)
    df = client.query("""
        SELECT
            name,
            salary,
            CASE
                WHEN salary >= 90000 THEN 'High'
                WHEN salary >= 80000 THEN 'Medium'
                ELSE 'Entry Level'
            END as salary_category
        FROM my_test_table
        ORDER BY salary DESC
    """)
    print(df)
    print()

    # ========================================
    # PART 3: UPDATE Operations
    # ========================================

    print("█" * 70)
    print("PART 3: UPDATE Operations")
    print("█" * 70)
    print()

    # UPDATE - Step 1: Update single row
    print("3.1 Updating a single employee's salary (Alice gets a raise!)")
    print("-" * 70)
    client.execute("""
        UPDATE my_test_table
        SET salary = 82000.00
        WHERE name = 'Alice Johnson'
    """)
    print()

    # READ to verify the update
    print("Verifying the update:")
    df = client.query("SELECT name, salary FROM my_test_table WHERE name = 'Alice Johnson'")
    print(df)
    print()

    # UPDATE - Step 2: Update multiple rows
    print("3.2 Updating multiple employees (10% raise for everyone over 30)")
    print("-" * 70)
    client.execute("""
        UPDATE my_test_table
        SET salary = salary * 1.10
        WHERE age > 30
    """)
    print()

    # READ to verify the update
    print("Verifying the bulk update:")
    df = client.query("""
        SELECT name, age, ROUND(salary, 2) as salary
        FROM my_test_table
        ORDER BY age DESC
    """)
    print(df)
    print()

    # UPDATE - Step 3: Update with calculated values
    print("3.3 Adding a bonus column and calculating values")
    print("-" * 70)

    # First, add the column
    client.execute("""
        ALTER TABLE my_test_table
        ADD COLUMN bonus DOUBLE
    """)

    # Then update with calculated values
    client.execute("""
        UPDATE my_test_table
        SET bonus = salary * 0.15
    """)
    print()

    print("Verifying the bonus calculation:")
    df = client.query("""
        SELECT name, ROUND(salary, 2) as salary, ROUND(bonus, 2) as bonus
        FROM my_test_table
    """)
    print(df)
    print()

    # ========================================
    # PART 4: DELETE Operations
    # ========================================

    print("█" * 70)
    print("PART 4: DELETE Operations")
    print("█" * 70)
    print()

    # DELETE - Step 1: Delete a single row
    print("4.1 Deleting a single employee (Eve is leaving)")
    print("-" * 70)
    client.execute("""
        DELETE FROM my_test_table
        WHERE name = 'Eve Anderson'
    """)
    print()

    print("Verifying deletion:")
    df = client.query("SELECT name FROM my_test_table ORDER BY id")
    print(df)
    print()

    # DELETE - Step 2: Delete with condition
    print("4.2 Deleting employees with salary > 100000")
    print("-" * 70)
    client.execute("""
        DELETE FROM my_test_table
        WHERE salary > 100000
    """)
    print()

    print("Remaining employees:")
    df = client.query("SELECT * FROM my_test_table ORDER BY id")
    print(df)
    print()

    # ========================================
    # PART 5: Advanced CREATE Operations
    # ========================================

    print("█" * 70)
    print("PART 5: Advanced Operations - Creating Views and Temporary Tables")
    print("█" * 70)
    print()

    # CREATE VIEW
    print("5.1 Creating a VIEW for high earners")
    print("-" * 70)
    client.execute("""
        CREATE OR REPLACE VIEW high_earners AS
        SELECT
            name,
            ROUND(salary, 2) as salary,
            ROUND(bonus, 2) as bonus,
            ROUND(salary + bonus, 2) as total_compensation
        FROM my_test_table
        WHERE salary > 80000
    """)
    print()

    print("Querying the view:")
    df = client.query("SELECT * FROM high_earners")
    print(df)
    print()

    # CREATE TEMPORARY TABLE from query
    print("5.2 Creating a summary table from aggregation")
    print("-" * 70)
    client.execute("""
        CREATE OR REPLACE TABLE salary_summary AS
        SELECT
            CASE
                WHEN age < 30 THEN '20s'
                WHEN age < 40 THEN '30s'
                ELSE '40s+'
            END as age_group,
            COUNT(*) as employee_count,
            ROUND(AVG(salary), 2) as avg_salary
        FROM my_test_table
        GROUP BY age_group
    """)
    print()

    print("Querying the summary table:")
    df = client.query("SELECT * FROM salary_summary ORDER BY age_group")
    print(df)
    print()

    # ========================================
    # PART 6: Working with Existing Data
    # ========================================

    print("█" * 70)
    print("PART 6: Combining Your Data with Existing Database Tables")
    print("█" * 70)
    print()

    print("6.1 Creating a table with electricity zone preferences")
    print("-" * 70)
    client.execute("""
        CREATE OR REPLACE TABLE employee_zones AS
        SELECT
            1 as employee_id, 'Alice Johnson' as name, 'BR' as favorite_zone UNION ALL
        SELECT 2, 'Bob Smith', 'FR' UNION ALL
        SELECT 3, 'Charlie Brown', 'US-NY-NYIS' UNION ALL
        SELECT 4, 'Diana Prince', 'US-FLA-FPL'
    """)
    print()

    print("6.2 Joining with the hourly_data to get their zone's statistics")
    print("-" * 70)
    df = client.query("""
        SELECT
            ez.name,
            ez.favorite_zone,
            ds.record_count,
            ds.start_date,
            ds.end_date
        FROM employee_zones ez
        JOIN data_summary ds ON ez.favorite_zone = ds.zone
        WHERE ds.year = 2024
    """)
    print(df)
    print()

    print("6.3 Getting actual carbon metrics for their favorite zones")
    print("-" * 70)
    df = client.query("""
        SELECT
            ez.name,
            ez.favorite_zone,
            ROUND(AVG(h.carbon_direct), 2) as avg_carbon_direct,
            ROUND(AVG(h.carbon_lifecycle), 2) as avg_carbon_lifecycle,
            ROUND(AVG(h.cfe_pct), 2) as avg_cfe_pct,
            ROUND(AVG(h.re_pct), 2) as avg_re_pct,
            CASE WHEN COUNT(h.cfe_pct) > 0 THEN 'Complete Data' ELSE 'Partial Data' END as data_status
        FROM employee_zones ez
        JOIN hourly_data h ON ez.favorite_zone = h.zone
        WHERE h.year = 2024
        GROUP BY ez.name, ez.favorite_zone
        ORDER BY ez.name
    """)
    print(df)
    print()

    # ========================================
    # PART 7: Cleanup
    # ========================================

    print("█" * 70)
    print("PART 7: Cleanup - Dropping Tables and Views")
    print("█" * 70)
    print()

    print("7.1 Dropping the view")
    print("-" * 70)
    client.execute("DROP VIEW IF EXISTS high_earners")
    print()

    print("7.2 Dropping all test tables")
    print("-" * 70)
    client.execute("DROP TABLE IF EXISTS my_test_table")
    client.execute("DROP TABLE IF EXISTS salary_summary")
    client.execute("DROP TABLE IF EXISTS employee_zones")
    print()

    print("7.3 Verifying cleanup - Current database tables:")
    print("-" * 70)
    info = client.get_info()
    print(f"\nTotal tables remaining: {info['table_count']}")
    for table in info['tables']:
        print(f"  - {table['table']:20s} {table['rows']:>10,} rows")
    print()

    # ========================================
    # SUMMARY
    # ========================================

    print("=" * 70)
    print("CRUD Operations Summary")
    print("=" * 70)
    print("""
✓ CREATE Operations:
  - Created tables with CREATE TABLE
  - Inserted single rows with INSERT INTO ... VALUES
  - Inserted multiple rows at once
  - Created views with CREATE VIEW
  - Created tables from queries with CREATE TABLE AS

✓ READ Operations:
  - Selected all columns with SELECT *
  - Selected specific columns
  - Filtered with WHERE clauses
  - Performed aggregations (COUNT, AVG, MIN, MAX)
  - Used CASE statements for conditional logic
  - Joined tables together

✓ UPDATE Operations:
  - Updated single rows
  - Updated multiple rows with conditions
  - Added new columns with ALTER TABLE
  - Updated with calculated values

✓ DELETE Operations:
  - Deleted single rows
  - Deleted multiple rows with conditions
  - Dropped tables and views

All CRUD operations completed successfully! 🎉
    """)
    print("=" * 70)


if __name__ == "__main__":
    main()
