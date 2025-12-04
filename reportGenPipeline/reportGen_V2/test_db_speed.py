import psycopg2
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv('/Users/ankushkapoor/Desktop/Rocket Frog/ReportGen/reportGenPipeline/reportGen_V2/.env')

db_password = os.getenv("DB_PASSWORD")

print("Testing database connection and insert speed...")

try:
    # Azure connection (commented)
    # conn = psycopg2.connect(
    #     host="qv-repgendev-psql1.postgres.database.azure.com",
    #     port="5432",
    #     database="test_reportgen",
    #     user="sondbadmin",
    #     password=db_password,
    #     connect_timeout=10
    # )

    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        database="test_reportgen",
        user="postgres",
        password="titandevil@12",
        connect_timeout=10
    )

    print("✓ Connected to PostgreSQL")

    cur = conn.cursor()

    # Test 1: Simple query
    start = time.time()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"✓ Simple query: {(time.time() - start)*1000:.1f}ms")

    # Test 2: Check table exists & row count
    cur.execute("SELECT COUNT(*) FROM new_car_wash_clearwater_latest")
    count = cur.fetchone()[0]
    print(f"✓ Current row count in table: {count:,}")

    # Test 3: Insert small batch
    test_data = [('test1', 'test2')] * 1000
    start = time.time()
    cur.executemany(
        "INSERT INTO new_car_wash_clearwater_latest (client_id, transaction_id) VALUES (%s, %s)",
        test_data
    )
    conn.commit()
    duration = time.time() - start
    print(f"✓ Inserted 1,000 test rows in {duration:.2f}s ({1000/duration:.0f} rows/sec)")

    # Clean up
    cur.execute("DELETE FROM new_car_wash_clearwater_latest WHERE client_id = 'test1'")
    conn.commit()
    print("✓ Cleaned up test data")

    cur.close()
    conn.close()

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
