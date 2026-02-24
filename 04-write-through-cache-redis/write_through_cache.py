# Requires: pip install redis psycopg2-binary
import psycopg2
import redis
import random
import time

DB_NAME = "testdb"
DB_USER = "test"
DB_PASSWORD = "test"
DB_HOST = "localhost"
DB_PORT = 6432
REDIS_HOST = "localhost"
REDIS_PORT = 6379
DB_BATCH = 1000

N_FIRST = 100_000
N_SECOND = 100_000
N_LOOKUPS = 1000


def setup_db(cur, conn):
    cur.execute("DROP TABLE IF EXISTS test;")
    cur.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);")
    conn.commit()

def insert_postgres_only(cur, conn):
    print(f"Inserting {N_FIRST} rows into Postgres only...")

    t0 = time.perf_counter()

    for i in range(0, N_FIRST, DB_BATCH):
        cur.executemany(
            "INSERT INTO test (id, value) VALUES (%s, %s);",
            [(k, random.randint(1, 1_000_000)) for k in range(i, i + DB_BATCH)]
        )
        conn.commit()

    t1 = time.perf_counter()
    total_time = t1 - t0

    print(
        f"Postgres only insert: "
        f"total={N_FIRST}, "
        f"time={total_time:.4f}s, "
        f"avg={total_time / N_FIRST * 1e6:.1f}µs/op"
    )

def insert_write_through(cur, conn, r):
    print(f"Inserting {N_SECOND} rows into Postgres and Redis (write-through)...")

    t0 = time.perf_counter()

    for i in range(N_FIRST, N_FIRST + N_SECOND, DB_BATCH):
        batch = [(k, random.randint(1, 1_000_000)) for k in range(i, i + DB_BATCH)]

        cur.executemany(
            "INSERT INTO test (id, value) VALUES (%s, %s);",
            batch
        )
        conn.commit()

        # Write-through to Redis
        with r.pipeline() as pipe:
            for k, v in batch:
                pipe.set(str(k), v)
            pipe.execute()

    t1 = time.perf_counter()
    total_time = t1 - t0

    print(
        f"Write-through insert: "
        f"total={N_SECOND}, "
        f"time={total_time:.4f}s, "
        f"avg={total_time / N_SECOND * 1e6:.1f}µs/op"
    )

def cache_lookup_test(cur, r, key_range, label):
    hits = 0
    misses = 0

    t0 = time.perf_counter()

    for _ in range(N_LOOKUPS):
        k = random.randint(*key_range)
        v = r.get(str(k))
        if v is not None:
            hits += 1
        else:
            cur.execute("SELECT value FROM test WHERE id = %s;", (k,))
            row = cur.fetchone()
            if row:
                misses += 1

    t1 = time.perf_counter()

    print(
        f"{label}: "
        f"hits={hits}, "
        f"misses={misses}, "
        f"total={N_LOOKUPS}, "
        f"time={t1 - t0:.4f}s, "
        f"avg={(t1 - t0) / N_LOOKUPS * 1e6:.1f}µs/op"
    )

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    setup_db(cur, conn)
    insert_postgres_only(cur, conn)
    insert_write_through(cur, conn, r)

    print("\nCache lookup test for first batch (should be all misses):")
    cache_lookup_test(cur, r, (0, N_FIRST-1), "First batch")

    print("\nCache lookup test for second batch (should be mostly hits):")
    cache_lookup_test(cur, r, (N_FIRST, N_FIRST+N_SECOND-1), "Second batch")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main() 
