# Requires: pip install redis psycopg2-binary
import os
import psycopg2
import redis
import random
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_NAME = os.getenv("DB_NAME", "testdb")
DB_USER = os.getenv("DB_USER", "test")
DB_PASSWORD = os.getenv("DB_PASSWORD", "test")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "6432"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

DB_BATCH = int(os.getenv("DB_BATCH", "1000"))

N_FIRST = int(os.getenv("N_FIRST", "100000"))
N_SECOND = int(os.getenv("N_SECOND", "100000"))

# Increase for more stable percentile stats
N_LOOKUPS = int(os.getenv("N_LOOKUPS", "100000"))

# Concurrency for lookup benchmark
N_THREADS = int(os.getenv("N_THREADS", "16"))


def connect_pg():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )


def connect_redis():
    # decode_responses=True keeps your current behavior (string decode cost included)
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


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


def _percentile(sorted_vals, p: float):
    """p in [0,1]. Uses nearest-rank on sorted list."""
    if not sorted_vals:
        return None
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


def _format_latency_stats(label, lat_us, hits, misses, total, wall_s):
    lat_sorted = sorted(lat_us)
    p50 = _percentile(lat_sorted, 0.50)
    p95 = _percentile(lat_sorted, 0.95)
    p99 = _percentile(lat_sorted, 0.99)
    avg = (sum(lat_sorted) / len(lat_sorted)) if lat_sorted else 0.0
    stdev = statistics.pstdev(lat_sorted) if len(lat_sorted) > 1 else 0.0

    print(
        f"{label}: "
        f"hits={hits}, "
        f"misses={misses}, "
        f"total={total}, "
        f"time={wall_s:.4f}s, "
        f"avg={avg:.1f}µs, "
        f"p50={p50:.1f}µs, "
        f"p95={p95:.1f}µs, "
        f"p99={p99:.1f}µs, "
        f"stdev={stdev:.1f}µs"
    )


def cache_lookup_test_singlethread(cur, r, key_range, label):
    """Single-thread version with percentiles (compatible with your current flow)."""
    hits = 0
    misses = 0
    lat_us = []

    t0 = time.perf_counter()

    for _ in range(N_LOOKUPS):
        k = random.randint(*key_range)

        t_op0 = time.perf_counter()
        v = r.get(str(k))
        if v is not None:
            hits += 1
        else:
            cur.execute("SELECT value FROM test WHERE id = %s;", (k,))
            row = cur.fetchone()
            if row:
                misses += 1
        t_op1 = time.perf_counter()

        lat_us.append((t_op1 - t_op0) * 1e6)

    t1 = time.perf_counter()
    _format_latency_stats(label, lat_us, hits, misses, N_LOOKUPS, t1 - t0)


def _lookup_worker(thread_id: int, n_ops: int, key_range):
    """
    Each worker creates its own PG connection/cursor (psycopg2 is not safe to share across threads).
    Redis client object is cheap; share is often ok, but per-thread keeps it simple and avoids contention.
    """
    conn = connect_pg()
    cur = conn.cursor()
    r = connect_redis()

    hits = 0
    misses = 0
    lat_us = []

    lo, hi = key_range

    for _ in range(n_ops):
        k = random.randint(lo, hi)

        t0 = time.perf_counter()
        v = r.get(str(k))
        if v is not None:
            hits += 1
        else:
            cur.execute("SELECT value FROM test WHERE id = %s;", (k,))
            row = cur.fetchone()
            if row:
                misses += 1
        t1 = time.perf_counter()

        lat_us.append((t1 - t0) * 1e6)

    cur.close()
    conn.close()

    return hits, misses, lat_us


def cache_lookup_test_concurrent(key_range, label):
    """
    Concurrent lookup benchmark: splits N_LOOKUPS across N_THREADS.
    Reports overall wall time + merged latency percentiles across all ops.
    """
    per_thread = N_LOOKUPS // N_THREADS
    remainder = N_LOOKUPS % N_THREADS

    jobs = []
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=N_THREADS) as ex:
        for i in range(N_THREADS):
            n_ops = per_thread + (1 if i < remainder else 0)
            jobs.append(ex.submit(_lookup_worker, i, n_ops, key_range))

        total_hits = 0
        total_misses = 0
        all_lat_us = []

        for fut in as_completed(jobs):
            hits, misses, lat_us = fut.result()
            total_hits += hits
            total_misses += misses
            all_lat_us.extend(lat_us)

    t1 = time.perf_counter()
    _format_latency_stats(label, all_lat_us, total_hits, total_misses, N_LOOKUPS, t1 - t0)


def main():
    conn = connect_pg()
    cur = conn.cursor()
    r = connect_redis()

    setup_db(cur, conn)
    insert_postgres_only(cur, conn)
    insert_write_through(cur, conn, r)

    # Single-thread percentiles (closer to your current output semantics)
    print("\nCache lookup test (single-thread) for first batch (should be all misses):")
    cache_lookup_test_singlethread(cur, r, (0, N_FIRST - 1), "First batch (1T)")

    print("\nCache lookup test (single-thread) for second batch (should be mostly hits):")
    cache_lookup_test_singlethread(cur, r, (N_FIRST, N_FIRST + N_SECOND - 1), "Second batch (1T)")

    cur.close()
    conn.close()

    # Concurrent tests (more production-informative)
    print(f"\nCache lookup test (concurrent) with N_THREADS={N_THREADS} for first batch (miss path):")
    cache_lookup_test_concurrent((0, N_FIRST - 1), f"First batch ({N_THREADS}T)")

    print(f"\nCache lookup test (concurrent) with N_THREADS={N_THREADS} for second batch (hit path):")
    cache_lookup_test_concurrent((N_FIRST, N_FIRST + N_SECOND - 1), f"Second batch ({N_THREADS}T)")


if __name__ == "__main__":
    main()
