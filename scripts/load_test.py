#!/usr/bin/env python3

import asyncio
import aiohttp
import json
import random
import glob
import os
from datetime import datetime
from statistics import mean, median
import argparse


async def send_request(
    session, payload, url="http://localhost:3000/execute", enable_cache=True
):
    """Send a single request to the server and measure response time."""
    headers = {
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Keep-Alive": "timeout=120",
    }
    if not enable_cache:
        headers["Cache-Control"] = "no-cache"

    start_time = datetime.now()
    try:
        connect_start = datetime.now()
        async with session.post(
            url, json=payload, headers=headers, raise_for_status=True
        ) as response:
            connect_time = (datetime.now() - connect_start).total_seconds() * 1000

            read_start = datetime.now()
            response_data = await response.json()
            read_time = (datetime.now() - read_start).total_seconds() * 1000

            total_duration = (datetime.now() - start_time).total_seconds() * 1000

            return {
                "status": response.status,
                "duration": total_duration,
                "connect_time": connect_time,
                "read_time": read_time,
                "server_duration": response_data.get("took_ms", 0),
                "success": response_data.get("success", False),
                "error": response_data.get("error", None),
            }
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds() * 1000
        return {
            "status": 0,
            "duration": duration,
            "connect_time": 0,
            "read_time": 0,
            "server_duration": 0,
            "success": False,
            "error": str(e),
        }


async def load_test(
    concurrent_users,
    requests_per_user,
    exclude_dangerous=True,
    exclude_remote=True,
    exclude_replay=True,
    enable_cache=True,
):
    """Run load test with specified number of concurrent users and requests."""
    # Load all JSON files from tests/resources
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    json_files = glob.glob(
        os.path.join(base_dir, "tests", "resources", "**", "*.json"), recursive=True
    )

    if not json_files:
        print("Error: No JSON files found!")
        print(f"Looked in: {os.path.join(base_dir, 'tests', 'resources')}")
        print("Please ensure you have JSON files in the tests/resources directory")
        return

    payloads = []
    for file_path in json_files:
        # Skip dangerous transform tests if excluded
        if exclude_dangerous and "dangerous_transform" in file_path:
            print(f"Skipping dangerous test file: {os.path.basename(file_path)}")
            continue

        # Skip remote tests if excluded
        if exclude_remote and "remote" in file_path:
            print(f"Skipping remote test file: {os.path.basename(file_path)}")
            continue

        # Skip replay tests if excluded
        if exclude_replay and "replay" in file_path:
            print(f"Skipping replay test file: {os.path.basename(file_path)}")
            continue

        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                content = f.read()
                # Replace common control characters
                content = (
                    content.replace("\r", "").replace("\n", " ").replace("\t", " ")
                )
                payload = json.loads(content)
                payloads.append((os.path.basename(file_path), payload))
                print(f"Loaded test file: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Warning: Error loading {file_path}: {str(e)}")

    if not payloads:
        print("Error: No valid JSON payloads found!")
        return

    print(f"\nStarting load test with:")
    print(f"- {concurrent_users} concurrent users")
    print(f"- {requests_per_user} requests per user")
    print(f"- {len(payloads)} different payloads")

    # Modify connector settings to be more aggressive with connection reuse
    connector = aiohttp.TCPConnector(
        limit=concurrent_users,
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=300,
        use_dns_cache=True,
        keepalive_timeout=120,
        limit_per_host=concurrent_users,
    )

    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        json_serialize=json.dumps,
    ) as session:
        # Execute requests in batches per user
        results = []
        for _user_id in range(concurrent_users):
            user_tasks = []
            for _ in range(requests_per_user):
                _file_name, payload = random.choice(payloads)
                user_tasks.append(
                    send_request(session, payload, enable_cache=enable_cache)
                )
            # Execute each user's requests sequentially but different users concurrently
            user_results = await asyncio.gather(*user_tasks)
            results.extend(user_results)

    # Analyze results
    successful_requests = [r for r in results if r["success"]]
    failed_requests = [r for r in results if not r["success"]]
    client_durations = [r["duration"] for r in successful_requests]
    server_durations = [r["server_duration"] for r in successful_requests]

    print("\nResults:")
    print(f"Total requests: {len(results)}")
    print(f"Successful requests: {len(successful_requests)}")
    print(f"Failed requests: {len(failed_requests)}")

    if client_durations:
        print("\nDetailed timing statistics (ms):")
        connect_times = [r["connect_time"] for r in successful_requests]
        read_times = [r["read_time"] for r in successful_requests]

        print(f"\nConnection timing:")
        print(f"Mean connect time: {mean(connect_times):.2f}")
        print(f"Median connect time: {median(connect_times):.2f}")

        print(f"\nResponse reading timing:")
        print(f"Mean read time: {mean(read_times):.2f}")
        print(f"Median read time: {median(read_times):.2f}")

        print(f"\nClient-side timing statistics (ms):")
        print(f"Mean response time: {mean(client_durations):.2f}")
        print(f"Median response time: {median(client_durations):.2f}")
        print(f"Min response time: {min(client_durations):.2f}")
        print(f"Max response time: {max(client_durations):.2f}")

        print(f"\nServer-side timing statistics (ms):")
        print(f"Mean response time: {mean(server_durations):.2f}")
        print(f"Median response time: {median(server_durations):.2f}")
        print(f"Min response time: {min(server_durations):.2f}")
        print(f"Max response time: {max(server_durations):.2f}")

        print(f"\nNetwork/Overhead timing statistics (ms):")
        overhead_durations = [c - s for c, s in zip(client_durations, server_durations)]
        print(f"Mean overhead time: {mean(overhead_durations):.2f}")
        print(f"Median overhead time: {median(overhead_durations):.2f}")
        print(f"Min overhead time: {min(overhead_durations):.2f}")
        print(f"Max overhead time: {max(overhead_durations):.2f}")

    if failed_requests:
        print("\nSample of failed requests:")
        for fail in failed_requests[:5]:  # Show first 5 failures
            print(f"Error: {fail['error']}")


def main():
    """
    Example usage:
        # Use defaults (10 users, 50 requests each)
        python scripts/load_test.py

        # Custom configuration
        python scripts/load_test.py --users 20 --requests 100

        # Disable caching and allow dangerous tests
        python scripts/load_test.py --disable-cache --allow-dangerous

        # Test against a different server
        python scripts/load_test.py --url http://localhost:3000/execute
    """
    parser = argparse.ArgumentParser(
        description="Load test for the DAG execution server"
    )
    parser.add_argument(
        "--users", type=int, default=10, help="Number of concurrent users (default: 10)"
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=50,
        help="Number of requests per user (default: 50)",
    )
    parser.add_argument(
        "--allow-dangerous",
        action="store_true",
        help="Include dangerous transform tests",
    )
    parser.add_argument(
        "--disable-cache", action="store_true", help="Disable server-side caching"
    )
    parser.add_argument(
        "--url",
        type=str,
        default="http://localhost:3000/execute",
        help="Server URL (default: http://localhost:3000/execute)",
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Include tests requiring remote services",
    )
    parser.add_argument(
        "--allow-replay",
        action="store_true",
        help="Include replay tests",
    )

    args = parser.parse_args()

    asyncio.run(
        load_test(
            concurrent_users=args.users,
            requests_per_user=args.requests,
            exclude_dangerous=not args.allow_dangerous,
            exclude_remote=not args.allow_remote,
            exclude_replay=not args.allow_replay,
            enable_cache=not args.disable_cache,
        )
    )


if __name__ == "__main__":
    main()
