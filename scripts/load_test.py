#!/usr/bin/env python3

import asyncio
import aiohttp
import json
import random
import glob
import os
from datetime import datetime
from statistics import mean, median


async def send_request(
    session, payload, url="http://localhost:3000/execute", enable_cache=True
):
    """Send a single request to the server and measure response time."""
    start_time = datetime.now()
    headers = {}
    if not enable_cache:
        headers["Cache-Control"] = "no-cache"

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            response_data = await response.json()
            duration = (datetime.now() - start_time).total_seconds() * 1000  # ms
            return {
                "status": response.status,
                "duration": duration,
                "server_duration": response_data.get("took_ms", 0),
                "success": response_data.get("success", False),
                "error": response_data.get("error", None),
            }
    except Exception as e:
        return {
            "status": 0,
            "duration": 0,
            "server_duration": 0,
            "success": False,
            "error": str(e),
        }


async def load_test(
    concurrent_users, requests_per_user, exclude_dangerous=True, enable_cache=True
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

    # Configure connection pooling
    connector = aiohttp.TCPConnector(
        limit=concurrent_users,  # Max connections
        force_close=False,  # Keep connections alive
        enable_cleanup_closed=True,
        ttl_dns_cache=300,  # Cache DNS lookups
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        results = []
        tasks = []
        for _ in range(concurrent_users):
            for _ in range(requests_per_user):
                _file_name, payload = random.choice(payloads)
                tasks.append(send_request(session, payload, enable_cache=enable_cache))

        results = await asyncio.gather(*tasks)

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
    # Test parameters
    CONCURRENT_USERS = 10
    REQUESTS_PER_USER = 50
    EXCLUDE_DANGEROUS = True
    ENABLE_CACHE = False

    asyncio.run(
        load_test(
            CONCURRENT_USERS,
            REQUESTS_PER_USER,
            exclude_dangerous=EXCLUDE_DANGEROUS,
            enable_cache=ENABLE_CACHE,
        )
    )


if __name__ == "__main__":
    main()
