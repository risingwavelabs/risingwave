#!/usr/bin/env -S uv run --isolated
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "requests",
# ]
# ///

import requests
import os
import json
from datetime import datetime, timedelta, timezone

# --- Configuration ---
ORGANIZATION_SLUG = "risingwavelabs"
PIPELINE_SLUG = "main-cron"
JOB_KEY = "e2e-test-release-parallel-memory"
START_DATE_STR = "2025-01-01T00:00:00Z"
BUILDKITE_API_TOKEN = os.environ.get("BUILDKITE_API_TOKEN")
# --- End Configuration ---

if not BUILDKITE_API_TOKEN:
    print("Error: BUILDKITE_API_TOKEN environment variable not set.")
    exit(1)

API_BASE_URL = f"https://api.buildkite.com/v2/organizations/{ORGANIZATION_SLUG}/pipelines/{PIPELINE_SLUG}"
HEADERS = {"Authorization": f"Bearer {BUILDKITE_API_TOKEN}"}


def get_builds(start_date_iso):
    """Fetches builds from the BuildKite API, handling pagination."""
    builds = []
    page = 1
    per_page = 100  # Max allowed by API
    while True:
        print(f"Fetching page {page}...")
        params = {
            "per_page": per_page,
            "page": page,
            "created_from": start_date_iso,
            "branch": "main",  # Assuming we only care about main branch cron jobs
        }
        try:
            response = requests.get(
                f"{API_BASE_URL}/builds", headers=HEADERS, params=params, timeout=30
            )
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()
            if not data:
                break
            builds.extend(data)
            # Check if there are more pages based on Link header or if less than per_page results returned
            if "next" not in response.links and len(data) < per_page:
                break
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching builds: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            print(f"Response text: {response.text}")
            return None
    print(f"Fetched a total of {len(builds)} builds.")
    return builds


def parse_duration(job):
    """Calculates the duration of a job in seconds."""
    if job.get("started_at") and job.get("finished_at"):
        try:
            start = datetime.fromisoformat(job["started_at"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(job["finished_at"].replace("Z", "+00:00"))
            duration = end - start
            return duration.total_seconds()
        except (ValueError, TypeError):
            return None
    return None


def analyze_job_history(builds, job_key):
    """Analyzes the history of a specific job across builds."""
    results = []
    print(f"\nAnalyzing job '{job_key}'...")
    for build in builds:
        build_number = build.get("number")
        created_at = build.get("created_at")
        web_url = build.get("web_url")

        found_job = None
        for job in build.get("jobs", []):
            if job.get("step_key") == job_key:
                found_job = job
                break

        if found_job:
            state = found_job.get("state")
            duration_seconds = parse_duration(found_job)
            duration_str = (
                f"{duration_seconds:.2f}s" if duration_seconds is not None else "N/A"
            )
            results.append(
                {
                    "build_number": build_number,
                    "created_at": created_at,
                    "job_state": state,
                    "duration": duration_str,
                    "build_url": web_url,
                }
            )
        # else:
        #     print(f"  Job '{job_key}' not found in build {build_number}")

    return results


def main():
    start_date = datetime.fromisoformat(START_DATE_STR.replace("Z", "+00:00"))
    print(
        f"Fetching build history for pipeline '{PIPELINE_SLUG}' since {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')}..."
    )

    builds = get_builds(START_DATE_STR)

    if builds is None:
        print("Failed to fetch builds. Exiting.")
        exit(1)

    if not builds:
        print("No builds found for the specified period.")
        exit(0)

    # Sort builds by creation date (oldest first)
    builds.sort(
        key=lambda b: datetime.fromisoformat(b["created_at"].replace("Z", "+00:00"))
    )

    job_history = analyze_job_history(builds, JOB_KEY)

    if not job_history:
        print(f"No runs found for job '{JOB_KEY}' in the fetched builds.")
        exit(0)

    print("\n--- Job Run History ---")
    print(f"{'Build':<8} {'Created At':<25} {'State':<15} {'Duration':<10} {'URL'}")
    print("-" * 80)
    passed_count = 0
    failed_count = 0
    timed_out_count = 0
    other_count = 0
    total_duration = 0
    valid_durations = 0

    for entry in job_history:
        print(
            f"{entry['build_number']:<8} {entry['created_at']:<25} {entry['job_state']:<15} {entry['duration']:<10} {entry['build_url']}"
        )
        if entry["job_state"] == "passed":
            passed_count += 1
        elif entry["job_state"] == "failed":
            if entry.get(
                "timed_out"
            ):  # Buildkite sometimes marks timed out jobs as failed
                timed_out_count += 1
            else:
                failed_count += 1
        elif entry["job_state"] == "timed_out":  # Explicit timeout state
            timed_out_count += 1
        else:
            other_count += 1

        if entry["duration"] != "N/A":
            try:
                duration_sec = float(entry["duration"][:-1])
                total_duration += duration_sec
                valid_durations += 1
            except ValueError:
                pass  # Ignore if duration is not a valid float

    print("-" * 80)
    print("\n--- Summary ---")
    total_runs = len(job_history)
    print(f"Total Runs Analyzed: {total_runs}")
    print(f"Passed: {passed_count} ({passed_count/total_runs:.1%})")
    print(f"Failed (not timeout): {failed_count} ({failed_count/total_runs:.1%})")
    print(f"Timed Out: {timed_out_count} ({timed_out_count/total_runs:.1%})")
    if other_count > 0:
        print(f"Other States: {other_count} ({other_count/total_runs:.1%})")

    if valid_durations > 0:
        avg_duration = total_duration / valid_durations
        print(
            f"\nAverage Duration (for {valid_durations} runs with duration): {avg_duration:.2f}s"
        )
    else:
        print("\nNo valid durations found to calculate average.")


if __name__ == "__main__":
    main()
