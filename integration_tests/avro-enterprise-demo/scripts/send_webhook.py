from __future__ import annotations

import argparse
import json
from pathlib import Path

import requests


WEBHOOK_URL = "http://risingwave-standalone:4560/webhook/dev/demo_core/customer_actions_raw"


def write_artifact(name: str, content: str) -> None:
    path = Path("/workspace/artifacts") / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def post_json(payload: dict) -> requests.Response:
    return requests.post(WEBHOOK_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload), timeout=10)


def scenario_valid() -> None:
    events = [
        {
            "customer_key": "c-1001",
            "action_type": "purchase",
            "amount": "32.50",
            "action_ts": "2026-04-07 09:05:00",
            "channel": "web",
            "team": "marketing",
        },
        {
            "customer_key": "c-1001",
            "action_type": "purchase",
            "amount": "48.00",
            "action_ts": "2026-04-07 09:06:00",
            "channel": "mobile",
            "team": "marketing",
        },
        {
            "customer_key": "c-1004",
            "action_type": "case_opened",
            "amount": "0.00",
            "action_ts": "2026-04-07 09:12:00",
            "channel": "support",
            "team": "ops",
        },
    ]
    for event in events:
        response = post_json(event)
        response.raise_for_status()


def scenario_sink_failure() -> None:
    response = post_json(
        {
            "customer_key": "c-1004",
            "action_type": "purchase",
            "amount": "19.99",
            "action_ts": "2026-04-07 09:13:00",
            "channel": "mobile",
            "team": "marketing",
        }
    )
    response.raise_for_status()


def scenario_poison() -> None:
    response = requests.post(
        WEBHOOK_URL,
        headers={"Content-Type": "application/json"},
        data='{"customer_key": "c-poison", "action_type": ',
        timeout=10,
    )
    write_artifact("webhook_poison.out", f"status={response.status_code}\nbody={response.text}\n")
    if response.status_code < 400:
        raise RuntimeError("Expected webhook poison payload to be rejected")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True, choices=["valid", "sink_failure", "poison"])
    args = parser.parse_args()

    if args.scenario == "valid":
        scenario_valid()
    elif args.scenario == "sink_failure":
        scenario_sink_failure()
    else:
        scenario_poison()


if __name__ == "__main__":
    main()
