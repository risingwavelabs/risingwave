#!/usr/bin/env python3

import signal
import sys
import time


def signal_handler(sig, frame):
    print(f"\n[SUB] Received signal {sig}. Cleaning up...")
    time.sleep(3)  # Simulate cleanup time
    print("[SUB] Cleanup complete. Exiting.")
    sys.exit(-1)


def main():
    # Register the signal handler for SIGINT and SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("[SUB] Starting sleep loop.")

    try:
        # Infinite loop that keeps the program running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # This is another way to catch Ctrl+C, but the signal handler above will run first
        print("\n[SUB] Exiting via KeyboardInterrupt")


if __name__ == "__main__":
    print("[SUB] This is sub.py")
    main()
