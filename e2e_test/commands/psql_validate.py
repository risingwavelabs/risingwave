#!/usr/bin/env python3

"""
Connects to a PostgreSQL database, executes a given SQL query, and compares the
output against an expected template using wildcard matching.

Handles standard query results and DESCRIBE plan output differently.

Usage:
    python psql_validate.py --sql "SELECT ..." --expected "Expected output with % wildcards" [--debug] [options]

Arguments:
    --sql:          The SQL query string to execute.
    --expected:     A string containing the expected output format. The wildcard
                    character '%' can be used to match one or more characters.
    --debug:        Enable debug logging to stderr (default: False).

Wildcard Matching:
    The script converts the expected template into a regular expression.
    - All literal characters in the template are escaped.
    - The wildcard character '%' is converted to the regex `.+?` which matches
      one or more characters non-greedily.
    - The comparison uses `re.fullmatch` to ensure the entire actual output
      matches the pattern derived from the template.
"""

import argparse
import psycopg2
import re
import os
import sys
import logging  # Import logging module
from psycopg2 import sql


# Configure logging
# Basic configuration sets the default level; handler level is set later
logging.basicConfig(format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def format_results(cursor):
    """Formats query results similar to psql output."""
    if cursor.description is None:
        # Likely a command that doesn't return rows, e.g., INSERT, UPDATE, CREATE
        status_message = cursor.statusmessage
        # Try to get row count if available (e.g., INSERT 0 1)
        match = re.search(r"\b(\d+)$", status_message)
        if match:
            return f"Rows affected: {match.group(1)}"
        else:
            return status_message  # Return command status like CREATE TABLE

    headers = [desc[0] for desc in cursor.description]
    try:
        rows = cursor.fetchall()
    except psycopg2.ProgrammingError:
        # Handle cases where fetchall is not appropriate after certain commands
        return (
            cursor.statusmessage
        )  # Or potentially an empty string or specific message

    if not rows:
        # Command completed successfully but returned no description and no rows
        return cursor.statusmessage if cursor.statusmessage else "OK"

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(
                col_widths[i], len(str(cell) if cell is not None else "NULL")
            )

    # Format rows
    row_lines = []
    for i, row in enumerate(rows):
        line = " | ".join(
            f"{str(cell) if cell is not None else 'NULL':<{col_widths[i]}}"
            for i, cell in enumerate(row)
        )
        if line.isspace():
            row_lines.append("(empty)" if i != len(rows) - 1 else "")
        else:
            row_lines.append(line)
    return "\n".join(row_lines)


def get_raw_results(cursor):
    """Fetches results as raw lines, suitable for DESCRIBE output."""
    try:
        rows = cursor.fetchall()
        # Assuming DESCRIBE returns rows with one column containing plan lines
        return "\n".join(row[0] for row in rows)
    except psycopg2.ProgrammingError:
        # Handle commands that don't return standard rows
        return cursor.statusmessage or "OK"
    except IndexError:
        # Handle cases where rows might not have expected structure
        logger.warning(
            "Unexpected row structure in raw results, returning status message."
        )
        return cursor.statusmessage or "OK"


def create_regex_from_template(template: str, wildcard="%"):
    """Converts a template string with wildcards into a regex pattern."""
    parts = template.split(wildcard)
    escaped_parts = [re.escape(part) for part in parts]
    # Use .+? to match one or more characters non-greedily
    # If wildcard is at the start/end, split results in empty strings, correctly handled.
    regex_pattern = ".+?".join(escaped_parts)
    return f"^{regex_pattern}$"  # Anchor to start and end for full match


def main():
    parser = argparse.ArgumentParser(
        description="Execute SQL and validate output against a template using '%s' wildcards."
    )
    parser.add_argument("--sql", required=True, help="SQL query to execute.")
    parser.add_argument(
        "--expected",
        required=True,
        help="Expected output template with '%' as wildcard.",
    )
    parser.add_argument(
        "--db", default="dev", help="Database name to connect to (default: dev)."
    )
    parser.add_argument(
        "--debug",
        action="store_true",  # Add debug flag
        help="Enable debug logging to stderr.",
    )

    args = parser.parse_args()

    # Set logging level based on debug flag
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        # Set logger level, not the basicConfig root logger level after init
        logger.setLevel(logging.INFO)  # Or WARNING

    conn = None
    actual_output_raw = ""  # Initialize to handle potential errors before assignment
    try:
        dbname = args.db
        conn_string = f"host='{os.environ.get("RISEDEV_RW_FRONTEND_LISTEN_ADDRESS")}' port='{os.environ.get('RISEDEV_RW_FRONTEND_PORT')}' dbname='{dbname}' user='root' password=''"
        logger.debug(
            f"Connecting to: {conn_string.replace('password=\'\'', 'password=***')}..."
        )  # Hide password if any
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cur = conn.cursor()

        logger.debug(f"Executing SQL: {args.sql}")
        cur.execute(sql.SQL(args.sql))

        actual_output_raw = format_results(cur)

        logger.debug(
            f"--- Actual Output (Raw) ---\n{actual_output_raw}\n--------------------------"
        )
        cur.close()

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")  # Use logger.error for actual errors
        # Treat the error message as the actual output for comparison purposes
        actual_output_raw = f"Error: {e}".strip()
        # Remove the flattening of error messages to support line-by-line comparison
        # actual_output_raw = actual_output_raw.replace(
        #     "\n", " "
        # )

    except Exception as e:
        logger.exception(
            f"An unexpected error occurred: {e}"
        )  # Use logger.exception to include stack trace
        sys.exit(1)
    finally:
        if conn:
            logger.debug("Closing database connection.")
            conn.close()

    # --- Start: Line-by-line comparison logic ---
    # Use splitlines() to handle different newline conventions and avoid empty strings from trailing newlines
    actual_lines = actual_output_raw.splitlines()
    expected_lines = args.expected.strip().splitlines()

    # Check if the number of lines matches
    if len(actual_lines) != len(expected_lines):
        print(f"Result: MISMATCH")
        logger.error(
            f"Line count mismatch: Expected {len(expected_lines)} lines, got {len(actual_lines)} lines."
        )
        # Provide full context on line count mismatch
        expected_log_message = f"--- Expected Template ({len(expected_lines)} lines) ---\n{args.expected.strip()}"
        logger.error(expected_log_message)
        actual_log_message = (
            f"--- Actual Output ({len(actual_lines)} lines) ---\n{actual_output_raw}"
        )
        logger.error(actual_log_message)
        sys.exit(1)

    # Compare line by line
    mismatch_found = False
    for i, (actual_line, expected_template_line) in enumerate(
        zip(actual_lines, expected_lines)
    ):
        actual_line = actual_line.rstrip()  # Strip trailing whitespace from actual line
        expected_template_line = (
            expected_template_line.rstrip()
        )  # Strip trailing whitespace from expected line

        # Convert expected template line with wildcards to a regex pattern
        expected_pattern = create_regex_from_template(
            expected_template_line, wildcard="%"
        )

        logger.debug(
            f"Line {i+1}: Comparing actual='{actual_line}' with pattern='{expected_pattern}'"
        )

        # Compare actual line against the regex pattern for the expected line
        match = re.fullmatch(expected_pattern, actual_line)

        if not match:
            print(f"Result: MISMATCH")  # Print overall mismatch result
            logger.error(f"Mismatch on line {i+1}:")
            logger.error(f"  Expected pattern: r'{expected_pattern}'")
            logger.error(f"  Actual line:      '{actual_line}'")
            # Optionally log full context in debug mode
            if args.debug:
                expected_log_message_debug = (
                    f"--- Expected Template (Full) ---\n{args.expected.strip()}"
                )
                logger.debug(expected_log_message_debug)
                actual_log_message_debug = (
                    f"--- Actual Output (Full) ---\n{actual_output_raw}"
                )
                logger.debug(actual_log_message_debug)
            mismatch_found = True
            break  # Stop comparison on first mismatch

    if mismatch_found:
        sys.exit(1)
    else:
        # If loop completed without mismatch
        print("Result: MATCH")
        sys.exit(0)
    # --- End: Line-by-line comparison logic ---


if __name__ == "__main__":
    main()
