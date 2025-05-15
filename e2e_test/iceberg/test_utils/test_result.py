from datetime import timedelta
from typing import List, Optional, NamedTuple

from .log import log, LogLevel, Color


class TestResult(NamedTuple):
    name: str
    success: bool
    duration: timedelta
    error: Optional[Exception]


def print_test_summary(results: List[TestResult]):
    """Print a summary of all test results."""
    log("\nTest Summary", level=LogLevel.HEADER)
    log("", level=LogLevel.SEPARATOR)

    total_duration = timedelta()
    passed = 0
    failed = 0

    # Print individual test results
    for result in results:
        status = "PASSED" if result.success else "FAILED"
        duration_str = f"{result.duration.total_seconds():.2f}s"
        log(
            f"{result.name}: {status} ({duration_str})",
            level=LogLevel.INFO if result.success else LogLevel.ERROR,
        )
        if not result.success and result.error:
            log(f"  Error: {str(result.error)}", level=LogLevel.ERROR, indent=2)

        total_duration += result.duration
        if result.success:
            passed += 1
        else:
            failed += 1

    # Print summary statistics
    log("", level=LogLevel.SEPARATOR)
    total_tests = len(results)
    pass_rate = (passed / total_tests * 100) if total_tests > 0 else 0

    log(f"Total Tests: {total_tests}", level=LogLevel.RESULT)
    log(f"Passed: {passed}", level=LogLevel.RESULT)
    log(f"Failed: {failed}", level=LogLevel.RESULT)
    log(f"Pass Rate: {pass_rate:.1f}%", level=LogLevel.RESULT)
    log(f"Total Duration: {total_duration.total_seconds():.2f}s", level=LogLevel.RESULT)
    log("", level=LogLevel.SEPARATOR)
