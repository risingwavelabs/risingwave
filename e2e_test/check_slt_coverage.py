#!/usr/bin/env python3

import os
import re
import glob
import json
import argparse
from collections import defaultdict

# Directory containing the SLT files
E2E_TEST_DIR = "e2e_test"
# Directory containing the CI scripts
CI_SCRIPTS_DIR = "ci/scripts"
# Coverage ignore file
COVERAGE_IGNORE_FILE = f"{E2E_TEST_DIR}/.coverageignore"
# Current working directory
WORKSPACE_DIR = os.getcwd()


def to_real_path(path):
    """Convert path to its real path (resolving symlinks)"""
    return os.path.realpath(path)


def to_relative_path(path):
    """Convert absolute path to relative path for display"""
    if path.startswith(WORKSPACE_DIR):
        return os.path.relpath(path, WORKSPACE_DIR)
    return path


def path_for_storage(path):
    """Convert path to real path for internal storage"""
    return to_real_path(path)


def path_for_display(path):
    """Convert path to relative path for display"""
    return to_relative_path(path)


def read_coverageignore():
    """Read patterns from .coverageignore file"""
    ignore_patterns = []
    if os.path.exists(COVERAGE_IGNORE_FILE):
        with open(COVERAGE_IGNORE_FILE, "r") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith("#"):
                    ignore_patterns.append(line)
    return ignore_patterns


def should_ignore_file(file_path, ignore_patterns):
    """Check if a file should be ignored based on ignore patterns"""
    rel_path = to_relative_path(file_path)
    for pattern in ignore_patterns:
        # Handle glob patterns
        glob_files = glob.glob(pattern, recursive=True)
        if rel_path in glob_files:
            return True
    return False


def find_all_slt_files(include_ignored=False):
    """Find all .slt, .slt.serial, and .slt.part files in the e2e_test directory"""
    slt_files = set(
        path_for_storage(f)
        for suffix in (".slt", ".slt.serial")
        for f in glob.glob(f"{E2E_TEST_DIR}/**/*{suffix}", recursive=True)
    )
    slt_part_files = set(
        path_for_storage(f)
        for f in glob.glob(f"{E2E_TEST_DIR}/**/*.slt.part", recursive=True)
    )

    # Filter out ignored files unless explicitly included
    ignore_patterns = read_coverageignore() if not include_ignored else []
    if ignore_patterns:
        slt_files = {f for f in slt_files if not should_ignore_file(f, ignore_patterns)}
        slt_part_files = {
            f for f in slt_part_files if not should_ignore_file(f, ignore_patterns)
        }

    return slt_files, slt_part_files, ignore_patterns


def extract_slt_patterns_from_script(script_path):
    """Extract SLT file patterns from a CI script"""
    patterns = []
    excluded_patterns = []

    with open(script_path, "r") as f:
        content = f.read()

        # Match quoted regular/serial paths and globs, with or without './'.
        slt_commands = re.finditer(
            r'''(?:risedev\s+slt|sqllogictest)\s+.*?['"]((?:\./)?e2e_test/[^'"]*\.slt(?:\.serial)?)['"]''',
            content,
            re.DOTALL,
        )

        for command in slt_commands:
            pattern = command.group(1)
            if pattern.startswith("./"):
                pattern = pattern[2:]
            patterns.append(handle_unescaped_pattern(pattern))

        # Find find commands that look for SLT files
        find_commands = re.finditer(
            r'''find\s+(?:\./)?e2e_test[^\n]*-name\s+['"]([^'"]*\.slt(?:\.serial)?)['"]''',
            content,
        )
        for cmd in find_commands:
            find_cmd = cmd.group(0)
            # Extract the directory part
            dir_match = re.search(r"find\s+((?:\.\/)?e2e_test\/[^\s]+)", find_cmd)
            if dir_match:
                dir_path = dir_match.group(1)
                # Remove ./ prefix if present
                if dir_path.startswith("./"):
                    dir_path = dir_path[2:]
                pattern = f"{dir_path}/**/{cmd.group(1)}"
                # Check if there's a grep exclusion in this command
                if "grep -L" in find_cmd or "grep -v" in find_cmd:
                    grep_pattern_match = re.search(
                        r"grep\s+-[Lv]\s+[\'\"]([^\'\"]+)[\'\"]", find_cmd
                    )
                    if grep_pattern_match:
                        excluded_pattern = grep_pattern_match.group(1)
                        excluded_patterns.append((pattern, excluded_pattern))
                else:
                    patterns.append(pattern)

    return patterns, excluded_patterns


def handle_unescaped_pattern(pattern):
    """Handle unescaped patterns that might appear in the scripts"""
    # Replace escaped characters
    pattern = pattern.replace('\\"', '"').replace("\\'", "'")
    # Remove any unnecessary escaping
    pattern = pattern.replace("\\*", "*").replace("\\?", "?")
    return pattern


def expand_glob_pattern(pattern):
    """Expand a glob pattern to get matching files"""
    # Remove leading './' if present for glob to work
    if pattern.startswith("./"):
        pattern = pattern[2:]
    # Use realpath to resolve symbolic links
    return [path_for_storage(f) for f in glob.glob(pattern, recursive=True)]


def find_included_files(slt_files):
    """Find all included files referenced in regular, serial, and partial SLTs"""
    include_map = {}  # Map of file to files it includes

    include_regex = re.compile(r"include\s+(.*\.slt(?:\.part|\.serial)?)")

    for slt_file in slt_files:
        include_map[slt_file] = []
        try:
            with open(slt_file, "r", errors="ignore") as f:
                content = f.read()
                for match in include_regex.finditer(content):
                    included_path = match.group(1)
                    # Handle relative paths
                    if not included_path.startswith("/"):
                        # Resolve relative path with base directory
                        base_dir = os.path.dirname(slt_file)

                        # Handle glob patterns in include paths
                        if "*" in included_path or "?" in included_path:
                            # This is a glob pattern, expand it
                            full_pattern = os.path.normpath(
                                os.path.join(base_dir, included_path)
                            )
                            matching_files = glob.glob(full_pattern, recursive=True)
                            for matching_file in matching_files:
                                real_matching_file = path_for_storage(matching_file)
                                if real_matching_file in slt_files:
                                    include_map[slt_file].append(real_matching_file)
                        else:
                            # Regular path
                            full_path = os.path.normpath(
                                os.path.join(base_dir, included_path)
                            )
                            real_path = path_for_storage(full_path)
                            if os.path.exists(real_path) and real_path in slt_files:
                                include_map[slt_file].append(real_path)
        except Exception as e:
            # Skip files that can't be read
            print(f"Error reading {slt_file}, skipping: {e}")
            pass

    return include_map


def group_by_directory(file_list):
    """Group files by their directory paths"""
    directory_files = defaultdict(list)
    for file in file_list:
        directory = os.path.dirname(file)
        directory_files[directory].append(os.path.basename(file))
    # Sort each directory's files
    for directory in directory_files:
        directory_files[directory].sort()
    return directory_files


def analyze_uncovered_directories(all_files, covered_files):
    """Analyze which directories have uncovered SLT files"""
    uncovered_files = list(all_files - set(covered_files))
    directory_files = group_by_directory(uncovered_files)

    # Get all directories with SLT files
    all_directories = set()
    for file in all_files:
        all_directories.add(os.path.dirname(file))

    # Get fully uncovered directories (all files in directory are uncovered)
    fully_uncovered = {}
    for directory, files in directory_files.items():
        # Count total files in this directory
        total_files = len([f for f in all_files if os.path.dirname(f) == directory])
        if len(files) == total_files:
            fully_uncovered[directory] = files

    # Calculate coverage percentage for each directory
    directory_coverage = {}
    for directory in all_directories:
        total_files = len([f for f in all_files if os.path.dirname(f) == directory])
        uncovered = len([f for f in uncovered_files if os.path.dirname(f) == directory])
        coverage_pct = (
            ((total_files - uncovered) / total_files) * 100 if total_files > 0 else 0
        )
        directory_coverage[directory] = {
            "total_files": total_files,
            "covered_files": total_files - uncovered,
            "uncovered_files": uncovered,
            "coverage_percentage": coverage_pct,
        }

    return {
        "directory_files": dict(sorted(directory_files.items())),
        "fully_uncovered_directories": dict(sorted(fully_uncovered.items())),
        "directory_coverage": dict(sorted(directory_coverage.items())),
    }


def propagate_coverage_through_includes(
    covered_slt_files, include_map, all_slt_part_files
):
    """Propagate coverage through include relationships recursively

    If a file is covered, all .slt, .slt.serial, and .slt.part files it includes are covered.
    This function handles recursive include relationships to any depth.
    """
    # Start with directly covered regular and serial SLT files
    covered_directly = set(covered_slt_files)
    covered_through_includes = set()

    # Process all includes recursively starting from covered files
    def process_includes(slt_file, visited=None):
        if visited is None:
            visited = set()

        if slt_file in visited:
            # Avoid circular includes
            return

        visited.add(slt_file)

        if slt_file in include_map:
            for included_file in include_map[slt_file]:
                if (
                    included_file not in covered_directly
                    and included_file not in covered_through_includes
                ):
                    covered_through_includes.add(included_file)
                    # Recursively process includes of this file
                    process_includes(included_file, visited)

    # Process each covered file
    for slt_file in covered_directly:
        process_includes(slt_file)

    # Coverage flows from a caller to its includes, never back to other callers.
    # Sharing a covered include does not prove that an unselected test runs.
    covered_part_files = all_slt_part_files & (
        covered_directly | covered_through_includes
    )

    return covered_directly, covered_through_includes, covered_part_files


def main():
    parser = argparse.ArgumentParser(
        description="Check SLT test coverage in CI scripts"
    )
    parser.add_argument("--output", "-o", help="Output file for results (JSON format)")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed information"
    )
    parser.add_argument(
        "--dir-analysis",
        "-d",
        action="store_true",
        help="Show directory-level analysis",
    )
    parser.add_argument(
        "--include-ignored",
        action="store_true",
        help="Include files specified in .coverageignore",
    )
    args = parser.parse_args()

    all_slt_files, all_slt_part_files, ignore_patterns = find_all_slt_files(
        include_ignored=args.include_ignored
    )
    all_files = all_slt_files.union(all_slt_part_files)

    print(f"Total SLT files: {len(all_slt_files)}")
    print(f"Total SLT part files: {len(all_slt_part_files)}")
    print(f"Total files: {len(all_files)}")

    if ignore_patterns:
        print(f"Ignored patterns from .coverageignore: {len(ignore_patterns)}")
        if args.verbose:
            for pattern in ignore_patterns:
                print(f"  - {pattern}")

    # Find include relationships
    include_map = find_included_files(all_files)

    scripts = [f for f in os.listdir(CI_SCRIPTS_DIR) if f.endswith(".sh")]

    # Dictionary to track which SLT files are covered by which scripts
    covered_files = defaultdict(list)
    all_covered_files = set()

    # Dictionary to store all details for output
    results = {
        "total_slt_files": len(all_slt_files),
        "total_slt_part_files": len(all_slt_part_files),
        "total_files": len(all_files),
        "ignore_patterns": ignore_patterns,
        "scripts": [],
        "uncovered_files": [],
    }

    for script in scripts:
        script_path = os.path.join(CI_SCRIPTS_DIR, script)
        if not os.path.exists(script_path):
            print(f"Warning: Script {script_path} not found, skipping")
            continue

        patterns, excluded_patterns = extract_slt_patterns_from_script(script_path)

        if not patterns and not excluded_patterns:
            continue

        print(f"\nScript: {script}")
        if args.verbose:
            print(f"  Patterns found: {len(patterns)}")
            for pattern in patterns:
                print(f"    - {pattern}")
            print(f"  Exclusion patterns found: {len(excluded_patterns)}")

        script_result = {
            "name": script,
            "patterns": patterns,
            "excluded_patterns": excluded_patterns,
            "files_covered": [],
        }

        files_covered_by_script = set()

        # Process inclusion patterns
        for pattern in patterns:
            # Remove leading './' if present for glob to work
            clean_pattern = pattern[2:] if pattern.startswith("./") else pattern
            matched_files = expand_glob_pattern(clean_pattern)

            if args.verbose:
                print(f"  Pattern: {pattern}")
                print(f"    Matches: {len(matched_files)}")

            for file in matched_files:
                if file not in all_files:
                    continue
                covered_files[file].append(script)
                files_covered_by_script.add(file)
                all_covered_files.add(file)
                script_result["files_covered"].append(path_for_display(file))

        # Handle exclusion patterns
        for glob_pattern, exclude_regex in excluded_patterns:
            clean_glob = (
                glob_pattern[2:] if glob_pattern.startswith("./") else glob_pattern
            )
            matched_files = expand_glob_pattern(clean_glob)

            if args.verbose:
                print(f"  Exclusion pattern: {glob_pattern} (exclude: {exclude_regex})")

            # Filter files that don't match the exclude regex
            included_files = []
            for file in matched_files:
                # Skip ignored and already covered files.
                if file not in all_files or file in files_covered_by_script:
                    continue

                with open(file, "r", errors="ignore") as f:
                    try:
                        content = f.read()
                        if not re.search(exclude_regex, content):
                            included_files.append(file)
                            covered_files[file].append(script)
                            files_covered_by_script.add(file)
                            all_covered_files.add(file)
                            script_result["files_covered"].append(
                                path_for_display(file)
                            )
                    except UnicodeDecodeError:
                        # Skip files that can't be read as text
                        pass

            if args.verbose:
                print(f"    Included after exclusion: {len(included_files)}")

        print(f"  Total files covered by script: {len(files_covered_by_script)}")
        results["scripts"].append(script_result)

    # Find covered files through includes
    directly_covered, covered_through_includes, covered_part_files = (
        propagate_coverage_through_includes(
            all_covered_files, include_map, all_slt_part_files
        )
    )

    # Update all covered files
    all_covered_files = directly_covered.union(covered_through_includes).union(
        covered_part_files
    )

    # Categorize covered files
    covered_slt_files = set(
        f for f in all_covered_files if f.endswith((".slt", ".slt.serial"))
    )
    covered_slt_part_files = set(
        f for f in all_covered_files if f.endswith(".slt.part")
    )

    # Find uncovered files
    uncovered_files = all_files - all_covered_files
    uncovered_slt_files = all_slt_files - covered_slt_files
    uncovered_slt_part_files = all_slt_part_files - covered_slt_part_files

    # Update results
    results["covered_files_count"] = len(all_covered_files)
    results["uncovered_files_count"] = len(uncovered_files)
    results["covered_slt_files_count"] = len(covered_slt_files)
    results["uncovered_slt_files_count"] = len(uncovered_slt_files)
    results["covered_slt_part_files_count"] = len(covered_slt_part_files)
    results["uncovered_slt_part_files_count"] = len(uncovered_slt_part_files)
    results["directly_covered_count"] = len(directly_covered)
    results["covered_through_includes_count"] = len(covered_through_includes)
    results["uncovered_files"] = sorted(path_for_display(f) for f in uncovered_files)
    results["uncovered_slt_files"] = sorted(
        path_for_display(f) for f in uncovered_slt_files
    )
    results["uncovered_slt_part_files"] = sorted(
        path_for_display(f) for f in uncovered_slt_part_files
    )

    # Directory-level analysis
    directory_analysis = analyze_uncovered_directories(all_files, all_covered_files)

    # Convert directory paths to relative paths for display
    display_directory_analysis = {
        "directory_files": {
            path_for_display(dir_path): files
            for dir_path, files in directory_analysis["directory_files"].items()
        },
        "fully_uncovered_directories": {
            path_for_display(dir_path): files
            for dir_path, files in directory_analysis[
                "fully_uncovered_directories"
            ].items()
        },
        "directory_coverage": {
            path_for_display(dir_path): stats
            for dir_path, stats in directory_analysis["directory_coverage"].items()
        },
    }

    results["directory_analysis"] = display_directory_analysis

    # Print summary
    print("\n--- SUMMARY ---")
    print(f"Total SLT files: {len(all_slt_files)}")
    print(f"Total SLT part files: {len(all_slt_part_files)}")
    print(f"Total files: {len(all_files)}")

    print(
        f"Covered SLT files: {len(covered_slt_files)} "
        f"({len(covered_slt_files) / len(all_slt_files) * 100:.2f}%)"
    )
    print(
        f"Covered SLT part files: {len(covered_slt_part_files)} "
        f"({len(covered_slt_part_files) / len(all_slt_part_files) * 100:.2f}%)"
    )
    print(
        f"Total covered files: {len(all_covered_files)} "
        f"({len(all_covered_files) / len(all_files) * 100:.2f}%)"
    )

    print(f"  - Directly covered in scripts: {len(directly_covered)}")
    print(f"  - Covered through includes: {len(covered_through_includes)}")
    print(f"  - Covered part files: {len(covered_part_files)}")

    print(
        f"Uncovered SLT files: {len(uncovered_slt_files)} "
        f"({len(uncovered_slt_files) / len(all_slt_files) * 100:.2f}%)"
    )
    print(
        f"Uncovered SLT part files: {len(uncovered_slt_part_files)} "
        f"({len(uncovered_slt_part_files) / len(all_slt_part_files) * 100:.2f}%)"
    )
    print(
        f"Total uncovered files: {len(uncovered_files)} "
        f"({len(uncovered_files) / len(all_files) * 100:.2f}%)"
    )

    if uncovered_files and args.verbose:
        print("\nUncovered SLT files:")
        for file in sorted(path_for_display(f) for f in uncovered_slt_files):
            print(f"  {file}")

        print("\nUncovered SLT part files:")
        for file in sorted(path_for_display(f) for f in uncovered_slt_part_files):
            print(f"  {file}")

    # Print directory-level analysis if requested
    if args.dir_analysis:
        fully_uncovered = directory_analysis["fully_uncovered_directories"]
        print(f"\nFully uncovered directories: {len(fully_uncovered)}")
        for directory, files in sorted(fully_uncovered.items()):
            print(f"  {path_for_display(directory)} ({len(files)} files)")

        print("\nDirectory coverage:")
        directory_coverage = directory_analysis["directory_coverage"]
        poor_coverage = {
            d: stats
            for d, stats in directory_coverage.items()
            if stats["coverage_percentage"] < 50 and stats["total_files"] > 1
        }

        for directory, stats in sorted(
            poor_coverage.items(), key=lambda x: x[1]["coverage_percentage"]
        ):
            print(
                f"  {path_for_display(directory)}: {stats['coverage_percentage']:.1f}% coverage "
                f"({stats['covered_files']}/{stats['total_files']} files)"
            )

    # Print files covered by multiple scripts
    multi_covered = {
        file: scripts for file, scripts in covered_files.items() if len(scripts) > 1
    }
    if multi_covered and args.verbose:
        print("\nFiles covered by multiple scripts:")
        for file, scripts in sorted(list(multi_covered.items())):
            print(f"  {path_for_display(file)} covered by: {', '.join(scripts)}")

    # Save results to file if requested
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
