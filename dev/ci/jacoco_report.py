#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Parse JaCoCo XML coverage reports and generate a markdown summary for PRs.

Usage:
    python3 jacoco_report.py \
        --base-ref <branch> \
        --report-pattern '**/build/JacocoReport/test/jacocoTestReport.xml' \
        --min-overall 40 \
        --min-changed 60 \
        --output coverage-report.md
"""

import argparse
import glob
import os
import subprocess
import sys
import xml.etree.ElementTree as ET

COUNTER_TYPES = ["INSTRUCTION", "BRANCH", "LINE", "COMPLEXITY", "METHOD", "CLASS"]


def parse_counters(element):
    """Extract coverage counters from a JaCoCo XML element."""
    counters = {}
    for counter in element.findall("counter"):
        ctype = counter.get("type")
        counters[ctype] = {
            "missed": int(counter.get("missed", 0)),
            "covered": int(counter.get("covered", 0)),
        }
    return counters


def merge_counters(target, source):
    """Merge source counters into target."""
    for ctype, vals in source.items():
        if ctype not in target:
            target[ctype] = {"missed": 0, "covered": 0}
        target[ctype]["missed"] += vals["missed"]
        target[ctype]["covered"] += vals["covered"]


def coverage_pct(counter):
    """Calculate coverage percentage from a counter dict."""
    total = counter["missed"] + counter["covered"]
    return round(counter["covered"] / total * 100, 2) if total > 0 else 0.0


def parse_reports(pattern):
    """Parse all JaCoCo XML reports matching the glob pattern.

    Returns:
        overall: aggregated counters across all modules
        source_files: dict mapping "package/SourceFile.java" to its counters
    """
    xml_files = glob.glob(pattern, recursive=True)
    overall = {}
    source_files = {}

    for xml_file in xml_files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            merge_counters(overall, parse_counters(root))

            for pkg in root.findall(".//package"):
                pkg_name = pkg.get("name", "")
                for sf in pkg.findall("sourcefile"):
                    sf_name = sf.get("name", "")
                    key = f"{pkg_name}/{sf_name}"
                    if key not in source_files:
                        source_files[key] = {}
                    merge_counters(source_files[key], parse_counters(sf))
        except ET.ParseError as e:
            print(f"Warning: Could not parse {xml_file}: {e}", file=sys.stderr)

    return overall, source_files


def get_changed_java_files(base_ref):
    """Get list of changed Java main source files compared to base ref."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR",
             f"origin/{base_ref}...HEAD"],
            capture_output=True, text=True, check=True,
        )
        files = [f for f in result.stdout.strip().split("\n") if f]
        return [f for f in files
                if f.endswith(".java") and "/src/main/java/" in f]
    except subprocess.CalledProcessError:
        return []


def java_path_to_jacoco_key(java_path):
    """Convert a Java source file path to JaCoCo package/file key.

    Example:
        "core/src/main/java/org/apache/gravitino/Metalake.java"
        -> "org/apache/gravitino/Metalake.java"
    """
    parts = java_path.split("/src/main/java/")
    if len(parts) == 2:
        return parts[1]
    return None


def generate_report(overall, source_files, changed_java_files,
                    min_overall, min_changed, pass_emoji, fail_emoji):
    """Generate a markdown coverage report."""
    lines = ["## Code Coverage Report", ""]

    # Overall coverage table
    lines.append("### Overall Project Coverage")
    lines.append("")
    lines.append("| Type | Covered | Missed | Total | Coverage |")
    lines.append("|------|---------|--------|-------|----------|")
    for ctype in COUNTER_TYPES:
        if ctype in overall:
            c = overall[ctype]
            total = c["covered"] + c["missed"]
            pct = coverage_pct(c)
            lines.append(
                f"| {ctype} | {c['covered']} | {c['missed']} "
                f"| {total} | {pct}% |"
            )

    overall_line_pct = coverage_pct(
        overall.get("LINE", {"missed": 0, "covered": 0})
    )
    emoji = pass_emoji if overall_line_pct >= min_overall else fail_emoji
    lines.append("")
    lines.append(
        f"> **Overall Line Coverage: {overall_line_pct}%** {emoji} "
        f"(minimum: {min_overall}%)"
    )

    # Changed files coverage
    lines.append("")
    if changed_java_files:
        changed_total = {}
        changed_file_rows = []
        for jf in changed_java_files:
            key = java_path_to_jacoco_key(jf)
            if key and key in source_files:
                counters = source_files[key]
                merge_counters(changed_total, counters)
                line_c = counters.get("LINE", {"missed": 0, "covered": 0})
                branch_c = counters.get("BRANCH", {"missed": 0, "covered": 0})
                changed_file_rows.append({
                    "file": jf,
                    "line_pct": coverage_pct(line_c),
                    "branch_pct": coverage_pct(branch_c),
                })

        if changed_file_rows:
            lines.append("### Changed Files Coverage")
            lines.append("")
            lines.append("| File | Line Coverage | Branch Coverage |")
            lines.append("|------|:------------:|:--------------:|")
            for row in sorted(changed_file_rows, key=lambda r: r["line_pct"]):
                lines.append(
                    f"| {row['file']} | {row['line_pct']}% "
                    f"| {row['branch_pct']}% |"
                )

            changed_line_pct = coverage_pct(
                changed_total.get("LINE", {"missed": 0, "covered": 0})
            )
            emoji2 = (pass_emoji if changed_line_pct >= min_changed
                      else fail_emoji)
            lines.append("")
            lines.append(
                f"> **Changed Files Line Coverage: {changed_line_pct}%** "
                f"{emoji2} (minimum: {min_changed}%)"
            )
        else:
            lines.append(
                "_No coverage data found for the changed Java source files._"
            )
    else:
        lines.append("_No Java source files changed in this PR._")

    return "\n".join(lines), overall_line_pct


def write_github_outputs(overall_pct, changed_pct):
    """Write outputs to GITHUB_OUTPUT file for subsequent workflow steps."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a") as f:
            f.write(f"has_reports=true\n")
            f.write(f"coverage-overall={overall_pct}\n")
            f.write(f"coverage-changed-files={changed_pct}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate JaCoCo coverage report for PRs"
    )
    parser.add_argument("--base-ref", default="main",
                        help="Base branch ref for diff")
    parser.add_argument(
        "--report-pattern",
        default="**/build/reports/jacoco/test/jacocoTestReport.xml",
        help="Glob pattern for JaCoCo XML reports",
    )
    parser.add_argument("--min-overall", type=float, default=40,
                        help="Minimum overall line coverage percentage")
    parser.add_argument("--min-changed", type=float, default=60,
                        help="Minimum changed files line coverage percentage")
    parser.add_argument("--output", default="coverage-report.md",
                        help="Output markdown file path")
    parser.add_argument("--pass-emoji", default=":green_circle:",
                        help="Emoji for passing coverage")
    parser.add_argument("--fail-emoji", default=":red_circle:",
                        help="Emoji for failing coverage")
    args = parser.parse_args()

    overall, source_files = parse_reports(args.report_pattern)

    if not overall:
        print("No JaCoCo reports found.")
        output_file = os.environ.get("GITHUB_OUTPUT")
        if output_file:
            with open(output_file, "a") as f:
                f.write("has_reports=false\n")
        return

    changed_java_files = get_changed_java_files(args.base_ref)

    report, overall_pct = generate_report(
        overall, source_files, changed_java_files,
        args.min_overall, args.min_changed,
        args.pass_emoji, args.fail_emoji,
    )

    with open(args.output, "w") as f:
        f.write(report)

    print(report)

    # Compute changed files coverage for output
    changed_total = {}
    for jf in changed_java_files:
        key = java_path_to_jacoco_key(jf)
        if key and key in source_files:
            merge_counters(changed_total, source_files[key])
    changed_pct = coverage_pct(
        changed_total.get("LINE", {"missed": 0, "covered": 0})
    )

    write_github_outputs(overall_pct, changed_pct)


if __name__ == "__main__":
    main()
