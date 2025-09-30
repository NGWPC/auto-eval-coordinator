#!/usr/bin/env python3

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple
from datetime import datetime


def setup_arguments():
    """Sets up and parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyzes local pipeline logs to categorize AOIs, providing detailed intermediate outputs for verification.",
        epilog="""Examples:
  # Analyze logs in a log directory
  %(prog)s --run_list run_list.txt --log_dir /path/to/log_dir --report_dir /path/to/report_dir
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--run_list",
        type=Path,
        required=True,
        help="Path to file containing list of AOIs to process",
    )
    parser.add_argument(
        "--log_dir",
        type=Path,
        required=True,
        help="Path to log directory containing logs",
    )
    parser.add_argument(
        "--report_dir",
        type=Path,
        required=True,
        help="Directory where ALL report files will be written",
    )
    return parser.parse_args()


def extract_aoi_from_dirname(dirname: str) -> str:
    """Extracts AOI name from a directory name."""
    pattern = re.compile(r"aoi_name=([^,\]]+)")
    match = pattern.search(dirname)
    return match.group(1) if match else None


def parse_log_file(file_path: Path) -> List[Dict]:
    """Parses a log file and returns list of messages."""
    messages = []
    if not file_path.exists():
        print(f"File does not exist: {file_path}")
        return messages
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        # Try to extract JSON messages wrapped in curly braces
        json_pattern = re.compile(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}')
        json_matches = json_pattern.findall(content)
        
        for match in json_matches:
            try:
                msg = json.loads(match)
                messages.append(msg)
            except json.JSONDecodeError:
                # If it's not valid JSON, treat it as a plain message
                messages.append({"message": match})
        
        # Also get non-JSON lines (lines that don't start with {)
        lines = content.split('\n')
        non_json_count = 0
        for line in lines:
            line = line.strip()
            if line and not line.startswith('{'):
                messages.append({"message": line})
                non_json_count += 1
        
                
    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)
    
    return messages


def check_message_pattern(message: str, patterns: List[str]) -> bool:
    """Checks if a message matches any of the given patterns."""
    for pattern in patterns:
        if re.search(pattern, message, re.IGNORECASE):
            return True
    return False


def analyze_job_logs(log_dir: Path, job_task_pairs: List[Tuple[str, str]]) -> Dict[str, Set[str]]:
    """Analyzes logs for specific job/task pairs and categorizes AOIs."""
    results = {
        "success": set(),
        "early_exit": set(),
        "failed": set(),
        "job_errors": set(),
        "agr_mos_errors": set(),
        "inundate_errors": set(),
        "ignorable_errors": set(),
        # Store error messages by category
        "job_error_messages": [],
        "agr_mos_error_messages": [],
        "inundate_error_messages": [],
        "ignorable_error_messages": []
    }
    
    # Pattern definitions
    success_patterns = ["Pipeline SUCCESS"]
    early_exit_patterns = ["Pipeline exiting early"]
    failed_patterns = ["Pipeline FAILED"]
    
    job_error_patterns = [
        r"OOM",
        r"HTTPConnectionPool",
        r"BaseNomadException",
        r"JobStatus\.(LOST|STOPPED|CANCELLED)",
        r"botocore\.exceptions\.ClientError"
    ]
    
    agr_mos_error_patterns = [
        r"[Ee][Rr][Rr][Oo][Rr]",
        r"[Ww][Aa][Rr][Nn]"
    ]
    
    agr_mos_exclude_patterns = [
        r"Agreement map contains no valid data",
        r"distributed\.shuffle",
        r"No features found",
        r"Worker is at",
        r"distributed\.worker\.memory - WARNING - Unmanaged memory use is high",
        r"gc\.collect",
        r"Warning 1: Request for \d+-\d+ failed with response_code=\d+",
        r"Failed to communicate with scheduler during heartbeat",
        r"NotGeoreferencedWarning",
        r"distributed\.comm\.core\.CommClosedError",
        r"UserWarning: Sending large graph",
	r"is already in use",
        r"warnings\.warn",
        r"has GPKG application_id",
        r"distributed\.nanny - WARNING - Worker process still alive"
    ]
    
    inundate_error_patterns = [
        r"[Ee][Rr][Rr][Oo][Rr]",
        r"[Ww][Aa][Rr][Nn]"
    ]
    
    inundate_exclude_patterns = [
        r"No matching forecast data",
        r"'NoneType' is not iterable",
        r"No catchments with negative LakeID"
    ]
    
    ignorable_patterns = ["Agreement map contains no valid data"]
    
    
    # Iterate through job directories (agreement_maker, fim_mosaicker, etc.)
    job_type_dirs = list(log_dir.iterdir())
  
    
    for job_type_dir in job_type_dirs:
        if not job_type_dir.is_dir():
            continue
        
        # Check if this directory matches one of our job types
        matching_job = None
        matching_task = None
        for job_name, task_name in job_task_pairs:
            if job_name == job_type_dir.name:
                matching_job = job_name
                matching_task = task_name
                break
        
        if not matching_job:
            continue
        
        
        # Now iterate through dispatch directories within this job type
        dispatch_dirs = list(job_type_dir.iterdir())
        
        for dispatch_dir in dispatch_dirs:
            if not dispatch_dir.is_dir():
                continue
            
            aoi_name = extract_aoi_from_dirname(dispatch_dir.name)
            if not aoi_name:
                continue
            
            # Look for stderr files for this task
            stderr_files = list(dispatch_dir.glob(f"{matching_task}.stderr.*"))
            
            
            for stderr_file in stderr_files:
                messages = parse_log_file(stderr_file)
                
                for msg in messages:
                    msg_text = msg.get("message", "") if isinstance(msg, dict) else str(msg)
                    
                    # Create message info object for tracking
                    message_info = {
                        "aoi": aoi_name,
                        "job": matching_job,
                        "task": matching_task,
                        "file": str(stderr_file.relative_to(log_dir)),  # Use relative path for cleaner output
                        "message": msg_text
                    }
                    
                    # Check for pipeline status
                    if matching_job == "pipeline":
                        if check_message_pattern(msg_text, success_patterns):
                            results["success"].add(aoi_name)
                        elif check_message_pattern(msg_text, early_exit_patterns):
                            results["early_exit"].add(aoi_name)
                        elif check_message_pattern(msg_text, failed_patterns):
                            results["failed"].add(aoi_name)
                        elif check_message_pattern(msg_text, job_error_patterns):
                            results["job_errors"].add(aoi_name)
                            results["job_error_messages"].append(message_info)
                    
                    # Check for agreement_maker/fim_mosaicker errors
                    elif matching_job in ["agreement_maker", "fim_mosaicker"]:
                        if check_message_pattern(msg_text, agr_mos_error_patterns):
                            if not check_message_pattern(msg_text, agr_mos_exclude_patterns):
                                results["agr_mos_errors"].add(aoi_name)
                                results["agr_mos_error_messages"].append(message_info)
                        if check_message_pattern(msg_text, ignorable_patterns):
                            results["ignorable_errors"].add(aoi_name)
                            results["ignorable_error_messages"].append(message_info)
                    
                    # Check for hand_inundator errors
                    elif matching_job == "hand_inundator":
                        if check_message_pattern(msg_text, inundate_error_patterns):
                            if not check_message_pattern(msg_text, inundate_exclude_patterns):
                                results["inundate_errors"].add(aoi_name)
                                results["inundate_error_messages"].append(message_info)
    
    return results


def write_aois_to_file(file_path: Path, aois: Set[str]):
    """Writes a sorted set of AOIs to a text file in the report directory."""
    print(f"  Writing {len(aois)} AOIs to {file_path}")
    with file_path.open("w") as f:
        for aoi in sorted(list(aois)):
            f.write(f"{aoi}\n")


def write_data_to_json(file_path: Path, data):
    """Writes data to a pretty-printed JSON file in the report directory."""
    print(f"  Writing {len(data) if isinstance(data, list) else 'data'} items to {file_path}")
    with file_path.open("w") as f:
        json.dump(data, f, indent=2, default=str)


def main():
    """Main function to orchestrate the pipeline analysis."""
    args = setup_arguments()

    if not args.run_list.is_file():
        print(f"Error: Run list file not found: {args.run_list}", file=sys.stderr)
        sys.exit(1)
    
    if not args.log_dir.is_dir():
        print(f"Error: Log directory not found: {args.log_dir}", file=sys.stderr)
        sys.exit(1)

    args.report_dir.mkdir(parents=True, exist_ok=True)
    
    batch_name = args.log_dir.name
    print(f"Starting analysis for batch: {batch_name}")
    print(f"Log directory: {args.log_dir}")
    

    # Define job/task pairs to analyze
    job_task_pairs = [
        ("pipeline", "coordinator"),
        ("hand_inundator", "processor"),
        ("fim_mosaicker", "processor"),
        ("agreement_maker", "processor")
    ]
    
    
    # Analyze logs
    print("\nAnalyzing local log files...")
    results = analyze_job_logs(args.log_dir, job_task_pairs)
    
    # Write intermediate files - AOI lists
    for category in ["success", "early_exit", "failed", "job_errors", 
                     "agr_mos_errors", "inundate_errors"]:
        if category in results:
            write_aois_to_file(args.report_dir / f"{category}_aois.txt", results[category])
    
    # Write error message JSON files for each error category
    error_categories = [
        ("job_error_messages", "job_errors.json"),
        ("agr_mos_error_messages", "agr_mos_errors.json"),
        ("inundate_error_messages", "inundate_errors.json"),
        ("ignorable_error_messages", "ignorable_errors.json")
    ]
    
    for message_key, json_filename in error_categories:
        if results.get(message_key):
            write_data_to_json(args.report_dir / json_filename, results[message_key])
    
    # --- Categorize AOIs using set logic ---
    print("\nCategorizing all AOIs...")
    initial_aois = {line.strip() for line in args.run_list.open() if line.strip()}
    write_aois_to_file(args.report_dir / "sorted-run-list.txt", initial_aois)

    successful_aois = results["success"].copy()
    successful_aois.update(results["early_exit"])

    failed_aois = results["failed"].copy()
    all_found_aois = successful_aois.union(failed_aois)
    silent_failures = initial_aois - all_found_aois
    
    # Write the silent failures list
    write_aois_to_file(args.report_dir / "silent_failures.txt", silent_failures)

    failed_aois.update(silent_failures)
    failed_aois.update(results["job_errors"])
    failed_aois.update(results["agr_mos_errors"])
    failed_aois.update(results["inundate_errors"])

    successful_aois -= failed_aois

    # AOIs with ignorable errors but no real errors should be considered successful
    real_error_aois = results["agr_mos_errors"].union(results["inundate_errors"])
    truly_ignorable = results["ignorable_errors"] - real_error_aois
    write_aois_to_file(args.report_dir / "ignorable_errors_aois.txt", truly_ignorable)
    successful_aois.update(truly_ignorable)
    failed_aois -= truly_ignorable

    # --- Final Summary and Output ---
    print("\n" + "=" * 10 + " SUMMARY " + "=" * 10)
    print(f"Batch analyzed:  {batch_name}")
    print(f"Input AOIs:      {len(initial_aois)}")
    print(f"Successful AOIs: {len(successful_aois)}")
    print(f"Failed AOIs:     {len(failed_aois)}")
    total_output = len(successful_aois) + len(failed_aois)
    print(f"Total processed: {total_output}\n")

    # Write final output files
    write_aois_to_file(args.report_dir / "unique_success_aoi_names.txt", successful_aois)
    write_aois_to_file(args.report_dir / "unique_fail_aoi_names.txt", failed_aois)

    if len(initial_aois) != total_output:
        missing_aois = initial_aois - successful_aois - failed_aois
        print(f"WARNING: Input count ({len(initial_aois)}) does not match output count ({total_output})!")
        print(f"Missing AOIs: {len(missing_aois)}")
        write_aois_to_file(args.report_dir / "missing_aois.txt", missing_aois)
    else:
        print("All AOIs accounted for!")

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
