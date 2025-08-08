#!/bin/bash
 
## A script to run batches in succession for all the evals to be run. 

# Find the repository root by looking for .git directory
find_repo_root() {
    local dir="$(pwd)"
    while [[ "$dir" != "/" ]]; do
        if [[ -d "$dir/.git" ]]; then
            echo "$dir"
            return 0
        fi
        dir="$(dirname "$dir")"
    done
    echo "Error: Could not find repository root (no .git directory found)" >&2
    exit 1
}

REPO_ROOT="$(find_repo_root)"
INPUTS_DIR="$REPO_ROOT/inputs"

echo "Repository root: $REPO_ROOT"
echo ""

# Change to repo root to ensure all relative paths work correctly
cd "$REPO_ROOT"

declare -a RESOLUTIONS=("10" "5" "3")

declare -a COLLECTIONS=("ble-collection" "nws-fim-collection" "ripple-fim-collection" "usgs-fim-collection")

# Space-separated list of completed runs in format "collection:resolution"
# Example: COMPLETED="ble-collection:10 nws-fim-collection:3"
COMPLETED="ble-collection:10 nws-fim-collection:10"

# Generate timestamp in format: YYYY-MM-DD-HH (e.g., 2025-08-01-14 for 2PM on Aug 1, 2025)
TIMESTAMP=$(date +"%Y-%m-%d-%H")

# Source the nomad memory monitoring functions
source "$REPO_ROOT/tools/nomad_memory_monitor.sh"

confirm_command() {
    local command="$1"
    echo "About to execute:"
    echo "$command"
    echo ""
    read -p "Proceed? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping command..."
        return 1
    fi
    return 0
}

echo "=== Autoeval Batch Processing Script ==="
echo "Processing ${#RESOLUTIONS[@]} resolutions x ${#COLLECTIONS[@]} collections"
echo "Timestamp: $TIMESTAMP"
echo ""

# Start background Nomad garbage collection with log file path
MEMORY_LOG_FILE="$REPO_ROOT/nomad_memory_usage_${TIMESTAMP}.log"
echo "Memory usage will be logged to: $MEMORY_LOG_FILE"
start_nomad_gc "$MEMORY_LOG_FILE"

for resolution in "${RESOLUTIONS[@]}"; do
    echo "=== Processing Resolution: ${resolution}m ==="
    
    # Define resolution-specific parameters
    batch_name="fim100_huc12_${resolution}m_${TIMESTAMP}"
    output_root="s3://fimc-data/autoeval/batches/fim100_huc12_${resolution}m_non_calibrated/"
    hand_index_path="s3://fimc-data/autoeval/hand_output_indices/fim100_huc12_${resolution}m_index/"
    
    # loop over collections
    for collection in "${COLLECTIONS[@]}"; do
        echo "--- Processing Collection: $collection ---"
        
        # Check if this collection:resolution combination has been completed
        if [[ " $COMPLETED " =~ " ${collection}:${resolution} " ]]; then
            echo "Already completed. Skipping ${collection} at ${resolution}m..."
            continue
        fi
        
        # Define collection-specific parameters
        item_list_file="$INPUTS_DIR/${collection}.txt"
        
        # Check if the input file exists
        if [[ ! -f "$item_list_file" ]]; then
            echo "Error: Input file $item_list_file not found!"
            continue
        fi
        
        if confirm_command "update_aws_creds.sh fimbucket"; then
            echo "Running update_aws_creds.sh..."
            update_aws_creds.sh cloudwatch
            if [[ $? -ne 0 ]]; then
                echo "Warning: update_aws_creds.sh failed"
            fi
        fi
        
        # Build the submit_stac_batch.py command
        submit_cmd="python tools/submit_stac_batch.py --batch_name $batch_name --output_root $output_root --hand_index_path $hand_index_path --benchmark_sources \"$collection\" --item_list $item_list_file --wait_seconds 10 --stop_threshold 100 --resume_threshold 70" # Scaling tests showed you shouldn't go above 100 pipelines with current Nomad deploy (36 core server + 80 r5a.xlarge) and job defs. Resume at 70 to allow pipelines to get past inundate stage.
        
        # Get user confirmation and execute
        if confirm_command "$submit_cmd"; then
            echo "Executing submit_stac_batch.py..."
            eval $submit_cmd
            if [[ $? -ne 0 ]]; then
                echo "Error: submit_stac_batch.py failed for $collection"
                read -p "Continue with next collection? (y/n): " -n 1 -r
                echo ""
                [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
            fi
        else
            continue
        fi
        
        if confirm_command "update_aws_creds.sh cloudwatch"; then
            echo "Running update_aws_creds.sh..."
            update_aws_creds.sh cloudwatch
            if [[ $? -ne 0 ]]; then
                echo "Warning: update_aws_creds.sh failed"
            fi
        fi
        
        echo "--- Generating Report for $collection at ${resolution}m ---"
        collection_reports_dir="$REPO_ROOT/reports/${batch_name}_${collection}"
        collection_report_cmd="python tools/batch_run_reports.py --batch_name $batch_name --output_dir $collection_reports_dir --pipeline_log_group /aws/ec2/nomad-client-linux-test --job_log_group /aws/ec2/nomad-client-linux-test --s3_output_root $output_root --aoi_list $item_list_file --collection $collection --html"
        
        if confirm_command "$collection_report_cmd"; then
            echo "Executing batch_run_reports.py for $collection..."
            eval $collection_report_cmd
            if [[ $? -ne 0 ]]; then
                echo "Warning: batch_run_reports.py failed for $collection"
            else
                echo "Reports generated for $collection at: $collection_reports_dir"
                echo "View HTML dashboard in: $collection_reports_dir/"
            fi
        fi
      
        echo "--- Purging Dispatch Jobs after $collection ---"
        purge_cmd="python tools/purge_dispatch_jobs.py && nomad system gc"
        
        if confirm_command "$purge_cmd"; then
            echo "Executing purge_dispatch_jobs.py..."
            eval $purge_cmd
            if [[ $? -ne 0 ]]; then
                echo "Warning: purge_dispatch_jobs.py failed"
            fi
        fi
        
        echo ""
    done
    
    echo "--- Creating Master Metrics for ${resolution}m ---"
    master_metrics_cmd="python tools/make_master_metrics.py $output_root --hand-version \"fim100_huc12\" --resolution \"$resolution\""
    
    if confirm_command "$master_metrics_cmd"; then
        echo "Executing make_master_metrics.py..."
        eval $master_metrics_cmd
        if [[ $? -ne 0 ]]; then
            echo "Error: make_master_metrics.py failed for ${resolution}m"
            read -p "Continue with next resolution? (y/n): " -n 1 -r
            echo ""
            [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
        fi
    fi
    
    echo "=== Completed Resolution: ${resolution}m ==="
    echo ""
done
