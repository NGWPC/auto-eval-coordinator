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
COMPLETED=""

# Generate timestamp in format: YYYY-MM-DD-HH (e.g., 2025-08-01-14 for 2PM on Aug 1, 2025)
TIMESTAMP=$(date +"%Y-%m-%d-%H")

get_nomad_memory_usage_percentage() {
    # Get memory usage from Nomad server using operator metrics
    local metrics_json
    
    # Get allocated and system memory from Nomad metrics using jq
    # Note: nomad can print "Unable to retrieve credentials" to stderr even when successful
    # We use PIPESTATUS to check nomad command exit code, not grep exit code
    metrics_json=$(nomad operator metrics -json 2>&1 | grep -v "Unable to retrieve credentials")
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "Error: 'nomad operator metrics' command failed" >&2
        return 1
    fi

    local alloc_bytes
    alloc_bytes=$(echo "$metrics_json" | jq -r '.Gauges[] | select(.Name == "nomad.runtime.alloc_bytes") | .Value')
    
    local sys_bytes
    sys_bytes=$(echo "$metrics_json" | jq -r '.Gauges[] | select(.Name == "nomad.runtime.sys_bytes") | .Value')
    
    if [[ -z "$alloc_bytes" || "$alloc_bytes" == "null" || -z "$sys_bytes" || "$sys_bytes" == "null" ]]; then
        echo "Error: Could not parse memory metrics from Nomad. Raw metrics JSON:" >&2
        echo "$metrics_json" >&2
        return 1
    fi
    
    # Calculate percentage: (allocated / system) * 100
    local percentage
    percentage=$((alloc_bytes * 100 / sys_bytes))
    echo $percentage
}

start_nomad_gc() {
    echo "Starting Nomad garbage collection background process (triggered when memory >= 20%)..."
    (
        # Ensure NOMAD_TOKEN and NOMAD_ADDR are available in the background process
        export NOMAD_TOKEN="${NOMAD_TOKEN}"
        export NOMAD_ADDR="${NOMAD_ADDR:-http://nomad-server-test.test.nextgenwaterprediction.com:4646}"
        
        while true; do
            local memory_usage
            memory_usage=$(get_nomad_memory_usage_percentage 2>&1)
            local exit_code=$?
            if [[ $exit_code -eq 0 ]]; then
                echo -e "\n$(date): Nomad memory usage: ${memory_usage}%"
                if [[ $memory_usage -ge 20 ]]; then
                    echo "$(date): Memory usage >= 20% - Running nomad system gc"
                    nomad system gc
                fi
            else
                echo -e "\n$(date): Warning - Could not check Nomad memory usage. Debug info: $memory_usage"
            fi
            sleep 120  # Check every N seconds
        done
    ) &
    NOMAD_GC_PID=$!
    echo "Nomad GC background process started with PID: $NOMAD_GC_PID"
    
    # Function to cleanup background process on script exit
    cleanup_nomad_gc() {
        if [[ -n "$NOMAD_GC_PID" ]]; then
            echo "Stopping Nomad GC background process (PID: $NOMAD_GC_PID)..."
            kill $NOMAD_GC_PID 2>/dev/null
        fi
    }
    
    # Set trap to cleanup on script exit
    trap cleanup_nomad_gc EXIT
}

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

# Start background Nomad garbage collection
start_nomad_gc

for resolution in "${RESOLUTIONS[@]}"; do
    echo "=== Processing Resolution: ${resolution}m ==="
    
    # Define resolution-specific parameters
    batch_name="fim100_huc12_${resolution}m_${TIMESTAMP}"
    output_root="s3://fimc-data/autoeval/batches/fim100_huc12_${resolution}_non_calibrated/"
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
        
        # Build the submit_stac_batch.py command
        submit_cmd="python tools/submit_stac_batch.py --batch_name $batch_name --output_root $output_root --hand_index_path $hand_index_path --benchmark_sources \"$collection\" --item_list $item_list_file --wait_seconds 5 --max_pipelines 150" # Scaling tests showed you shouldn't go above ~200 pipelines with current Nomad deploy.
        
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
        
        # Refresh AWS credentials before generating report
        echo "--- Refreshing AWS Credentials for $collection report ---"
        aws sso login --profile AWSPowerUserAccess-591210920133
        if [[ $? -ne 0 ]]; then
            echo "Error: AWS SSO login failed"
            read -p "Continue anyway? (y/n): " -n 1 -r
            echo ""
            [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
        fi
        
        if [[ -f "./update_aws_creds.sh" ]]; then
            echo "Running update_aws_creds.sh..."
            ./update_aws_creds.sh
            if [[ $? -ne 0 ]]; then
                echo "Warning: update_aws_creds.sh failed"
            fi
        else
            echo "Warning: update_aws_creds.sh not found, skipping"
        fi
        
        # Generate report for this collection before purging
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
                echo "View dashboard: $collection_reports_dir/batch_analysis_dashboard.html"
            fi
        fi
        
        # Purge dispatch jobs after each collection
        echo "--- Purging Dispatch Jobs after $collection ---"
        purge_cmd="python tools/purge_dispatch_jobs.py"
        
        if confirm_command "$purge_cmd"; then
            echo "Executing purge_dispatch_jobs.py..."
            eval $purge_cmd
            if [[ $? -ne 0 ]]; then
                echo "Warning: purge_dispatch_jobs.py failed"
            fi
        fi
        
        echo ""
    done
    
    # After processing all collections for this resolution, run make_master_metrics
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
