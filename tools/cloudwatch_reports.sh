#!/bin/bash

# Pipeline Analysis Script
# Analyzes pipeline logs to categorize AOIs into success, failure, and error states

set -euo pipefail

# Check arguments
if [ $# -lt 3 ] || [ $# -gt 5 ]; then
    echo "Usage: $0 <run_list> <batch_name> <output_dir> [start_datetime] [end_datetime]"
    echo "  run_list      : Path to file containing list of AOIs to process"
    echo "  batch_name    : Batch name for filtering logs (e.g., fim100_huc12_10m_2025-08-06-12)"
    echo "  output_dir    : Directory where output files will be written"
    echo "  start_datetime: Start date-time in YYYY-MM-DD-HH format (optional)"
    echo "  end_datetime  : End date-time in YYYY-MM-DD-HH format (optional)"
    echo ""
    echo "Examples:"
    echo "  # Use default 7 days back"
    echo "  $0 run_list.txt batch_name output_dir"
    echo ""
    # You should use a narrow time range when you have more than 10000 results in your queries. In that case use the AWS console to see when the messages were written and then query for multiple time ranges during that time to get to less than 10000 for each query
    echo "  # Use specific date-hour range"
    echo "  $0 run_list.txt batch_name output_dir 2025-08-06-12 2025-08-09-18"
    exit 1
fi

RUN_LIST="$1"
BATCH_NAME="$2"
OUTPUT_DIR="$3"
LOG_GROUP="/aws/batch/job"

# Validate run list exists
if [ ! -f "$RUN_LIST" ]; then
    echo "Error: Run list file not found: $RUN_LIST"
    exit 1
fi

# Handle date range
if [ $# -eq 5 ]; then
    # Absolute date range provided
    START_DATETIME="$4"
    END_DATETIME="$5"
    
    # Parse date-hour format (YYYY-MM-DD-HH)
    if [[ ! "$START_DATETIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "Error: Invalid start datetime format. Use YYYY-MM-DD-HH (e.g., 2025-08-06-12)"
        exit 1
    fi
    if [[ ! "$END_DATETIME" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}$ ]]; then
        echo "Error: Invalid end datetime format. Use YYYY-MM-DD-HH (e.g., 2025-08-09-18)"
        exit 1
    fi
    
    # Extract components
    START_DATE="${START_DATETIME:0:10}"  # YYYY-MM-DD
    START_HOUR="${START_DATETIME:11:2}"  # HH
    END_DATE="${END_DATETIME:0:10}"      # YYYY-MM-DD
    END_HOUR="${END_DATETIME:11:2}"      # HH
    
    # Validate dates
    if ! date -d "$START_DATE" >/dev/null 2>&1; then
        echo "Error: Invalid date in start datetime"
        exit 1
    fi
    if ! date -d "$END_DATE" >/dev/null 2>&1; then
        echo "Error: Invalid date in end datetime"
        exit 1
    fi
    
    # Validate hours
    if [ "$START_HOUR" -lt 0 ] || [ "$START_HOUR" -gt 23 ]; then
        echo "Error: Invalid hour in start datetime (must be 00-23)"
        exit 1
    fi
    if [ "$END_HOUR" -lt 0 ] || [ "$END_HOUR" -gt 23 ]; then
        echo "Error: Invalid hour in end datetime (must be 00-23)"
        exit 1
    fi
    
    # Convert to Unix timestamps
    START_TIME=$(date -d "$START_DATE $START_HOUR:00:00" +%s)
    END_TIME=$(date -d "$END_DATE $END_HOUR:59:59" +%s)
    
    TIME_RANGE_DESC="$START_DATETIME to $END_DATETIME"
else
    # Default to 7 days back
    START_TIME=$(date -u -d "7 days ago" +%s)
    END_TIME=$(date +%s)
    TIME_RANGE_DESC="last 7 days"
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Starting pipeline analysis for batch: $BATCH_NAME"
echo "Output directory: $OUTPUT_DIR"
echo "Time range: $TIME_RANGE_DESC"

# Sort the run list for later comparisons
sort "$RUN_LIST" > "$OUTPUT_DIR/sorted-run-list.txt"

# Function to run CloudWatch query
run_query() {
    local query="$1"
    local output_file="$2"
    
    echo "Running query and saving to: $output_file"
    
    # Start the query
    query_id=$(aws logs start-query \
        --log-group-name "$LOG_GROUP" \
        --start-time "$START_TIME" \
        --end-time "$END_TIME" \
        --query-string "$query" \
        --output text --query 'queryId')
    
    # Wait for completion
    while true; do
        status=$(aws logs get-query-results --query-id "$query_id" --output text --query 'status')
        if [ "$status" = "Complete" ]; then
            break
        fi
        echo "  Query status: $status. Waiting..."
        sleep 5
    done
    
    # Save results
    aws logs get-query-results --query-id "$query_id" --output text > "$output_file"
    
    # Check if we hit the 10000 result limit
    result_count=$(grep -c "^[0-9]" "$output_file" || echo 0)
    if [ "$result_count" -eq 10000 ]; then
        echo " WARNING: Query returned exactly 10000 results (CloudWatch limit reached)"
        echo " Some results may be missing. Consider using a narrower time range."
        echo "Current time range: $TIME_RANGE_DESC"
    else
        echo "  Retrieved $result_count results"
    fi
}

echo ""
echo "Finding pipelines with different statuses..."

# Find SUCCESS pipelines
run_query "fields @logStream
| filter @logStream like /pipeline\/dispatch-\[batch_name=$BATCH_NAME/
| filter strcontains(@message, \"Pipeline SUCCESS\")
| dedup @logStream" "$OUTPUT_DIR/success-pipeline-list.txt"

# Find early exit pipelines
run_query "fields @logStream
| filter @logStream like /pipeline\/dispatch-\[batch_name=$BATCH_NAME/
| filter strcontains(@message, \"Pipeline exiting early\")
| dedup @logStream" "$OUTPUT_DIR/early-pipeline-list.txt"

# Find FAILED pipelines
run_query "fields @logStream
| filter @logStream like /pipeline\/dispatch-\[batch_name=$BATCH_NAME/
| filter strcontains(@message, \"Pipeline FAILED\")
| dedup @logStream" "$OUTPUT_DIR/fail-pipeline-list.txt"

# Extract AOI names from each list
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/success-pipeline-list.txt" | sort -u > "$OUTPUT_DIR/unique_success_aoi_names.txt" || touch "$OUTPUT_DIR/unique_success_aoi_names.txt"
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/early-pipeline-list.txt" | sort -u > "$OUTPUT_DIR/unique_early_aoi_names.txt" || touch "$OUTPUT_DIR/unique_early_aoi_names.txt"
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/fail-pipeline-list.txt" | sort -u > "$OUTPUT_DIR/unique_fail_aoi_names.txt" || touch "$OUTPUT_DIR/unique_fail_aoi_names.txt"

# Remove failures that succeeded eventually
if [ -s "$OUTPUT_DIR/unique_fail_aoi_names.txt" ] && [ -s "$OUTPUT_DIR/unique_success_aoi_names.txt" ]; then
    comm -23 "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/unique_success_aoi_names.txt" > "$OUTPUT_DIR/temp_fails.txt"
    mv "$OUTPUT_DIR/temp_fails.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt"
fi

# Add early exits to success list
sort -u -o "$OUTPUT_DIR/unique_success_aoi_names.txt" "$OUTPUT_DIR/unique_success_aoi_names.txt" "$OUTPUT_DIR/unique_early_aoi_names.txt"

echo ""
echo "Finding silent failures (pipelines that didn't run or have no terminal messages)..."

# Find AOIs that aren't in success or fail lists
comm -23 <(comm -23 "$OUTPUT_DIR/sorted-run-list.txt" "$OUTPUT_DIR/unique_success_aoi_names.txt") "$OUTPUT_DIR/unique_fail_aoi_names.txt" > "$OUTPUT_DIR/silent.txt"

# Add silent failures to fail list
if [ -s "$OUTPUT_DIR/silent.txt" ]; then
    cat "$OUTPUT_DIR/silent.txt" >> "$OUTPUT_DIR/unique_fail_aoi_names.txt"
    sort -u -o "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt"
fi

echo ""
echo "Finding job error messages in failed pipeline logs..."

run_query "fields @logStream, @message
| filter @logStream like /pipeline\/dispatch-\[batch_name=$BATCH_NAME/
| filter @message like /OOM|HTTPConnectionPool\(host='nomad-server-test\.test\.nextgenwaterprediction\.com', port=4646\): Read timed out|BaseNomadException|JobStatus\.LOST|JobStatus\.STOPPED|JobStatus\.CANCELLED|botocore\.exceptions\.ClientError/
| dedup @logStream" "$OUTPUT_DIR/error-pipeline-list.txt"

# Extract AOIs with errors
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/error-pipeline-list.txt" | sort -u > "$OUTPUT_DIR/unique_error_aoi_names.txt" || touch "$OUTPUT_DIR/unique_error_aoi_names.txt"

# Add to failure list
if [ -s "$OUTPUT_DIR/unique_error_aoi_names.txt" ]; then
    sort -u -o "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/unique_error_aoi_names.txt"
fi

echo ""
echo "Finding errors in agreement_maker/fim_mosaicker jobs..."

run_query "fields @logStream, @message
| filter @logStream like /(agreement_maker|fim_mosaicker)\/dispatch-\[batch_name=$BATCH_NAME/
| filter (@message like /[Ee][Rr][Rr][Oo][Rr]/ or @message like /[Ww][Aa][Rr][Nn]/)
  and @message not like /Agreement map contains no valid data - all pixels are either nodata or masked/
  and @message not like /distributed\.shuffle\._scheduler_plugin - WARNING/
  and @message not like /No features found in bounding box for levees/
  and @message not like /No features found in bounding box for waterbodies/
  and @message not like /Worker is at ... memory usage/
  and @message not like /WARNING - gc\.collect\(\) took/
  and @message not like /UserWarning: Sending large graph of size/
  and @message not like /warnings.warn\(/
  and @message not like /has GPKG application_id, but non conformant file extension/
| dedup @logStream" "$OUTPUT_DIR/agr-and-mos-error-pipeline-list.txt"

# Extract AOIs with agreement/mosaic errors
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/agr-and-mos-error-pipeline-list.txt" | sort -u > "$OUTPUT_DIR/unique_agg_and_mos_error_aoi_names.txt" || touch "$OUTPUT_DIR/unique_agg_and_mos_error_aoi_names.txt"

# Add to failure list
if [ -s "$OUTPUT_DIR/unique_agg_and_mos_error_aoi_names.txt" ]; then
    sort -u -o "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/unique_agg_and_mos_error_aoi_names.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt"
fi

echo ""
echo "Inspecting inundate jobs for ERROR messages..."

run_query "fields @logStream, @message
| filter @logStream like /hand_inundator\/dispatch-\[batch_name=$BATCH_NAME/
| filter (@message like /[Ee][Rr][Rr][Oo][Rr]/ or @message like /[Ww][Aa][Rr][Nn]/)
  and @message not like /hand_inundator run failed: No matching forecast data for catchment features/
  and @message not like /hand_inundator run failed: argument of type 'NoneType' is not iterable/
  and @message not like /hand_inundator run failed: No catchments with negative LakeID -999 found in hydrotable CSV/
| dedup @logStream" "$OUTPUT_DIR/true-inundate-failures.txt"

# Extract AOIs with real inundate failures
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/true-inundate-failures.txt" | sort -u > "$OUTPUT_DIR/inundate_fail_aoi_names.txt" || touch "$OUTPUT_DIR/inundate_fail_aoi_names.txt"

if [ -s "$OUTPUT_DIR/inundate_fail_aoi_names.txt" ]; then
    # Add to failure list
    sort -u -o "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/inundate_fail_aoi_names.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt"
fi

# Update success list to remove pipelines with fixable errors from inunundate, mosaic, or agreement stages
comm -23 "$OUTPUT_DIR/unique_success_aoi_names.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt" > "$OUTPUT_DIR/temp.txt"
mv "$OUTPUT_DIR/temp.txt" "$OUTPUT_DIR/unique_success_aoi_names.txt"

echo ""
echo "Step: Adding pipelines with valid errors to success list..."

run_query "fields @logStream, @message
| filter @logStream like /(agreement_maker|fim_mosaicker)\/dispatch-\[batch_name=$BATCH_NAME/
| filter @message like /Agreement map contains no valid data - all pixels are either nodata or masked/
| dedup @logStream" "$OUTPUT_DIR/error-ignore-pipelines.txt"

# Extract AOIs to ignore
grep -oP 'aoi_name=\K[^,\]]+' "$OUTPUT_DIR/error-ignore-pipelines.txt" | sort -u > "$OUTPUT_DIR/error_ignore_aoi_names.txt" || touch "$OUTPUT_DIR/error_ignore_aoi_names.txt"

if [ -s "$OUTPUT_DIR/error_ignore_aoi_names.txt" ]; then
    # Don't ignore pipelines that have other serious errors
    if [ -s "$OUTPUT_DIR/unique_agg_and_mos_error_aoi_names.txt" ]; then
        comm -23 "$OUTPUT_DIR/error_ignore_aoi_names.txt" "$OUTPUT_DIR/unique_agg_and_mos_error_aoi_names.txt" > "$OUTPUT_DIR/temp.txt"
        mv "$OUTPUT_DIR/temp.txt" "$OUTPUT_DIR/error_ignore_aoi_names.txt"
    fi
    
    if [ -s "$OUTPUT_DIR/inundate_fail_aoi_names.txt" ]; then
        comm -23 "$OUTPUT_DIR/error_ignore_aoi_names.txt" "$OUTPUT_DIR/inundate_fail_aoi_names.txt" > "$OUTPUT_DIR/temp.txt"
        mv "$OUTPUT_DIR/temp.txt" "$OUTPUT_DIR/error_ignore_aoi_names.txt"
    fi
    
    # Add to success list
    if [ -s "$OUTPUT_DIR/error_ignore_aoi_names.txt" ]; then
        sort -u -o "$OUTPUT_DIR/unique_success_aoi_names.txt" "$OUTPUT_DIR/error_ignore_aoi_names.txt" "$OUTPUT_DIR/unique_success_aoi_names.txt"
        
        # Remove from failure list
        comm -23 "$OUTPUT_DIR/unique_fail_aoi_names.txt" "$OUTPUT_DIR/error_ignore_aoi_names.txt" > "$OUTPUT_DIR/temp.txt"
        mv "$OUTPUT_DIR/temp.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt"
    fi
fi

echo ""
echo "Verifying counts..."

total_input=$(wc -l < "$RUN_LIST")
total_success=$(wc -l < "$OUTPUT_DIR/unique_success_aoi_names.txt" || echo 0)
total_fail=$(wc -l < "$OUTPUT_DIR/unique_fail_aoi_names.txt" || echo 0)
total_output=$((total_success + total_fail))

echo ""
echo "========== SUMMARY =========="
echo "Time range queried: $TIME_RANGE_DESC"
echo "Input AOIs: $total_input"
echo "Successful AOIs: $total_success"
echo "Failed AOIs: $total_fail"
echo "Total processed: $total_output"
echo ""

if [ "$total_input" -ne "$total_output" ]; then
    echo "WARNING: Input count ($total_input) does not match output count ($total_output)!"
    echo "Missing AOIs: $((total_input - total_output))"
    echo ""
    echo "Creating list of missing AOIs..."
    comm -23 "$OUTPUT_DIR/sorted-run-list.txt" <(sort -u "$OUTPUT_DIR/unique_success_aoi_names.txt" "$OUTPUT_DIR/unique_fail_aoi_names.txt") > "$OUTPUT_DIR/missing_aois.txt"
    echo "Missing AOIs saved to: $OUTPUT_DIR/missing_aois.txt"
else
    echo "All AOIs accounted for!"
fi

echo ""
echo "Output files:"
echo "  Success list: $OUTPUT_DIR/unique_success_aoi_names.txt"
echo "  Failure list: $OUTPUT_DIR/unique_fail_aoi_names.txt"
echo ""
echo "Analysis complete!"
