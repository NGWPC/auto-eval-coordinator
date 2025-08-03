#!/bin/bash

## Nomad Memory Monitoring and Garbage Collection Script
## This script monitors Nomad memory usage and triggers garbage collection when needed.

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
    echo "Starting Nomad garbage collection background process (triggered when memory >= 40%)..."
    (
        # Ensure NOMAD_TOKEN and NOMAD_ADDR are available in the background process
        export NOMAD_TOKEN="${NOMAD_TOKEN}"
        export NOMAD_ADDR="${NOMAD_ADDR:-http://nomad-server-test.test.nextgenwaterprediction.com:4646}"
        
        while true; do
            local memory_usage
            memory_usage=$(get_nomad_memory_usage_percentage)
            local exit_code=$?
            if [[ $exit_code -eq 0 ]]; then
                echo -e "\n$(date): Nomad memory usage: ${memory_usage}%\n"
                if [[ $memory_usage -ge 40 ]]; then
                    echo "$(date): Memory usage >= 40% - Running nomad system gc"
                    nomad system gc
                fi
            else
                echo -e "\n$(date): Warning - Could not check Nomad memory usage\n"
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

# If script is run directly (not sourced), start the memory monitoring
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "=== Nomad Memory Monitor ==="
    start_nomad_gc
    
    # Keep the script running
    echo "Memory monitoring active. Press Ctrl+C to stop."
    wait $NOMAD_GC_PID
fi