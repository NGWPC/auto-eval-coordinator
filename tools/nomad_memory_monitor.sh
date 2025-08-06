#!/bin/bash

## Nomad Memory Monitoring and Garbage Collection Script

## This script monitors Nomad memory usage and triggers garbage collection when needed. Need to change this if you change you server instance type!
TOTAL_SYSTEM_GIB=70

# The percentage of total system memory you want to RESERVE for the OS. Set this conservatively.
RESERVATION_PERCENTAGE=20

MEMORY_CEILING_BYTES=$(( TOTAL_SYSTEM_GIB * 1024 * 1024 * 1024 * (100 - RESERVATION_PERCENTAGE) / 100 ))

get_nomad_sys_bytes() {
    local metrics_json
    metrics_json=$(nomad operator metrics -json 2>&1 | grep -v "Unable to retrieve credentials")
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "Error: 'nomad operator metrics' command failed" >&2
        return 1
    fi
    # sys_bytes indicates the number of bytes that the nomad server has requested from the system
    local sys_bytes
    sys_bytes=$(echo "$metrics_json" | jq -r '.Gauges[] | select(.Name == "nomad.runtime.sys_bytes") | .Value')
    
    if [[ -z "$sys_bytes" || "$sys_bytes" == "null" ]]; then
        echo "Error: Could not parse 'nomad.runtime.sys_bytes' from Nomad." >&2
        return 2
    fi
    echo "$sys_bytes"
}

start_nomad_gc() {
    # Set default log file path if not provided
    local log_file="${1:-nomad_memory_usage.log}"
    echo "Starting Nomad garbage collection background process..."
    echo "Memory usage logs will be written to: $log_file"
    
    (
        export NOMAD_TOKEN="${NOMAD_TOKEN}"
        export NOMAD_ADDR="${NOMAD_ADDR:-http://nomad-server-test.test.nextgenwaterprediction.com:4646}"
        
        while true; do
            local current_sys_bytes
            current_sys_bytes=$(get_nomad_sys_bytes)
            local exit_code=$?

            if [[ $exit_code -eq 0 ]]; then
                # Write memory usage to log file
                echo "$(date): Current sys_bytes: $((current_sys_bytes / 1024 / 1024 / 1024))GB | Ceiling: $((MEMORY_CEILING_BYTES / 1024 / 1024 / 1024))GB" >> "$log_file"
                
                # Direct comparison: trigger if allocated bytes are greater than or equal to the ceiling
                if [[ $current_sys_bytes -ge $MEMORY_CEILING_BYTES ]]; then
                    echo "$(date): Memory usage ($((current_sys_bytes / 1024 / 1024 / 1024))GB) has breached the ceiling ($((MEMORY_CEILING_BYTES / 1024 / 1024 / 1024))GB). Running 'nomad system gc'..." >> "$log_file"
                    nomad system gc
                fi
            else
                echo "$(date): Warning - Could not check Nomad memory usage (exit code: $exit_code)" >> "$log_file"
            fi
            sleep 120
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
