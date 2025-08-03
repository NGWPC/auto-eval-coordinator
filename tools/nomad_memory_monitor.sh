#!/bin/bash

## Nomad Memory Monitoring and Garbage Collection Script

## This script monitors Nomad memory usage and triggers garbage collection when needed.
TOTAL_SYSTEM_GIB=94

# The percentage of total system memory you want to RESERVE for the OS. Set this conservatively because sometimes nomad server can use more memory than alloc_bytes reports for inactive objects in memory
RESERVATION_PERCENTAGE=50

MEMORY_CEILING_BYTES=$(( TOTAL_SYSTEM_GIB * 1024 * 1024 * 1024 * (100 - RESERVATION_PERCENTAGE) / 100 ))

get_nomad_alloc_bytes() {
    local metrics_json
    metrics_json=$(nomad operator metrics -json 2>&1 | grep -v "Unable to retrieve credentials")
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "Error: 'nomad operator metrics' command failed" >&2
        return 1
    fi

    local alloc_bytes
    alloc_bytes=$(echo "$metrics_json" | jq -r '.Gauges[] | select(.Name == "nomad.runtime.alloc_bytes") | .Value')
    
    if [[ -z "$alloc_bytes" || "$alloc_bytes" == "null" ]]; then
        echo "Error: Could not parse 'nomad.runtime.alloc_bytes' from Nomad." >&2
        return 2
    fi
    echo "$alloc_bytes"
}

start_nomad_gc() {
    echo "Starting Nomad garbage collection background process..."
    (
        export NOMAD_TOKEN="${NOMAD_TOKEN}"
        export NOMAD_ADDR="${NOMAD_ADDR:-http://nomad-server-test.test.nextgenwaterprediction.com:4646}"
        
        while true; do
            local current_alloc_bytes
            current_alloc_bytes=$(get_nomad_alloc_bytes)
            local exit_code=$?

            if [[ $exit_code -eq 0 ]]; then
                echo -e "\n$(date): Current alloc_bytes: ${current_alloc_bytes} | Ceiling: ${MEMORY_CEILING_BYTES}"
                
                # Direct comparison: trigger if allocated bytes are greater than or equal to the ceiling
                if [[ $current_alloc_bytes -ge $MEMORY_CEILING_BYTES ]]; then
                    echo "$(date): Memory usage (${current_alloc_bytes}) has breached the ceiling (${MEMORY_CEILING_BYTES}). Running 'nomad system gc'..."
                    nomad system gc
                fi
            else
                echo -e "\n$(date): Warning - Could not check Nomad memory usage (exit code: $exit_code)"
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
