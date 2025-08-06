#!/bin/bash

export NOMAD_TOKEN=d6be4d3c-e2cd-4678-ea57-a699915db436
export NOMAD_ADDR=http://nomad-server-test.test.nextgenwaterprediction.com:4646

echo "Fetching dispatched jobs from Nomad..."

# Get all jobs and filter for dispatched ones from our test jobs
dispatched_jobs=$(curl -s -H "X-Nomad-Token: $NOMAD_TOKEN" $NOMAD_ADDR/v1/jobs | \
  jq -r '.[] | select(.ParentID != null) | select(.ParentID | test("agreement_maker|fim_mosaicker|hand_inundator|pipeline")) | .ID')

if [ -z "$dispatched_jobs" ]; then
  echo "No dispatched jobs found for the test job definitions."
  exit 0
fi

echo "Found dispatched jobs. Checking allocations and exit codes..."
echo

# Track jobs with non-standard exit codes
jobs_with_issues=""

for job_id in $dispatched_jobs; do
  echo "Checking job: $job_id"
  
  # Get allocations for this job
  allocations=$(curl -s -H "X-Nomad-Token: $NOMAD_TOKEN" $NOMAD_ADDR/v1/job/$job_id/allocations | \
    jq -r '.[] | .ID')
  
  for alloc_id in $allocations; do
    # Get allocation details including task states
    alloc_details=$(curl -s -H "X-Nomad-Token: $NOMAD_TOKEN" $NOMAD_ADDR/v1/allocation/$alloc_id)
    
    # Extract task states and exit codes
    task_states=$(echo "$alloc_details" | jq -r '.TaskStates | to_entries[] | 
      select(.value.State == "dead") | 
      "\(.key): Exit Code = \(.value.ExitCode // "N/A")"')
    
    if [ ! -z "$task_states" ]; then
      # Check for non-standard exit codes (not 0 or 1)
      non_standard=$(echo "$alloc_details" | jq -r '.TaskStates | to_entries[] | 
        select(.value.State == "dead" and .value.ExitCode != null and .value.ExitCode != 0 and .value.ExitCode != 1) | 
        "\(.key): Exit Code = \(.value.ExitCode)"')
      
      if [ ! -z "$non_standard" ]; then
        echo "  Found non-standard exit code in allocation $alloc_id:"
        echo "     $non_standard"
        jobs_with_issues="$jobs_with_issues\nJob: $job_id, Allocation: $alloc_id\n$non_standard"
      fi
    fi
  done
done

echo
echo "========================================="
echo "SUMMARY: Jobs with exit codes other than 0 or 1:"
echo "========================================="

if [ -z "$jobs_with_issues" ]; then
  echo "All completed jobs exited with codes 0 or 1"
else
  echo -e "$jobs_with_issues"
fi
