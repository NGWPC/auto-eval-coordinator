variable "repo_root" {
  description = "Path to the repository root directory"
  type        = string
}

job "hand_inundator" {
  datacenters = ["dc1"] 
  type        = "batch"

  parameterized {
    meta_required = [
      "catchment_data_path",
      "forecast_path",
      "output_path",
    ]
    meta_optional = [
      "fim_type", 
      "registry_token", # Required if using private registry 
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
    ]
  }

  group "inundator-processor" {
    reschedule {
      attempts = 0 # this needs to only be 0 re-attempts or will mess up pipeline job tracking
    }

    # inundate fails for predictible reasons. Restarting takes alot of time. Will rely on manually querying inundate job failures in AWS console to detect if a batch had inundate failures that were novel.
    restart {
      attempts = 0        # Try N times on the same node
      mode     = "fail"   # Fail after attempts exhausted
    }

    task "processor" {
      driver = "docker"

      config {
        # Use local development image - must use specific tag (not 'latest')
        # to prevent Nomad from trying to pull from a registry
        image = "autoeval-jobs:local" 
        force_pull = false
        
        # Mount local test data and output directory
        volumes = [
          "${var.repo_root}/testdata:/testdata:ro",
          "/tmp/autoeval-outputs:/outputs:rw",
          "/tmp:/tmp:rw",
          "${var.repo_root}/local-batches:/local-batches:rw"
        ]
        
        command = "python3"
        args = [
          "/deploy/hand_inundator/inundate.py",
          "--catchment_data_path", "${NOMAD_META_catchment_data_path}",
          "--forecast_path", "${NOMAD_META_forecast_path}",
          "--fim_output_path", "${NOMAD_META_output_path}",
          "--fim_type", "${NOMAD_META_fim_type}",
        ]

      }

      # --- Environment Variables (for AWS SDK inside container) ---
      # Pass AWS creds if provided in meta, otherwise rely on IAM instance profile
      env {
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION = "us-east-1"
        GDAL_CACHEMAX         = "1024"
        
        # GDAL Configuration
        GDAL_NUM_THREADS = "1"
        GDAL_TIFF_DIRECT_IO = "YES"
        GDAL_DISABLE_READDIR_ON_OPEN = "TRUE"
        CPL_LOG_ERRORS = "ON"
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tif,.vrt"
        VSI_CACHE_SIZE = "268435456"
        CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE = "YES"
        
        # Processing Defaults
        LAKE_ID_FILTER_VALUE = "-999"
        
        # Nodata Values
        DEPTH_NODATA_VALUE = "-9999"
        INUNDATION_NODATA_VALUE = "255"
        
        # Output Configuration
        INUNDATION_COMPRESS_TYPE = "lzw"
        INUNDATION_BLOCK_SIZE = "256"
        
        # Logging
        LOG_SUCCESS_LEVEL_NUM = "25"
      }

      resources {
        # set small here for github runner test (8gb total memory)
        memory = 4000
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
    task "persist-logs" {
      lifecycle {
        hook = "poststop"
        sidecar = false
      }
      
      driver = "docker"
      
      config {
        image = "docker.io/library/alpine:3.19.1"
        privileged = true
        command = "/bin/sh"
        args = ["-c", <<-EOF
          set -e
          
          # Extract batch name
          BATCH_NAME=$(echo "${NOMAD_JOB_ID}" | sed -n 's/.*\[batch_name=\([^,]*\),.*/\1/p')
          
          # Create log directory for this job id
          LOG_DIR="/persistent-logs/$BATCH_NAME/${NOMAD_JOB_ID}"
          mkdir -p "$LOG_DIR"
          
          # Debug: Check the mounted nomad-data directory structure
          echo "=== Checking /nomad-data structure ==="
          ls -la /nomad-data/ 2>/dev/null || echo "nomad-data not found"
          
          # Look for our allocation in the mounted Nomad data directory
          ALLOC_LOG_DIR="/nomad-data/alloc/${NOMAD_ALLOC_ID}/alloc/logs"
          
          echo "=== Looking for logs at: $ALLOC_LOG_DIR ==="
          
          if [ -d "$ALLOC_LOG_DIR" ]; then
            echo "Found logs at $ALLOC_LOG_DIR"
            ls -la "$ALLOC_LOG_DIR"
            
            # Copy the logs
            cp -r "$ALLOC_LOG_DIR/"* "$LOG_DIR/" || true
            
            # Create metadata file
            {
              echo "Job ID: ${NOMAD_JOB_ID}"
              echo "BATCH_NAME: $BATCH_NAME"
              echo "Allocation ID: ${NOMAD_ALLOC_ID}"
              echo "Catchment Data Path: ${NOMAD_META_catchment_data_path}"
              echo "Forecast Path: ${NOMAD_META_forecast_path}"
              echo "Output Path: ${NOMAD_META_output_path}"
              echo "FIM Type: ${NOMAD_META_fim_type}"
            } > "$LOG_DIR/run_metadata.txt"
            
            echo "Logs successfully persisted to $LOG_DIR"
          else
            echo "No logs found at $ALLOC_LOG_DIR"
            echo "Checking parent directories..."
            echo "Contents of /nomad-data/alloc/${NOMAD_ALLOC_ID}:"
            ls -la "/nomad-data/alloc/${NOMAD_ALLOC_ID}/" 2>/dev/null || echo "Allocation directory not found"
            
            echo "Contents of /nomad-data/alloc:"
            ls -la "/nomad-data/alloc/" 2>/dev/null | head -10 || echo "No alloc directory"
            
            # Create placeholder
            echo "No logs found at $(date)" > "$LOG_DIR/no_logs_found.txt"
          fi
        EOF
        ]
        
        # Mount the persistent logs directory and Nomad data directory
        volumes = [
          "${var.repo_root}/local-logs:/persistent-logs:rw",
          "${var.repo_root}/.data/nomad/data:/nomad-data:ro"
        ] 
      }
      
      resources {
        memory = 128
        cpu = 100
      }
      
      logs {
        max_files = 5
        max_file_size = 10
      } 
    }
  }
}
