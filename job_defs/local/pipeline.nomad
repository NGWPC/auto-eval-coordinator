variable "repo_root" {
  description = "Path to the repository root directory"
  type        = string
}

job "pipeline" {
  datacenters = ["dc1"] 
  type        = "batch"

  parameterized {
    meta_required = [
      "aoi",              
      "outputs_path",     
      "hand_index_path",  
    ]
    meta_optional = [
      "benchmark_sources",# Comma-separated list 
      "fim_type",         # extent or depth (default: extent)
      "registry_token",   # Required if using private registry
      "aws_access_key",
      "aws_secret_key", 
      "aws_session_token",
      "stac_datetime_filter", 
      "nomad_token",     # Required for test environment dispatch never used here
      "tags",            # Space-separated list of key=value pairs
    ]
  }

  group "pipeline-coordinator" {
    # don't reschedule or reattempt a failed pipeline. Just want until the next batch run. This saves on compute and makes it easier to scrape logs for failures.
  
    reschedule {
      attempts = 0
    }

    restart {
      attempts = 0        # Try N times on the same node
    }

    task "coordinator" {
      driver = "docker"

      config {
        # Use local development image - must use specific tag (not 'latest')
        # to prevent Nomad from trying to pull from a registry
        image = "autoeval-coordinator:local" 
        force_pull = false
        network_mode = "host"
        
        # Mount local test data and output directory
        volumes = [
          "${var.repo_root}/testdata:/testdata:ro",
          "/tmp/autoeval-outputs:/outputs:rw",
          "/tmp:/tmp:rw",
          "${var.repo_root}/local-batches:/local-batches:rw"
        ]

        args = [
          "--aoi", "${NOMAD_META_aoi}",
          "--outputs_path", "${NOMAD_META_outputs_path}",
          "--hand_index_path", "${NOMAD_META_hand_index_path}",
          "--benchmark_sources", "${NOMAD_META_benchmark_sources}",
          "--tags", "${NOMAD_META_tags}",
          # remove this if test cases don't correspond to unique Benchmark STAC items
          "--aoi_is_item",
          ]
      }

      env {
        # Pipeline ID (using Nomad job ID)
        NOMAD_PIPELINE_JOB_ID = "${NOMAD_JOB_ID}"
        
        # AWS Configuration
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION    = "us-east-1"
        
        # Nomad Configuration
        NOMAD_ADDRESS         = "http://127.0.0.1:4646"
        NOMAD_TOKEN           = "${NOMAD_TOKEN}" # this will be changed to a meta variable when the test version of the job is created
        NOMAD_NAMESPACE       = "default"
        NOMAD_REGISTRY_TOKEN  = "${NOMAD_META_registry_token}"
 
        # Pipeline Configuration
        FIM_TYPE              = "extent"
        HTTP_CONNECTION_LIMIT = "100"
        
        # HAND Index Configuration
        HAND_INDEX_OVERLAP_THRESHOLD_PERCENT = "1.0"
        
        # STAC Configuration
        # STAC_API_URL          = "http://127.0.0.1:8888/" # local test api
        STAC_API_URL            = "http://127.0.0.1:8082/" # local api that can be used to query full benchmark STAC
        # STAC_API_URL            = "http://benchmark-stac.test.nextgenwaterprediction.com:8000/" # STAC api served from AWS test account that can be used to query full benchmark STAC
        STAC_OVERLAP_THRESHOLD_PERCENT = "90.0"
        STAC_DATETIME_FILTER  = "${NOMAD_META_stac_datetime_filter}"
        
        # Job Names for dispatching child jobs
        HAND_INUNDATOR_JOB_NAME = "hand_inundator"
        FIM_MOSAICKER_JOB_NAME  = "fim_mosaicker"
        AGREEMENT_MAKER_JOB_NAME = "agreement_maker"
        
        FLOW_SCENARIOS_OUTPUT_DIR = "combined_flowfiles"
   
        LOG_LEVEL             = "INFO"
        PYTHONUNBUFFERED      = "1"
      }

      resources {
        memory = 3000 
      }

      logs {
        max_files     = 5
        max_file_size = 20 # MB
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
          
          # Look for logs in the mounted Nomad data directory
          ALLOC_LOG_DIR="/nomad-data/alloc/${NOMAD_ALLOC_ID}/alloc/logs"
          
          if [ -d "$ALLOC_LOG_DIR" ]; then
            echo "Copying logs from $ALLOC_LOG_DIR to $LOG_DIR" 
            cp -r "$ALLOC_LOG_DIR/"* "$LOG_DIR/" || true
            
            # Create metadata file with run information
            {
              echo "Job ID: ${NOMAD_JOB_ID}"
              echo "BATCH_NAME: $BATCH_NAME"
              echo "Allocation ID: ${NOMAD_ALLOC_ID}"
              echo "AOI: ${NOMAD_META_aoi}"
              echo "Outputs Path: ${NOMAD_META_outputs_path}"
              echo "HAND Index Path: ${NOMAD_META_hand_index_path}"
              echo "Benchmark Sources: ${NOMAD_META_benchmark_sources}"
              echo "FIM Type: ${NOMAD_META_fim_type}"
              echo "Tags: ${NOMAD_META_tags}"
            } > "$LOG_DIR/run_metadata.txt"
            
            echo "Logs successfully persisted to $LOG_DIR"
          else
            echo "ERROR: Logs directory not found at $ALLOC_LOG_DIR"
            exit 1
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
        max_file_size = 20
      } 
    }
  }
}
