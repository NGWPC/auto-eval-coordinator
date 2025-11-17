variable "repo_root" {
  description = "Path to the repository root directory"
  type        = string
}

job "pipeline" {
  datacenters = ["dc1"]
  type        = "batch"

  parameterized {
    meta_required = [
      "outputs_path",
      "hand_index_path",
    ]
    meta_optional = [
      "aoi_geom_path",      # GPKG file path (optional - requires either this or aoi_stac_item_id)
      "aoi_stac_item_id",   # STAC item ID for direct querying (optional)
      "benchmark_sources",  # Comma-separated list
      "fim_type",           # extent or depth (default: extent)
      "registry_token",     # Required if using private registry
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
      "stac_datetime_filter",
      "nomad_token",        # Required for test environment dispatch never used here
      "tags",               # Space-separated list of key=value pairs
    ]
  }

  group "pipeline-coordinator" {
    reschedule { attempts = 0 }
    restart { attempts = 0 }

task "coordinator" {
      driver = "docker"

      config {
        image        = "autoeval-coordinator:local"
        force_pull   = false
        network_mode = "host"

        volumes = [
          "${var.repo_root}/testdata:/testdata:ro",
          "/tmp/autoeval-outputs:/outputs:rw",
          "/tmp:/tmp:rw",
          "${var.repo_root}/local-batches:/local-batches:rw"
        ]

        # Explicitly override the Docker image's entrypoint.
        # This forces Nomad to execute your script with /bin/sh
        # instead of passing it as an argument to the image's default command.
        entrypoint = ["/bin/sh", "-c"]

        args = [<<-EOF
          #!/bin/sh
          set -e

          echo "==== LAUNCH SCRIPT IS EXECUTING ===="

          # Build the command with required args.
          # Use $$ to escape the $ for HCL parsing, allowing the shell
          # inside the container to expand the variables at runtime.
          CMD="python /app/src/main.py \
            --outputs_path '$${NOMAD_META_outputs_path}' \
            --hand_index_path '$${NOMAD_META_hand_index_path}'"

          # Conditionally append optional arguments if their meta variables are set.
          if [ -n "$${NOMAD_META_aoi_stac_item_id}" ]; then
            CMD="$CMD --aoi_stac_item_id '$${NOMAD_META_aoi_stac_item_id}'"
          fi

          if [ -n "$${NOMAD_META_aoi_geom_path}" ]; then
            CMD="$CMD --aoi_geom_path '$${NOMAD_META_aoi_geom_path}'"
          fi

          if [ -n "$${NOMAD_META_benchmark_sources}" ]; then
            CMD="$CMD --benchmark_sources '$${NOMAD_META_benchmark_sources}'"
          fi

          if [ -n "$${NOMAD_META_tags}" ]; then
            CMD="$CMD --tags $${NOMAD_META_tags}"
          fi

          echo "Executing Command: $CMD"

          # Quote the variable in eval to handle spaces safely.
          eval "$CMD"
        EOF
        ]
      }

      env {
        # These variables are interpolated by Nomad at submission time, so they use a single $.
        NOMAD_PIPELINE_JOB_ID = "${NOMAD_JOB_ID}"
        NOMAD_TOKEN           = "${NOMAD_TOKEN}"

        # These variables pull from the job's meta parameters.
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        NOMAD_REGISTRY_TOKEN  = "${NOMAD_META_registry_token}"
        STAC_DATETIME_FILTER  = "${NOMAD_META_stac_datetime_filter}"
        
        # Static environment variables
        AWS_DEFAULT_REGION    = "us-east-1"
        NOMAD_ADDRESS         = "http://127.0.0.1:4646"
        NOMAD_NAMESPACE       = "default"
        FIM_TYPE              = "extent"
        HTTP_CONNECTION_LIMIT = "100"
        HAND_INDEX_OVERLAP_THRESHOLD_PERCENT = "1.0"
        STAC_API_URL            = "http://127.0.0.1:8082/"
        STAC_OVERLAP_THRESHOLD_PERCENT = "90.0"
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
        max_file_size = 20
      }
    }
    
    task "persist-logs" {
      lifecycle {
        hook    = "poststop"
        sidecar = false
      }
      driver = "docker"
      config {
        image   = "docker.io/library/alpine:3.19.1"
        privileged = true
        command = "/bin/sh"
        args = ["-c", <<-EOF
          set -e
          # Variables like NOMAD_JOB_ID are available at submission time, so $ is fine.
          BATCH_NAME=$(echo "${NOMAD_JOB_ID}" | sed -n 's/.*\[batch_name=\([^,]*\),.*/\1/p')
          LOG_DIR="/persistent-logs/$BATCH_NAME/${NOMAD_JOB_ID}"
          mkdir -p "$LOG_DIR"
          ALLOC_LOG_DIR="/nomad-data/alloc/${NOMAD_ALLOC_ID}/alloc/logs"
          if [ -d "$ALLOC_LOG_DIR" ]; then
            echo "Copying logs from $ALLOC_LOG_DIR to $LOG_DIR"
            cp -r "$ALLOC_LOG_DIR/"* "$LOG_DIR/" || true
            # NOMAD_META_* variables are runtime only, so they must be escaped with $$.
            {
              echo "Job ID: ${NOMAD_JOB_ID}"
              echo "BATCH_NAME: $BATCH_NAME"
              echo "Allocation ID: ${NOMAD_ALLOC_ID}"
              echo "AOI Geom Path: $${NOMAD_META_aoi_geom_path}"
              echo "AOI STAC Item ID: $${NOMAD_META_aoi_stac_item_id}"
              echo "Outputs Path: $${NOMAD_META_outputs_path}"
              echo "HAND Index Path: $${NOMAD_META_hand_index_path}"
              echo "Benchmark Sources: $${NOMAD_META_benchmark_sources}"
              echo "FIM Type: $${NOMAD_META_fim_type}"
              echo "Tags: $${NOMAD_META_tags}"
            } > "$LOG_DIR/run_metadata.txt"
            echo "Logs successfully persisted to $LOG_DIR"
          else
            echo "ERROR: Logs directory not found at $ALLOC_LOG_DIR"
            exit 1
          fi
        EOF
        ]
        volumes = [
          "${var.repo_root}/local-logs:/persistent-logs:rw",
          "${var.repo_root}/.data/nomad/data:/nomad-data:ro"
        ]
      }
      resources {
        memory = 128
        cpu    = 100
      }
      logs {
        max_files     = 5
        max_file_size = 20
      }
    }
  }
}
