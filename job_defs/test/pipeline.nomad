job "pipeline" {
  datacenters = ["dc1"] 
  type        = "batch"

  constraint {
    attribute = "${node.class}"
    value     = "linux"
  }

  parameterized {
    meta_required = [
      "outputs_path",
      "hand_index_path",
      "nomad_token",     # Required for test environment
    ]
    meta_optional = [
      "aoi_geom_path",    # GPKG file path (optional - requires either this or aoi_stac_item_id)
      "aoi_stac_item_id", # STAC item ID for direct querying (optional)
      "benchmark_sources",# Comma-separated list
      "fim_type",         # extent or depth (default: extent)
      "registry_token",   # Required if using private registry
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
      "stac_datetime_filter",
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

      # Template for conditional AOI argument handling
      template {
        data = <<EOF
#!/bin/bash
set -e

# Conditional AOI argument handling
{{ if env "NOMAD_META_aoi_stac_item_id" }}
AOI_ARG="--aoi_stac_item_id {{ env "NOMAD_META_aoi_stac_item_id" }}"
{{ else if env "NOMAD_META_aoi_geom_path" }}
AOI_ARG="--aoi_geom_path {{ env "NOMAD_META_aoi_geom_path" }}"
{{ else }}
echo "ERROR: Must provide either aoi_stac_item_id or aoi_geom_path in job meta"
exit 1
{{ end }}

# Build command with conditional AOI argument
python /app/src/main.py $AOI_ARG \
  --outputs_path {{ env "NOMAD_META_outputs_path" }} \
  --hand_index_path {{ env "NOMAD_META_hand_index_path" }} \
{{ if env "NOMAD_META_benchmark_sources" }}  --benchmark_sources {{ env "NOMAD_META_benchmark_sources" }} \{{ end }}
{{ if env "NOMAD_META_tags" }}  --tags {{ env "NOMAD_META_tags" }}{{ end }}
EOF

        destination = "local/run_pipeline.sh"
        perms = "755"
      }

      config {
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-coordinator-v0.1"
        force_pull = false
        # force_pull = true # use a cached image on client if available. To force a pull need to change back to force_pull = true
        network_mode = "host"

        # Docker registry authentication
        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token"
          password = "${NOMAD_META_registry_token}"
        }

        command = "/bin/bash"
        args = ["${NOMAD_TASK_DIR}/run_pipeline.sh"]

        logging {
          type = "awslogs"
          config {
            awslogs-group        = "/aws/ec2/nomad-client-linux-test"
            awslogs-region       = "us-east-1"
            awslogs-stream       = "${NOMAD_JOB_ID}"
            awslogs-create-group = "true"
          }
        }
      }

      env {
        # Pipeline ID (using Nomad job ID)
        NOMAD_PIPELINE_JOB_ID = "${NOMAD_JOB_ID}"
        
        # AWS Configuration
        # Test nomad clients can use IAM
        AWS_DEFAULT_REGION    = "us-east-1"      
        # AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        # AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        # AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"

        # Nomad Configuration
        NOMAD_ADDRESS         = "http://nomad-server-test.test.nextgenwaterprediction.com:4646/"
        NOMAD_TOKEN           = "${NOMAD_META_nomad_token}" # Changed to use meta parameter for test
        NOMAD_NAMESPACE       = "default"
        NOMAD_REGISTRY_TOKEN        = "${NOMAD_META_registry_token}"
 
        # Pipeline Configuration
        FIM_TYPE              = "extent"
        HTTP_CONNECTION_LIMIT = "100"
        
        # HAND Index Configuration
        HAND_INDEX_OVERLAP_THRESHOLD_PERCENT = "1.0" # Be generous in what gets included here
        
        # STAC Configuration
        STAC_API_URL            = "http://benchmark-stac.test.nextgenwaterprediction.com:8000/" # Using production STAC API for test
        STAC_OVERLAP_THRESHOLD_PERCENT = "90.0" # set high when doing evals per STAC item. Only want one STAC item per eval
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
  }
}
