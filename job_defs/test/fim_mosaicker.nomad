job "fim_mosaicker" {
  datacenters = ["dc1"] 
  type        = "batch"

  constraint {
    attribute = "${node.class}"
    value     = "linux"
  }

  parameterized {
    meta_required = [
      "raster_paths", 
      "output_path",
      "fim_type",
    ]
    meta_optional = [
      "registry_token", # Required if using private registry auth below
      "aws_access_key",
      "aws_secret_key",
      "aws_session_token",
      "clip_geometry_path",
    ]
  }

  group "mosaicker-processor" {
    reschedule {
      attempts = 0 # this needs to only be 0 re-attempts or will mess up pipeline job tracking
    }

    # This restart block is configured like this to spread out the load on cloudwatch when it is under stress
    restart {
      attempts = 5        # Try N times on the same node
      interval = "20m"    # Within an N minute window. Needs to be slightly longer than attempts * delay
      delay    = "2m"    # Wait 2m between attempts
      mode     = "fail"   # Fail after attempts exhausted
    }

    task "processor" {
      driver = "docker"

      config {
        # use last known stable version in test
        image = "registry.sh.nextgenwaterprediction.com/ngwpc/fim-c/flows2fim_extents:autoeval-jobs-v0.2" 
        force_pull = false
        # force_pull = true # use a cached image on client if available. To force a pull need to change back to force_pull = true

        auth {
          username = "ReadOnly_NGWPC_Group_Deploy_Token"
          password = "${NOMAD_META_registry_token}"
        }

        command = "python3"
        args = [
          "/deploy/fim_mosaicker/mosaic.py",
          "--raster_paths", "${NOMAD_META_raster_paths}",
          "--mosaic_output_path", "${NOMAD_META_output_path}",
          "--fim_type", "${NOMAD_META_fim_type}",
          "--clip_geometry_path", "${NOMAD_META_clip_geometry_path}",
        ]

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
        AWS_DEFAULT_REGION = "us-east-1"
        # AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        # AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        # AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"

        GDAL_CACHEMAX         = "1024"
        
        # GDAL Configuration
        GDAL_NUM_THREADS = "1"
        GDAL_TIFF_DIRECT_IO = "YES"
        GDAL_DISABLE_READDIR_ON_OPEN = "TRUE"
        CPL_LOG_ERRORS = "ON"
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tif,.vrt"
        VSI_CACHE_SIZE = "268435456"
        CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE = "YES"
        
        # Output Configuration
        MOSAIC_BLOCK_SIZE = "1024" # use bigger blocks to boost the  effect of using multiple threads
        MOSAIC_COMPRESS_TYPE = "LZW"
        MOSAIC_PREDICTOR = "2"
        
        EXTENT_NODATA_VALUE = "255"
        DEPTH_NODATA_VALUE = "-9999"
        
        LOG_SUCCESS_LEVEL_NUM = "25"
      }

      resources {
        memory = 5000
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
