variable "repo_root" {
  description = "Path to the repository root directory"
  type        = string
}

job "fim_mosaicker" {
  datacenters = ["dc1"] 
  type        = "batch"

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

    restart {
      attempts = 3        # Try N times on the same node
      delay    = "15s"    # Wait between attempts
      mode     = "fail"   # Fail after attempts exhausted
    }

    task "processor" {
      driver = "docker"

      config {
        # Use local development image - must use specific tag (not 'latest')
        # to prevent Nomad from trying to pull from a registry
        image = "autoeval-jobs:local" 
        force_pull = false
        network_mode = "host"
        
        # Mount local test data and output directory
        volumes = [
          "${var.repo_root}/testdata:/testdata:ro",
          "/tmp/autoeval-outputs:/outputs:rw",
          "${var.repo_root}/local-batches:/local-batches:rw"
        ]

        command = "python3"
        args = [
          "/deploy/fim_mosaicker/mosaic.py",
          "--raster_paths", "${NOMAD_META_raster_paths}",
          "--mosaic_output_path", "${NOMAD_META_output_path}",
          "--fim_type", "${NOMAD_META_fim_type}",
          "--clip_geometry_path", "${NOMAD_META_clip_geometry_path}",
        ]

      }

      env {
        AWS_ACCESS_KEY_ID     = "${NOMAD_META_aws_access_key}"
        AWS_SECRET_ACCESS_KEY = "${NOMAD_META_aws_secret_key}"
        AWS_SESSION_TOKEN     = "${NOMAD_META_aws_session_token}"
        AWS_DEFAULT_REGION = "us-east-1"
        
        # GDAL Configuration
        GDAL_NUM_THREADS = "1"
        GDAL_TIFF_DIRECT_IO = "YES"
        GDAL_DISABLE_READDIR_ON_OPEN = "TRUE"
        CPL_LOG_ERRORS = "ON"
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tif,.vrt"
        VSI_CACHE_SIZE = "512000000" # s3 data cache size
        GDAL_SWATH_SIZE = "512000000"  # swath size for translate operations
        GDAL_CACHEMAX = "2048"
        CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE = "YES"
        
        # Output Configuration
        MOSAIC_BLOCK_SIZE = "512"
        MOSAIC_COMPRESS_TYPE = "LZW"
        MOSAIC_PREDICTOR = "2"
        
        # Nodata Values
        EXTENT_NODATA_VALUE = "255"
        DEPTH_NODATA_VALUE = "-9999"
        
        # Logging
        LOG_SUCCESS_LEVEL_NUM = "25"
      }

      resources {
        memory = 4000 
      }

      logs {
        max_files     = 5
        max_file_size = 10 # MB
      }
    }
  }
}
