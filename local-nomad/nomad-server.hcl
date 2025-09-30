datacenter = "dc1"
data_dir = "/nomad/data/"
bind_addr = "0.0.0.0"
server {
  enabled = true
  # bootstrap_expect = 1 means this is a single-node cluster that doesn't need to wait for other servers
  bootstrap_expect = 1

  # Garbage collection settings
  job_gc_interval = "30m"      # Run GC every X interval (also controls eval GC interval)
  job_gc_threshold = "1h"      # Length of time for which evaluations must be terminal
  eval_gc_threshold = "1h"      # Evaluations must be terminal for X amount of time before eligible for GC
  batch_eval_gc_threshold = "1h"  # Batch job evaluations must be terminal for X amount of time before GC
}
client {
  enabled = true
  # cgroup_parent specifies the parent cgroup for all Nomad-managed processes
  # This ensures proper resource isolation and prevents conflicts with system cgroups
  cgroup_parent = "nomad"
  
  # Limit total memory to on local computer
  # memory_total_mb = 14336
}
plugin "docker" {
  config {
    allow_privileged = true
    volumes {
      enabled = true
    }
    extra_labels = ["job_name", "task_group_name", "task_name", "namespace"]
    
    # Disable automatic image cleanup
    gc {
      container = true # if true remove containers after tasks complete. If you don't remove you can see logs in /tmp
      image = false          # Keep docker images cached locally so they don't dissappear. If you update an image you need to remove nomad's data directory to refresh the images on a local nomad cluster.
    }
  }
}
# Advertise tells other Nomad agents how to reach this node
# GetPrivateIP dynamically resolves to the node's private IP address
# This is necessary for proper cluster communication in containerized environments
advertise {
  http = "{{ GetPrivateIP }}:4646"
  rpc = "{{ GetPrivateIP }}:4647"
  serf = "{{ GetPrivateIP }}:4648"
}
ports {
  http = 4646
  rpc = 4647
  serf = 4648
}
ui {
  enabled = true
}
log_level = "INFO"
enable_debug = true
