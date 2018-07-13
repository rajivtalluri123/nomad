# Increase log verbosity
log_level = "DEBUG"
datacenter = "dc2"
# Setup data dir
data_dir = "/tmp/client2"

# Give the agent a unique name. Defaults to hostname
name = "client2"

# Enable the client
client {
  enabled = true

  node_class = "m1"
  meta {
   "team" = "mobile"
  }
 
  server_join {
    retry_join = ["127.0.0.1:4647", "127.0.0.1:5647", "127.0.0.1:6647"]
  }
}

ports {
  http = 8646
}
