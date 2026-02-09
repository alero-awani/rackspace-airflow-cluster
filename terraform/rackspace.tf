
resource "spot_cloudspace" "airflow-cluster-multiregion" {
  cloudspace_name    = "airflow-cluster-multiregion"
  region             = "us-central-dfw-1"  # Primary region
  hacontrol_plane    = false
  wait_until_ready   = true
  kubernetes_version = "1.31.1"
  cni                = "calico"
}
###############################################################################
# NOTE: Scheduling strategy for Airflow components
#
# Terraform only provisions node pools and applies labels/taints.
# It does NOT enforce placement or multi-region spreading by itself.
#
# The Kubernetes manifests / Helm chart for Airflow must include:
#
# 1. nodeAffinity / nodeSelector:
#    - Control components (scheduler, webserver, triggerer)
#      target nodes with:
#         workload=airflow-control-plane
#
# 2. tolerations:
#    - Control components tolerate the taint:
#         key=workload
#         value=airflow-control-plane
#
# 3. topologySpreadConstraints:
#    - Control components are spread across regions using:
#         topologyKey: site
#    - This ensures replicas land across dfw + ord control nodes
#
# Without these Kubernetes-side settings, the multi-region redundancy
# intended by this Terraform configuration will NOT be enforced.
###############################################################################


# --- Region 1: Dallas (Primary Control + Workers) ---

resource "spot_spotnodepool" "control_plane_dfw" {
  cloudspace_name = spot_cloudspace.airflow-cluster-multiregion.cloudspace_name
  server_class          = "gp.vs1.large-dfw" # General purpose Large: 4 vCPU, 15 GiB
  bid_price       = 0.15
  desired_server_count = 1 

  labels = {
    "workload" = "airflow-control-plane"
    "site"     = "dfw"

  }

  # This taint "repels" any pod that doesn't explicitly tolerate being a control-plane component
  taints = [{
    key    = "workload"
    value  = "airflow-control-plane"
    effect = "NoSchedule"
  }]
}

resource "spot_spotnodepool" "workers_dfw" {
  cloudspace_name = spot_cloudspace.airflow-cluster-multiregion.cloudspace_name
  server_class    = "ch.vs1.medium-dfw" # Compute Heavy Medium: 2 vCPU, 3.75 GiB
  bid_price       =  0.05
  autoscaling = { min_nodes = 1, max_nodes = 3 }

  labels = { 
    "workload" = "airflow-workers" 
    "site"     = "dfw"}
}

# --- Region 2: Chicago (Replica Control + Workers) ---

resource "spot_spotnodepool" "control_plane_ord" {
  cloudspace_name = spot_cloudspace.airflow-cluster-multiregion.cloudspace_name
  server_class         = "gp.vs1.large-ord" # General purpose Large: 4 vCPU, 15 GiB
  bid_price       = 0.15
  desired_server_count = 1

  labels = { 
    "workload" = "airflow-control-plane" 
    "site"     = "ord"}

  taints = [{
    key    = "workload"
    value  = "airflow-control-plane"
    effect = "NoSchedule"
  }]
}

resource "spot_spotnodepool" "workers_ord" {
  cloudspace_name = spot_cloudspace.airflow-cluster-multiregion.cloudspace_name
  server_class         = "ch.vs1.medium-ord" # Compute Heavy Medium: 2 vCPU, 3.75 GiB
  bid_price       = 0.05
  autoscaling = { min_nodes = 1, max_nodes = 3 }

  labels = { 
    "workload" = "airflow-workers"
    "site"     = "ord"
    }
}