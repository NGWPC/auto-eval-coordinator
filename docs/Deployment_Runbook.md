# Auto-Eval Coordinator: Deployment Guide [DRAFT]

> **Disclaimer:** These steps are a draft and have not been fully run end-to-end against a live deployment. They are based on the PI-7 UAT procedure, the [nomad-runner](https://github.com/NGWPC/nomad-runner) repo as currently understood, and [batch-run-guide-AWS-Test.md](./batch-run-guide-AWS-Test.md). They are subject to change if `nomad-runner`'s configuration changes, and should be expected to need corrections once actually verified in practice.

> **TODO:** `NGWPC/nomad-runner` is currently a private repo — OWP does not yet have access to it, and every link to it in this document is currently unreachable for that audience. Either make it public or create a delivery copy (e.g. `NOAA-OWP/nomad-runner`) before handing this document off. Remove this note once resolved.

## Overview & Architecture

The auto-eval coordinator orchestrates batch FIM evaluation pipelines on a HashiCorp Nomad cluster. It dispatches child jobs (inundation, mosaicking, agreement) to Nomad worker nodes, queries a STAC catalog for benchmark data, and writes evaluation outputs to S3.

Every job is containerized and parameterized — each runs as a standalone Docker image invoked with a fixed set of inputs, with no dependency on Nomad-specific scheduling logic beyond dispatch. This makes the architecture portable to other container-orchestrated or cloud-native job runners (e.g. AWS Batch, Kubernetes Jobs) with minimal rework; see [Future: AWS Batch Migration](#future-aws-batch-migration) below for a concrete migration path.

This runbook covers the OWP deployment procedure. It aligns with **PI-7 UAT Test Procedure A (TP-A), FIMC_EVAL** (Document G6591031), which verifies GVAL enhancements including STAC integration and the auto-eval pipeline supporting HAND evaluations.

| Component | Details |
|-----------|---------|
| Coordinator | `ghcr.io/ngwpc/auto-eval-coordinator:owp-latest` |
| Jobs image | `ghcr.io/ngwpc/auto-eval-jobs:owp-latest` |
| Jobs (GVAL) image | `ghcr.io/ngwpc/auto-eval-jobs-gval:owp-latest` |
| Nomad cluster | Provisioned separately via [nomad-runner](https://github.com/NGWPC/nomad-runner) |
| STAC API | BenchmarkCat STAC (URL from Terraform output); local `stac-fastapi-pgstac` for dev |
| Registry | Public GHCR — no auth required |

> **TODO:** Image tags are currently `owp-latest`, built from the `owp-deployment` branch. Once `owp-deployment` is merged into `main`, update all image references to `latest` (e.g. `ghcr.io/ngwpc/auto-eval-coordinator:latest`) and update CI to push `latest` on merges to `main`.

The Nomad cluster (server + EC2 worker fleet) is **not provisioned by this repo**. Refer to the `nomad-runner` repo for Terraform-based cluster setup before proceeding.

---

## Phase 0: Prerequisites

> **Machine: admin machine**

- Nomad cluster running and reachable (`NOMAD_ADDR` accessible, `NOMAD_TOKEN` in hand)
- AWS credentials with S3 read/write access to the OWP eval output bucket
- Docker and docker-compose installed
- Nomad CLI installed ([install guide](https://developer.hashicorp.com/nomad/tutorials/get-started/gs-install)) — required for `nomad` CLI commands throughout this guide and for `tools/nomad_memory_monitor.sh`
- HAND index available on OWP S3 (see below)

### HAND Index — Migration or Generation

The HAND index is required before any pipeline run. There are two paths:

**Option A: Migrate existing index from NGWPC S3**

The HAND index currently lives at `s3://fimc-data/autoeval/hand_output_indices/` on NGWPC's S3. Coordinate with NGWPC to transfer the relevant index to an OWP-owned bucket — Nomad client nodes will not have cross-account access to `fimc-data`.

```bash
# Run from a machine with read access to fimc-data and write access to the OWP bucket
aws s3 sync s3://fimc-data/autoeval/hand_output_indices/<index-name>/ \
  s3://<owp-bucket>/autoeval/hand_output_indices/<index-name>/
```

**Option B: Generate a new HAND index**

Clone and build the [`hand-index`](https://github.com/NGWPC/hand-index) container:

```bash
git clone https://github.com/NGWPC/hand-index.git
cd hand-index
docker build -t hand-index:latest .
```

Create a `.env` file in the repository root with your AWS credentials (do not quote the values):

```bash
AWS_ACCESS_KEY_ID=<your-access-key-id>
AWS_SECRET_ACCESS_KEY=<your-secret-access-key>
AWS_SESSION_TOKEN=<your-session-token>
AWS_DEFAULT_REGION=us-east-1
```

Then generate the index:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/schema:/schema \
  --env-file .env \
  hand-index:latest python load.py \
    --db-path /data/<unique_name>.ddb \
    --schema-path /schema/hand-index-ver-fim100.sql \
    --hand-dir s3://fimc-data/hand_fim/outputs/<hlp_list>/ \
    --hand-version fim100 \
    --h3-resolution 1 \
    --output-dir s3://<owp-bucket>/autoeval/hand_output_indices/trials/<unique_output_folder> \
    --batch-size 20
```

Provide a unique `--db-path` name and a unique `--output-dir` folder for each index generation run. See the `hand-index` repo's README for details on the index schema and on querying the resulting index.

**Verify the index (both options):**

```bash
aws s3 ls s3://<owp-bucket>/autoeval/hand_output_indices/<index-name>/ | head
# Expected: Catchments, Hydrotables, HAND_REM_Rasters, Hand_Catchment_Rasters parquet files present
```

Record the S3 path — it is passed as `--hand_index_path` when running a batch in [Verification_Guide.md](./Verification_Guide.md).

### Verify Nomad Cluster Connectivity

```bash
export NOMAD_ADDR="http://<nomad-server-host>:4646"  # e.g. http://localhost:4646 for a local/dev cluster
export NOMAD_TOKEN="<your-token>"
nomad status
```

Export both `NOMAD_ADDR` and `NOMAD_TOKEN` as a standard step for any nomad-runner-provisioned cluster (test/prod) — the token is provisioned independently in the [nomad-runner](https://github.com/NGWPC/nomad-runner) repo, not by this repo. A local/dev cluster with ACLs disabled (see `example.env`) tolerates a blank or placeholder `NOMAD_TOKEN`, but default to exporting it.

**Gate:** Do not proceed until `nomad status` returns successfully against your cluster and the HAND index is confirmed on OWP S3.

---

## Phase 1: Pull (or Build) and Verify Container Images

Before registering jobs or running any pipeline commands, pull the images from GHCR and confirm the names match what the Nomad job definitions and `docker run` commands expect.

```bash
docker pull ghcr.io/ngwpc/auto-eval-coordinator:owp-latest
docker pull ghcr.io/ngwpc/auto-eval-jobs:owp-latest
docker pull ghcr.io/ngwpc/auto-eval-jobs-gval:owp-latest
```

Verify the images are present and tagged correctly:

```bash
docker images | grep ngwpc
# Expected output (names must match exactly):
# ghcr.io/ngwpc/auto-eval-coordinator   owp-latest   ...
# ghcr.io/ngwpc/auto-eval-jobs          owp-latest   ...
# ghcr.io/ngwpc/auto-eval-jobs-gval     owp-latest   ...
```

**Gate:** Do not proceed until all three images are present locally, either pulled from GHCR or built via the fallback below.

**Where each image actually needs to pull:** `auto-eval-coordinator` only ever runs on the admin machine (via `docker run`/`docker compose`), so a successful pull here is sufficient for that image. `auto-eval-jobs` and `auto-eval-jobs-gval` are pulled by Nomad *client* nodes when a dispatched job is scheduled — a successful pull from the admin machine does not guarantee those images are reachable from the client fleet (different network path, security groups, etc). The real confirmation that Nomad clients can pull `auto-eval-jobs`/`auto-eval-jobs-gval` is the Single Pipeline Smoke Test in [Verification_Guide.md](./Verification_Guide.md) succeeding.

### Fallback: Build Images Locally

> **TODO:** GHCR publishing for `auto-eval-jobs` and `auto-eval-jobs-gval` is currently blocked on org permissions. Once images are published and `docker pull` above succeeds for all three, remove this fallback section. If GHCR access turns out not to be appropriate for this deployment, keep this section as the primary path instead.

Build the images locally and tag them to match what the Nomad job definitions expect:

```bash
# auto-eval-coordinator (this repo)
git clone https://github.com/NGWPC/auto-eval-coordinator.git -b owp-deployment
docker build -t ghcr.io/ngwpc/auto-eval-coordinator:owp-latest ./auto-eval-coordinator

# auto-eval-jobs and auto-eval-jobs-gval (separate repo, built from two Dockerfiles at its root; Dockerfiles live on main)
git clone https://github.com/NGWPC/auto-eval-jobs.git
docker build -t ghcr.io/ngwpc/auto-eval-jobs:owp-latest -f ./auto-eval-jobs/Dockerfile ./auto-eval-jobs
docker build -t ghcr.io/ngwpc/auto-eval-jobs-gval:owp-latest -f ./auto-eval-jobs/Dockerfile.gval ./auto-eval-jobs
```

Tagging locally-built images with the same `ghcr.io/...` names lets the Nomad job definitions reference them unmodified — `docker pull` is simply skipped for that image. This only works if the Nomad client nodes pull from the **same Docker daemon** the images were built on (e.g. a single-node dev cluster). For a multi-node cluster, push the locally-built images to a registry that all client nodes can reach (an internal registry, or GHCR once access is restored) instead of relying on the local Docker image cache.

---

## Phase 2: Clone & Configure

> **Machine: admin machine**

```bash
git clone https://github.com/NGWPC/auto-eval-coordinator.git -b owp-deployment
cd auto-eval-coordinator
cp example.env .env
```

Edit `.env` and set the following:

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_SESSION_TOKEN` | Session token (if using temporary credentials) |
| `NOMAD_ADDR` | Nomad server URL, e.g. `http://nomad-server-test.test.nextgenwaterprediction.com:4646` |

The remaining defaults in `example.env` are suitable for local development. For the test environment, `NOMAD_ADDR` must point at the remote cluster.

---

## Phase 3: Register Nomad Job Definitions

> **Machine: admin machine**

Job definitions are in `job_defs/test/`. Before registering, update any environment-specific values — at minimum, confirm `STAC_API_URL` and `NOMAD_ADDRESS` in `pipeline.nomad` match your deployment.

Register all four jobs:

```bash
nomad job run job_defs/test/pipeline.nomad
nomad job run job_defs/test/hand_inundator.nomad
nomad job run job_defs/test/fim_mosaicker.nomad
nomad job run job_defs/test/agreement_maker.nomad
```

Verify:

```bash
nomad job status pipeline
nomad job status hand_inundator
nomad job status fim_mosaicker
nomad job status agreement_maker
# Expected: each job shows type = batch, status = running (parameterized jobs stay running)
```

All images pull from public GHCR — no registry token is required.

**Gate:** Do not proceed until all four jobs appear in `nomad job list`.

---

## Phase 4: Configure STAC API

> **Machine: admin machine**

The coordinator queries a STAC API for benchmark data. Two options depending on environment:

### Option A: Use the deployed BenchmarkCat STAC (recommended for OWP test/prod)

The BenchmarkCat STAC API URL is an output of the [BenchmarkCat Terraform deployment](https://github.com/NGWPC/benchmarkcat/blob/owp-deployment/deployment/terraform/TF_README.md). Retrieve it from the BenchmarkCat Terraform state:

```bash
cd <benchmarkcat-terraform-dir>
terraform output
# Note the STAC API URL (e.g. http://<ec2-ip-or-dns>:8000)
```

Confirm it is reachable from the Nomad client nodes:

```bash
export STAC_API_URL="http://<benchmarkcat-url>:8000"
curl $STAC_API_URL/collections | python3 -m json.tool | grep '"id"'
# Expected: benchmark collection IDs (ble-collection, ripple-fim-collection, usgs-fim-collection, etc.)
```

Set `STAC_API_URL` in `job_defs/test/pipeline.nomad` to the value retrieved above before registering jobs in Phase 3.

**Networking requirement:** The Nomad client security group must be able to reach the BenchmarkCat EC2 instance on port `8000` within the shared VPC. Confirm that both are in the same VPC or that the appropriate security group rules are in place.

### Option B: Run a local STAC stack (local development only)

For local development without access to the deployed BenchmarkCat instance, spin up a local stack:

```bash
docker compose -f docker-compose-local.yml up -d
```

Load the test benchmark data:

```bash
./testdata/benchmark/load-test-stac-data.sh
curl "http://localhost:8082/collections" | python3 -m json.tool | grep '"id"'
# Expected: usgs-fim-collection listed
```

Set `STAC_API_URL` in `pipeline.nomad` to `http://localhost:8082/` for local use.

**Gate:** Do not proceed until the `curl .../collections` command above (Option A or B) returns the expected benchmark collection IDs.

---

**Deployment setup is complete.** Before running any operational test case or a real batch, proceed to [Verification_Guide.md](./Verification_Guide.md) to confirm the deployment actually works end-to-end.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Job stuck in `pending` | No client matches node class constraint | Confirm clients are registered with `node.class = linux` via `nomad node status`; if misconfigured, that's set in [nomad-runner](https://github.com/NGWPC/nomad-runner)'s client agent config, not this repo |
| Image pull failures | GHCR package visibility | Confirm `ghcr.io/ngwpc/auto-eval-*` packages are public |

---

## Future: AWS Batch Migration

AWS Batch is a natural long-term successor to the Nomad cluster. The jobs are already containerized and parameterized, so the translation is straightforward. This is not in scope for the current OWP handoff but is worth understanding as a migration path.

**Why AWS Batch**
- Eliminates the Nomad cluster entirely — no server to manage, no ASG desired capacity to set manually, no memory monitor script
- Native AWS service with built-in IAM, CloudWatch, and ECR integration
- Scales to zero between runs — no idle EC2 cost

**What maps directly**

| Nomad concept | AWS Batch equivalent |
|---------------|----------------------|
| Parameterized job | Job definition (container + command) |
| `NOMAD_META_*` dispatch params | Environment variable overrides at submit time |
| `meta_required` / `meta_optional` | Required vs. optional env vars in job submission |
| Nomad job dispatch | `aws batch submit-job` |
| Nomad job status polling | Batch job status polling via Boto3 |
| Nomad server + ASG client fleet | Batch Compute Environment (managed EC2) |
| `awslogs` driver in job definitions | Batch native CloudWatch logging — carries over unchanged |

**What requires rework (~1 month estimate)**
- `src/nomad_job_manager.py` dispatches and polls Nomad jobs via `python-nomad` — needs to be rewritten against the Boto3 Batch client
- Job chaining (pipeline → hand_inundator → fim_mosaicker → agreement_maker) currently relies on the coordinator polling Nomad job status — in Batch this would use job dependencies or Step Functions
- `submit_stac_batch.py` stop/resume throttling logic would be replaced by Batch concurrency limits on the job queue
- `tools/nomad_memory_monitor.sh` and `tools/purge_dispatch_jobs.py` become unnecessary

**Suggested Batch architecture**
- One Compute Environment (managed EC2, instance family matching current r5a.xlarge workers)
- One Job Queue per environment (test / prod)
- Four Job Definitions: `hand_inundator`, `fim_mosaicker`, `agreement_maker`, `depth_evaluator`
- Images pulled from public GHCR or migrated to ECR
- Coordinator updated to submit Batch jobs and poll via Boto3 instead of `python-nomad`

This migration would eliminate the operational complexity of the Nomad cluster while keeping the containerized job architecture intact.
