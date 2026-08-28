# Auto-Eval Coordinator: Verification Guide [DRAFT]

> **Disclaimer:** These steps are a draft and have not been run end-to-end against a live deployment — testing and verification of this deployment were descoped and will not be performed by this team. They are based on the PI-7 UAT procedure, the [nomad-runner](https://github.com/NOAA-OWP/nomad-runner) repo as currently understood, and [batch-run-guide-AWS-Test.md](./batch-run-guide-AWS-Test.md). Treat this as a starting point rather than a validated procedure: it is subject to change if `nomad-runner`'s configuration changes, and should be expected to need corrections when whoever deploys next actually works through it.

Run this guide after completing all phases of [Deployment_Runbook.md](./Deployment_Runbook.md). Part 1 is intended to prove the deployment works end-to-end using a fixed, known-good test unit — the same way the system was verified in the previous (NGWPC) environment — but has not itself been executed. Part 2 generalizes that same smoke test into routine batch operations for a real evaluation run.

**Predefined test unit:** STAC item `01080203-shvm3-usgs` from `usgs-fim-collection` (HUC8 01080203, gauge shvm3). Test benchmark assets and HAND index data for this unit are included in the repository under `testdata/`. All smoke test commands below use this unit.

---

# Part 1: Verify the Deployment

## 1. Nomad Cluster Health

Check the Nomad server is reachable:

```bash
nomad status
# Pass: no error; server responds
```

Check at least one client node is registered and ready:

```bash
nomad node status
# Pass: one or more nodes in "ready" status with node.class = linux
```

Check all four parameterized jobs are registered:

```bash
nomad job list
# Pass: pipeline, hand_inundator, fim_mosaicker, agreement_maker all present with type=batch
```

Confirm each job's status:

```bash
nomad job status pipeline
nomad job status hand_inundator
nomad job status fim_mosaicker
nomad job status agreement_maker
# Expect: type = batch, status = running (parameterized jobs remain running between dispatches)
```

If any of the checks above fail (server unreachable, no client nodes, jobs stuck `pending`), see [Nomad client provisioning issues](#nomad-client-provisioning-issues) in Troubleshooting before digging further — several common causes are fixed in the `nomad-runner` repo, not here.

---

## 2. Container Image Availability

Image pull verification from the admin machine is covered in Deployment_Runbook.md Phase 1. That check confirms `auto-eval-coordinator` is pullable (sufficient, since it only ever runs on the admin machine) but does **not** confirm `auto-eval-jobs`/`auto-eval-jobs-gval` are reachable from the Nomad client fleet — those images are pulled by Nomad client nodes on dispatch, over a different network path. The Single Pipeline Smoke Test (§6) below is the real confirmation that Nomad clients can pull them.

- **Pass:** `force_pull = true` confirmed in all four job definitions — images will refresh on each dispatch

**Why `force_pull = true`:** job definitions reference the floating `latest` tag, and Nomad caches images per client by tag, not digest. Without `force_pull`, a client keeps running whatever it cached under `latest`, even after CI pushes a newer image. `force_pull` trades a small per-dispatch pull cost for guaranteeing every dispatch runs the current image.

---

## 3. STAC API & Benchmark Data

Start the local STAC stack if not already running:

```bash
docker compose -f docker-compose-local.yml up -d
```

Load the test benchmark data for `01080203-shvm3-usgs`:

```bash
./testdata/benchmark/load-test-stac-data.sh
# Expect: "Collection loaded successfully" and "Item loaded successfully" (or "already exists" if re-running)
```

Verify the test item is queryable:

```bash
curl -s http://localhost:8082/collections/usgs-fim-collection/items/01080203-shvm3-usgs | python3 -m json.tool | grep '"id"'
# Expect: "id": "01080203-shvm3-usgs"
```

- **Pass:** STAC API root responds: `curl http://localhost:8082/`
- **Pass:** `usgs-fim-collection` appears in `/collections`
- **Pass:** `STAC_API_URL` in `pipeline.nomad` matches the deployed STAC instance

---

## 4. HAND Index Integration

The HAND index for the test unit is included in the repo at `testdata/hand/parquet-index`. Verify the coordinator can query it by entering the coordinator container and running a direct query:

```bash
docker compose -f docker-compose-dev.yml up -d
docker compose -f docker-compose-dev.yml exec autoeval-dev bash
```

Inside the container:

```bash
python -c "
from src.data_service import query_hand_index
results = query_hand_index('/testdata/hand/parquet-index', '/testdata/benchmark/assets/01080203-shvm3-usgs.json')
print(f'Catchments found: {len(results)}')
"
# Expect: one or more catchments returned without error
```

For the remote (AWS) environment, verify the deployed index is reachable:

```bash
aws s3 ls s3://<owp-bucket>/autoeval/hand_output_indices/<index-name>/
# Expect: parquet files listed (Catchments, Hydrotables, HAND_REM_Rasters, Hand_Catchment_Rasters)
```

- **Pass:** HAND index query returns catchments for HUC8 01080203
- **Pass:** Index path in `submit_stac_batch.py` invocation matches the deployed index on S3

---

## 5. AWS Credentials & S3 Access

Confirm credentials are valid and S3 is reachable from the coordinator container:

```bash
# Inside the coordinator container:
aws sts get-caller-identity
# Expect: JSON with Account and UserId — no error

aws s3 ls s3://<output-bucket>/
# Expect: bucket contents listed without error
```

- **Pass:** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` set in `.env` or host shell
- **Pass:** Credentials have read/write access to the eval output bucket and read access to `fimc-data` (for mask dictionary)

---

## 6. Single Pipeline Smoke Test (01080203-shvm3-usgs)

Dispatch a single pipeline against the predefined test unit to confirm end-to-end execution. This is the first real operational test case exercising the whole system — see Part 2 below for how this same mechanism generalizes to routine batch runs.

Create a single-item input file:

```bash
echo "01080203-shvm3-usgs" > inputs/smoke-test-items.txt
```

Submit from inside the coordinator container:

```bash
python tools/submit_stac_batch.py \
  --batch_name smoke-test_$(date +%Y-%m-%d-%H) \
  --output_root s3://<owp-bucket>/autoeval/smoke-test/ \
  --hand_index_path s3://<owp-bucket>/autoeval/hand_output_indices/<index-name>/ \
  --benchmark_sources "usgs-fim-collection" \
  --item_list inputs/smoke-test-items.txt \
  --wait_seconds 10 \
  --stop_threshold 5 \
  --resume_threshold 2
```

Monitor the dispatched pipeline:

```bash
nomad job status pipeline
# Expect: one allocation in "running" or "complete" state
```

Verify child jobs were dispatched and completed:

```bash
nomad job status hand_inundator
nomad job status fim_mosaicker
nomad job status agreement_maker
# Expect: recent allocations in "complete" status with exit code 0
```

Check output was written to S3:

```bash
aws s3 ls s3://<owp-bucket>/autoeval/smoke-test/ --recursive | grep "01080203-shvm3-usgs"
# Expect: agreement.tif, metrics.csv, and logs.txt present for the test item
```

- **Pass:** Pipeline job completes without error
- **Pass:** All three child jobs complete with exit code 0
- **Pass:** Output files present in S3 at the expected path for `01080203-shvm3-usgs`

---

## 7. CloudWatch Logging

Confirm logs are flowing from Nomad client nodes to CloudWatch. Note the AWS account holding the CloudWatch logs may require different credentials than the ones used for S3/batch submission — see the callout in Part 2 §2.7 for the same gotcha when generating batch reports.

```bash
aws logs describe-log-streams \
  --log-group-name /aws/ec2/nomad-client-linux-test \
  --order-by LastEventTime \
  --descending \
  --max-items 5
# Expect: recent log streams named after Nomad job IDs from the smoke test
```

Tail a specific job's logs:

```bash
aws logs get-log-events \
  --log-group-name /aws/ec2/nomad-client-linux-test \
  --log-stream-name <nomad-job-id> \
  --limit 50
# Expect: structured log output with no ERROR lines
```

- **Pass:** Log streams appear within ~60 seconds of job start
- **Pass:** No `awslogs` driver errors in Nomad client agent logs

---

## 8. Batch Report

After the smoke test completes, generate a report to confirm metrics were written correctly. `tools/cloudwatch_reports.py` takes positional arguments: `run_list batch_name output_dir`.

```bash
./tools/cloudwatch_reports.py \
  inputs/smoke-test-items.txt \
  smoke-test_<YYYY-MM-DD-HH> \
  reports/smoke-test
```

```bash
cat reports/smoke-test/unique_fail_aoi_names.txt
# Expect: empty (no failures for the smoke test unit)

ls reports/smoke-test/
# Expect: summary CSV and failure list present
```

- **Pass:** Report generates without error
- **Pass:** Failure list is empty for `01080203-shvm3-usgs`

---

## 9. Component Test Matrix

| Test | Description | Pass Criteria |
|------|-------------|---------------|
| TC1 | **Nomad cluster health** — all four jobs registered, at least one client node ready | `nomad job list` shows all jobs; `nomad node status` shows ready nodes |
| TC2 | **Image pull** — `auto-eval-coordinator` pulls cleanly on the admin machine (Deployment_Runbook.md Phase 1); `auto-eval-jobs`/`auto-eval-jobs-gval` pull cleanly on Nomad clients | No auth errors on admin pull; TC6 (smoke test) confirms client-side pull for the job images |
| TC3 | **STAC API** — test item `01080203-shvm3-usgs` loaded and queryable | Item returns from `/collections/usgs-fim-collection/items/01080203-shvm3-usgs` |
| TC4 | **HAND index query** — coordinator queries parquet index for HUC8 01080203 | One or more catchments returned without error |
| TC5 | **AWS credentials** — S3 read/write confirmed from coordinator container | `aws sts get-caller-identity` and `aws s3 ls` succeed |
| TC6 | **Single pipeline execution** — `01080203-shvm3-usgs` runs end-to-end | All three child jobs complete with exit code 0; output files in S3 |
| TC7 | **CloudWatch logging** — logs stream from Nomad clients | Log streams appear in `/aws/ec2/nomad-client-linux-test` within 60s |
| TC8 | **Batch report** — `cloudwatch_reports.py` generates valid report | Report generated; failure list empty for smoke test unit |

---

# Part 2: Running a Batch

Once Part 1 confirms the deployment works end-to-end, use this procedure for routine batch runs. It's the same mechanism as the smoke test in §6, generalized to arbitrary item lists and batch names.

## 2.1 Scale the Nomad Worker Fleet

Set the ASG desired capacity before submitting. A good rule of thumb: set client count to half the number of concurrent pipelines you intend to run. The AWS Test account's reference sizing used a `c5.9xlarge` Nomad server with 10-40 `r5a.xlarge` clients — beyond ~40 clients the server struggled to communicate with the fleet effectively, so treat that as a practical ceiling unless the server is sized up. See [job-sizing-guide.md](./job-sizing-guide.md) for guidance on sizing individual job memory requirements based on data resolution.

The Nomad client fleet and its autoscaling are provisioned by [nomad-runner](https://github.com/NOAA-OWP/nomad-runner), not this repo — see its README's "Autoscaling Overview" section for full detail. Two separate things need to change, both there:

**1. Disable the Nomad Autoscaler job** (a Nomad job named `autoscaler` that automatically adjusts ASG desired capacity based on cluster utilization — it will fight you if left running while you set capacity manually):

```bash
nomad job inspect autoscaler > autoscaler.hcl
# Edit autoscaler.hcl: set `enabled = false` on the `linux_cluster_scaling` block (and `windows_cluster_scaling` if relevant)
nomad job plan autoscaler.hcl
nomad job run autoscaler.hcl
```

**2. Set the ASG desired capacity.** Get the exact ASG name from the `nomad-runner` Terraform state (it's environment-specific, not a fixed name):

```bash
# From the relevant nomad-runner terraform workspace:
terraform output asg_name

# Then:
aws autoscaling set-desired-capacity --auto-scaling-group-name <asg-name> --desired-capacity <n>
```

Reverse both steps at shutdown (§2.8): set desired capacity back to 1, then set `enabled = true` on the autoscaler policy and re-run the job.

## 2.2 Set Up Environment

```bash
cd auto-eval-coordinator
export NOMAD_ADDR="http://<nomad-server-host>:4646"
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export AWS_SESSION_TOKEN=<your-token>  # if using temporary credentials
```

## 2.3 Start the Coordinator Container

```bash
docker compose -f docker-compose-dev.yml up -d
docker compose -f docker-compose-dev.yml exec autoeval-dev bash
```

**Every command in §2.4–§2.8 below runs from inside this container shell** (working directory `/app`, with `tools/`, `src/`, `inputs/`, and `data/` mounted from the repo). Each command block restates the `docker compose exec` line so you can jump to any step directly — but if you already have a shell open from this step, you don't need to re-run it.

## 2.4 Start the Memory Monitor (Separate Terminal)

Open a second terminal and exec into a new shell in the same container:

```bash
docker compose -f docker-compose-dev.yml exec autoeval-dev bash
# Inside container:
tools/nomad_memory_monitor.sh
```

The memory monitor triggers `nomad system gc` when the server's active allocation exceeds `MEMORY_THRESHOLD_GIB` (set to ~25-30% of server max memory). This prevents the Nomad server from becoming unresponsive during long batch runs. Leave this running for the duration of the batch.

## 2.5 Submit the Batch

Inside the container shell from §2.3 (`docker compose -f docker-compose-dev.yml exec autoeval-dev bash` if you need a new one):

```bash
python tools/submit_stac_batch.py \
  --batch_name <batch_name> \
  --output_root s3://<owp-bucket>/autoeval/batches/<batch_name> \
  --hand_index_path s3://<owp-bucket>/autoeval/hand_output_indices/<index-name>/ \
  --benchmark_sources "usgs-fim-collection" \
  --item_list inputs/<item-list>.txt \
  --wait_seconds 10 \
  --stop_threshold 30 \
  --resume_threshold 15
```

| Argument | Description |
|----------|-------------|
| `--batch_name` | Unique name included in Nomad job IDs and CloudWatch log streams |
| `--output_root` | S3 path for all pipeline outputs |
| `--hand_index_path` | S3 path to the HAND index (see Deployment_Runbook.md Phase 0) |
| `--benchmark_sources` | Comma-separated STAC collections to evaluate against |
| `--item_list` | File with one STAC item ID per line |
| `--wait_seconds` | Delay between job submissions (minimum 10) |
| `--stop_threshold` | Pause submission above this many concurrent pipelines |
| `--resume_threshold` | Resume submission once concurrent count drops below this |

**AWS credentials for dispatched jobs:** the `AWS_*` vars exported in §2.2 populate the *coordinator* container's environment (via `env_file: .env` in `docker-compose-dev.yml`). By default `submit_stac_batch.py` assumes the dispatched `pipeline` job gets its own AWS credentials from an IAM role attached to the Nomad client nodes — the coordinator's env vars are not forwarded to the job. If the OWP Nomad clients do **not** have an IAM instance role for S3 access, add `--use-local-creds` to the command above; this forwards the container's AWS credentials into the dispatched job's metadata instead. Confirm which credential model the OWP cluster uses before running a batch.

## 2.6 Monitor Pipeline Progress

Navigate to the Nomad UI at `http://<nomad-server-host>:4646/ui` to watch job status in real time. Each dispatched pipeline (`pipeline`, `hand_inundator`, `fim_mosaicker`, `agreement_maker`) shows individual allocation status. Successful allocations appear green; failed allocations appear red.

A small number of `hand_inundator` failures are expected when NWM flow data is unavailable for a gauge — these are not pipeline errors.

## 2.7 Generate the Batch Report

Once the batch completes, generate a report to confirm outcomes. `tools/cloudwatch_reports.py` takes positional arguments: `run_list batch_name output_dir`.

**The CloudWatch logs account may require different AWS credentials than the ones used for S3/batch submission in §2.2.** If applicable to your deployment, re-export `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` for the account that holds the CloudWatch logs before running the command below.

Inside the container shell from §2.3:

```bash
./tools/cloudwatch_reports.py \
  inputs/<item-list>.txt \
  <batch_name> \
  local-reports/<batch_name>
```

Review outputs:

```bash
cat local-reports/<batch_name>/unique_success_aoi_names.txt
cat local-reports/<batch_name>/unique_fail_aoi_names.txt
```

S3 outputs for each evaluated AOI are written to `s3://<owp-bucket>/autoeval/batches/<batch_name>/<aoi-id>/`. The `stac_aois/` subfolder contains STAC item representations of the pipeline outputs. Refer to FIM EVALUATION ENHANCEMENTS (Document G6587265) and [interpreting-reports.md](./interpreting-reports.md) for guidance on reading these reports.

Failed AOIs are usually transient (credential rotation, S3 timeouts) and can be resubmitted by re-running §2.5 (`submit_stac_batch.py`) with `--item_list` pointed at `unique_fail_aoi_names.txt`.

## 2.8 Shutdown

Stop the memory monitor (`Ctrl+C` in its terminal from §2.4), then, inside the container shell from §2.3:

```bash
nomad system gc
./tools/purge_dispatch_jobs.py
```

`nomad system gc` clears completed allocations from the server's memory; `purge_dispatch_jobs.py` then removes the dispatch job records for this batch so the next batch's status is easy to distinguish in the Nomad UI.

Finally, reverse the two steps from §2.1: set the ASG desired capacity back to 1, then re-enable the Nomad Autoscaler job (`enabled = true`, `nomad job run autoscaler.hcl`).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `403` on S3 writes | Stale AWS credentials | Re-export `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` |
| Nomad server unresponsive | Memory pressure | Run `nomad system gc`; lower `MEMORY_THRESHOLD_GIB` in the monitor script |
| Pipeline jobs lost after scale event | Autoscaler fired mid-batch | Disable autoscaler before batch; resubmit failed items |

### Nomad client provisioning issues

The symptoms below can surface while running this guide (typically at §1 or during the smoke test in §6), but the root cause and fix live in cluster provisioning — the [nomad-runner](https://github.com/NOAA-OWP/nomad-runner) repo, not this one. If you hit these, go there rather than trying to work around them here.

| Symptom | Likely cause | Where to look |
|---------|-------------|----------------|
| `auto-eval-jobs`/`auto-eval-jobs-gval` fail to pull on dispatch (job stuck in `pending` or fails immediately with an image pull error) | Nomad client EC2s lack outbound network access to GHCR (missing NAT gateway, restrictive security group/NACL) | `nomad-runner` Terraform: client subnet routing and security group egress rules |
| Client node never appears in `nomad node status` | Client agent can't reach the Nomad server (wrong advertise address, security group blocking Nomad's RPC/Serf ports between server and clients) | `nomad-runner` Terraform: client agent config and server/client security group rules |
| Job stuck in `pending` with no matching client | Client not registered with the expected `node.class` | `nomad-runner` Terraform/client agent config: `node.class` setting |
| No log streams in CloudWatch / `awslogs` driver errors (§7) | Nomad client IAM instance role missing `logs:CreateLogStream`/`logs:PutLogEvents` | `nomad-runner` Terraform: IAM role attached to the client ASG |

---

## 10. Production Readiness Sign-Off

This checklist has not been worked through or signed off by this team — testing and verification were descoped. It's left here as the criteria whoever deploys next should confirm before calling the system production-ready.

**Infrastructure**
- **Pass:** Nomad cluster provisioned via `nomad-runner` Terraform
- **Pass:** EC2 client nodes in correct ASG, registered with `node.class = linux`
- **Pass:** IAM role on Nomad clients grants CloudWatch Logs write access
- **Pass:** Nomad server reachable at `NOMAD_ADDR`; `NOMAD_TOKEN` available

**Container Registry**
- **Pass:** All three `ghcr.io/ngwpc/auto-eval-*` packages are public
- **Pass:** CI on `main` completed and pushed `latest` tags

**Job Definitions**
- **Pass:** All four jobs registered in Nomad
- **Pass:** `STAC_API_URL` and `NOMAD_ADDRESS` in `pipeline.nomad` match deployment environment
- **Pass:** `force_pull = true` set in all job definitions

**HAND Index**
- **Pass:** HAND index for target HAND version present on S3
- **Pass:** Index path confirmed reachable from coordinator container
- **Pass:** Index covers HUCs included in the planned batch

**AWS**
- **Pass:** AWS credentials valid and rotated as needed
- **Pass:** S3 output bucket accessible with read/write
- **Pass:** Read access to `fimc-data` bucket (required for mask dictionary in agreement job)
- **Pass:** CloudWatch log group `/aws/ec2/nomad-client-linux-test` exists

**Testing**
- **Pass:** All component tests passed (TC1 – TC8)
- **Pass:** Smoke test pipeline completed end-to-end for `01080203-shvm3-usgs`
- **Pass:** Output files verified in S3
- **Pass:** Batch report generated with no failures
