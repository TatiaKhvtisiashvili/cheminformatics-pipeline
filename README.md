# Cheminformatics Pipeline

An Airflow DAG that processes chemical scaffold and R-group data from S3, generating
molecules, calculating molecular properties, clustering them by structural similarity,
running data quality checks, notifying scientists via MS Teams, and (optionally)
predicting properties with ChemProp and building an interactive Faerun visualization.

## Overview

Scientists upload two CSV files per dataset to S3:
- `<id>_scaffolds.csv` — scaffold SMILES strings
- `<id>_r_groups.csv` — R-group SMILES strings

Both files share the same format:
```csv
smiles
CCC*
...
```

The pipeline combines scaffolds with R-groups to generate full molecules, calculates
molecular properties, checks data quality, clusters the results, notifies scientists
of the outcome, and (optionally) runs a ChemProp model and builds a Faerun graph.

## Pipeline steps

1. **Molecule generation** — combine scaffold + R-group SMILES (same `id`) into full
   molecules using RDKit
2. **Property calculation** — molecular weight, logP, HBA, HBD, TPSA
3. **Data quality checks** — validates the generated properties (non-empty dataset,
   no null/non-positive molecular weight, no null logP, no blank SMILES)
4. **Clustering** — group molecules by structural similarity (KMeans over Morgan
   fingerprints); only runs if quality checks pass
5. **Notification** — sends an MS Teams message with the outcome (success or failure,
   with details)
6. *(Optional)* **ChemProp prediction** — see caveat below
7. *(Optional)* **Faerun visualization** — interactive 2D cluster map

## Current status: complete (including optional items)

The DAG runs on a **weekly schedule**, automatically discovers new datasets in S3
since its last successful run, and for each one:
- Generates molecules and calculates properties
- Runs data quality checks — a dataset that fails is **halted before clustering**
  and reported to Teams with the specific issues found
- On success, clusters the molecules, runs the optional ChemProp/Faerun steps
  (best-effort, never block the required pipeline), and sends a Teams success
  notification
- Any task-level failure (not just quality-check failures) triggers a Teams alert
  via `on_failure_callback`

An `overwrite` parameter (default `False`) can be set to `True` to force
reprocessing of all existing files, bypassing the "already processed" filter.

### Note on S3

This project uses **LocalStack** (an S3-API-compatible emulator) for local
development and testing, instead of requiring a real AWS account. All S3-facing
functions talk to S3 through a standard `boto3` client — swapping `AWS_ENDPOINT_URL`
for a real AWS endpoint (or removing it) is all that's needed to point this at
production S3.

### Note on MS Teams notifications

`notify_teams()` reads `MS_TEAMS_WEBHOOK_URL` from the environment. If it's unset,
the function logs a warning and returns without error — this keeps local dev, CI,
and testing working without requiring real Teams access. When a real webhook URL is
configured, messages are sent as Connector Cards with a title
(`Cheminformatics Pipeline — Success` / `— FAILED`) and a text body describing the
outcome.

## Project structure

```
cheminformatics-pipeline/
├── .env.example              # template for local environment variables
├── Dockerfile                 # custom Airflow image with chem dependencies
├── docker-compose.yaml        # Airflow + Postgres + Redis + LocalStack (S3 emulator)
├── pytest.ini
├── requirements.txt
├── dags/
│   └── scaffold_pipeline_dag.py
├── plugins/
│   └── chem_utils/
│       ├── __init__.py
│       ├── molecule_gen.py       # combines scaffolds + r-groups into molecules
│       ├── properties.py         # calculates molecular properties
│       ├── clustering.py         # KMeans clustering on fingerprints
│       ├── discovery.py          # S3 dataset discovery logic (new-files + overwrite)
│       ├── quality_checks.py     # data quality validation on generated properties
│       ├── notifications.py      # MS Teams notification helper
│       ├── chemprop_predict.py   # optional: ChemProp regression (see caveat)
│       └── faerun_build.py       # optional: Faerun 2D cluster visualization
└── tests/
    ├── test_chem_utils.py
    ├── test_dag_discovery.py
    ├── test_quality_checks.py
    ├── test_notifications.py
    ├── test_optional_features.py
    ├── batch001_scaffolds.csv   # valid dataset
    ├── batch001_r_groups.csv
    ├── batch002_scaffolds.csv   # valid dataset
    ├── batch002_r_groups.csv
    ├── batch003_scaffolds.csv   # deliberately invalid — exercises the quality-check failure path
    └── batch003_r_groups.csv
```

## Setup

### Prerequisites
- Docker Desktop (with at least 4GB RAM allocated — ChemProp's PyTorch training
  is resource-intensive; see caveats below)
- AWS CLI (used to interact with the local S3 emulator)
- An MS Teams incoming webhook URL (optional — see note above)

### 1. Configure environment

```powershell
copy .env.example .env
```

Edit `.env` and set `MS_TEAMS_WEBHOOK_URL` if you have a real Teams webhook.
Everything else works with the defaults.

### 2. Start the stack

```powershell
docker compose up -d --build
```

This starts Postgres, Redis, LocalStack (S3 emulator), and all Airflow services
(webserver, scheduler, worker, triggerer). The first build is slow — RDKit,
scikit-learn, ChemProp, and PyTorch are all installed into the image.

Wait for all services to report `healthy`:
```powershell
docker compose ps
```

### 3. Create the local S3 bucket and load sample data

Set dummy credentials for the AWS CLI to talk to LocalStack. LocalStack accepts any
non-empty values — add these to your PowerShell `$PROFILE` so they persist across
terminal sessions instead of re-setting them every time:
```powershell
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_DEFAULT_REGION = "eu-central-1"
$env:AWS_REQUEST_CHECKSUM_CALCULATION = "when_required"
```

Create the bucket:
```powershell
aws --endpoint-url=http://localhost:4566 s3 mb s3://cheminformatics-local --region eu-central-1
```

Upload the sample fixture files — two valid datasets and one deliberately invalid
dataset, to exercise both the success and quality-check-failure paths:
```powershell
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch001_scaffolds.csv s3://cheminformatics-local/batch001_scaffolds.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch001_r_groups.csv s3://cheminformatics-local/batch001_r_groups.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch002_scaffolds.csv s3://cheminformatics-local/batch002_scaffolds.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch002_r_groups.csv s3://cheminformatics-local/batch002_r_groups.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch003_scaffolds.csv s3://cheminformatics-local/batch003_scaffolds.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch003_r_groups.csv s3://cheminformatics-local/batch003_r_groups.csv
```

Verify:
```powershell
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/
```

### 4. Run the DAG

Open the Airflow UI at [http://localhost:8080](http://localhost:8080) (login:
`airflow` / `airflow`).

Unpause the DAG (DAGs are paused by default on creation):
```powershell
docker compose run --rm airflow-cli airflow dags unpause scaffold_pipeline
```

The DAG runs automatically on a weekly schedule. To test without waiting a week,
trigger it manually:
```powershell
docker compose run --rm airflow-cli airflow dags trigger scaffold_pipeline --conf '{\"overwrite\": true}'
```

- `overwrite: true` reprocesses every dataset found in S3, regardless of prior runs
- `overwrite: false` (default) only processes datasets whose scaffold file was
  modified after the DAG's last successful run

Or trigger from the UI: **DAGs → scaffold_pipeline → ▶ Trigger DAG w/ config** →
paste `{"overwrite": true}`.

**With ChemProp/Faerun enabled, each dataset takes noticeably longer to process**
(ChemProp training alone can take a few minutes per dataset even at 3 epochs on
CPU). Be patient and check task state rather than repeatedly re-triggering:
```powershell
docker compose run --rm airflow-cli airflow tasks states-for-dag-run scaffold_pipeline <run_id>
```

### 5. Verify output and quality-check behavior

In the Airflow UI **Graph** view, you should see `discover_new_datasets` followed by
a **mapped** `process_dataset` task group — one instance per discovered dataset.
`batch001` and `batch002` should succeed; `batch003` should **fail** (its scaffold
file has no valid SMILES, so no molecules are generated and the quality check flags
`"No molecules generated for dataset"`).

Confirm output landed in LocalStack:
```powershell
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/generated/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/properties/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/clustered/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/predictions/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/faerun/
```

`clustered/`, `predictions/`, and `faerun/` should contain entries for `batch001`
and `batch002`, but **not** `batch003` — confirming the quality gate correctly
blocked bad data from reaching clustering or the optional steps.

### 6. Verify MS Teams notifications actually fired

`print()` output is not reliably captured in Airflow task logs — `notify_teams()`
uses Python's `logging` module instead, so its outcome is always visible in the
task log regardless.

Find the DAG run's task logs directly (bypasses the Airflow UI, which can be
unreliable at showing full log output):
```powershell
docker compose exec airflow-scheduler find /opt/airflow/logs -path "*process_dataset*" -name "*.log"
```

Cat the most recent `process_dataset` log for each map index:
```powershell
docker compose exec airflow-scheduler cat "<path-from-above>"
```

Look for one of these lines, confirming the notification code path executed:
```
INFO - [notify_teams] Sent Teams notification: Dataset `batch001` processed successfully.
```
or, if `MS_TEAMS_WEBHOOK_URL` isn't configured:
```
WARNING - [notify_teams] No MS_TEAMS_WEBHOOK_URL configured, skipping. Message: ...
```

A `[notify_teams] Sent ...` line with no exception means `pymsteams` completed the
HTTP POST to the webhook without error — this is confirmation from the pipeline's
side that the notification was dispatched. Whether you personally see the message
appear in a Teams channel depends on which channel the webhook is connected to;
confirm with whoever owns the webhook if you can't see it yourself.

### 7. View the Faerun graph

```powershell
aws --endpoint-url=http://localhost:4566 s3 cp s3://cheminformatics-local/faerun/batch001_faerun.zip .
Expand-Archive batch001_faerun.zip -DestinationPath batch001_faerun
cd batch001_faerun
python -m http.server 8000
```
Open `http://localhost:8000` in a browser. **Do not** open the file directly via
`file://` — browser security blocks the JS module loading Faerun needs.

## Optional features

### ChemProp property prediction
Trains a small regression model per dataset using our own RDKit-computed `logp`
as a pseudo-label, purely to demonstrate the ChemProp integration.

**Important caveat**: this is not a genuine predictive use case. ChemProp is
designed to predict properties that *can't* be computed exactly — experimentally
measured activity, solubility, toxicity, etc. Our dataset only has `smiles`, and
every property we report (including `logp`) is computed exactly by RDKit, not
measured. Using `logp` as a training target here is circular — it demonstrates
that the ChemProp training/prediction pipeline is correctly wired end-to-end, but
it is not evidence of a real predictive capability. A production version of this
step would replace the `logp` pseudo-label with a genuine experimental target
column supplied by the scientists.

Training and prediction subprocess calls have explicit timeouts (120s / 60s
respectively) and never raise — a hang, timeout, or failure here is logged and
skipped, and never blocks the required pipeline. This was added after observing
that CPU-only ChemProp/PyTorch training is resource-intensive enough to
occasionally stall in a local Docker Desktop environment. Output (when
successful): `predictions/<id>_predictions.csv`.

### Faerun graph
Builds an interactive 2D scatter plot of molecules, colored by cluster, using
PCA over Morgan fingerprints. This intentionally does not use `tmap` (the layout
engine Faerun is often paired with) — `tmap` is a fragile native C++ dependency
with a history of platform-specific install failures, which wasn't worth the risk
for an optional feature. PCA is a reasonable substitute for a 2D projection here.
Output: `faerun/<id>_faerun.zip` — download, unzip, and serve with
`python -m http.server` (see step 7 above).

Both optional features are best-effort: a failure, timeout, or exception in
either is caught, logged, and skipped — it never fails the required pipeline
(generate → properties → quality check → cluster → notify).

## Running tests

```powershell
pip install -r requirements.txt
pytest tests/ -v
```

Tests mock the S3 client (`_get_s3_client`), the `pymsteams` connector card, and
subprocess calls to ChemProp, so no running LocalStack instance, real Teams
webhook, or actual ChemProp training is required to run the test suite. Discovery,
quality-check, and optional-feature logic all live outside the DAG file
specifically so they can be tested without importing Airflow (which is not
reliably installable in a native Windows Python environment — see Troubleshooting).

## Troubleshooting

- **`docker compose run` leaves orphan containers behind** — always use
  `docker compose run --rm ...` so one-off CLI containers are removed automatically.
- **`InvalidRequest: x-amz-trailer header is not supported`** on `aws s3 cp` — set
  `$env:AWS_REQUEST_CHECKSUM_CALCULATION = "when_required"` before retrying; this is
  a compatibility gap between newer AWS CLI versions and older LocalStack S3 support.
- **DAG runs stay `queued` forever** — the DAG is paused by default
  (`AIRFLOW_CORE_DAGS_ARE_PAUSED_AT_CREATION: 'true'`). Unpause it before triggering.
- **`NoSuchBucket` errors after any Docker restart** — LocalStack's state does not
  reliably persist across restarts even with a mounted volume unless `PERSISTENCE=1`
  is set (already configured in `docker-compose.yaml`). If you still hit this, the
  bucket and sample files need to be recreated (see Setup step 3). This happens
  frequently in local dev — treat it as a normal, expected step, not an error.
- **Airflow does not run natively on Windows** — `airflow db init` and any command
  that imports `airflow` directly will fail in a local Windows Python venv. Use
  Docker for anything Airflow-related; local `pytest` only works for modules that
  don't import `airflow` (which is why business logic is isolated in `chem_utils/`,
  outside the DAG file).
- **A mapped `process_dataset` task fails with**
  `"the task instance's state attribute is queued"` — a known Airflow/Celery
  executor state-sync quirk under local resource contention, not a code bug. The
  configured `retries: 1` handles it; if it persists, check
  `docker compose logs airflow-scheduler`.
- **Docker Desktop crashes with a `500 Internal Server Error` on the Docker API,
  or `docker compose ps` returns an empty table** — this was observed repeatedly
  during development, correlated with heavy CPU/memory load from ChemProp/PyTorch
  training. Recovery: `wsl --shutdown`, wait ~15s, reopen Docker Desktop, then
  `docker compose up -d`. If this recurs often, increase Docker Desktop's CPU/
  memory limits (Settings → Resources), or disable the ChemProp step.
- **A `process_dataset` task runs for a very long time with no progress** —
  ChemProp training has no reliable upper bound on local CPU-only hardware.
  `execution_timeout` is set on the `process_dataset` operator, and the ChemProp
  subprocess calls have their own internal timeouts, as defense in depth. If a
  task still appears stuck well past these limits, check
  `docker stats --no-stream` for resource contention and consider restarting
  Docker Desktop.
- **No Teams message visible after a run** — check the task log for
  `[notify_teams] Sent ...` vs `[notify_teams] No MS_TEAMS_WEBHOOK_URL configured...`.
  If "Sent" appears with no exception, the webhook POST succeeded from the
  pipeline's side; visibility then depends on which Teams channel the webhook is
  connected to.

## Branching strategy

This repo follows a `feature → dev → prod` branch policy:
- Work happens on `feature/*` branches
- Features are merged into `dev` via PR
- `dev` is merged into `prod` once a milestone (step) is complete and verified

## Roadmap

- [x] **Step 1** — Manual trigger with `dataset_id` parameter
- [x] **Step 2** — Weekly schedule, process new files since last run, `overwrite` param
- [x] **Step 3** — Data quality checks + MS Teams notifications
- [x] *(Optional)* ChemProp property prediction — demonstration only, see caveat above
- [x] *(Optional)* Faerun graph visualization
