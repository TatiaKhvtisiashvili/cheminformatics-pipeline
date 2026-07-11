# Cheminformatics Pipeline

An Airflow DAG that processes chemical scaffold and R-group data from S3, generating
molecules, calculating molecular properties, and clustering them by structural similarity.

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
molecular properties, and clusters the results.

## Pipeline steps

1. **Molecule generation** — combine scaffold + R-group SMILES (same `id`) into full
   molecules using RDKit
2. **Property calculation** — molecular weight, logP, HBA, HBD, TPSA
3. **Clustering** — group molecules by structural similarity (KMeans over Morgan
   fingerprints)

## Current status: Step 2

The DAG runs on a **weekly schedule** and automatically discovers new datasets in S3
since its last successful run. For each discovered `dataset_id`, it runs the full
chain (`generate_molecules → calculate_properties → cluster_molecules`).

An `overwrite` parameter (default `False`) can be set to `True` to force
reprocessing of all existing files, bypassing the "already processed" filter.

Data quality checks and MS Teams notifications (Step 3) are not yet implemented —
see the `dev` branch for progress.

### Note on S3

This project uses **LocalStack** (an S3-API-compatible emulator) for local
development and testing, instead of requiring a real AWS account. All S3-facing
functions talk to S3 through a standard `boto3` client — swapping `AWS_ENDPOINT_URL`
for a real AWS endpoint (or removing it) is all that's needed to point this at
production S3.

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
│       ├── molecule_gen.py    # combines scaffolds + r-groups into molecules
│       ├── properties.py      # calculates molecular properties
│       ├── clustering.py      # KMeans clustering on fingerprints
│       └── discovery.py       # S3 dataset discovery logic (new-files + overwrite)
└── tests/
    ├── test_chem_utils.py
    ├── test_dag_discovery.py
    ├── batch001_scaffolds.csv
    ├── batch001_r_groups.csv
    ├── batch002_scaffolds.csv
    └── batch002_r_groups.csv
```

## Setup

### Prerequisites
- Docker Desktop
- AWS CLI (used to interact with the local S3 emulator)

### 1. Configure environment

```powershell
copy .env.example .env
```

Adjust values in `.env` if needed (defaults work for local development).

### 2. Start the stack

```powershell
docker compose up -d
```

This starts Postgres, Redis, LocalStack (S3 emulator), and all Airflow services
(webserver, scheduler, worker, triggerer). First boot takes a few minutes while the
custom image builds.

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

Upload the sample fixture files (two datasets, to exercise multi-dataset discovery):
```powershell
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch001_scaffolds.csv s3://cheminformatics-local/batch001_scaffolds.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch001_r_groups.csv s3://cheminformatics-local/batch001_r_groups.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch002_scaffolds.csv s3://cheminformatics-local/batch002_scaffolds.csv
aws --endpoint-url=http://localhost:4566 s3 cp tests/batch002_r_groups.csv s3://cheminformatics-local/batch002_r_groups.csv
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

### 5. Verify output

Check the Airflow UI **Graph** view — you should see `discover_new_datasets`
followed by a **mapped** `process_dataset` task group, with one instance per
discovered dataset. Confirm output landed in LocalStack:

```powershell
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/generated/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/properties/
aws --endpoint-url=http://localhost:4566 s3 ls s3://cheminformatics-local/clustered/
```

## Running tests

```powershell
pip install -r requirements.txt
pytest tests/ -v
```

Tests mock the S3 client (`_get_s3_client`), so no running LocalStack instance is
required to run the test suite. Discovery logic lives in `chem_utils/discovery.py`
specifically so it can be tested without importing Airflow (which is not reliably
installable in a native Windows Python environment — see Troubleshooting).

## Troubleshooting

- **`docker compose run` leaves orphan containers behind** — always use
  `docker compose run --rm ...` so one-off CLI containers are removed automatically.
- **`InvalidRequest: x-amz-trailer header is not supported`** on `aws s3 cp` — set
  `$env:AWS_REQUEST_CHECKSUM_CALCULATION = "when_required"` before retrying; this is
  a compatibility gap between newer AWS CLI versions and older LocalStack S3 support.
- **DAG runs stay `queued` forever** — the DAG is paused by default
  (`AIRFLOW_CORE_DAGS_ARE_PAUSED_AT_CREATION: 'true'`). Unpause it before triggering.
- **`NoSuchBucket` errors after a `docker compose down`** — LocalStack's state does
  not reliably persist across restarts even with a mounted volume unless
  `PERSISTENCE=1` is set (already configured in `docker-compose.yaml`). If you still
  hit this, the bucket and sample files need to be recreated (see Setup step 3).
- **Airflow does not run natively on Windows** — `airflow db init` and any command
  that imports `airflow` directly will fail in a local Windows Python venv. Use
  Docker for anything Airflow-related; local `pytest` only works for modules that
  don't import `airflow` (which is why discovery logic is isolated in
  `chem_utils/discovery.py`).

## Branching strategy

This repo follows a `feature → dev → prod` branch policy:
- Work happens on `feature/*` branches
- Features are merged into `dev` via PR
- `dev` is merged into `prod` once a milestone (step) is complete and verified

## Roadmap

- [x] **Step 1** — Manual trigger with `dataset_id` parameter
- [x] **Step 2** — Weekly schedule, process new files since last run, `overwrite` param
- [ ] **Step 3** — Data quality checks + MS Teams notifications
- [ ] *(Optional)* ChemProp property prediction
- [ ] *(Optional)* Faerun graph visualization
