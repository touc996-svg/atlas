# ATLAS Automated Incremental Scheduling

## What it does

The scheduler invokes the existing `atlas.processing.pipeline_runner` with:

- `--mode incremental`
- `--lookback-hours` (default 48)
- S3 publishing enabled
- dbt run and dbt tests enabled

It does not reproduce the pipeline stages or their retry implementation. The current pipeline runner remains responsible for stage retries and its database execution history.

The launcher adds:

- macOS `launchd` scheduling (default every 30 minutes)
- non-overlapping execution using an OS file lock
- `.env` loading inside the Python process
- per-run logs under `data/processed/scheduler/runs/`
- atomic `latest.json` and per-run status files
- retention of the latest 30 run logs
- optional failure webhook using `ATLAS_ALERT_WEBHOOK_URL`

## Install the scheduler code

```bash
cd ~/Projects/atlas
unzip -o ~/Downloads/atlas_automated_orchestration_upgrade.zip -d /tmp
bash /tmp/atlas_automated_orchestration_upgrade/install_upgrade.sh ~/Projects/atlas
```

## Preview without running the pipeline

```bash
python scripts/run_scheduled_pipeline.py --dry-run
```

## Install the macOS schedule

Use the project virtual environment's Python:

```bash
.venv/bin/python scripts/install_launchd.py \
  --project-root ~/Projects/atlas \
  --interval-seconds 1800
```

`RunAtLoad` triggers a run when the agent is loaded; subsequent runs occur every 1800 seconds (30 minutes). Change the interval to suit your machine and workload. The Mac must be awake and the user session logged in for this per-user LaunchAgent to execute.

## Check schedule state

```bash
launchctl print gui/$(id -u)/com.atlas.incremental-pipeline
```

Latest status:

```bash
cat data/processed/scheduler/latest.json
```

Recent logs:

```bash
ls -lt data/processed/scheduler/runs | head
```

LaunchAgent stdout/stderr:

```bash
tail -n 100 logs/atlas-launchd.stdout.log
tail -n 100 logs/atlas-launchd.stderr.log
```

## Optional failure notification

Set this in `.env` (do not commit the webhook URL):

```dotenv
ATLAS_ALERT_WEBHOOK_URL=https://your-incoming-webhook
```

The scheduler sends a minimal JSON text message on failure. It does not send raw logs or credentials.

## Change or remove the schedule

Reinstall with a different interval:

```bash
.venv/bin/python scripts/install_launchd.py \
  --project-root ~/Projects/atlas \
  --interval-seconds 3600
```

Remove:

```bash
.venv/bin/python scripts/install_launchd.py \
  --project-root ~/Projects/atlas \
  --uninstall
```

## Safety

- The schedule runs incremental mode; it does not invoke `--full-refresh`.
- A file lock prevents a scheduled run from overlapping another scheduled run.
- A lock skip is treated as a normal skip, not a pipeline failure.
- Existing pipeline retries are preserved.
- `.env`, database rows, Docker Compose, and Parquet data are not modified by installation.
- This is local macOS scheduling, not a highly available orchestrator. For a production deployment across hosts, use Airflow, Dagster, or a managed workflow orchestrator.
