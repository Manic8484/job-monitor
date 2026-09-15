JOB MONITOR SERVICE - FIRST CUT

Service name:
  job-monitor

Endpoints:
  GET  /health
  POST /job-monitor-email

Environment variables:
  DB_NAME=job_monitor
  DB_USER=job_monitor_api
  INSTANCE_CONNECTION_NAME=<your Cloud SQL instance connection name>

Secret-backed environment variables:
  DB_PASSWORD
  JOB_MONITOR_INGEST_TOKEN

Cloud Run:
  Attach the same Cloud SQL instance that contains the job_monitor database.
  Runtime service account needs Cloud SQL Client.

POST JSON:
{
  "message_id": "<internet message id>",
  "received_at": "2026-09-15T14:25:00+01:00",
  "source_event": "FREEDOM",
  "body": "<HTML-to-text Freedom snapshot>"
}

Header:
  X-Ingest-Token: <JOB_MONITOR_INGEST_TOKEN>

Initial grouping behaviour:
  Each new Freedom job gets its own monitor_operation.
  Existing updates retain that operation_id.
  We will add management grouping later so several Freedom jobs/dockets can
  be combined into one operation.

Allocation:
  A blank Agent is treated as unallocated.
  The database view v_monitor_operation_allocation rolls that pessimistically
  up to the operation level.

BOARD
-----
GET /board
Default 7-day view plus active jobs. Red=unallocated, amber=active, blue=future, green=complete.
