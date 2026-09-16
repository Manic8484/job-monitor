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

V3: run 002_monitor_presentation_controls.sql before deploy. Adds current-day active column, smaller cards, removes first pickup marker, and modal controls for Collected/Complete/Hide.


V3.1 ACTIVE MONITORING RULE
---------------------------
On the Today screen, Active Monitoring contains only incomplete operations
whose known booked/required start is before today.

Jobs starting today remain on today's 24-hour timeline even after collection.
Future jobs remain on their actual booked day.

V3.2 CARD JOURNEY TITLES
------------------------
- Removed the textual status from card faces; colour remains the status cue.
- GB-only routes show first and last postcode areas, e.g. EH11 → W1.
- Routes containing any non-GB stop show ordered country codes, e.g. GB → FR → DE.
- International cards also carry a small globe/INTL badge on the right.

V3.3 MODAL JOB SUMMARY
----------------------
- Lead job summary moved directly under the account/status summary.
- Driver now displays as Callsign · Firstname Lastname.
- Vehicle now displays as Tariff code · Vehicle Description.
- Goods line unchanged.

V3.4 MODAL ORDER
----------------
Lead job summary is now immediately below the modal title
(Account — Status) and above the Operation ID / Allocation metadata.

V3.5 MODAL DEDUPLICATION
------------------------
Removed the repeated driver / tariff / goods summary above the lower stop table.
The lead job block at the top remains the single summary.
For multi-docket operations, the lower section keeps only a small Job heading
before each stop table so the tables remain attributable.

V3.6 JOB IDENTITY ON CARDS
--------------------------
- Single-job operations now show the Freedom job_ref directly on the card.
- If an operation later contains multiple linked jobs, the card shows the count
  rather than pretending they are duplicates.
- job_ref remains the immutable identity; matching content never merges jobs.

V3.7 CARD DENSITY / VISIBILITY
------------------------------
- Planned cards are around 20% smaller again.
- Account/title text is smaller and tighter to reduce multi-line card growth.
- Planning cards no longer recycle through five lanes.
- Every planned operation gets its own row, so identical-time jobs cannot sit
  behind one another.
- Timeline height grows with the number of operations. The page can extend
  vertically rather than clipping or hiding a live/planned card.
