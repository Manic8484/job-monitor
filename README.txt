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

V3.8 PLANNING SELECTION FIX
---------------------------
Planning membership is now determined by monitor_jobs.booked_at only.
Different Freedom job_refs are never deduplicated because their content matches.
Stop timing enriches the route display but cannot remove a booked job from the
selected-day board.

Today's Active Monitoring is queried separately and contains only older,
incomplete carry-over jobs.

V3.9 SELECTED-DAY / EXPANSION FIX
--------------------------------
- Selected-day planning now uses the exact local calendar date of booked_at:
  (booked_at AT TIME ZONE 'Europe/London')::date = selected_date.
- This removes any boundary mismatch between the planning query and date tiles.
- Timeline container no longer clips overflowing cards.
- Each planned operation still gets its own lane.
- Row spacing increased slightly for wrapped account names.
- A small 'N planned operations' count is shown beside the legend so the
  rendered-card count can be compared directly with the 10-day tile count.

V4.0 TIMELINE EXPANSION / CARD WIDTH
------------------------------------
- Timeline height now uses the same 68px row pitch as the rendered cards.
- The 10-day strip therefore moves down with the timeline instead of cards
  spilling across it.
- Planning-card account names, job/vehicle line and journey no longer wrap.
- Timeline cards are allowed to grow horizontally to fit their content.
- Active Monitoring cards still wrap normally inside the fixed left column.

V4.1 DENSER ROW SPACING
-----------------------
- Planning row pitch reduced from 68px to 34px.
- Timeline height calculation updated to match.
- Marker vertical offsets tightened slightly.

V4.2 TIMELINE BOTTOM CLEARANCE
------------------------------
- Keeps the 34px dense row spacing from V4.1.
- Adds explicit bottom clearance beneath the final card before the 10-day strip.
- Prevents the final row being clipped while preserving the compact vertical layout.

V4.3 EXTRA BOTTOM SAFETY CLEARANCE
---------------------------------
- Keeps 34px dense row spacing.
- Increases minimum timeline height to 560px.
- Adds a much larger fixed bottom clearance before the 10-day band.
- This deliberately leaves obvious empty space below the final card so it is
  visually clear that no card is being clipped or hidden behind the date strip.

V4.4 STRONGER BOTTOM SAFETY ZONE
--------------------------------
- Keeps 34px row spacing.
- Raises minimum timeline height to 620px.
- Adds a much larger bottom safety zone below the final card before the 10-day band.
- The empty area is intentionally greater than the normal row separation so it is
  visually obvious that no further cards are hidden below.

V4.5 CLEAR END-OF-LIST GAP
--------------------------
- Keeps 34px row spacing.
- Increases the internal bottom safety area substantially.
- Minimum timeline height raised to 720px.
- Adds a 48px external gap before the 10-day strip.
- The combined effect is deliberately obvious: the last card should sit well
  above the date band so there is no suggestion that further cards are hidden.
