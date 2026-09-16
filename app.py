
import os
import re
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

LONDON = ZoneInfo("Europe/London")

DB_NAME = os.getenv("DB_NAME", "job_monitor")
DB_USER = os.getenv("DB_USER", "job_monitor_api")
DB_PASSWORD = os.getenv("DB_PASSWORD")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
INGEST_TOKEN = os.getenv("JOB_MONITOR_INGEST_TOKEN")

def db_connect(row_factory=None):
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return psycopg.connect(database_url, row_factory=row_factory)

    if not (DB_PASSWORD and INSTANCE_CONNECTION_NAME):
        raise RuntimeError(
            "Set DATABASE_URL locally, or DB_PASSWORD and INSTANCE_CONNECTION_NAME on Cloud Run."
        )

    kwargs = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "host": f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
    }
    if row_factory is not None:
        kwargs["row_factory"] = row_factory
    return psycopg.connect(**kwargs)

def parse_dt(value):
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None

    formats = [
        "%d %b %Y %H:%M",
        "%d %b %y %H:%M:%S",
        "%d %b %Y %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=LONDON)
        except ValueError:
            pass
    return None

def parse_snapshot(body):
    lines = [line.strip() for line in body.replace("\r", "").split("\n")]
    result = {
        "stops": [],
    }

    current_stop = None

    job_map = {
        "Job": "job_ref",
        "Agent": "agent_callsign",
        "Firstname": "driver_firstname",
        "Lastname": "driver_lastname",
        "Account": "account",
        "Vehicle": "vehicle",
        "Vehicle Description": "vehicle_description",
        "Cancelled": "cancelled",
        "Booked": "booked",
        "Goods": "goods",
        "Special Instructions": "special_instructions",
        "Despatch Instructions": "despatch_instructions",
        "Status": "freedom_status",
        "Status Text": "freedom_status_text",
    }

    stop_map = {
        "Drop": "drop_order",
        "Drop Type": "drop_type",
        "Address Name": "address_name",
        "Address Line 1": "address_line_1",
        "Address Line 2": "address_line_2",
        "Postcode": "postcode",
        "Country": "country",
        "Courntry Code": "country_code",
        "Country Code": "country_code",
        "Required From": "required_from",
        "Required To": "required_to",
        "Date Completed": "date_completed",
        "Stop ID": "stop_id",
    }

    in_stops = False

    for line in lines:
        if not line:
            continue

        if line == "---":
            if current_stop:
                result["stops"].append(current_stop)
                current_stop = None
            in_stops = True
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if in_stops and key in stop_map:
            if current_stop is None:
                current_stop = {}
            current_stop[stop_map[key]] = value
        elif key in job_map:
            result[job_map[key]] = value

    if current_stop:
        result["stops"].append(current_stop)

    result["cancelled_at"] = parse_dt(result.get("cancelled"))
    result["booked_at"] = parse_dt(result.get("booked"))

    for s in result["stops"]:
        try:
            s["drop_order"] = int(s.get("drop_order") or 0)
        except ValueError:
            s["drop_order"] = 0

        s["required_from_dt"] = parse_dt(s.get("required_from"))
        s["required_to_dt"] = parse_dt(s.get("required_to"))
        s["date_completed_dt"] = parse_dt(s.get("date_completed"))

    return result

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "job-monitor",
        "db_name": DB_NAME,
        "db_user": DB_USER,
        "instance_connection_name": INSTANCE_CONNECTION_NAME,
        "db_password_set": bool(DB_PASSWORD),
        "ingest_token_set": bool(INGEST_TOKEN),
    })

@app.post("/job-monitor-email")
def job_monitor_email():
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    message_id = payload.get("message_id")
    received_at_raw = payload.get("received_at")
    source_event = payload.get("source_event") or "FREEDOM"
    body = payload.get("body") or ""

    if not body.strip():
        return jsonify({"ok": False, "error": "body is required"}), 400

    received_at = None
    if received_at_raw:
        try:
            received_at = datetime.fromisoformat(received_at_raw.replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"ok": False, "error": "invalid received_at"}), 400

    parsed = parse_snapshot(body)
    job_ref = parsed.get("job_ref")
    if not job_ref:
        return jsonify({"ok": False, "error": "Could not parse Job"}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Dedupe
            if message_id:
                cur.execute(
                    "SELECT id FROM public.monitor_events WHERE message_id = %s",
                    (message_id,),
                )
                dup = cur.fetchone()
                if dup:
                    return jsonify({
                        "ok": True,
                        "duplicate": True,
                        "event_id": dup["id"],
                        "job_ref": job_ref,
                    })

            # Stale protection by job.
            cur.execute(
                "SELECT last_received_at, operation_id FROM public.monitor_jobs WHERE job_ref = %s",
                (job_ref,),
            )
            existing = cur.fetchone()

            if (
                existing
                and existing["last_received_at"]
                and received_at
                and received_at < existing["last_received_at"]
            ):
                cur.execute(
                    """
                    INSERT INTO public.monitor_events
                        (message_id, source_event, job_ref, raw_payload,
                         processing_status, processing_detail, received_at)
                    VALUES (%s,%s,%s,%s,'STALE','Older snapshot ignored',%s)
                    RETURNING id
                    """,
                    (message_id, source_event, job_ref, body, received_at),
                )
                event_id = cur.fetchone()["id"]
                conn.commit()
                return jsonify({
                    "ok": True,
                    "stale": True,
                    "event_id": event_id,
                    "job_ref": job_ref,
                })

            operation_id = existing["operation_id"] if existing else None

            # One operation per job initially.
            if operation_id is None:
                cur.execute(
                    """
                    INSERT INTO public.monitor_operations (title)
                    VALUES (%s)
                    RETURNING id
                    """,
                    (f"Job {job_ref}",),
                )
                operation_id = cur.fetchone()["id"]

            cur.execute(
                """
                INSERT INTO public.monitor_jobs (
                    job_ref, operation_id,
                    agent_callsign, driver_firstname, driver_lastname,
                    account, vehicle, vehicle_description,
                    cancelled_at, booked_at,
                    goods, special_instructions, despatch_instructions,
                    freedom_status, freedom_status_text,
                    last_source_event, last_message_id, last_received_at
                )
                VALUES (
                    %(job_ref)s, %(operation_id)s,
                    %(agent_callsign)s, %(driver_firstname)s, %(driver_lastname)s,
                    %(account)s, %(vehicle)s, %(vehicle_description)s,
                    %(cancelled_at)s, %(booked_at)s,
                    %(goods)s, %(special_instructions)s, %(despatch_instructions)s,
                    %(freedom_status)s, %(freedom_status_text)s,
                    %(last_source_event)s, %(last_message_id)s, %(last_received_at)s
                )
                ON CONFLICT (job_ref) DO UPDATE SET
                    operation_id = EXCLUDED.operation_id,
                    agent_callsign = EXCLUDED.agent_callsign,
                    driver_firstname = EXCLUDED.driver_firstname,
                    driver_lastname = EXCLUDED.driver_lastname,
                    account = EXCLUDED.account,
                    vehicle = EXCLUDED.vehicle,
                    vehicle_description = EXCLUDED.vehicle_description,
                    cancelled_at = EXCLUDED.cancelled_at,
                    booked_at = EXCLUDED.booked_at,
                    goods = EXCLUDED.goods,
                    special_instructions = EXCLUDED.special_instructions,
                    despatch_instructions = EXCLUDED.despatch_instructions,
                    freedom_status = EXCLUDED.freedom_status,
                    freedom_status_text = EXCLUDED.freedom_status_text,
                    last_source_event = EXCLUDED.last_source_event,
                    last_message_id = EXCLUDED.last_message_id,
                    last_received_at = EXCLUDED.last_received_at
                """,
                {
                    "job_ref": job_ref,
                    "operation_id": operation_id,
                    "agent_callsign": parsed.get("agent_callsign") or None,
                    "driver_firstname": parsed.get("driver_firstname") or None,
                    "driver_lastname": parsed.get("driver_lastname") or None,
                    "account": parsed.get("account") or None,
                    "vehicle": parsed.get("vehicle") or None,
                    "vehicle_description": parsed.get("vehicle_description") or None,
                    "cancelled_at": parsed.get("cancelled_at"),
                    "booked_at": parsed.get("booked_at"),
                    "goods": parsed.get("goods") or None,
                    "special_instructions": parsed.get("special_instructions") or None,
                    "despatch_instructions": parsed.get("despatch_instructions") or None,
                    "freedom_status": parsed.get("freedom_status") or None,
                    "freedom_status_text": parsed.get("freedom_status_text") or None,
                    "last_source_event": source_event,
                    "last_message_id": message_id,
                    "last_received_at": received_at,
                },
            )

            seen_stops = []
            for s in parsed["stops"]:
                stop_id = s.get("stop_id")
                if not stop_id:
                    continue
                seen_stops.append(stop_id)

                cur.execute(
                    """
                    INSERT INTO public.monitor_stops (
                        stop_id, job_ref, drop_order, drop_type,
                        address_name, address_line_1, address_line_2,
                        postcode, country, country_code,
                        required_from, required_to, date_completed,
                        last_seen_at
                    )
                    VALUES (
                        %(stop_id)s, %(job_ref)s, %(drop_order)s, %(drop_type)s,
                        %(address_name)s, %(address_line_1)s, %(address_line_2)s,
                        %(postcode)s, %(country)s, %(country_code)s,
                        %(required_from)s, %(required_to)s, %(date_completed)s,
                        %(last_seen_at)s
                    )
                    ON CONFLICT (stop_id) DO UPDATE SET
                        job_ref = EXCLUDED.job_ref,
                        drop_order = EXCLUDED.drop_order,
                        drop_type = EXCLUDED.drop_type,
                        address_name = EXCLUDED.address_name,
                        address_line_1 = EXCLUDED.address_line_1,
                        address_line_2 = EXCLUDED.address_line_2,
                        postcode = EXCLUDED.postcode,
                        country = EXCLUDED.country,
                        country_code = EXCLUDED.country_code,
                        required_from = EXCLUDED.required_from,
                        required_to = EXCLUDED.required_to,
                        date_completed = EXCLUDED.date_completed,
                        last_seen_at = EXCLUDED.last_seen_at
                    """,
                    {
                        "stop_id": stop_id,
                        "job_ref": job_ref,
                        "drop_order": s.get("drop_order"),
                        "drop_type": s.get("drop_type") or None,
                        "address_name": s.get("address_name") or None,
                        "address_line_1": s.get("address_line_1") or None,
                        "address_line_2": s.get("address_line_2") or None,
                        "postcode": s.get("postcode") or None,
                        "country": s.get("country") or None,
                        "country_code": s.get("country_code") or None,
                        "required_from": s.get("required_from_dt"),
                        "required_to": s.get("required_to_dt"),
                        "date_completed": s.get("date_completed_dt"),
                        "last_seen_at": received_at,
                    },
                )

            cur.execute(
                """
                INSERT INTO public.monitor_events (
                    message_id, source_event, job_ref, raw_payload,
                    processing_status, processing_detail, received_at
                )
                VALUES (%s,%s,%s,%s,'PROCESSED',%s,%s)
                RETURNING id
                """,
                (
                    message_id,
                    source_event,
                    job_ref,
                    body,
                    f"{len(seen_stops)} stop(s) processed",
                    received_at,
                ),
            )
            event_id = cur.fetchone()["id"]

            conn.commit()

    return jsonify({
        "ok": True,
        "job_ref": job_ref,
        "operation_id": operation_id,
        "event_id": event_id,
        "stops_processed": len(seen_stops),
        "cancelled": bool(parsed.get("cancelled_at")),
        "allocated": bool((parsed.get("agent_callsign") or "").strip()),
    })



@app.post("/operations/<int:operation_id>/presentation-state")
def update_operation_presentation_state(operation_id):
    payload = request.get_json(silent=True) or {}
    collected = bool(payload.get("collected"))
    complete = bool(payload.get("complete"))
    hidden = bool(payload.get("hidden"))

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE public.monitor_operations
                SET manual_collected_at = CASE WHEN %s THEN COALESCE(manual_collected_at, now()) ELSE NULL END,
                    manual_completed_at = CASE WHEN %s THEN COALESCE(manual_completed_at, now()) ELSE NULL END,
                    presentation_hidden = %s,
                    manual_updated_at = now(),
                    manual_updated_by = 'BOARD'
                WHERE id = %s
                RETURNING id, manual_collected_at, manual_completed_at, presentation_hidden
            """, (collected, complete, hidden, operation_id))
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "error": "operation not found"}), 404
            conn.commit()

    return jsonify({
        "ok": True,
        "operation_id": operation_id,
        "collected": row["manual_collected_at"] is not None,
        "complete": row["manual_completed_at"] is not None,
        "hidden": bool(row["presentation_hidden"]),
    })



def postcode_area(postcode):
    if not postcode:
        return ""
    value = postcode.strip().upper()
    m = re.match(r"^([A-Z]{1,2}\d[A-Z\d]?)", value)
    return m.group(1) if m else value.split()[0]


def operation_journey(jobs):
    """
    Use the linked docket with the most stops as the representative route.
    For GB-only routes show postcode areas, e.g. EH11 → W1.
    For any international route show the ordered country-code journey.
    """
    jobs_with_stops = [j for j in jobs if j.get("stops")]
    if not jobs_with_stops:
        return {
            "journey_text": "",
            "international": False,
            "country_codes": [],
        }

    representative = max(
        jobs_with_stops,
        key=lambda j: (len(j["stops"]), str(j.get("job_ref") or "")),
    )
    stops = sorted(representative["stops"], key=lambda s: s.get("drop_order") or 0)

    codes = []
    for s in stops:
        code = (s.get("country_code") or "").strip().upper()
        if code and (not codes or codes[-1] != code):
            codes.append(code)

    international = any(code not in ("", "GB") for code in codes)

    if international:
        journey_text = " → ".join(codes) if codes else "International"
    else:
        areas = [postcode_area(s.get("postcode")) for s in stops if postcode_area(s.get("postcode"))]
        if len(areas) >= 2:
            journey_text = f"{areas[0]} → {areas[-1]}"
        elif len(areas) == 1:
            journey_text = areas[0]
        else:
            journey_text = "GB"

    return {
        "journey_text": journey_text,
        "international": international,
        "country_codes": codes,
    }


@app.get("/board")
def board():
    today = datetime.now(LONDON).date()
    raw_date = request.args.get("date")
    selected_date = date.fromisoformat(raw_date) if raw_date else today
    show_active_column = selected_date == today

    day_start = datetime.combine(selected_date, time.min).replace(tzinfo=LONDON)
    day_end = day_start + timedelta(days=1)
    lookahead_start = datetime.combine(today, time.min).replace(tzinfo=LONDON)
    lookahead_end = lookahead_start + timedelta(days=10)

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # PLANNING:
            # A job belongs on a selected day's planning timeline because its
            # Freedom booked_at is on that day. Stop timing enriches the card
            # but cannot remove a booked job from the board.
            cur.execute("""
                SELECT
                    o.id AS op_id,
                    o.title,
                    o.manual_note,
                    o.manual_collected_at,
                    o.manual_completed_at,
                    o.presentation_hidden,
                    oa.allocation_status,
                    oa.active_job_count,
                    oa.unallocated_job_count,
                    j.job_ref,
                    j.operation_id,
                    j.agent_callsign,
                    j.driver_firstname,
                    j.driver_lastname,
                    j.account,
                    j.vehicle,
                    j.vehicle_description,
                    j.component_role,
                    j.cancelled_at,
                    j.booked_at,
                    j.goods,
                    j.special_instructions,
                    j.despatch_instructions,
                    j.freedom_status,
                    j.freedom_status_text,
                    j.monitoring_enabled
                FROM public.monitor_jobs j
                JOIN public.monitor_operations o
                  ON o.id = j.operation_id
                JOIN public.v_monitor_operation_allocation oa
                  ON oa.operation_id = o.id
                WHERE (j.booked_at AT TIME ZONE 'Europe/London')::date = %s
                  AND j.monitoring_enabled = TRUE
                  AND o.monitoring_enabled = TRUE
                  AND COALESCE(o.presentation_hidden, FALSE) = FALSE
                ORDER BY j.booked_at, j.job_ref
            """, (selected_date,))
            planning_rows = [dict(r) for r in cur.fetchall()]

            # ACTIVE MONITORING:
            # Today only: incomplete operations that began before today.
            carry_rows = []
            if show_active_column:
                cur.execute("""
                    SELECT
                        o.id AS op_id,
                        o.title,
                        o.manual_note,
                        o.manual_collected_at,
                        o.manual_completed_at,
                        o.presentation_hidden,
                        oa.allocation_status,
                        oa.active_job_count,
                        oa.unallocated_job_count,
                        j.job_ref,
                        j.operation_id,
                        j.agent_callsign,
                        j.driver_firstname,
                        j.driver_lastname,
                        j.account,
                        j.vehicle,
                        j.vehicle_description,
                        j.component_role,
                        j.cancelled_at,
                        j.booked_at,
                        j.goods,
                        j.special_instructions,
                        j.despatch_instructions,
                        j.freedom_status,
                        j.freedom_status_text,
                        j.monitoring_enabled
                    FROM public.monitor_jobs j
                    JOIN public.monitor_operations o
                      ON o.id = j.operation_id
                    JOIN public.v_monitor_operation_allocation oa
                      ON oa.operation_id = o.id
                    WHERE j.booked_at < %s
                      AND j.cancelled_at IS NULL
                      AND j.monitoring_enabled = TRUE
                      AND o.monitoring_enabled = TRUE
                      AND COALESCE(o.presentation_hidden, FALSE) = FALSE
                      AND o.manual_completed_at IS NULL
                      AND EXISTS (
                          SELECT 1
                          FROM public.monitor_stops sx
                          WHERE sx.job_ref = j.job_ref
                            AND sx.date_completed IS NULL
                      )
                    ORDER BY j.booked_at, j.job_ref
                """, (day_start,))
                carry_rows = [dict(r) for r in cur.fetchall()]

            all_rows = planning_rows + carry_rows
            job_refs = sorted({r["job_ref"] for r in all_rows})

            stops_by_job = {}
            if job_refs:
                cur.execute("""
                    SELECT *
                    FROM public.monitor_stops
                    WHERE job_ref = ANY(%s)
                    ORDER BY job_ref, drop_order
                """, (job_refs,))
                for s in cur.fetchall():
                    stops_by_job.setdefault(s["job_ref"], []).append(dict(s))

            # Look-ahead is also based on booked_at, so the day-count tiles and
            # the planning view use the same rule.
            cur.execute("""
                SELECT
                    (j.booked_at AT TIME ZONE 'Europe/London')::date AS booked_day,
                    COUNT(DISTINCT j.operation_id) AS operation_count
                FROM public.monitor_jobs j
                JOIN public.monitor_operations o
                  ON o.id = j.operation_id
                WHERE j.booked_at >= %s
                  AND j.booked_at < %s
                  AND j.cancelled_at IS NULL
                  AND j.monitoring_enabled = TRUE
                  AND o.monitoring_enabled = TRUE
                  AND COALESCE(o.presentation_hidden, FALSE) = FALSE
                  AND o.manual_completed_at IS NULL
                GROUP BY 1
            """, (lookahead_start, lookahead_end))
            look_rows = [dict(r) for r in cur.fetchall()]

    def build_operations(source_rows):
        ops = {}
        for r in source_rows:
            op_id = r["op_id"]
            op = ops.setdefault(op_id, {
                "operation_id": op_id,
                "title": r["title"],
                "manual_note": r["manual_note"],
                "manual_collected_at": r["manual_collected_at"],
                "manual_completed_at": r["manual_completed_at"],
                "presentation_hidden": bool(r["presentation_hidden"]),
                "allocation_status": r["allocation_status"],
                "active_job_count": r["active_job_count"],
                "unallocated_job_count": r["unallocated_job_count"],
                "jobs": [],
            })

            stops = stops_by_job.get(r["job_ref"], [])
            all_completed = bool(stops) and all(s["date_completed"] for s in stops)

            op["jobs"].append({
                "job_ref": r["job_ref"],
                "agent_callsign": r["agent_callsign"],
                "driver_name": " ".join(
                    p for p in [r["driver_firstname"], r["driver_lastname"]] if p
                ),
                "account": r["account"],
                "vehicle": r["vehicle"],
                "vehicle_description": r["vehicle_description"],
                "component_role": r["component_role"],
                "cancelled_at": r["cancelled_at"].isoformat() if r["cancelled_at"] else None,
                "booked_at": r["booked_at"].isoformat() if r["booked_at"] else None,
                "goods": r["goods"],
                "special_instructions": r["special_instructions"],
                "despatch_instructions": r["despatch_instructions"],
                "freedom_status": r["freedom_status"],
                "freedom_status_text": r["freedom_status_text"],
                "active": r["cancelled_at"] is None and not all_completed,
                "stops": [{
                    "stop_id": s["stop_id"],
                    "drop_order": s["drop_order"],
                    "postcode": s["postcode"],
                    "country": s["country"],
                    "country_code": s["country_code"],
                    "required_from": s["required_from"].isoformat() if s["required_from"] else None,
                    "date_completed": s["date_completed"].isoformat() if s["date_completed"] else None,
                } for s in stops],
            })
        return ops

    planning_ops = build_operations(planning_rows)
    carry_ops = build_operations(carry_rows)

    now = datetime.now(LONDON)

    def decorate(op, planning=True):
        timed = []
        active_any = False
        cancelled_all = True

        for j in op["jobs"]:
            active_any = active_any or j["active"]
            if j["cancelled_at"] is None:
                cancelled_all = False
            for s in j["stops"]:
                if s["required_from"]:
                    timed.append((datetime.fromisoformat(s["required_from"]), s, j["job_ref"]))

        # Planning anchor is booked_at, not stop data.
        booked_times = [
            datetime.fromisoformat(j["booked_at"])
            for j in op["jobs"]
            if j["booked_at"]
        ]
        anchor = min(booked_times, default=day_start)

        manual_complete = op["manual_completed_at"] is not None
        manual_collected = op["manual_collected_at"] is not None

        if manual_complete:
            status, cls = "COMPLETE", "complete"
        elif op["allocation_status"] == "UNALLOCATED":
            status, cls = "UNALLOCATED", "unallocated"
        elif cancelled_all:
            status, cls = "CANCELLED", "cancelled"
        elif manual_collected or (active_any and anchor <= now):
            status, cls = "ACTIVE", "active"
        elif active_any:
            status, cls = "FUTURE", "future"
        else:
            status, cls = "COMPLETE", "complete"

        accounts = sorted({j["account"] for j in op["jobs"] if j["account"]})
        vehicles = [j["vehicle"] for j in op["jobs"] if j["vehicle"]]
        journey = operation_journey(op["jobs"])

        # First pickup marker remains omitted; later timed stops only.
        timed_today = sorted(
            [x for x in timed if day_start <= x[0] < day_end],
            key=lambda x: x[0]
        )
        markers = []
        for dt, s, job_ref in timed_today[1:]:
            if not s:
                continue
            pct = max(0, min(100, ((dt - day_start).total_seconds() / 86400) * 100))
            markers.append({
                "left_pct": pct,
                "postcode": s["postcode"] or "",
                "country_code": s["country_code"] or "",
                "job_ref": job_ref,
                "drop_order": s["drop_order"],
            })

        result = {
            **op,
            "status": status,
            "status_class": cls,
            "primary_account": accounts[0] if accounts else (op["title"] or f"Operation {op['operation_id']}"),
            "vehicles": vehicles,
            "job_count": len(op["jobs"]),
            "markers": markers,
            "manual_collected": manual_collected,
            "manual_complete": manual_complete,
            "journey_text": journey["journey_text"],
            "international": journey["international"],
            "country_codes": journey["country_codes"],
        }

        if planning:
            result["left_pct"] = max(
                0,
                min(100, ((anchor - day_start).total_seconds() / 86400) * 100)
            )
            result["width_pct"] = 24 / 1440 * 100

        return result

    timeline_operations = [decorate(op, planning=True) for op in planning_ops.values()]
    active_operations = [decorate(op, planning=False) for op in carry_ops.values()]

    timeline_operations.sort(key=lambda x: (x["left_pct"], x["operation_id"]))
    active_operations.sort(key=lambda x: (x["status"] != "UNALLOCATED", x["operation_id"]))

    for idx, op in enumerate(timeline_operations):
        op["lane"] = idx

    # Height must grow with every visible planning row.
    # Row pitch in the template is 68px, with room above/below the first/last card.
    # Keep the denser 34px row pitch, but add enough bottom clearance for
    # the full final card (including border/shadow) before the 10-day strip.
    ticks = [{"label": f"{h:02d}:00", "left_pct": h / 24 * 100} for h in range(0, 25, 2)]

    counts = {today + timedelta(days=i): 0 for i in range(10)}
    for r in look_rows:
        d = r["booked_day"]
        if d in counts:
            counts[d] = r["operation_count"]

    lookahead_days = []
    for i in range(10):
        d = today + timedelta(days=i)
        lookahead_days.append({
            "date": d,
            "count": counts[d],
            "selected": d == selected_date,
            "today": d == today,
        })

    return render_template(
        "board.html",
        active_operations=active_operations,
        timeline_operations=timeline_operations,
        show_active_column=show_active_column,
        ticks=ticks,
        selected_date=selected_date,
        prev_date=selected_date - timedelta(days=1),
        next_date=selected_date + timedelta(days=1),
        lookahead_days=lookahead_days,
        planned_count=len(timeline_operations),
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
