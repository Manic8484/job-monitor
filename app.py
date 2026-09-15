
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from flask import Flask, jsonify, request

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
