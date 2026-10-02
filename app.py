
import os
import re
import html
import base64
import hashlib
import mimetypes
import uuid
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from flask import Flask, jsonify, render_template, request, Response

from google.cloud import storage

app = Flask(__name__)

LONDON = ZoneInfo("Europe/London")

DB_NAME = os.getenv("DB_NAME", "job_monitor")
DB_USER = os.getenv("DB_USER", "job_monitor_api")
DB_PASSWORD = os.getenv("DB_PASSWORD")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
INGEST_TOKEN = os.getenv("JOB_MONITOR_INGEST_TOKEN")
JOB_MEDIA_BUCKET = os.getenv("JOB_MEDIA_BUCKET", "warehouse-media")
JOB_MEDIA_MAX_BYTES = int(os.getenv("JOB_MEDIA_MAX_BYTES", str(20 * 1024 * 1024)))
JOB_BOARD_VIEW = os.getenv("JOB_BOARD_VIEW", "public.v_job_board_special")
JOB_BOARD_TITLE = os.getenv("JOB_BOARD_TITLE", "High Importance Jobs")

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




def parse_freedom_snapshot(body):
    """Parse the V4 Freedom mirror feed.

    Accepts either the plain-text body produced by the mailbox automation or
    the original HTML template body containing <br> line breaks.
    """
    value = html.unescape(body or "")
    value = re.sub(r"(?i)<br\s*/?>", "\n", value)
    value = re.sub(r"(?i)</p\s*>", "\n", value)
    value = re.sub(r"<[^>]+>", "", value)
    lines = [line.strip() for line in value.replace("\r", "").split("\n")]

    result = {"stops": []}
    current_stop = None
    in_stops = False

    job_map = {
        "Job": "job_ref",
        "Driver Callsign": "driver_callsign",
        "Driver Firstname": "driver_firstname",
        "Driver Lastname": "driver_lastname",
        "Account Name": "account_name",
        # Temporary aliases while V4 replaces earlier test templates.
        "Account": "account_name",
        "Account Code": "account_name",
        "Agent Code": "agent_code",
        "Agent Name": "agent_name",
        "Vehicle": "vehicle_code",
        "Vehicle Description": "vehicle_description",
        "Cancelled": "cancelled",
        "Booked": "booked",
        "Goods": "goods",
        "Special Instructions": "special_instructions",
        "Despatch Instructions": "despatch_instructions",
        "Status": "freedom_status",
        "Job Flags": "job_flags",
    }

    stop_map = {
        "Drop": "drop_order",
        "Drop Type": "drop_type",
        "Address Name": "address_name",
        "Address Line 1": "address_line_1",
        "Address Line 2": "address_line_2",
        "Postcode": "postcode",
        "Country": "country",
        "Country Code": "country_code",
        "Courntry Code": "country_code",
        "Required From": "required_from",
        "Required To": "required_to",
        "Date Completed": "date_completed",
        "Stop ID": "stop_id",
    }

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

        key, raw = line.split(":", 1)
        key = key.strip()
        raw = raw.strip()

        if in_stops and key in stop_map:
            if current_stop is None:
                current_stop = {}
            current_stop[stop_map[key]] = raw
        elif key in job_map:
            result[job_map[key]] = raw

    if current_stop:
        result["stops"].append(current_stop)

    result["cancelled_at"] = parse_dt(result.get("cancelled"))
    result["booked_at"] = parse_dt(result.get("booked"))

    flags = (result.get("job_flags") or "").strip()
    result["job_flags"] = flags or None

    for stop in result["stops"]:
        try:
            stop["drop_order"] = int(stop.get("drop_order") or 0)
        except (TypeError, ValueError):
            stop["drop_order"] = 0
        stop["required_from_dt"] = parse_dt(stop.get("required_from"))
        stop["required_to_dt"] = parse_dt(stop.get("required_to"))
        stop["date_completed_dt"] = parse_dt(stop.get("date_completed"))

    return result


@app.post("/freedom-email")
def freedom_email():
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    message_id = payload.get("message_id")
    received_at_raw = payload.get("received_at")
    source_event = payload.get("source_event") or "FREEDOM_MIRROR"
    body = payload.get("body") or ""

    if not body.strip():
        return jsonify({"ok": False, "error": "body is required"}), 400

    received_at = datetime.now(LONDON)
    if received_at_raw:
        try:
            received_at = datetime.fromisoformat(received_at_raw.replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"ok": False, "error": "invalid received_at"}), 400

    parsed = parse_freedom_snapshot(body)
    job_ref = (parsed.get("job_ref") or "").strip()
    if not job_ref:
        return jsonify({"ok": False, "error": "Could not parse Job"}), 400

    if not parsed["stops"]:
        return jsonify({"ok": False, "error": "No stops parsed; snapshot not applied"}), 400

    invalid_stops = [s for s in parsed["stops"] if not (s.get("stop_id") or "").strip()]
    if invalid_stops:
        return jsonify({"ok": False, "error": "One or more stops have no Stop ID"}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if message_id:
                cur.execute(
                    "SELECT id FROM public.freedom_events WHERE message_id = %s",
                    (message_id,),
                )
                duplicate = cur.fetchone()
                if duplicate:
                    return jsonify({
                        "ok": True,
                        "duplicate": True,
                        "event_id": duplicate["id"],
                        "job_ref": job_ref,
                    })

            cur.execute(
                "SELECT last_received_at FROM public.freedom_jobs WHERE job_ref = %s",
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
                    INSERT INTO public.freedom_events
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

            cur.execute(
                """
                INSERT INTO public.freedom_jobs (
                    job_ref,
                    driver_callsign, driver_firstname, driver_lastname,
                    account_name, agent_code, agent_name,
                    vehicle_code, vehicle_description,
                    cancelled_at, booked_at,
                    goods, special_instructions, despatch_instructions,
                    freedom_status, job_flags,
                    last_source_event, last_message_id, last_received_at,
                    updated_at
                )
                VALUES (
                    %(job_ref)s,
                    %(driver_callsign)s, %(driver_firstname)s, %(driver_lastname)s,
                    %(account_name)s, %(agent_code)s, %(agent_name)s,
                    %(vehicle_code)s, %(vehicle_description)s,
                    %(cancelled_at)s, %(booked_at)s,
                    %(goods)s, %(special_instructions)s, %(despatch_instructions)s,
                    %(freedom_status)s, %(job_flags)s,
                    %(last_source_event)s, %(last_message_id)s, %(last_received_at)s,
                    now()
                )
                ON CONFLICT (job_ref) DO UPDATE SET
                    driver_callsign = EXCLUDED.driver_callsign,
                    driver_firstname = EXCLUDED.driver_firstname,
                    driver_lastname = EXCLUDED.driver_lastname,
                    account_name = EXCLUDED.account_name,
                    agent_code = EXCLUDED.agent_code,
                    agent_name = EXCLUDED.agent_name,
                    vehicle_code = EXCLUDED.vehicle_code,
                    vehicle_description = EXCLUDED.vehicle_description,
                    cancelled_at = EXCLUDED.cancelled_at,
                    booked_at = EXCLUDED.booked_at,
                    goods = EXCLUDED.goods,
                    special_instructions = EXCLUDED.special_instructions,
                    despatch_instructions = EXCLUDED.despatch_instructions,
                    freedom_status = EXCLUDED.freedom_status,
                    job_flags = EXCLUDED.job_flags,
                    last_source_event = EXCLUDED.last_source_event,
                    last_message_id = EXCLUDED.last_message_id,
                    last_received_at = EXCLUDED.last_received_at,
                    updated_at = now()
                """,
                {
                    "job_ref": job_ref,
                    "driver_callsign": parsed.get("driver_callsign") or None,
                    "driver_firstname": parsed.get("driver_firstname") or None,
                    "driver_lastname": parsed.get("driver_lastname") or None,
                    "account_name": parsed.get("account_name") or None,
                    "agent_code": parsed.get("agent_code") or None,
                    "agent_name": parsed.get("agent_name") or None,
                    "vehicle_code": parsed.get("vehicle_code") or None,
                    "vehicle_description": parsed.get("vehicle_description") or None,
                    "cancelled_at": parsed.get("cancelled_at"),
                    "booked_at": parsed.get("booked_at"),
                    "goods": parsed.get("goods") or None,
                    "special_instructions": parsed.get("special_instructions") or None,
                    "despatch_instructions": parsed.get("despatch_instructions") or None,
                    "freedom_status": parsed.get("freedom_status") or None,
                    "job_flags": parsed.get("job_flags"),
                    "last_source_event": source_event,
                    "last_message_id": message_id,
                    "last_received_at": received_at,
                },
            )

            seen_stop_ids = []
            for stop in parsed["stops"]:
                stop_id = stop["stop_id"].strip()
                seen_stop_ids.append(stop_id)
                cur.execute(
                    """
                    INSERT INTO public.freedom_stops (
                        stop_id, job_ref, drop_order, drop_type,
                        address_name, address_line_1, address_line_2,
                        postcode, country, country_code,
                        required_from, required_to, date_completed,
                        last_seen_at, updated_at
                    )
                    VALUES (
                        %(stop_id)s, %(job_ref)s, %(drop_order)s, %(drop_type)s,
                        %(address_name)s, %(address_line_1)s, %(address_line_2)s,
                        %(postcode)s, %(country)s, %(country_code)s,
                        %(required_from)s, %(required_to)s, %(date_completed)s,
                        %(last_seen_at)s, now()
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
                        last_seen_at = EXCLUDED.last_seen_at,
                        updated_at = now()
                    """,
                    {
                        "stop_id": stop_id,
                        "job_ref": job_ref,
                        "drop_order": stop.get("drop_order") or 0,
                        "drop_type": stop.get("drop_type") or None,
                        "address_name": stop.get("address_name") or None,
                        "address_line_1": stop.get("address_line_1") or None,
                        "address_line_2": stop.get("address_line_2") or None,
                        "postcode": stop.get("postcode") or None,
                        "country": stop.get("country") or None,
                        "country_code": stop.get("country_code") or None,
                        "required_from": stop.get("required_from_dt"),
                        "required_to": stop.get("required_to_dt"),
                        "date_completed": stop.get("date_completed_dt"),
                        "last_seen_at": received_at,
                    },
                )

            # V4 is a full job snapshot, so a stop absent from the newest snapshot
            # is no longer part of the current Freedom job.
            cur.execute(
                """
                DELETE FROM public.freedom_stops
                WHERE job_ref = %s
                  AND NOT (stop_id = ANY(%s))
                """,
                (job_ref, seen_stop_ids),
            )
            removed_stops = cur.rowcount

            cur.execute(
                """
                INSERT INTO public.freedom_events (
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
                    f"{len(seen_stop_ids)} stop(s) mirrored; {removed_stops} removed",
                    received_at,
                ),
            )
            event_id = cur.fetchone()["id"]
            conn.commit()

    return jsonify({
        "ok": True,
        "job_ref": job_ref,
        "event_id": event_id,
        "stops_mirrored": len(seen_stop_ids),
        "stops_removed": removed_stops,
        "special_job": bool(parsed.get("job_flags") and parsed["job_flags"][1:2] == "1"),
        "priority_job": bool(parsed.get("job_flags") and parsed["job_flags"][10:11] == "1"),
    })



def _plain_email_body(body):
    """Convert a mailbox body to readable plain text while retaining line breaks."""
    value = html.unescape(body or "")
    value = re.sub(r"(?i)<br\s*/?>", "\n", value)
    value = re.sub(r"(?i)</p\s*>", "\n", value)
    value = re.sub(r"(?i)<p(?:\s+[^>]*)?>", "", value)
    value = re.sub(r"<[^>]+>", "", value)
    value = value.replace("\r", "")
    lines = [line.rstrip() for line in value.split("\n")]
    # Collapse excessive blank lines without flattening intentional paragraphs.
    out = []
    blank = False
    for line in lines:
        is_blank = not line.strip()
        if is_blank and blank:
            continue
        out.append(line)
        blank = is_blank
    return "\n".join(out).strip()


def _clean_task_reply_body(body):
    """Keep the new reply text and discard the quoted previous message where possible."""
    text = _plain_email_body(body)
    lines = text.split("\n")
    kept = []
    for line in lines:
        stripped = line.strip()
        if re.match(r"^-{2,}\s*Original Message\s*-{2,}$", stripped, re.I):
            break
        if re.match(r"^From:\s+", stripped, re.I):
            # Outlook-style quoted message header. Only treat as a cut point once
            # we already have some reply content.
            if any(x.strip() for x in kept):
                break
        kept.append(line)
    return "\n".join(kept).strip()


def _warehouse_task_ref_from_subject(subject):
    match = re.search(r"\bWarehouse\s+Tasking\s+(\d+)\b", subject or "", re.I)
    return match.group(1) if match else None


def _ops_note_ref_from_subject(subject):
    match = re.search(r"\bOps\s+Note\s+(\d+)\b", subject or "", re.I)
    return match.group(1) if match else None


def _insert_job_note(cur, *, job_ref, source, source_message_id=None, sender=None, subject=None, note_text=None, raw_body=None, received_at=None):
    """Insert one source-neutral note against an existing Freedom job.

    Returns (note_id, duplicate). The unique source/message key makes mailbox or
    WhatsApp retries idempotent.
    """
    source = (source or "").strip().upper()
    if not source:
        raise ValueError("source is required")
    note_text = (note_text or "").strip()
    if not note_text:
        raise ValueError("note_text is required")

    if source_message_id:
        cur.execute(
            """
            SELECT id
            FROM public.job_notes
            WHERE source = %s AND source_message_id = %s
            """,
            (source, source_message_id),
        )
        duplicate = cur.fetchone()
        if duplicate:
            return duplicate["id"], True

    cur.execute(
        """
        INSERT INTO public.job_notes (
            job_ref, source, source_message_id, sender, subject,
            note_text, raw_body, received_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (job_ref, source, source_message_id, sender, subject, note_text, raw_body, received_at),
    )
    return cur.fetchone()["id"], False


@app.post("/job-note")
def job_note():
    """Generic note ingestion for any mirrored Freedom job.

    Intended for trusted integrations such as WhatsApp, manual tooling or future
    mailbox flows where the job reference is already known.
    """
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    job_ref = str(payload.get("job_ref") or "").strip()
    source = str(payload.get("source") or "MANUAL").strip().upper()
    source_message_id = payload.get("source_message_id") or payload.get("message_id")
    sender = (payload.get("sender") or payload.get("from") or "").strip() or None
    subject = (payload.get("subject") or "").strip() or None
    note_text = payload.get("note_text") or payload.get("body") or ""
    raw_body = payload.get("raw_body") or payload.get("body")
    received_at_raw = payload.get("received_at")

    if not job_ref:
        return jsonify({"ok": False, "error": "job_ref is required"}), 400
    if not str(note_text).strip():
        return jsonify({"ok": False, "error": "note_text is required"}), 400

    received_at = datetime.now(LONDON)
    if received_at_raw:
        try:
            received_at = datetime.fromisoformat(str(received_at_raw).replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"ok": False, "error": "invalid received_at"}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT job_ref FROM public.freedom_jobs WHERE job_ref = %s", (job_ref,))
            if not cur.fetchone():
                return jsonify({"ok": False, "error": "Freedom job not found", "job_ref": job_ref}), 404

            try:
                note_id, duplicate = _insert_job_note(
                    cur, job_ref=job_ref, source=source,
                    source_message_id=source_message_id, sender=sender, subject=subject,
                    note_text=note_text, raw_body=raw_body, received_at=received_at,
                )
            except ValueError as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400

            if not duplicate:
                conn.commit()

    return jsonify({
        "ok": True,
        "duplicate": duplicate,
        "job_ref": job_ref,
        "note_id": note_id,
        "source": source,
    })



def _safe_filename(value):
    name = os.path.basename((value or "attachment").strip()) or "attachment"
    name = re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip(" .")
    return name[:180] or "attachment"


def _decode_base64_content(value):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("content_base64 is required")
    if raw.startswith("data:") and "," in raw:
        raw = raw.split(",", 1)[1]

    try:
        data = base64.b64decode(raw, validate=True)
    except Exception as exc:
        raise ValueError("invalid base64 attachment content") from exc

    # Power Automate/Outlook can sometimes hand us contentBytes that are already
    # Base64, and wrapping those in base64() produces Base64-of-Base64. Detect
    # that representation and unwrap one extra layer. This keeps the endpoint
    # tolerant of both normal and double-encoded attachment payloads.
    try:
        candidate = data.strip()
        if (
            candidate
            and len(candidate) % 4 == 0
            and re.fullmatch(rb"[A-Za-z0-9+/=\r\n]+", candidate)
        ):
            nested = base64.b64decode(candidate, validate=True)
            known_magic = (
                b"\x89PNG\r\n\x1a\n",
                b"\xff\xd8\xff",          # JPEG
                b"%PDF-",                    # PDF
                b"GIF87a",
                b"GIF89a",
                b"PK\x03\x04",             # ZIP / DOCX / XLSX
            )
            looks_like_heif = len(nested) >= 12 and nested[4:8] == b"ftyp"
            if nested.startswith(known_magic) or looks_like_heif:
                data = nested
    except Exception:
        pass

    if not data:
        raise ValueError("attachment is empty")
    if len(data) > JOB_MEDIA_MAX_BYTES:
        raise ValueError(f"attachment exceeds {JOB_MEDIA_MAX_BYTES} byte limit")
    return data


def _storage_bucket():
    if not JOB_MEDIA_BUCKET:
        raise RuntimeError("JOB_MEDIA_BUCKET is not configured")
    return storage.Client().bucket(JOB_MEDIA_BUCKET)


def _insert_job_media(cur, *, job_ref, source, content_bytes, filename=None,
                      content_type=None, source_message_id=None, source_media_id=None,
                      sender=None, caption=None, received_at=None):
    """Store one media object in Cloud Storage and its generic job metadata row.

    Returns (media_id, duplicate). Retries are deduplicated by source_media_id when
    available, otherwise by source_message_id + content SHA256.
    """
    source = (source or "").strip().upper()
    if not source:
        raise ValueError("source is required")
    filename = _safe_filename(filename)
    content_type = (content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream").strip()
    digest = hashlib.sha256(content_bytes).hexdigest()

    if source_media_id:
        cur.execute(
            """
            SELECT id FROM public.job_media
            WHERE source = %s AND source_media_id = %s
            """,
            (source, str(source_media_id)),
        )
        row = cur.fetchone()
        if row:
            return row["id"], True

    if source_message_id:
        cur.execute(
            """
            SELECT id FROM public.job_media
            WHERE source = %s AND source_message_id = %s AND content_sha256 = %s
            """,
            (source, str(source_message_id), digest),
        )
        row = cur.fetchone()
        if row:
            return row["id"], True

    object_name = f"jobs/{job_ref}/{source.lower()}/{uuid.uuid4().hex}_{filename}"
    bucket = _storage_bucket()
    blob = bucket.blob(object_name)
    blob.upload_from_string(content_bytes, content_type=content_type)

    cur.execute(
        """
        INSERT INTO public.job_media (
            job_ref, source, source_message_id, source_media_id,
            sender, caption, original_filename, content_type, byte_size,
            content_sha256, storage_bucket, storage_object, received_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
        """,
        (
            job_ref, source, source_message_id, source_media_id,
            sender, caption, filename, content_type, len(content_bytes),
            digest, JOB_MEDIA_BUCKET, object_name, received_at,
        ),
    )
    return cur.fetchone()["id"], False


def _parse_received_at(value):
    if not value:
        return datetime.now(LONDON)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid received_at") from exc


@app.post("/job-media")
def job_media_upload():
    """Generic media ingestion for an existing Freedom job.

    JSON payload uses base64 file content. Suitable for Power Automate, OpsBot and
    other trusted internal integrations.
    """
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    job_ref = str(payload.get("job_ref") or "").strip()
    source = str(payload.get("source") or "MANUAL").strip().upper()
    source_message_id = payload.get("source_message_id") or payload.get("message_id")
    source_media_id = payload.get("source_media_id") or payload.get("attachment_id") or payload.get("media_id")
    sender = (payload.get("sender") or payload.get("from") or "").strip() or None
    caption = payload.get("caption") or None
    filename = payload.get("filename") or payload.get("name") or "attachment"
    content_type = payload.get("content_type") or payload.get("mime_type")

    if not job_ref:
        return jsonify({"ok": False, "error": "job_ref is required"}), 400

    try:
        received_at = _parse_received_at(payload.get("received_at"))
        content_bytes = _decode_base64_content(payload.get("content_base64") or payload.get("content"))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT job_ref FROM public.freedom_jobs WHERE job_ref = %s", (job_ref,))
            if not cur.fetchone():
                return jsonify({"ok": False, "error": "Freedom job not found", "job_ref": job_ref}), 404
            try:
                media_id, duplicate = _insert_job_media(
                    cur, job_ref=job_ref, source=source, content_bytes=content_bytes,
                    filename=filename, content_type=content_type,
                    source_message_id=source_message_id, source_media_id=source_media_id,
                    sender=sender, caption=caption, received_at=received_at,
                )
            except (ValueError, RuntimeError) as exc:
                return jsonify({"ok": False, "error": str(exc)}), 400
            if not duplicate:
                conn.commit()

    return jsonify({
        "ok": True, "duplicate": duplicate, "job_ref": job_ref,
        "media_id": media_id, "source": source,
    })


@app.post("/job-whatsapp")
def job_whatsapp():
    """Normalised OpsBot handoff: one WhatsApp message plus zero or more media files."""
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    job_ref = str(payload.get("job_ref") or "").strip()
    message_id = payload.get("message_id") or payload.get("source_message_id")
    sender = (payload.get("sender") or payload.get("from") or "").strip() or None
    note_text = str(payload.get("note_text") or payload.get("text") or "").strip()
    media_items = payload.get("media") or []

    if not job_ref:
        return jsonify({"ok": False, "error": "job_ref is required"}), 400
    if not note_text and not media_items:
        return jsonify({"ok": False, "error": "text or media is required"}), 400
    if not isinstance(media_items, list):
        return jsonify({"ok": False, "error": "media must be an array"}), 400

    try:
        received_at = _parse_received_at(payload.get("received_at"))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT job_ref FROM public.freedom_jobs WHERE job_ref = %s", (job_ref,))
            if not cur.fetchone():
                return jsonify({"ok": False, "error": "Freedom job not found", "job_ref": job_ref}), 404

            note_id = None
            note_duplicate = False
            if note_text:
                note_id, note_duplicate = _insert_job_note(
                    cur, job_ref=job_ref, source="WHATSAPP",
                    source_message_id=message_id, sender=sender,
                    note_text=note_text, raw_body=payload.get("raw_body") or note_text,
                    received_at=received_at,
                )

            media_results = []
            for item in media_items:
                if not isinstance(item, dict):
                    return jsonify({"ok": False, "error": "each media item must be an object"}), 400
                try:
                    content_bytes = _decode_base64_content(item.get("content_base64") or item.get("content"))
                    media_id, duplicate = _insert_job_media(
                        cur, job_ref=job_ref, source="WHATSAPP", content_bytes=content_bytes,
                        filename=item.get("filename") or item.get("name") or "whatsapp-media",
                        content_type=item.get("content_type") or item.get("mime_type"),
                        source_message_id=message_id,
                        source_media_id=item.get("media_id") or item.get("source_media_id"),
                        sender=sender, caption=item.get("caption"), received_at=received_at,
                    )
                except (ValueError, RuntimeError) as exc:
                    return jsonify({"ok": False, "error": str(exc)}), 400
                media_results.append({"media_id": media_id, "duplicate": duplicate})

            conn.commit()

    return jsonify({
        "ok": True, "job_ref": job_ref,
        "note_id": note_id, "note_duplicate": note_duplicate,
        "media": media_results,
    })


@app.get("/job/<job_ref>/media")
def job_media_list(job_ref):
    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, job_ref, source, source_message_id, source_media_id,
                       sender, caption, original_filename, content_type, byte_size,
                       received_at, created_at
                FROM public.job_media
                WHERE job_ref = %s
                ORDER BY COALESCE(received_at, created_at), id
                """,
                (job_ref,),
            )
            rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        row["url"] = f"/job-media/{row['id']}"
    return jsonify({"ok": True, "job_ref": job_ref, "count": len(rows), "media": rows})


@app.get("/job-media/<int:media_id>")
def job_media_download(media_id):
    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT storage_bucket, storage_object, original_filename, content_type
                FROM public.job_media WHERE id = %s
                """,
                (media_id,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"ok": False, "error": "media not found"}), 404

    bucket = storage.Client().bucket(row["storage_bucket"])
    data = bucket.blob(row["storage_object"]).download_as_bytes()
    headers = {"Content-Disposition": f'inline; filename="{_safe_filename(row["original_filename"])}"'}
    return Response(data, mimetype=row["content_type"] or "application/octet-stream", headers=headers)


@app.post("/warehouse-task-email")
def warehouse_task_email():
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    message_id = payload.get("message_id")
    subject = (payload.get("subject") or "").strip()
    sender = (payload.get("sender") or payload.get("from") or "").strip() or None
    body = payload.get("body") or ""
    received_at_raw = payload.get("received_at")

    job_ref = _warehouse_task_ref_from_subject(subject)
    if not job_ref:
        return jsonify({"ok": False, "error": "No Warehouse Tasking reference found in subject"}), 400

    instruction = _clean_task_reply_body(body)
    if not instruction:
        return jsonify({"ok": False, "error": "No instruction text found in message body"}), 400

    received_at = datetime.now(LONDON)
    if received_at_raw:
        try:
            received_at = datetime.fromisoformat(received_at_raw.replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"ok": False, "error": "invalid received_at"}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_ref, account_name, vehicle_description
                FROM public.freedom_jobs
                WHERE job_ref = %s
                """,
                (job_ref,),
            )
            job = cur.fetchone()
            if not job:
                return jsonify({"ok": False, "error": "Freedom job not found", "job_ref": job_ref}), 404

            note_id, duplicate = _insert_job_note(
                cur,
                job_ref=job_ref,
                source="EMAIL",
                source_message_id=message_id,
                sender=sender,
                subject=subject,
                note_text=instruction,
                raw_body=body,
                received_at=received_at,
            )

            attachment_results = []
            attachments = payload.get("attachments") or []
            if not isinstance(attachments, list):
                return jsonify({"ok": False, "error": "attachments must be an array"}), 400
            for item in attachments:
                if not isinstance(item, dict):
                    return jsonify({"ok": False, "error": "each attachment must be an object"}), 400
                try:
                    content_bytes = _decode_base64_content(item.get("content_base64") or item.get("content"))
                    media_id, media_duplicate = _insert_job_media(
                        cur, job_ref=job_ref, source="EMAIL", content_bytes=content_bytes,
                        filename=item.get("filename") or item.get("name") or "attachment",
                        content_type=item.get("content_type") or item.get("mime_type"),
                        source_message_id=message_id,
                        source_media_id=item.get("attachment_id") or item.get("source_media_id"),
                        sender=sender, caption=item.get("caption"), received_at=received_at,
                    )
                except (ValueError, RuntimeError) as exc:
                    return jsonify({"ok": False, "error": str(exc)}), 400
                attachment_results.append({"media_id": media_id, "duplicate": media_duplicate})

            conn.commit()

    return jsonify({
        "ok": True,
        "duplicate": duplicate,
        "job_ref": job_ref,
        "note_id": note_id,
        "source": "EMAIL",
        "instruction_chars": len(instruction),
        "attachments": attachment_results,
    })


@app.post("/ops-note-email")
def ops_note_email():
    """Attach an Ops Note email and any attachments to an existing Freedom job."""
    if INGEST_TOKEN and request.headers.get("X-Ingest-Token") != INGEST_TOKEN:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    message_id = payload.get("message_id")
    subject = (payload.get("subject") or "").strip()
    sender = (payload.get("sender") or payload.get("from") or "").strip() or None
    body = payload.get("body") or ""
    received_at_raw = payload.get("received_at")

    job_ref = _ops_note_ref_from_subject(subject)
    if not job_ref:
        return jsonify({"ok": False, "error": "No Ops Note reference found in subject"}), 400

    note_text = _clean_task_reply_body(body)
    if not note_text:
        return jsonify({"ok": False, "error": "No note text found in message body"}), 400

    received_at = datetime.now(LONDON)
    if received_at_raw:
        try:
            received_at = datetime.fromisoformat(str(received_at_raw).replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"ok": False, "error": "invalid received_at"}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT job_ref
                FROM public.freedom_jobs
                WHERE job_ref = %s
                """,
                (job_ref,),
            )
            if not cur.fetchone():
                return jsonify({
                    "ok": False,
                    "error": "Freedom job not found",
                    "job_ref": job_ref,
                }), 404

            note_id, duplicate = _insert_job_note(
                cur,
                job_ref=job_ref,
                source="EMAIL",
                source_message_id=message_id,
                sender=sender,
                subject=subject,
                note_text=note_text,
                raw_body=body,
                received_at=received_at,
            )

            attachment_results = []
            attachments = payload.get("attachments") or []
            if not isinstance(attachments, list):
                return jsonify({"ok": False, "error": "attachments must be an array"}), 400

            for item in attachments:
                if not isinstance(item, dict):
                    return jsonify({
                        "ok": False,
                        "error": "each attachment must be an object",
                    }), 400

                try:
                    content_bytes = _decode_base64_content(
                        item.get("content_base64") or item.get("content")
                    )
                    media_id, media_duplicate = _insert_job_media(
                        cur,
                        job_ref=job_ref,
                        source="EMAIL",
                        content_bytes=content_bytes,
                        filename=item.get("filename") or item.get("name") or "attachment",
                        content_type=item.get("content_type") or item.get("mime_type"),
                        source_message_id=message_id,
                        source_media_id=item.get("attachment_id") or item.get("source_media_id"),
                        sender=sender,
                        caption=item.get("caption"),
                        received_at=received_at,
                    )
                except (ValueError, RuntimeError) as exc:
                    return jsonify({"ok": False, "error": str(exc)}), 400

                attachment_results.append({
                    "media_id": media_id,
                    "duplicate": media_duplicate,
                })

            conn.commit()

    return jsonify({
        "ok": True,
        "duplicate": duplicate,
        "job_ref": job_ref,
        "note_id": note_id,
        "source": "EMAIL",
        "note_chars": len(note_text),
        "attachments": attachment_results,
    })


@app.get("/warehouse-tasks")
def warehouse_tasks_board():
    """Warehouse management board: allocation columns plus today's completions."""
    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM public.v_warehouse_task_board
                WHERE task_status <> 'CANCELLED'
                  AND (
                        task_status <> 'COMPLETE'
                        OR (completed_at AT TIME ZONE 'Europe/London')::date
                           = (now() AT TIME ZONE 'Europe/London')::date
                  )
                ORDER BY
                    CASE task_status
                        WHEN 'UNALLOCATED' THEN 0
                        WHEN 'IN_PROGRESS' THEN 1
                        WHEN 'ALLOCATED' THEN 2
                        WHEN 'COMPLETE' THEN 3
                        ELSE 9
                    END,
                    priority_job DESC,
                    queue_rank ASC NULLS LAST,
                    booked_at ASC NULLS LAST,
                    job_ref
                """
            )
            tasks = [dict(r) for r in cur.fetchall()]

    unallocated = [t for t in tasks if t["task_status"] == "UNALLOCATED"]
    allocated = [t for t in tasks if t["task_status"] != "UNALLOCATED"]

    worker_map = {}
    for t in allocated:
        key = t.get("allocation_code") or t.get("driver_callsign") or "UNKNOWN"
        label = t.get("driver_firstname") or t.get("allocation_name") or key
        if key not in worker_map:
            worker_map[key] = {"code": key, "label": label, "tasks": []}
        worker_map[key]["tasks"].append(t)

    workers = sorted(worker_map.values(), key=lambda w: (str(w["label"]).lower(), w["code"]))
    return render_template(
        "warehouse_tasks.html",
        unallocated=unallocated,
        workers=workers,
        total_tasks=len(tasks),
    )


@app.get("/warehouse-tasks/<allocation_code>")
def warehouse_operative_tasks(allocation_code):
    """Simple operative queue. Completed work disappears immediately."""
    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM public.v_warehouse_task_board
                WHERE task_status IN ('IN_PROGRESS', 'ALLOCATED')
                  AND allocation_code = %s
                ORDER BY
                    CASE WHEN task_status = 'IN_PROGRESS' THEN 0 ELSE 1 END,
                    started_at ASC NULLS LAST,
                    priority_job DESC,
                    queue_rank ASC NULLS LAST,
                    booked_at ASC NULLS LAST,
                    job_ref
                """,
                (allocation_code,),
            )
            tasks = [dict(r) for r in cur.fetchall()]

    worker_name = None
    if tasks:
        worker_name = tasks[0].get("driver_firstname") or tasks[0].get("allocation_name")
    return render_template(
        "warehouse_operative_tasks.html",
        tasks=tasks,
        allocation_code=allocation_code,
        worker_name=worker_name or allocation_code,
    )


@app.post("/api/warehouse-task-queue/reorder")
def warehouse_task_queue_reorder():
    """Persist management order for queued jobs within one allocation pile."""
    payload = request.get_json(silent=True) or {}
    allocation_code = str(payload.get("allocation_code") or "").strip()
    job_refs = payload.get("job_refs") or []

    if not allocation_code or not isinstance(job_refs, list):
        return jsonify({"ok": False, "error": "allocation_code and job_refs are required"}), 400

    job_refs = [str(x).strip() for x in job_refs if str(x).strip()]
    if len(job_refs) != len(set(job_refs)):
        return jsonify({"ok": False, "error": "duplicate job_ref in reorder request"}), 400

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            for rank, job_ref in enumerate(job_refs, start=1):
                cur.execute(
                    """
                    SELECT job_ref
                    FROM public.v_warehouse_task_board
                    WHERE job_ref = %s
                      AND allocation_code = %s
                      AND task_status = 'ALLOCATED'
                    """,
                    (job_ref, allocation_code),
                )
                if not cur.fetchone():
                    return jsonify({
                        "ok": False,
                        "error": f"{job_ref} is no longer a waiting job for {allocation_code}",
                    }), 409

                cur.execute(
                    """
                    INSERT INTO public.warehouse_queue_control
                        (job_ref, allocation_code, queue_rank, updated_at, updated_by)
                    VALUES (%s, %s, %s, now(), 'BOARD')
                    ON CONFLICT (job_ref) DO UPDATE SET
                        allocation_code = EXCLUDED.allocation_code,
                        queue_rank = EXCLUDED.queue_rank,
                        updated_at = now(),
                        updated_by = 'BOARD'
                    """,
                    (job_ref, allocation_code, rank * 10),
                )
            conn.commit()

    return jsonify({"ok": True, "allocation_code": allocation_code, "job_refs": job_refs})


@app.get("/api/warehouse-task/<job_ref>")
def warehouse_task_detail(job_ref):
    """Return one warehouse task plus all notes and media for the card modal."""
    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM public.v_warehouse_task_board WHERE job_ref = %s",
                (job_ref,),
            )
            task = cur.fetchone()
            if not task:
                return jsonify({"ok": False, "error": "warehouse task not found"}), 404

            cur.execute(
                """
                SELECT id, source, source_message_id, sender, subject, note_text,
                       received_at, created_at
                FROM public.job_notes
                WHERE job_ref = %s
                ORDER BY COALESCE(received_at, created_at), id
                """,
                (job_ref,),
            )
            notes = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT id, source, source_message_id, source_media_id, sender, caption,
                       original_filename, content_type, byte_size, received_at, created_at
                FROM public.job_media
                WHERE job_ref = %s
                ORDER BY COALESCE(received_at, created_at), id
                """,
                (job_ref,),
            )
            media = [dict(r) for r in cur.fetchall()]

    for item in media:
        item["url"] = f"/job-media/{item['id']}"

    return jsonify({
        "ok": True,
        "task": dict(task),
        "notes": notes,
        "media": media,
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


@app.get("/api/job-board/<job_ref>")
def job_board_detail(job_ref):
    """Common job-card detail: Freedom job + stops + Operations notes/media."""
    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    job_ref, driver_callsign, driver_firstname, driver_lastname,
                    account_name, agent_code, agent_name,
                    vehicle_code, vehicle_description, cancelled_at, booked_at,
                    goods, special_instructions, despatch_instructions,
                    freedom_status, job_flags
                FROM public.freedom_jobs
                WHERE job_ref = %s
                """,
                (job_ref,),
            )
            job = cur.fetchone()
            if not job:
                return jsonify({"ok": False, "error": "job not found"}), 404

            cur.execute(
                """
                SELECT stop_id, drop_order, drop_type, address_name,
                       address_line_1, address_line_2, postcode, country,
                       country_code, required_from, required_to, date_completed
                FROM public.freedom_stops
                WHERE job_ref = %s
                ORDER BY drop_order, stop_id
                """,
                (job_ref,),
            )
            stops = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT id, source, source_message_id, sender, subject,
                       note_text, received_at, created_at
                FROM public.job_notes
                WHERE job_ref = %s
                ORDER BY COALESCE(received_at, created_at), id
                """,
                (job_ref,),
            )
            notes = [dict(r) for r in cur.fetchall()]

            cur.execute(
                """
                SELECT id, source, source_message_id, source_media_id, sender,
                       caption, original_filename, content_type, byte_size,
                       received_at, created_at
                FROM public.job_media
                WHERE job_ref = %s
                ORDER BY COALESCE(received_at, created_at), id
                """,
                (job_ref,),
            )
            media = [dict(r) for r in cur.fetchall()]

    for item in media:
        item["url"] = f"/job-media/{item['id']}"

    return jsonify({
        "ok": True,
        "job": dict(job),
        "stops": stops,
        "notes": notes,
        "media": media,
    })


def _board_view_identifier():
    """Return a safely quoted schema/view identifier from JOB_BOARD_VIEW."""
    parts = JOB_BOARD_VIEW.split(".")
    if len(parts) == 1:
        parts = ["public", parts[0]]
    if len(parts) != 2 or not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p or "") for p in parts):
        raise RuntimeError("JOB_BOARD_VIEW must be a simple schema.view identifier")
    return sql.Identifier(parts[0]), sql.Identifier(parts[1])


@app.get("/board")
def board():
    """Generic live job board backed by a common database-view contract."""
    today = datetime.now(LONDON).date()
    raw_date = request.args.get("date")
    selected_date = date.fromisoformat(raw_date) if raw_date else today
    show_active_column = selected_date == today

    day_start = datetime.combine(selected_date, time.min).replace(tzinfo=LONDON)
    day_end = day_start + timedelta(days=1)
    lookahead_start = datetime.combine(today, time.min).replace(tzinfo=LONDON)
    lookahead_end = lookahead_start + timedelta(days=10)
    schema_ident, view_ident = _board_view_identifier()
    board_view = sql.SQL("{}.{}").format(schema_ident, view_ident)

    with db_connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            planning_query = sql.SQL("""
                SELECT *
                FROM {view}
                WHERE (booked_at AT TIME ZONE 'Europe/London')::date = %s
                  AND COALESCE(is_complete, FALSE) = FALSE
                  AND cancelled_at IS NULL
                ORDER BY booked_at, job_ref
            """).format(view=board_view)
            cur.execute(planning_query, (selected_date,))
            planning_rows = [dict(r) for r in cur.fetchall()]

            carry_rows = []
            if show_active_column:
                carry_query = sql.SQL("""
                    SELECT *
                    FROM {view}
                    WHERE booked_at < %s
                      AND COALESCE(is_complete, FALSE) = FALSE
                      AND cancelled_at IS NULL
                    ORDER BY booked_at, job_ref
                """).format(view=board_view)
                cur.execute(carry_query, (day_start,))
                carry_rows = [dict(r) for r in cur.fetchall()]

            job_refs = sorted({r["job_ref"] for r in planning_rows + carry_rows})
            stops_by_job = {}
            if job_refs:
                cur.execute(
                    """
                    SELECT stop_id, job_ref, drop_order, drop_type,
                           address_name, postcode, country, country_code,
                           required_from, required_to, date_completed
                    FROM public.freedom_stops
                    WHERE job_ref = ANY(%s)
                    ORDER BY job_ref, drop_order, stop_id
                    """,
                    (job_refs,),
                )
                for row in cur.fetchall():
                    stops_by_job.setdefault(row["job_ref"], []).append(dict(row))

            look_query = sql.SQL("""
                SELECT
                    (booked_at AT TIME ZONE 'Europe/London')::date AS booked_day,
                    COUNT(*) AS job_count
                FROM {view}
                WHERE booked_at >= %s
                  AND booked_at < %s
                  AND COALESCE(is_complete, FALSE) = FALSE
                  AND cancelled_at IS NULL
                GROUP BY 1
            """).format(view=board_view)
            cur.execute(look_query, (lookahead_start, lookahead_end))
            look_rows = [dict(r) for r in cur.fetchall()]

    now = datetime.now(LONDON)

    def decorate(row, planning=True):
        stops = stops_by_job.get(row["job_ref"], [])
        booked_at = row.get("booked_at")
        driver_callsign = row.get("driver_callsign") or row.get("agent_code")
        driver_name = " ".join(
            p for p in [row.get("driver_firstname"), row.get("driver_lastname")] if p
        ) or row.get("agent_name") or ""

        if not driver_callsign:
            status, cls = "UNALLOCATED", "unallocated"
        elif booked_at and booked_at <= now:
            status, cls = "ACTIVE", "active"
        else:
            status, cls = "FUTURE", "future"

        job = {
            "job_ref": row["job_ref"],
            "driver_callsign": driver_callsign,
            "driver_name": driver_name,
            "account": row.get("account_name"),
            "vehicle": row.get("vehicle_code"),
            "vehicle_description": row.get("vehicle_description"),
            "booked_at": booked_at.isoformat() if booked_at else None,
            "goods": row.get("goods"),
            "special_instructions": row.get("special_instructions"),
            "despatch_instructions": row.get("despatch_instructions"),
            "freedom_status": row.get("freedom_status"),
            "job_flags": row.get("job_flags"),
            "stops": [{
                "stop_id": s["stop_id"],
                "drop_order": s["drop_order"],
                "postcode": s["postcode"],
                "country": s["country"],
                "country_code": s["country_code"],
                "required_from": s["required_from"].isoformat() if s["required_from"] else None,
                "date_completed": s["date_completed"].isoformat() if s["date_completed"] else None,
            } for s in stops],
        }
        journey = operation_journey([job])

        anchor = booked_at or day_start
        result = {
            "operation_id": row["job_ref"],
            "job_ref": row["job_ref"],
            "status": status,
            "status_class": cls,
            "primary_account": row.get("account_name") or row["job_ref"],
            "vehicles": [row.get("vehicle_code")] if row.get("vehicle_code") else [],
            "jobs": [job],
            "job_count": 1,
            "journey_text": journey["journey_text"],
            "international": journey["international"],
            "country_codes": journey["country_codes"],
            "note_count": int(row.get("note_count") or 0),
            "media_count": int(row.get("media_count") or 0),
        }
        if planning:
            result["left_pct"] = max(0, min(100, ((anchor - day_start).total_seconds() / 86400) * 100))
        return result

    timeline_operations = [decorate(r, planning=True) for r in planning_rows]
    active_operations = [decorate(r, planning=False) for r in carry_rows]
    timeline_operations.sort(key=lambda x: (x["left_pct"], x["job_ref"]))
    active_operations.sort(key=lambda x: (x["status"] != "UNALLOCATED", x["job_ref"]))

    ticks = [{"label": f"{h:02d}:00", "left_pct": h / 24 * 100} for h in range(0, 25, 2)]
    counts = {today + timedelta(days=i): 0 for i in range(10)}
    for row in look_rows:
        if row["booked_day"] in counts:
            counts[row["booked_day"]] = row["job_count"]

    lookahead_days = [{
        "date": today + timedelta(days=i),
        "count": counts[today + timedelta(days=i)],
        "selected": (today + timedelta(days=i)) == selected_date,
        "today": i == 0,
    } for i in range(10)]

    return render_template(
        "board.html",
        board_title=JOB_BOARD_TITLE,
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
