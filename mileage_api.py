"""Isolated viewing mileage endpoints. Register with register_mileage_api(app, db_connect).

All endpoints require X-Mileage-Api-Key.  Uses the existing psycopg connection helper.
GOOGLE_MAPS_API_KEY is required for calculation, not for agent maintenance/report retrieval.
"""
import csv
import io
import json
import os
import re
import hmac
from datetime import date, timedelta, datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

from flask import Blueprint, Response, jsonify, request
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

UK = ZoneInfo('Europe/London')
BLUEPRINT = Blueprint('viewing_mileage', __name__, url_prefix='/v1/viewing-mileage')
_DB_CONNECT = None
POSTCODE = re.compile(r'^(GIR\s?0AA|(?:[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}))$', re.I)
REF = re.compile(r'^[A-Za-z0-9_.-]{1,80}$')
OFFICE_POSTCODE = 'E1 6BD'


def register_mileage_api(app, db_connect):
    global _DB_CONNECT
    _DB_CONNECT = db_connect
    app.register_blueprint(BLUEPRINT)


def _conn():
    return _DB_CONNECT(row_factory=dict_row)


def _authorized():
    expected = os.environ.get('VIEWING_MILEAGE_API_KEY', '')
    supplied = request.headers.get('X-Mileage-Api-Key', '')
    return bool(expected) and hmac.compare_digest(expected, supplied)

@app.get("/v1/viewing-mileage/auth-diagnostic")
def mileage_auth_diagnostic():
    import os
    import hashlib
    from flask import request, jsonify

    expected = os.environ.get("VIEWING_MILEAGE_API_KEY", "")
    supplied = request.headers.get("X-Mileage-Api-Key", "")

    def fingerprint(value):
        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()[:12] if value else None

    return jsonify({
        "server_key_present": bool(expected),
        "server_key_length": len(expected),
        "server_key_fingerprint": fingerprint(expected),
        "header_present": bool(supplied),
        "header_length": len(supplied),
        "header_fingerprint": fingerprint(supplied)
    })


@BLUEPRINT.before_request
def check_key():
    if not _authorized():
        return jsonify({'error': 'unauthorized'}), 401


def _money(value):
    return str(Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def _decimal(value, max_value='10000'):
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d < 0 or d > Decimal(max_value):
            raise ValueError()
        return d
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError('rate must be a nonnegative finite number')


def _postcode(value):
    normalized = ' '.join((value or '').upper().split())
    if normalized and not POSTCODE.fullmatch(normalized):
        raise ValueError(f'invalid UK postcode: {value}')
    if normalized and ' ' not in normalized:
        normalized = normalized[:-3] + ' ' + normalized[-3:]
    return normalized


def _tsv(rows, columns):
    # Avoid delimiter/control-character injection; explicitly quote Excel-formula-like strings.
    out = io.StringIO(newline='')
    writer = csv.writer(out, delimiter='\t', lineterminator='\r\n')
    writer.writerow(columns)
    for row in rows:
        values = []
        for col in columns:
            val = row.get(col)
            s = '' if val is None else str(val).replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')
            if s.startswith(('=', '+', '-', '@')) and not (col in {'mileage_rate_override','stop_rate_override','mileage_rate','stop_rate','miles','mileage_payment','stop_payment','total_payment','viewings'}):
                s = "'" + s
            values.append(s)
        writer.writerow(values)
    return Response(out.getvalue(), mimetype='text/tab-separated-values')


@BLUEPRINT.get('/agents')
def agents():
    with _conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT mileage_rate,stop_rate FROM public.viewing_mileage_settings WHERE singleton=true')
        settings = cur.fetchone()
        cur.execute('''SELECT driver_reference,driver_firstname,driver_lastname,driver_callsign,
                           home_postcode,mileage_rate_override,stop_rate_override,is_active
                       FROM public.viewing_agents
                       ORDER BY driver_lastname NULLS LAST,driver_firstname NULLS LAST,driver_reference''')
        result = cur.fetchall()
    if request.args.get('format') == 'tsv':
        return _tsv(result, ['driver_reference','driver_firstname','driver_lastname','driver_callsign',
                             'home_postcode','mileage_rate_override','stop_rate_override','is_active'])
    return jsonify({'defaults': {k: str(v) for k, v in settings.items()}, 'agents': [dict(r) for r in result]})


@BLUEPRINT.get('/settings')
def settings():
    with _conn() as conn, conn.cursor() as cur:
        cur.execute('SELECT mileage_rate,stop_rate FROM public.viewing_mileage_settings WHERE singleton=true')
        r = cur.fetchone()
    if request.args.get('format') == 'tsv':
        return _tsv([r], ['mileage_rate', 'stop_rate'])
    return jsonify({k:str(v) for k,v in r.items()})


@BLUEPRINT.put('/settings')
def save_settings():
    payload = request.get_json(silent=True) or {}
    try:
        mileage = _decimal(payload['mileage_rate'], '50')
        stop = _decimal(payload['stop_rate'], '10000')
    except (KeyError, ValueError) as exc:
        return jsonify({'error': str(exc)}), 400
    with _conn() as conn, conn.cursor() as cur:
        cur.execute('''UPDATE public.viewing_mileage_settings
                       SET mileage_rate=%s,stop_rate=%s,updated_at=now()
                       WHERE singleton=true''', (mileage,stop))
    return jsonify({'ok':True})


@BLUEPRINT.put('/agents/<driver_reference>')
def save_agent(driver_reference):
    if not REF.fullmatch(driver_reference):
        return jsonify({'error':'invalid driver reference'}),400
    payload = request.get_json(silent=True) or {}
    try:
        home = _postcode(payload.get('home_postcode'))
        mileage = None if payload.get('mileage_rate_override') in ('',None) else _decimal(payload['mileage_rate_override'],'50')
        stop = None if payload.get('stop_rate_override') in ('',None) else _decimal(payload['stop_rate_override'],'10000')
    except ValueError as exc:
        return jsonify({'error': str(exc)}),400
    active = payload.get('is_active',True)
    if not isinstance(active,bool):
        return jsonify({'error':'is_active must be boolean'}),400
    with _conn() as conn, conn.cursor() as cur:
        cur.execute('''UPDATE public.viewing_agents
                       SET home_postcode=%s,mileage_rate_override=%s,stop_rate_override=%s,
                           is_active=%s,updated_at=now()
                       WHERE driver_reference=%s RETURNING driver_reference''',
                    (home or None,mileage,stop,active,driver_reference))
        if cur.fetchone() is None:
            return jsonify({'error':'unknown driver reference - await Freedom ingest'}),404
    return jsonify({'ok':True,'driver_reference':driver_reference})


def _date_range():
    end = date.fromisoformat(request.args.get('to') or datetime.now(UK).date().isoformat())
    start = date.fromisoformat(request.args.get('from') or (end-timedelta(days=6)).isoformat())
    if start > end or (end-start).days > 366:
        raise ValueError('invalid date range (maximum 367 days)')
    return start,end


@BLUEPRINT.get('/results')
def get_results():
    try:
        start,end = _date_range()
    except ValueError as exc:
        return jsonify({'error':str(exc)}),400
    with _conn() as conn, conn.cursor() as cur:
        cur.execute('''SELECT reporting_date,subject_key,status,result_data
                       FROM public.job_reporting_results
                       WHERE reporting_code=1 AND reporting_date BETWEEN %s AND %s
                       ORDER BY reporting_date DESC,subject_key''', (start,end))
        data=cur.fetchall()
    rows=[]
    for r in data:
        x=r['result_data']
        rows.append(dict(date=r['reporting_date'].isoformat(),driver_reference=x.get('driver_reference',''),
                         first_name=x.get('driver_firstname',''),last_name=x.get('driver_lastname',''),
                         job_refs=', '.join(x.get('job_refs',[])),viewings=x.get('viewing_count',0),
                         miles=x.get('total_miles',''),mileage_rate=x.get('mileage_rate',''),
                         mileage_payment=x.get('mileage_payment',''),stop_rate=x.get('stop_rate',''),
                         stop_payment=x.get('stop_payment',''),total_payment=x.get('total_payment',''),
                         status=r['status'],note=x.get('note','')))
    if request.args.get('format')=='tsv':
        return _tsv(rows,['date','driver_reference','first_name','last_name','job_refs','viewings','miles',
                          'mileage_rate','mileage_payment','stop_rate','stop_payment','total_payment','status','note'])
    return jsonify({'results': rows})


def _route_miles(origin,dest,api_key):
    url='https://routes.googleapis.com/directions/v2:computeRoutes'
    payload=json.dumps({'origin':{'address':origin+', UK'}, 'destination':{'address':dest+', UK'},
                        'travelMode':'DRIVE','routingPreference':'TRAFFIC_UNAWARE',
                        'units':'IMPERIAL'}).encode()
    req=Request(url,data=payload,method='POST',headers={
        'Content-Type':'application/json','X-Goog-Api-Key':api_key,
        'X-Goog-FieldMask':'routes.distanceMeters'})
    try:
        with urlopen(req,timeout=25) as response:
            data=json.load(response)
    except (HTTPError,URLError,TimeoutError) as exc:
        raise ValueError(f'routing provider error: {exc}') from exc
    routes=data.get('routes') or []
    if not routes or 'distanceMeters' not in routes[0]:
        raise ValueError('routing provider returned no driveable route')
    return Decimal(str(routes[0]['distanceMeters']))/Decimal('1609.344')


def _candidate_jobs(cur,day):
    # The day is defined by the scheduled start of the job in UK local time.
    cur.execute('''SELECT j.job_ref,j.driver_reference,j.driver_firstname,j.driver_lastname,
                          j.driver_callsign,j.booked_at,
                          s.drop_order,s.drop_type,s.postcode,s.required_from,
                          s.address_line_1,s.address_line_2
                   FROM public.freedom_jobs j
                   JOIN public.freedom_stops s ON s.job_ref=j.job_ref
                   WHERE j.account_ref='8' AND j.vehicle_code='VSP2'
                     AND j.cancelled_at IS NULL
                     AND (j.booked_at AT TIME ZONE 'Europe/London')::date=%s
                   ORDER BY j.booked_at,j.job_ref,s.drop_order''',(day,))
    return cur.fetchall()


def calculate_day(day,api_key):
    """Calculate all VSP2 agents on a date. Approved results are never overwritten."""
    with _conn() as conn, conn.cursor() as cur:
        raw=_candidate_jobs(cur,day)
        cur.execute('SELECT mileage_rate,stop_rate FROM public.viewing_mileage_settings WHERE singleton=true')
        default=cur.fetchone()
        cur.execute('SELECT * FROM public.viewing_agents')
        agent_map={r['driver_reference']:r for r in cur.fetchall()}
        cur.execute('''SELECT subject_key,status FROM public.job_reporting_results
                       WHERE reporting_code=1 AND reporting_date=%s''',(day,))
        approved={r['subject_key'] for r in cur.fetchall() if r['status']=='APPROVED'}
        # Select viewing stops only; a terminal office return is excluded.
        jobs={}
        for r in raw:
            jobs.setdefault(r['job_ref'],[]).append(r)
        grouped={}
        for jobref,stops in jobs.items():
            filtered=[s for i,s in enumerate(stops)
                      if not (i==len(stops)-1 and (s['postcode'] or '').replace(' ','').upper()==OFFICE_POSTCODE.replace(' ','')
                              and (s['drop_type'] or '').upper()=='YES')]
            if not filtered:
                continue
            ref=stops[0]['driver_reference']
            if not ref:
                ref='UNASSIGNED:'+jobref # Review exception, not a payable agent
            grouped.setdefault(ref,[]).append((stops[0],filtered))
        stats={'calculated':0,'review':0,'approved_skipped':0,'errors':[]}
        for ref,job_batches in grouped.items():
            if ref in approved:
                stats['approved_skipped']+=1
                continue
            agent=agent_map.get(ref)
            first,last=job_batches[0][0]['driver_firstname'],job_batches[0][0]['driver_lastname']
            ordered=sorted(job_batches,key=lambda b:(b[0]['booked_at'],b[0]['job_ref']))
            stops=[s for _, batch in ordered for s in batch]
            job_refs=[b[0]['job_ref'] for b in ordered]
            points=[(s['postcode'] or '').upper().strip() for s in stops]
            missing=[]
            if not agent: missing.append('agent record or driver reference missing')
            if agent and not agent.get('is_active'): missing.append('agent inactive')
            home=(agent or {}).get('home_postcode') or ''
            if not home:missing.append('home postcode missing')
            if not all(points):missing.append('viewing postcode missing')
            if not api_key:missing.append('Google Maps API key missing')
            mr=Decimal(str(agent['mileage_rate_override'] if agent and agent['mileage_rate_override'] is not None else default['mileage_rate']))
            sr=Decimal(str(agent['stop_rate_override'] if agent and agent['stop_rate_override'] is not None else default['stop_rate']))
            legs=[];total=Decimal(0)
            if not missing:
                route=[home]+points+[home]
                try:
                    for a,b in zip(route,route[1:]):
                        d=Decimal(0) if a.replace(' ','')==b.replace(' ','') else _route_miles(a,b,api_key)
                        total+=d
                        legs.append({'from':a,'to':b,'miles':str(d.quantize(Decimal('0.01'),rounding=ROUND_HALF_UP))})
                except ValueError as exc:
                    missing.append(str(exc))
            miles=total.quantize(Decimal('0.1'),rounding=ROUND_HALF_UP) if not missing else None
            mp=(miles*mr).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP) if miles is not None else None
            sp=(Decimal(len(points))*sr).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP)
            result={'driver_reference':ref,'driver_firstname':first,'driver_lastname':last,
                    'job_refs':job_refs,'viewing_count':len(points),'postcodes':points,
                    'home_postcode':home or None,'legs':legs,
                    'total_miles':str(miles) if miles is not None else None,
                    'mileage_rate':str(mr),'stop_rate':str(sr),
                    'mileage_payment':str(mp) if mp is not None else None,
                    'stop_payment':str(sp),'total_payment':str(mp+sp) if mp is not None else None,
                    'note':'; '.join(missing)}
            status='REVIEW' if missing else 'DRAFT'
            cur.execute('''INSERT INTO public.job_reporting_results
                            (reporting_code,result_scope,subject_key,reporting_date,result_data,status)
                           VALUES(1,'AGENT_DAY',%s,%s,%s,%s)
                           ON CONFLICT (reporting_code,subject_key,reporting_date)
                           DO UPDATE SET result_data=EXCLUDED.result_data,
                             status=EXCLUDED.status,calculated_at=now()
                           WHERE public.job_reporting_results.status <> 'APPROVED'
                           RETURNING id''',(ref,day,Jsonb(result),status))
            updated=cur.fetchone()
            if updated:
                rid=updated['id']
                cur.execute('DELETE FROM public.job_reporting_result_jobs WHERE result_id=%s',(rid,))
                for jobref in job_refs:
                    cur.execute('''INSERT INTO public.job_reporting_result_jobs(result_id,job_ref)
                                   VALUES(%s,%s) ON CONFLICT DO NOTHING''',(rid,jobref))
            stats['calculated']+=1
            if missing:
                stats['review']+=1
                stats['errors'].append({'driver_reference':ref,'details':missing})
        return stats


@BLUEPRINT.post('/calculate')
def run_calculation():
    payload=request.get_json(silent=True) or {}
    try:
        day=date.fromisoformat(str(payload['date']))
    except (KeyError, ValueError):
        return jsonify({'error':'date required as YYYY-MM-DD'}),400
    if day > datetime.now(UK).date()+timedelta(days=1):
        return jsonify({'error':'date too far in future'}),400
    try:
        result=calculate_day(day,os.getenv('GOOGLE_MAPS_API_KEY',''))
        return jsonify({'ok':True,'date':day.isoformat(),**result})
    except Exception:
        from flask import current_app
        current_app.logger.exception('mileage calculation failed')
        return jsonify({'error':'calculation failed; check service logs'}),500

@BLUEPRINT.post('/calculate/today')
def calculate_today():
    """Cloud Scheduler entry point; always uses the current Europe/London date."""
    from flask import current_app
    day = datetime.now(UK).date()
    try:
        result = calculate_day(day, os.getenv('GOOGLE_MAPS_API_KEY', ''))
        return jsonify({'ok': True, 'date': day.isoformat(), **result})
    except Exception:
        current_app.logger.exception('scheduled mileage calculation failed')
        return jsonify({'error': 'scheduled calculation failed'}), 500
