"""Shared, fail-closed board authentication for Flask/Cloud Run.

Requires Firebase Authentication with Google provider, pre-authorised identities in
operations.public.auth_identities and OPERATIONS_MONITOR grant in auth_board_access.
"""
from __future__ import annotations

import os
import ipaddress
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import urlparse

import firebase_admin
from firebase_admin import auth as firebase_auth
import psycopg
from psycopg.rows import dict_row
from flask import Blueprint, abort, g, jsonify, make_response, redirect, render_template, request, url_for
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

BOARD_CODE = 'OPERATIONS_MONITOR'
COOKIE_NAME = '__Host-board_session'
SESSION_SECONDS = 8 * 60 * 60


def connect_auth():
    instance = os.environ['INSTANCE_CONNECTION_NAME']
    password = os.environ['AUTH_DB_PASSWORD']
    return psycopg.connect(
        host=f'/cloudsql/{instance}',
        dbname=os.getenv('AUTH_DB_NAME', 'operations'),
        user=os.environ['AUTH_DB_USER'],
        password=password,
        row_factory=dict_row,
        connect_timeout=8,
    )


def serializer():
    secret = os.environ.get('BOARD_SESSION_SECRET', '')
    if len(secret) < 32:
        raise RuntimeError('BOARD_SESSION_SECRET must be at least 32 characters')
    return URLSafeTimedSerializer(secret, salt='operations-monitor-board-session-v1')


def client_ip():
    """Use Cloud Run's last X-Forwarded-For entry when available.

    Google Front End appends the connecting client address. Earlier entries can
    be supplied by the caller and must not be trusted as the original client.
    This is indicative operational information, not a strong identity proof.
    """
    if os.environ.get('K_SERVICE'):
        forwarded = request.headers.get('X-Forwarded-For', '')
        if forwarded:
            candidate = forwarded.split(',')[-1].strip()
            try:
                return str(ipaddress.ip_address(candidate))
            except ValueError:
                pass
    try:
        return str(ipaddress.ip_address(request.remote_addr))
    except (ValueError, TypeError):
        return None


def audit(conn, event_type, identity_id=None, details=None):
    # Never record full URLs, query strings, cookies or authentication tokens.
    conn.execute('''INSERT INTO public.auth_audit
                    (identity_id, board_code, event_type, ip_address, details)
                    VALUES (%s,%s,%s,%s,%s::jsonb)''',
                 (identity_id, BOARD_CODE, event_type, client_ip(), __import__('json').dumps(details or {})))


def firebase_app():
    if firebase_admin._apps:
        return firebase_admin.get_app()
    return firebase_admin.initialize_app(options={'projectId': os.environ.get('FIREBASE_PROJECT_ID', 'warehouse-483921')})


def find_authorised(conn, uid):
    return conn.execute('''SELECT i.id, i.email, i.display_name
          FROM public.auth_identities i
          JOIN public.auth_board_access ba ON ba.identity_id=i.id
          JOIN public.auth_boards b ON b.id=ba.board_id
          WHERE i.identity_type='USER' AND i.is_active
            AND i.firebase_uid=%s AND b.board_code=%s AND b.is_active''', (uid, BOARD_CODE)).fetchone()


def require_origin():
    # Browser writes must carry a same-origin Origin header.
    origin = request.headers.get('Origin')
    if not origin:
        abort(403)
    u = urlparse(origin)
    if u.scheme != 'https' or u.netloc != request.host:
        abort(403)


def init_board_auth(app):
    bp = Blueprint('board_auth', __name__)

    @bp.get('/auth/login')
    def login():
        if getattr(g, 'board_user', None):
            return redirect(url_for('board'))
        config = {key: os.environ.get('FIREBASE_WEB_' + env, '') for key, env in [
            ('apiKey', 'API_KEY'), ('authDomain', 'AUTH_DOMAIN'),
            ('projectId', 'PROJECT_ID'), ('appId', 'APP_ID')]}
        if not all(config.values()):
            return 'Firebase web configuration is missing', 503
        return render_template('auth_login.html', firebase_config=config)

    @bp.post('/auth/session')
    def create_session():
        require_origin()
        raw = request.get_json(silent=True) or {}
        id_token = raw.get('idToken', '')
        if not isinstance(id_token, str) or len(id_token) > 10000 or not id_token:
            return jsonify(error='Missing or invalid token'), 401
        try:
            firebase_app()
            claims = firebase_auth.verify_id_token(id_token, check_revoked=True)
        except Exception:
            app.logger.warning('Firebase ID token verification failed', exc_info=False)
            return jsonify(error='Google authentication failed'), 401
        if (not claims.get('email_verified')
                or claims.get('firebase', {}).get('sign_in_provider') != 'google.com'):
            return jsonify(error='A verified Google sign-in is required'), 403
        uid = claims['uid']
        email = claims['email'].strip()
        try:
            with connect_auth() as conn:
                # Acquire an identity by UID first; a UID already assigned to an
                # identity must never be silently switched to another email.
                identity = conn.execute('''SELECT id, firebase_uid, email, is_active
                    FROM public.auth_identities
                    WHERE identity_type='USER' AND firebase_uid=%s FOR UPDATE''', (uid,)).fetchone()
                if identity is None:
                    identity = conn.execute('''SELECT id, firebase_uid, email, is_active
                        FROM public.auth_identities
                        WHERE identity_type='USER' AND lower(email)=lower(%s)
                        FOR UPDATE''', (email,)).fetchone()
                if not identity or not identity['is_active'] or identity['firebase_uid'] not in (None, uid):
                    audit(conn, 'ACCESS_DENIED', details={'reason': 'not_registered'})
                    return jsonify(error='This Google account is not authorised'), 403
                granted = conn.execute('''SELECT 1 FROM public.auth_board_access ba
                    JOIN public.auth_boards b ON b.id=ba.board_id
                    WHERE ba.identity_id=%s AND b.board_code=%s AND b.is_active''',
                    (identity['id'], BOARD_CODE)).fetchone()
                if not granted:
                    audit(conn, 'ACCESS_DENIED', identity['id'], {'reason': 'no_board_grant'})
                    return jsonify(error=f'{BOARD_CODE} permission not granted'), 403
                # Keep email as an administrative identifier; do not silently
                # change an approved email if the Google identity changes it.
                if identity['firebase_uid'] is None:
                    conn.execute('''UPDATE public.auth_identities SET firebase_uid=%s,
                         last_authenticated_at=now() WHERE id=%s AND firebase_uid IS NULL''', (uid, identity['id']))
                else:
                    conn.execute('''UPDATE public.auth_identities SET last_authenticated_at=now()
                         WHERE id=%s''', (identity['id'],))
                audit(conn, 'LOGIN_SUCCESS', identity['id'])
                conn.commit()
        except Exception:
            app.logger.exception('Authentication database unavailable')
            return jsonify(error='Authentication service unavailable'), 503
        token = serializer().dumps({'uid': uid, 'board_access_logged': False})
        response = make_response(jsonify(ok=True, redirect=url_for('board')))
        response.set_cookie(COOKIE_NAME, token, secure=True, httponly=True,
                            samesite='Lax', max_age=SESSION_SECONDS, path='/')
        return response

    @bp.post('/auth/logout')
    def logout():
        require_origin()
        response = make_response(jsonify(ok=True))
        response.delete_cookie(COOKIE_NAME, path='/', secure=True, httponly=True, samesite='Lax')
        return response

    app.register_blueprint(bp)

    @app.before_request
    def protect():
        g.board_user = None
        # Exact machine routes retain their ingest-token checks.
        machine_paths = {
            '/job-monitor-email', '/freedom-email', '/freedom-arrived',
            '/job-note', '/job-media', '/job-whatsapp',
            '/warehouse-task-email', '/ops-note-email',
        }
        if request.path in machine_paths and request.method == 'POST':
            return None
        if request.path == '/health' and request.method == 'GET':
            return None
        # The warehouse boards remain on their current access scheme during
        # the operations-monitor rollout. Their shared media route is secured
        # below; warehouse photo access therefore requires follow-up work.
        warehouse_get_paths = {'/warehouse-tasks'}
        warehouse_get_prefixes = ('/warehouse-tasks/', '/api/warehouse-task/')
        if request.method == 'GET' and (request.path in warehouse_get_paths or
            request.path.startswith(warehouse_get_prefixes)):
            return None
        if request.method == 'POST' and request.path == '/api/warehouse-task-queue/reorder':
            return None
        if request.path.startswith('/static/') or request.path in ('/auth/login', '/auth/session', '/auth/logout'):
            return None
        token = request.cookies.get(COOKIE_NAME)
        uid = None
        claims = {}
        if token:
            try:
                claims = serializer().loads(token, max_age=SESSION_SECONDS)
                if isinstance(claims, dict):
                    uid = claims.get('uid')
                else:
                    claims = {}
            except (BadSignature, SignatureExpired):
                pass
        if not isinstance(uid, str) or not uid:
            if request.path in ('/', '/board') and request.method == 'GET':
                return redirect(url_for('board_auth.login'))
            return jsonify(error='Authentication required'), 401
        if request.method not in ('GET', 'HEAD', 'OPTIONS'):
            require_origin()
        try:
            with connect_auth() as conn:
                user = find_authorised(conn, uid)
                if user:
                    g.board_user = dict(user)
                    # One access audit per browser authentication session,
                    # not on every automatic board refresh. Permission checks
                    # still happen on every request.
                    if (request.method == 'GET'
                            and request.path in ('/board',)
                            and claims.get('board_access_logged') is not True):
                        audit(conn, 'BOARD_ACCESS', user['id'])
                        g.mark_board_access_logged = True
                    conn.commit()
                else:
                    audit(conn, 'ACCESS_DENIED', details={'reason': 'permission_or_account_revoked'})
                    conn.commit()
        except Exception:
            app.logger.exception('Board permission check failed')
            return jsonify(error='Authentication service unavailable'), 503
        if not g.board_user:
            return jsonify(error='Permission denied'), 403
        return None

    @app.after_request
    def private_headers(response):
        # Upgrade the existing signed session cookie after the first board
        # visit. Subsequent 30-second refreshes will not re-audit the visit.
        if (getattr(g, 'mark_board_access_logged', False)
                and 200 <= response.status_code < 400):
            current = request.cookies.get(COOKIE_NAME)
            if current:
                try:
                    claims = serializer().loads(current, max_age=SESSION_SECONDS)
                    if isinstance(claims, dict) and claims.get('uid'):
                        claims['board_access_logged'] = True
                        response.set_cookie(
                            COOKIE_NAME, serializer().dumps(claims),
                            secure=True, httponly=True, samesite='Lax',
                            max_age=SESSION_SECONDS, path='/')
                except (BadSignature, SignatureExpired):
                    pass
        if request.path not in ('/static/',) and not request.path.startswith('/static/'):
            response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'same-origin'
        return response
