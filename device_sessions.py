# Board-side DEVICE sessions: permanent credential stays at broker origin.
import hashlib,os,secrets
from flask import Blueprint,request,redirect,jsonify,make_response
from itsdangerous import URLSafeTimedSerializer,BadSignature,SignatureExpired
from board_auth import connect_auth,BOARD_CODE
COOKIE='__Host-board_device'
TTL=12*3600
bp=Blueprint('device_sessions',__name__)
def digest(v):return hashlib.sha256(v.encode()).hexdigest()
def serializer():
    secret=os.environ['BOARD_SESSION_SECRET']
    if len(secret)<32:raise RuntimeError('BOARD_SESSION_SECRET must be >=32 chars')
    return URLSafeTimedSerializer(secret,salt='board-device-origin-v1')
def identity():
    raw=request.cookies.get(COOKIE,'')
    if not raw:return None
    try:p=serializer().loads(raw,max_age=TTL)
    except (BadSignature,SignatureExpired):return None
    if not isinstance(p,dict) or p.get('kind')!='DEVICE' or not isinstance(p.get('id'),int):return None
    with connect_auth() as c:
        r=c.execute('''SELECT i.id,i.display_name FROM public.auth_identities i
          JOIN public.auth_device_enrolments e ON e.identity_id=i.id
          JOIN public.auth_board_access a ON a.identity_id=i.id
          JOIN public.auth_boards b ON b.id=a.board_id
          WHERE i.id=%s AND i.identity_type='DEVICE' AND i.is_active
          AND e.credential_expires_at>now() AND b.board_code=%s AND b.is_active''',
          (p['id'],BOARD_CODE)).fetchone()
        return dict(r) if r else None
@bp.get('/auth/device/callback')
def callback():
    ticket=request.args.get('ticket','')
    if not (35<=len(ticket)<=100):return jsonify(error='Invalid authentication ticket'),403
    audience=os.environ['DEVICE_SERVICE_CODE']
    with connect_auth() as c:
        row=c.execute('SELECT public.auth_device_consume_ticket(%s,%s,%s) AS id',
                      (digest(ticket),audience,BOARD_CODE)).fetchone()
        if not row or row['id'] is None:return jsonify(error='Expired or invalid ticket'),403
        c.commit()
    response=make_response(redirect(os.environ.get('DEVICE_LANDING_PATH','/board')))
    response.set_cookie(COOKIE,serializer().dumps({'kind':'DEVICE','id':row['id']}),secure=True,httponly=True,
                        samesite='Lax',max_age=TTL,path='/')
    return response
@bp.get('/auth/device/start')
def start():
    url=os.environ['DEVICE_BROKER_ORIGIN'].rstrip('/')
    audience=os.environ['DEVICE_SERVICE_CODE']
    return redirect(url+'/device/start/'+audience)
