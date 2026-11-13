# -*- coding: utf-8 -*-
"""
SISTEM REKOD SURAT DOKUMEN KELUAR - PEJABAT REKTOR UiTM CAWANGAN SABAH 2026
Flask + Supabase (PostgreSQL + Storage) untuk Render
PIN pengambilan dijana semasa surat direkodkan (disimpan sebagai hash).
"""
import base64
import hashlib
import hmac
import io
import os
import re
import secrets
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from functools import wraps

import psycopg2
import psycopg2.errors
import requests
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from PIL import Image, ImageOps, UnidentifiedImageError

from flask import (Flask, Response, abort, flash, g, redirect, render_template,
                   request, send_file, session, url_for)
from jinja2 import DictLoader
from markupsafe import Markup
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

# ----------------------------------------------------------------------
# KONFIGURASI
# ----------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:
    pass

TZ = timezone(timedelta(hours=8))  # Malaysia (tiada DST)

BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
PROD = os.environ.get("RENDER") == "true" or BASE_URL.startswith("https")
DEBUG = os.environ.get("FLASK_DEBUG", "0") == "1" and not PROD
SEED_DEMO = os.environ.get("SEED_DEMO", "0") == "1"
PROXY_HOPS = int(os.environ.get("PROXY_HOPS", "1" if PROD else "0"))
ALLOW_INDEX = os.environ.get("ALLOW_INDEX", "0") == "1"   # benarkan Google index halaman utama

DATABASE_URL = os.environ.get("DATABASE_URL", "")
DB_SSLMODE = os.environ.get("DB_SSLMODE", "require")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "surat-gambar")
STORAGE_ON = bool(SUPABASE_URL and SUPABASE_KEY)

try:
    PIN_PANJANG = min(8, max(4, int(os.environ.get("PIN_PANJANG", "4"))))
except ValueError:
    PIN_PANJANG = 4


def _fatal(msg):
    if PROD:
        raise RuntimeError(msg)
    print("⚠️  AMARAN:", msg, file=sys.stderr)


SECRET_KEY = os.environ.get("SECRET_KEY", "")
if not SECRET_KEY or len(SECRET_KEY) < 32:
    _fatal("SECRET_KEY mesti ditetapkan (min 32 aksara).")
    SECRET_KEY = secrets.token_hex(32)  # sementara (dev sahaja)

_hash = os.environ.get("ADMIN_PASSWORD_HASH", "")
_plain = os.environ.get("ADMIN_PASSWORD", "")
if _hash:
    ADMIN_PASSWORD_HASH = _hash
elif _plain:
    if PROD and len(_plain) < 12:
        raise RuntimeError("ADMIN_PASSWORD mesti sekurang-kurangnya 12 aksara.")
    ADMIN_PASSWORD_HASH = generate_password_hash(_plain)
else:
    _fatal("Tetapkan ADMIN_PASSWORD_HASH (atau ADMIN_PASSWORD).")
    ADMIN_PASSWORD_HASH = generate_password_hash("admin123")  # dev sahaja

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL belum ditetapkan (sambungan Supabase).")
if PROD and not STORAGE_ON:
    print("ℹ️  SUPABASE_URL/SUPABASE_SERVICE_KEY tiada: ciri muat naik gambar dimatikan.",
          file=sys.stderr)

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config.update(
    SESSION_COOKIE_NAME="__Host-session" if PROD else "session",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=PROD,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=2),
    MAX_CONTENT_LENGTH=6 * 1024 * 1024,   # 6 MB (gambar)
    MAX_FORM_MEMORY_SIZE=600_000,
)
if PROXY_HOPS:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=PROXY_HOPS, x_proto=1, x_host=1)

IDLE_SAAT = 30 * 60
MAX_GAMBAR = 6 * 1024 * 1024
Image.MAX_IMAGE_PIXELS = 25_000_000

STATUS_LIST = ["Direkod", "Tindakan", "Surat Keluar", "Selesai"]
STATUS_META = {
    "Direkod":      {"emoji": "🟠", "bg": "#fd7e14", "fg": "#fff"},
    "Tindakan":     {"emoji": "🟡", "bg": "#ffc107", "fg": "#000"},
    "Surat Keluar": {"emoji": "🔵", "bg": "#0d6efd", "fg": "#fff"},
    "Selesai":      {"emoji": "🟢", "bg": "#198754", "fg": "#fff"},
}

SIG_RE = re.compile(r"data:image/png;base64,[A-Za-z0-9+/=]+")
ID_RE = re.compile(r"[A-Z0-9][A-Z0-9/.\-]{2,29}")

ADA_LOGO = os.path.exists(os.path.join(BASE_DIR, "static", "img", "logo.png"))
ADA_BANNER = os.path.exists(os.path.join(BASE_DIR, "static", "img", "banner.jpg"))


def now():
    return datetime.now(TZ)


def klien_ip():
    return request.remote_addr or "?"


# ----------------------------------------------------------------------
# PANGKALAN DATA (Supabase PostgreSQL)
# ----------------------------------------------------------------------
POOL = ThreadedConnectionPool(
    1, 4, DATABASE_URL, cursor_factory=RealDictCursor,
    connect_timeout=10, sslmode=DB_SSLMODE)


def get_db():
    if "db" not in g:
        conn = None
        for _ in range(2):
            conn = POOL.getconn()
            try:
                with conn.cursor() as c:
                    c.execute("SELECT 1")
                conn.rollback()
                break
            except psycopg2.Error:          # sambungan putus -> buang & cuba lagi
                POOL.putconn(conn, close=True)
                conn = None
        if conn is None:
            abort(503)
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    conn = g.pop("db", None)
    if conn is not None:
        try:
            conn.rollback()
        except Exception:
            pass
        POOL.putconn(conn)


def tanya(sql, args=(), one=False):
    with get_db().cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchone() if one else cur.fetchall()


def laksana(sql, args=()):
    with get_db().cursor() as cur:
        cur.execute(sql, args)
        return cur.rowcount


def init_db():
    conn = POOL.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(2026001)")  # elak race antara worker
            cur.execute("""
                CREATE TABLE IF NOT EXISTS surat (
                    id BIGSERIAL PRIMARY KEY,
                    no_siri TEXT NOT NULL UNIQUE,
                    no_rujukan TEXT NOT NULL,
                    tarikh_surat DATE NOT NULL,
                    perkara TEXT NOT NULL,
                    nama TEXT NOT NULL,
                    no_id TEXT NOT NULL,
                    jenis TEXT NOT NULL DEFAULT 'Staf'
                        CHECK (jenis IN ('Staf','Pelajar')),
                    status TEXT NOT NULL DEFAULT 'Direkod'
                        CHECK (status IN ('Direkod','Tindakan','Surat Keluar','Selesai')),
                    tarikh_kemaskini TIMESTAMPTZ NOT NULL DEFAULT now(),
                    tarikh_ambil TIMESTAMPTZ,
                    tandatangan TEXT,
                    disahkan_oleh TEXT,
                    gambar TEXT,
                    pin_hash TEXT,
                    token TEXT NOT NULL UNIQUE
                )""")
            # Migrasi (data lama kekal)
            cur.execute("ALTER TABLE surat ADD COLUMN IF NOT EXISTS gambar TEXT")
            cur.execute("ALTER TABLE surat ADD COLUMN IF NOT EXISTS pin_hash TEXT")
            # Buang lajur PIN teks-biasa daripada versi lama (jika ada)
            cur.execute("ALTER TABLE surat DROP COLUMN IF EXISTS pin, "
                        "DROP COLUMN IF EXISTS pin_tamat")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_surat_noid ON surat(no_id)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS had_cubaan (
                    id BIGSERIAL PRIMARY KEY,
                    kunci TEXT NOT NULL,
                    masa TIMESTAMPTZ NOT NULL DEFAULT now()
                )""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_had ON had_cubaan(kunci, masa DESC)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS audit (
                    id BIGSERIAL PRIMARY KEY,
                    masa TIMESTAMPTZ NOT NULL DEFAULT now(),
                    tindakan TEXT NOT NULL,
                    butiran TEXT,
                    ip TEXT
                )""")
            # Halang akses melalui API awam Supabase (anon key).
            for t in ("surat", "had_cubaan", "audit"):
                cur.execute(f"ALTER TABLE {t} ENABLE ROW LEVEL SECURITY")

            if SEED_DEMO:
                cur.execute("SELECT COUNT(*) AS n FROM surat")
                if cur.fetchone()["n"] == 0:
                    cur.execute("""INSERT INTO surat
                        (no_siri,no_rujukan,tarikh_surat,perkara,nama,no_id,jenis,status,token)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                ("5454", "100-UITMCSH (KAB/100/01)", "2026-10-28",
                                 "Permohonan Peruntukan Keraian...", "Melisa Cherrie Mariot",
                                 "STAF001", "Staf", "Surat Keluar", secrets.token_urlsafe(12)))
    finally:
        POOL.putconn(conn)


# ----------------------------------------------------------------------
# HAD CUBAAN (dikongsi antara semua worker, disimpan dalam DB)
# ----------------------------------------------------------------------
def terkunci(kunci, maks, saat):
    """Pulangkan baki minit jika sudah capai had, 0 jika belum."""
    rows = tanya("""SELECT EXTRACT(EPOCH FROM (now() - masa)) AS umur
                    FROM had_cubaan
                    WHERE kunci=%s AND masa > now() - (%s * interval '1 second')
                    ORDER BY masa DESC LIMIT %s""", (kunci, saat, maks))
    if len(rows) >= maks:
        return int((saat - float(rows[-1]["umur"])) // 60) + 1
    return 0


def catat(kunci):
    laksana("INSERT INTO had_cubaan (kunci) VALUES (%s)", (kunci,))
    if secrets.randbelow(100) == 0:   # pembersihan berkala
        laksana("DELETE FROM had_cubaan WHERE masa < now() - interval '1 day'")
    get_db().commit()


def kosongkan(kunci):
    laksana("DELETE FROM had_cubaan WHERE kunci=%s", (kunci,))
    get_db().commit()


def audit(tindakan, butiran=""):
    laksana("INSERT INTO audit (tindakan, butiran, ip) VALUES (%s,%s,%s)",
            (tindakan, (butiran or "")[:500], klien_ip()))
    get_db().commit()


# ----------------------------------------------------------------------
# PIN PENGAMBILAN (dijana semasa surat direkod, disimpan sebagai hash)
# ----------------------------------------------------------------------
def jana_pin():
    return f"{secrets.randbelow(10 ** PIN_PANJANG):0{PIN_PANJANG}d}"


def hash_pin(sid, pin):
    """HMAC dengan SECRET_KEY + id surat (PIN sama pada dua surat -> hash berbeza)."""
    return hmac.new(SECRET_KEY.encode(), f"pin:{sid}:{pin}".encode(),
                    hashlib.sha256).hexdigest()


# ----------------------------------------------------------------------
# SUPABASE STORAGE (bucket peribadi, diakses melalui server sahaja)
# ----------------------------------------------------------------------
class StorageError(Exception):
    pass


def _hdr(extra=None):
    h = {"apikey": SUPABASE_KEY}
    if SUPABASE_KEY.startswith("eyJ"):          # kunci service_role (JWT lama)
        h["Authorization"] = f"Bearer {SUPABASE_KEY}"
    h.update(extra or {})
    return h


def _obj(nama, auth=False):
    mid = "object/authenticated" if auth else "object"
    return f"{SUPABASE_URL}/storage/v1/{mid}/{SUPABASE_BUCKET}/{nama}"


def storage_ensure_bucket():
    if not STORAGE_ON:
        return
    try:
        requests.post(f"{SUPABASE_URL}/storage/v1/bucket", headers=_hdr(),
                      json={"id": SUPABASE_BUCKET, "name": SUPABASE_BUCKET,
                            "public": False, "file_size_limit": MAX_GAMBAR,
                            "allowed_mime_types": ["image/jpeg"]}, timeout=8)
    except requests.RequestException:
        pass   # bucket mungkin sudah wujud / dibuat manual


def storage_upload(nama, data):
    try:
        r = requests.post(_obj(nama), data=data, timeout=25,
                          headers=_hdr({"Content-Type": "image/jpeg", "x-upsert": "false"}))
        r.raise_for_status()
    except requests.RequestException as e:
        app.logger.error("Storage upload gagal: %s", e)
        raise StorageError() from e


def storage_get(nama):
    try:
        r = requests.get(_obj(nama, auth=True), headers=_hdr(), timeout=20)
        r.raise_for_status()
        return r.content
    except requests.RequestException as e:
        app.logger.error("Storage get gagal: %s", e)
        raise StorageError() from e


def storage_delete_senyap(nama):
    if not (STORAGE_ON and nama):
        return
    try:
        requests.delete(_obj(nama), headers=_hdr(), timeout=10)
    except requests.RequestException:
        pass


def proses_gambar(fail):
    """Sahkan & bersihkan gambar. Pulangkan bytes JPEG atau None. ValueError jika tak sah."""
    if not fail or not fail.filename:
        return None
    ext = fail.filename.rsplit(".", 1)[-1].lower() if "." in fail.filename else ""
    if ext not in ("jpg", "jpeg", "png", "webp"):
        raise ValueError("Gambar mesti berformat JPG, PNG atau WEBP.")
    data = fail.read(MAX_GAMBAR + 1)
    if len(data) > MAX_GAMBAR:
        raise ValueError("Saiz gambar melebihi 6 MB.")
    try:
        img = Image.open(io.BytesIO(data))
        if img.format not in ("JPEG", "PNG", "WEBP"):
            raise ValueError("Kandungan fail bukan gambar yang dibenarkan.")
        if img.width * img.height > 25_000_000:
            raise ValueError("Resolusi gambar terlalu besar.")
        img.load()
        img = ImageOps.exif_transpose(img)
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError):
        raise ValueError("Fail gambar rosak atau tidak sah.")
    if img.mode in ("RGBA", "LA", "P"):
        img = img.convert("RGBA")
        bg = Image.new("RGB", img.size, "white")
        bg.paste(img, mask=img.split()[-1])
        img = bg
    else:
        img = img.convert("RGB")
    img.thumbnail((1600, 1600))
    out = io.BytesIO()
    img.save(out, "JPEG", quality=82, optimize=True)   # EXIF/metadata terbuang
    return out.getvalue()


def sah_tandatangan(s):
    if not s or len(s) > 200_000 or not SIG_RE.fullmatch(s):
        return None
    try:
        raw = base64.b64decode(s.split(",", 1)[1], validate=True)
        img = Image.open(io.BytesIO(raw))
        fmt, (w, h) = img.format, img.size
        img.verify()
        if fmt != "PNG" or w * h > 4_000_000:
            return None
    except Exception:
        return None
    return s


# ----------------------------------------------------------------------
# KESELAMATAN: CSRF, HEADER, SESI ADMIN
# ----------------------------------------------------------------------
def csrf_token():
    if "_csrf" not in session:
        session["_csrf"] = secrets.token_hex(16)
    return session["_csrf"]


@app.before_request
def sebelum():
    g.nonce = secrets.token_urlsafe(16)
    if request.method == "POST":
        sah = session.get("_csrf", "")
        hantar = request.form.get("_csrf", "")
        if not sah or not secrets.compare_digest(hantar.encode(), sah.encode()):
            abort(400, "Token keselamatan tidak sah atau sesi tamat. Sila muat semula halaman.")


@app.after_request
def header_keselamatan(resp):
    n = g.get("nonce", "")
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        f"script-src 'self' 'nonce-{n}' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; font-src 'self' https://cdn.jsdelivr.net; "
        "connect-src 'self'; object-src 'none'; base-uri 'self'; "
        "form-action 'self'; frame-ancestors 'none'")
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "same-origin"
    resp.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
    if PROD:
        resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    if (request.path.startswith(("/admin", "/sahkan"))
            or (request.path == "/" and request.args.get("no_id"))):
        resp.headers["Cache-Control"] = "no-store"
    return resp


def admin_required(f):
    @wraps(f)
    def wrapper(*a, **kw):
        if not session.get("admin"):
            return redirect(url_for("admin_login", next=request.path))
        if time.time() - session.get("last", 0) > IDLE_SAAT:
            session.clear()
            flash("Sesi tamat kerana tidak aktif. Sila log masuk semula.", "warning")
            return redirect(url_for("admin_login", next=request.path))
        session["last"] = time.time()
        return f(*a, **kw)
    return wrapper


def next_selamat(n):
    if n and n.startswith("/") and not n.startswith("//") and "\\" not in n:
        return n
    return url_for("admin")


MESEJ = {
    400: "Permintaan tidak sah.", 403: "Akses ditolak.", 404: "Halaman tidak dijumpai.",
    413: "Fail terlalu besar (maksimum 6 MB).", 429: "Terlalu banyak permintaan.",
    500: "Ralat pelayan. Sila cuba lagi.", 501: "Ciri ini belum dipasang.",
    502: "Gagal mendapatkan gambar dari storan.",
    503: "Perkhidmatan tidak tersedia seketika.",
}


@app.errorhandler(HTTPException)
def ralat(e):
    kod = e.code or 500
    mesej = e.description if kod == 400 else MESEJ.get(kod, e.name)
    return render_template("ralat.html", kod=kod, mesej=mesej), kod


# ----------------------------------------------------------------------
# JINJA HELPERS
# ----------------------------------------------------------------------
def badge(status):
    m = STATUS_META.get(status, STATUS_META["Direkod"])
    return Markup(
        '<span class="badge rounded-pill px-3 py-2" style="background:{};color:{}">{} {}</span>'
    ).format(m["bg"], m["fg"], m["emoji"], status)


def tgl(v):
    if not v:
        return "-"
    if isinstance(v, datetime):
        v = v.astimezone(TZ)
    try:
        return v.strftime("%d/%m/%Y")
    except AttributeError:
        return str(v)


def tgl_masa(v):
    if not v:
        return "-"
    try:
        return v.astimezone(TZ).strftime("%d/%m/%Y %I:%M %p")
    except AttributeError:
        return str(v)


def topeng(nama):
    p = (nama or "").split()
    if not p:
        return "-"
    return " ".join([p[0]] + [x[0] + "***" for x in p[1:]])


app.jinja_env.globals.update(
    badge=badge, csrf_token=csrf_token, STATUS_LIST=STATUS_LIST,
    STATUS_META=STATUS_META, nonce=lambda: g.get("nonce", ""),
    ADA_LOGO=ADA_LOGO, ADA_BANNER=ADA_BANNER, STORAGE_ON=STORAGE_ON,
    PIN_PANJANG=PIN_PANJANG, ALLOW_INDEX=ALLOW_INDEX)
app.jinja_env.filters.update(tgl=tgl, tgl_masa=tgl_masa, topeng=topeng)


# ----------------------------------------------------------------------
# LALUAN AWAM
# ----------------------------------------------------------------------
@app.route("/healthz")
def healthz():
    return "ok"


@app.route("/robots.txt")
def robots():
    if ALLOW_INDEX:
        txt = "User-agent: *\nDisallow: /admin\nDisallow: /sahkan\nAllow: /\n"
    else:
        txt = "User-agent: *\nDisallow: /\n"
    return Response(txt, mimetype="text/plain")


@app.route("/qr.png")
def qr_laman():
    """QR ke halaman utama tapak (alamat dikesan automatik, tidak perlu BASE_URL)."""
    try:
        import qrcode
    except ImportError:
        abort(501)
    buf = io.BytesIO()
    qrcode.make(url_for("index", _external=True)).save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png", max_age=86400)


@app.route("/")
def index():
    no_id = request.args.get("no_id", "").strip().upper()
    senarai = None
    if no_id:
        if not ID_RE.fullmatch(no_id):
            flash("Format No. Pelajar/Staf tidak sah.", "warning")
        else:
            kunci = f"cari:{klien_ip()}"
            if terkunci(kunci, 30, 300):
                flash("Terlalu banyak carian. Sila cuba sebentar lagi.", "danger")
                return render_template("index.html", no_id=no_id, senarai=None), 429
            catat(kunci)
            senarai = tanya("""SELECT no_siri, no_rujukan, tarikh_surat, perkara, nama,
                                      jenis, status, tarikh_kemaskini, tarikh_ambil
                               FROM surat WHERE no_id=%s ORDER BY id DESC LIMIT 50""",
                            (no_id,))
    return render_template("index.html", no_id=no_id, senarai=senarai)


@app.route("/sahkan/<token>")
def sahkan_lama(token):
    """Keserasian QR lama -> halaman pengesahan baharu."""
    return redirect(url_for("sahkan"))


@app.route("/sahkan", methods=["GET", "POST"])
def sahkan():
    """Penerima masukkan No. ID + PIN (yang diberi semasa menyerahkan surat)."""
    if request.method == "GET":
        no_id = request.args.get("no_id", "").strip().upper()
        return render_template("sahkan.html", no_id=no_id if ID_RE.fullmatch(no_id) else "")

    no_id = request.form.get("no_id", "").strip().upper()
    pin = re.sub(r"\D", "", request.form.get("pin", ""))
    if not ID_RE.fullmatch(no_id) or len(pin) != PIN_PANJANG:
        flash(f"Sila masukkan No. Pelajar/Staf yang sah dan PIN {PIN_PANJANG} digit.", "warning")
        return render_template("sahkan.html", no_id=no_id if ID_RE.fullmatch(no_id) else "")

    k_ip = f"sahkan-ip:{klien_ip()}"
    k_id = f"sahkan-id:{no_id}"
    baki = terkunci(k_ip, 10, 900) or terkunci(k_id, 5, 1800)
    if baki:
        flash(f"Terlalu banyak cubaan salah. Cuba lagi dalam {baki} minit, "
              "atau hubungi pejabat.", "danger")
        return render_template("sahkan.html", no_id=no_id), 429

    rows = tanya("""SELECT id, no_siri, perkara, pin_hash FROM surat
                    WHERE no_id=%s AND status='Surat Keluar' AND pin_hash IS NOT NULL
                    LIMIT 50""", (no_id,))
    padan = None
    for r in rows:   # tiada 'break' supaya masa semakan lebih seragam
        ok = hmac.compare_digest(hash_pin(r["id"], pin), r["pin_hash"])
        if ok and padan is None:
            padan = r

    if padan is None:
        catat(k_ip)
        catat(k_id)
        # Mesej sama sama ada ID wujud atau tidak (elak enumerasi)
        flash("No. ID atau PIN tidak betul, atau tiada surat yang sedia diambil.", "danger")
        return render_template("sahkan.html", no_id=no_id)

    sig = sah_tandatangan(request.form.get("tandatangan", ""))
    # Atomik: sekali guna sahaja
    n = laksana("""UPDATE surat SET status='Selesai', tarikh_kemaskini=now(),
                   tarikh_ambil=now(), tandatangan=%s, disahkan_oleh='Penerima (PIN)',
                   pin_hash=NULL
                   WHERE id=%s AND status='Surat Keluar' AND pin_hash=%s""",
                (sig, padan["id"], padan["pin_hash"]))
    get_db().commit()
    if not n:
        flash("Pengesahan tidak dapat diselesaikan. Sila cuba semula.", "warning")
        return render_template("sahkan.html", no_id=no_id)

    kosongkan(k_id)
    audit("pengesahan_penerima", f"No. Siri {padan['no_siri']} (PIN)")
    return render_template("selesai.html", s=padan, masa=now())


# ----------------------------------------------------------------------
# LALUAN ADMIN
# ----------------------------------------------------------------------
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if session.get("admin"):
        return redirect(url_for("admin"))
    if request.method == "POST":
        kunci = f"login:{klien_ip()}"
        baki = terkunci(kunci, 5, 600)
        if baki:
            flash(f"Terlalu banyak cubaan. Cuba lagi dalam {baki} minit.", "danger")
            return render_template("login.html"), 429

        if check_password_hash(ADMIN_PASSWORD_HASH, request.form.get("password", "")):
            kosongkan(kunci)
            nxt = request.args.get("next")
            session.clear()                     # elak session fixation
            session["admin"] = True
            session["last"] = time.time()
            session.permanent = True
            audit("login_berjaya")
            return redirect(next_selamat(nxt))

        catat(kunci)
        audit("login_gagal")
        flash("Kata laluan salah.", "danger")
    return render_template("login.html")


@app.route("/admin/logout", methods=["POST"])
def admin_logout():
    session.clear()
    return redirect(url_for("index"))


def query_surat(had=300):
    q_ = request.args.get("q", "").strip()[:100]
    st = request.args.get("status", "").strip()
    sql, args = """SELECT id, no_siri, no_rujukan, tarikh_surat, perkara, nama, no_id,
                          jenis, status, tarikh_kemaskini, tarikh_ambil, tandatangan,
                          disahkan_oleh, gambar, (pin_hash IS NOT NULL) AS ada_pin
                   FROM surat WHERE TRUE""", []
    if q_:
        like = "%" + re.sub(r"([\\%_])", r"\\\1", q_) + "%"
        sql += """ AND (no_siri ILIKE %s OR no_rujukan ILIKE %s OR perkara ILIKE %s
                   OR nama ILIKE %s OR no_id ILIKE %s)"""
        args += [like] * 5
    if st in STATUS_LIST:
        sql += " AND status=%s"
        args.append(st)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(had)
    return tanya(sql, args), q_, st


@app.route("/admin")
@admin_required
def admin():
    rows, q_, st = query_surat()
    kira = {r["status"]: r["n"] for r in
            tanya("SELECT status, COUNT(*) AS n FROM surat GROUP BY status")}
    return render_template("admin.html", rows=rows, q=q_, st=st, kira=kira)


@app.route("/admin/cetak")
@admin_required
def admin_cetak():
    rows, _, _ = query_surat(had=2000)
    return render_template("cetak.html", rows=rows, tarikh=now())


@app.route("/admin/poster")
@admin_required
def admin_poster():
    return render_template("poster.html", laman=url_for("index", _external=True))


@app.route("/admin/log")
@admin_required
def admin_log():
    rows = tanya("SELECT * FROM audit ORDER BY id DESC LIMIT 200")
    return render_template("log.html", rows=rows)


def borang_data():
    f = request.form
    d = {k: f.get(k, "").strip() for k in
         ("no_siri", "no_rujukan", "tarikh_surat", "perkara", "nama",
          "no_id", "jenis", "status")}
    d["no_id"] = d["no_id"].upper()
    return d


def sah_borang(d):
    had = {"no_siri": 30, "no_rujukan": 100, "perkara": 500, "nama": 150}
    if not all(d[k] for k in ("no_siri", "no_rujukan", "tarikh_surat",
                              "perkara", "nama", "no_id")):
        return "Sila isi semua ruangan wajib."
    for k, mx in had.items():
        if len(d[k]) > mx:
            return f"Ruangan '{k}' terlalu panjang (maksimum {mx} aksara)."
    try:
        datetime.strptime(d["tarikh_surat"], "%Y-%m-%d")
    except ValueError:
        return "Format tarikh surat tidak sah."
    if not ID_RE.fullmatch(d["no_id"]):
        return "No. Pelajar/Staf tidak sah (3-30 aksara: huruf, nombor, - / .)."
    if d["jenis"] not in ("Pelajar", "Staf"):
        return "Jenis tidak sah."
    if d["status"] not in STATUS_LIST:
        return "Status tidak sah."
    return None


def _status_sql(baru):
    """Potongan SQL statik (tiada input pengguna) bila status ditukar."""
    if baru == "Selesai":
        return ", tarikh_ambil=now(), disahkan_oleh='Admin', pin_hash=NULL"
    return ", tarikh_ambil=NULL, tandatangan=NULL, disahkan_oleh=NULL"


@app.route("/admin/tambah", methods=["GET", "POST"])
@admin_required
def admin_tambah():
    if request.method == "POST":
        d = borang_data()
        err = sah_borang(d)
        img = None
        if not err:
            try:
                img = proses_gambar(request.files.get("gambar"))
                if img and not STORAGE_ON:
                    err = "Storan gambar belum dikonfigurasi."
            except ValueError as e:
                err = str(e)
        if not err:
            nama_g = f"{uuid.uuid4().hex}.jpg" if img else None
            try:
                with get_db().cursor() as cur:
                    cur.execute("""INSERT INTO surat (no_siri,no_rujukan,tarikh_surat,perkara,
                                   nama,no_id,jenis,status,gambar,token)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                                (d["no_siri"], d["no_rujukan"], d["tarikh_surat"], d["perkara"],
                                 d["nama"], d["no_id"], d["jenis"], d["status"], nama_g,
                                 secrets.token_urlsafe(12)))
                    sid = cur.fetchone()["id"]
                pin = jana_pin()
                laksana("UPDATE surat SET pin_hash=%s WHERE id=%s", (hash_pin(sid, pin), sid))
                if img:
                    storage_upload(nama_g, img)
                get_db().commit()
                audit("tambah", f"No. Siri {d['no_siri']}")
                # Papar slip PIN terus (PIN tidak disimpan di mana-mana dalam bentuk asal)
                return render_template("slip.html", s=d, pin=pin,
                                       laman=url_for("index", _external=True))
            except psycopg2.errors.UniqueViolation:
                get_db().rollback()
                err = "No. Siri sudah wujud."
            except StorageError:
                get_db().rollback()
                err = "Gagal memuat naik gambar. Cuba lagi."
        flash(err, "danger")
        return render_template("borang.html", s=d, tajuk="Tambah Rekod Surat", sid=None)

    r = tanya("SELECT no_siri FROM surat ORDER BY id DESC LIMIT 1", one=True)
    cadangan = str(int(r["no_siri"]) + 1) if r and r["no_siri"].isdigit() else ""
    s = {"no_siri": cadangan, "no_rujukan": "", "tarikh_surat": now().strftime("%Y-%m-%d"),
         "perkara": "", "nama": "", "no_id": "", "jenis": "Staf", "status": "Direkod"}
    return render_template("borang.html", s=s, tajuk="Tambah Rekod Surat", sid=None)


@app.route("/admin/edit/<int:sid>", methods=["GET", "POST"])
@admin_required
def admin_edit(sid):
    lama = tanya("SELECT * FROM surat WHERE id=%s", (sid,), one=True)
    if not lama:
        abort(404)
    if request.method == "POST":
        d = borang_data()
        err = sah_borang(d)
        img = None
        if not err:
            try:
                img = proses_gambar(request.files.get("gambar"))
                if img and not STORAGE_ON:
                    err = "Storan gambar belum dikonfigurasi."
            except ValueError as e:
                err = str(e)
        if not err:
            gambar_baru = lama["gambar"]
            if img:
                gambar_baru = f"{uuid.uuid4().hex}.jpg"
            elif request.form.get("buang_gambar") == "1":
                gambar_baru = None
            extra = _status_sql(d["status"]) if d["status"] != lama["status"] else ""
            pin_batal = False
            if d["no_id"] != lama["no_id"]:       # No. ID berubah -> PIN lama batal
                if "pin_hash" not in extra:
                    extra += ", pin_hash=NULL"
                pin_batal = True
            try:
                laksana(f"""UPDATE surat SET no_siri=%s,no_rujukan=%s,tarikh_surat=%s,
                            perkara=%s,nama=%s,no_id=%s,jenis=%s,status=%s,
                            tarikh_kemaskini=now(),gambar=%s {extra} WHERE id=%s""",
                        (d["no_siri"], d["no_rujukan"], d["tarikh_surat"], d["perkara"],
                         d["nama"], d["no_id"], d["jenis"], d["status"], gambar_baru, sid))
                if img:
                    storage_upload(gambar_baru, img)
                get_db().commit()
                if lama["gambar"] and gambar_baru != lama["gambar"]:
                    storage_delete_senyap(lama["gambar"])
                audit("edit", f"No. Siri {d['no_siri']}")
                flash("Rekod dikemas kini." + (
                    " No. ID ditukar, jadi PIN lama dibatalkan. Tekan butang 🔑 PIN "
                    "untuk jana PIN baharu." if pin_batal else ""), "success")
                return redirect(url_for("admin"))
            except psycopg2.errors.UniqueViolation:
                get_db().rollback()
                err = "No. Siri sudah wujud."
            except StorageError:
                get_db().rollback()
                err = "Gagal memuat naik gambar. Cuba lagi."
        flash(err, "danger")
        d["gambar"] = lama["gambar"]
        return render_template("borang.html", s=d, tajuk="Edit Rekod Surat", sid=sid)
    return render_template("borang.html", s=lama, tajuk="Edit Rekod Surat", sid=sid)


@app.route("/admin/status/<int:sid>", methods=["POST"])
@admin_required
def admin_status(sid):
    baru = request.form.get("status")
    if baru not in STATUS_LIST:
        abort(400)
    s = tanya("SELECT no_siri, status FROM surat WHERE id=%s", (sid,), one=True)
    if not s:
        abort(404)
    if baru != s["status"]:
        laksana(f"UPDATE surat SET status=%s, tarikh_kemaskini=now() {_status_sql(baru)} "
                "WHERE id=%s", (baru, sid))
        get_db().commit()
        audit("tukar_status", f"{s['no_siri']}: {s['status']} -> {baru}")
        flash(f"Status surat {s['no_siri']} ditukar kepada {baru}.", "success")
    return redirect(url_for("admin"))


@app.route("/admin/pin/<int:sid>", methods=["POST"])
@admin_required
def admin_pin(sid):
    """Jana PIN baharu (PIN lama tidak sah lagi) dan papar slip."""
    s = tanya("SELECT id, no_siri, perkara, nama, no_id, status FROM surat WHERE id=%s",
              (sid,), one=True)
    if not s:
        abort(404)
    if s["status"] == "Selesai":
        flash("Surat ini sudah diambil. PIN tidak diperlukan.", "info")
        return redirect(url_for("admin"))
    pin = jana_pin()
    laksana("UPDATE surat SET pin_hash=%s WHERE id=%s", (hash_pin(sid, pin), sid))
    get_db().commit()
    audit("jana_pin", f"No. Siri {s['no_siri']}")
    return render_template("slip.html", s=s, pin=pin, laman=url_for("index", _external=True))


@app.route("/admin/padam/<int:sid>", methods=["POST"])
@admin_required
def admin_padam(sid):
    s = tanya("SELECT no_siri, gambar FROM surat WHERE id=%s", (sid,), one=True)
    if s:
        laksana("DELETE FROM surat WHERE id=%s", (sid,))
        get_db().commit()
        storage_delete_senyap(s["gambar"])
        audit("padam", f"No. Siri {s['no_siri']}")
        flash("Rekod dipadam.", "info")
    return redirect(url_for("admin"))


@app.route("/admin/gambar/<int:sid>")
@admin_required
def admin_gambar(sid):
    """Paparkan gambar peribadi dari Supabase Storage (admin sahaja)."""
    r = tanya("SELECT gambar FROM surat WHERE id=%s", (sid,), one=True)
    if not r or not r["gambar"] or not STORAGE_ON:
        abort(404)
    try:
        data = storage_get(r["gambar"])
    except StorageError:
        abort(502)
    return Response(data, mimetype="image/jpeg",
                    headers={"Content-Disposition": "inline",
                             "X-Content-Type-Options": "nosniff"})


# ----------------------------------------------------------------------
# TEMPLATE HTML
# ----------------------------------------------------------------------
TEMPLATES = {}

TEMPLATES["base.html"] = """<!doctype html>
<html lang="ms"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
{% block robots %}<meta name="robots" content="noindex,nofollow">{% endblock %}
<title>{% block title %}Rekod Surat – Pejabat Rektor UiTM Cawangan Sabah 2026{% endblock %}</title>
{% if ADA_LOGO %}<link rel="icon" href="{{ url_for('static', filename='img/logo.png') }}">{% endif %}
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet"
 integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
<style>
 body{background:#f4f6f9}
 .navbar{background:#4b1d6b}
 .sig{border:2px dashed #999;border-radius:8px;background:#fff;width:100%;height:180px;touch-action:none}
 .table td,.table th{vertical-align:middle}
 .kad-surat{border-left:6px solid #0d6efd}
 .pin-kotak{background:#fff8e1;border:2px dashed #f0ad00;border-radius:10px;text-align:center;padding:1rem}
 .pin-nombor{font-size:3rem;font-weight:800;letter-spacing:.7rem;font-family:monospace;color:#4b1d6b}
 @media print{body{background:#fff}}
</style>
</head><body>
<nav class="navbar navbar-dark mb-3 d-print-none"><div class="container-fluid">
  <a class="navbar-brand d-flex align-items-center gap-2" href="{{ url_for('index') }}">
    {% if ADA_LOGO %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="UiTM" height="38">{% endif %}
    <span class="fs-6">Rekod Surat Keluar – Pejabat Rektor<br class="d-md-none"> UiTM Cawangan Sabah 2026</span></a>
  <div>
  {% if session.get('admin') %}
    <a class="btn btn-sm btn-outline-light" href="{{ url_for('admin') }}">Admin</a>
    <a class="btn btn-sm btn-outline-light" href="{{ url_for('admin_poster') }}">📱 Poster QR</a>
    <a class="btn btn-sm btn-outline-light" href="{{ url_for('admin_log') }}">Log</a>
    <form class="d-inline" method="post" action="{{ url_for('admin_logout') }}">
      <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
      <button class="btn btn-sm btn-light">Log Keluar</button></form>
  {% else %}
    <a class="btn btn-sm btn-outline-light" href="{{ url_for('admin_login') }}">Log Masuk Admin</a>
  {% endif %}
  </div></div></nav>
<div class="container-fluid px-3">
  {% with msgs = get_flashed_messages(with_categories=true) %}
    {% for c, m in msgs %}<div class="alert alert-{{ c }} alert-dismissible fade show d-print-none">{{ m }}
      <button type="button" class="btn-close" data-bs-dismiss="alert"></button></div>{% endfor %}
  {% endwith %}
  {% block content %}{% endblock %}
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"
 integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>
<script nonce="{{ nonce() }}">
document.querySelectorAll('select.auto-submit').forEach(e=>e.addEventListener('change',()=>e.form.submit()));
document.querySelectorAll('form[data-confirm]').forEach(f=>f.addEventListener('submit',ev=>{
  if(!confirm(f.dataset.confirm))ev.preventDefault();}));
document.querySelectorAll('[data-print]').forEach(b=>b.addEventListener('click',()=>print()));
</script>
{% block scripts %}{% endblock %}
</body></html>"""

TEMPLATES["ralat.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-md-6">
 <div class="card shadow-sm text-center"><div class="card-body py-5">
  <h1 class="display-4">{{ kod }}</h1>
  <p class="lead">{{ mesej }}</p>
  <a class="btn btn-primary" href="{{ url_for('index') }}">← Halaman Utama</a>
 </div></div></div></div>
{% endblock %}"""

TEMPLATES["index.html"] = """{% extends 'base.html' %}
{% block robots %}{% if ALLOW_INDEX and not no_id %}
<meta name="robots" content="index,follow">
<meta name="description" content="Semak status surat keluar Pejabat Rektor UiTM Cawangan Sabah.">
{% else %}<meta name="robots" content="noindex,nofollow">{% endif %}{% endblock %}
{% block content %}
<div class="row justify-content-center"><div class="col-lg-8">
 {% if ADA_BANNER %}<img class="w-100 rounded shadow-sm mb-3" alt="UiTM Cawangan Sabah"
   style="max-height:200px;object-fit:cover" src="{{ url_for('static', filename='img/banner.jpg') }}">{% endif %}
 <div class="card shadow-sm mb-4"><div class="card-body">
  <h4 class="mb-1">Semak Surat Anda</h4>
  <p class="text-muted">Masukkan <b>No. Pelajar</b> atau <b>No. Staf</b>. Tiada akaun diperlukan.</p>
  <form method="get" class="input-group input-group-lg">
    <input name="no_id" class="form-control" placeholder="cth: 2022123456 / STAF001"
           value="{{ no_id }}" required autofocus maxlength="30" autocomplete="off">
    <button class="btn btn-primary">🔍 Semak</button>
  </form>
  <div class="mt-3">
    <a class="btn btn-outline-success btn-sm" href="{{ url_for('sahkan', no_id=no_id) if no_id else url_for('sahkan') }}">
      ✍️ Sudah ambil surat? Sahkan pengambilan (guna PIN)</a></div>
 </div></div>

 {% if senarai is not none %}
  {% if not senarai %}
    <div class="alert alert-warning">Tiada surat dijumpai untuk <b>{{ no_id }}</b>.</div>
  {% else %}
   <h5>{{ senarai|length }} surat untuk <b>{{ no_id }}</b></h5>
   {% for s in senarai %}
   <div class="card kad-surat shadow-sm mb-3"
        style="border-left-color:{{ STATUS_META[s.status].bg }}">
    <div class="card-body">
      <div class="d-flex justify-content-between flex-wrap gap-2">
        <div><small class="text-muted">No. Siri {{ s.no_siri }}</small><br>
             <b>{{ s.perkara }}</b></div>
        <div>{{ badge(s.status) }}</div>
      </div>
      <hr class="my-2">
      <div class="small">
        <div>📎 <b>Rujukan:</b> {{ s.no_rujukan }} &nbsp;|&nbsp; {{ s.tarikh_surat|tgl }}</div>
        <div>👤 {{ s.nama|topeng }} ({{ s.jenis }})</div>
        <div>🕒 Kemas kini: {{ s.tarikh_kemaskini|tgl }}</div>
        {% if s.tarikh_ambil %}<div>✅ Diambil: {{ s.tarikh_ambil|tgl_masa }}</div>{% endif %}
      </div>
      {% if s.status == 'Surat Keluar' %}
        <div class="alert alert-info mt-3 mb-2 py-2 small">
          Surat anda sudah siap. Sila ambil di <b>kaunter Pejabat Rektor</b> bersama
          <b>kad matrik / kad staf</b>. Kemudian sahkan pengambilan menggunakan
          <b>PIN {{ PIN_PANJANG }} digit</b> yang diberikan semasa anda menyerahkan permohonan.</div>
        <a class="btn btn-success w-100" href="{{ url_for('sahkan', no_id=no_id) }}">
          ✍️ Sahkan Pengambilan (guna PIN)</a>
      {% endif %}
    </div></div>
   {% endfor %}
  {% endif %}
 {% endif %}
</div></div>
{% endblock %}"""

TEMPLATES["sahkan.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-lg-6">
 <div class="card shadow-sm"><div class="card-body">
  <h4>✍️ Pengesahan Pengambilan Surat</h4>
  <p class="text-muted small">Lakukan ini <b>di kaunter</b> semasa menerima surat. Petugas akan
    menyemak kad matrik / kad staf anda.</p>
  <form method="post" id="frm" autocomplete="off">
    <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
    <input type="hidden" name="tandatangan" id="tt">
    <div class="mb-3">
      <label class="form-label">No. Pelajar / No. Staf</label>
      <input name="no_id" class="form-control form-control-lg" required maxlength="30"
             value="{{ no_id }}" placeholder="cth: 2022123456 / STAF001">
    </div>
    <div class="mb-3">
      <label class="form-label">PIN {{ PIN_PANJANG }} digit (daripada slip anda)</label>
      <input name="pin" type="password" class="form-control form-control-lg text-center" required
             inputmode="numeric" pattern="[0-9]*" maxlength="{{ PIN_PANJANG }}" autocomplete="off"
             style="letter-spacing:.6rem;font-size:1.8rem" placeholder="••••">
      <div class="form-text">Hilang PIN? Minta petugas kaunter jana PIN baharu.</div>
    </div>
    <div class="mb-2">
      <label class="form-label">Tandatangan digital (pilihan)</label>
      <canvas id="pad" class="sig"></canvas>
      <button type="button" class="btn btn-sm btn-outline-secondary mt-1" id="clr">Padam tandatangan</button>
    </div>
    <button class="btn btn-success btn-lg w-100 mt-3">✅ Saya Sahkan Telah Menerima Surat</button>
  </form>
  <a href="{{ url_for('index') }}" class="btn btn-link mt-2">← Kembali</a>
 </div></div>
</div></div>
{% endblock %}
{% block scripts %}
<script nonce="{{ nonce() }}">
const c=document.getElementById('pad');
if(c){
  const ctx=c.getContext('2d');let draw=false,dirty=false;
  function size(){c.width=c.offsetWidth;c.height=c.offsetHeight;ctx.lineWidth=2.5;ctx.lineCap='round';}
  size();
  function pos(e){const r=c.getBoundingClientRect();return [e.clientX-r.left,e.clientY-r.top];}
  c.addEventListener('pointerdown',e=>{draw=true;c.setPointerCapture(e.pointerId);const[p,q]=pos(e);ctx.beginPath();ctx.moveTo(p,q);});
  c.addEventListener('pointermove',e=>{if(!draw)return;const[p,q]=pos(e);ctx.lineTo(p,q);ctx.stroke();dirty=true;});
  ['pointerup','pointercancel'].forEach(n=>c.addEventListener(n,()=>draw=false));
  document.getElementById('clr').onclick=()=>{ctx.clearRect(0,0,c.width,c.height);dirty=false;};
  document.getElementById('frm').addEventListener('submit',()=>{
    if(dirty)document.getElementById('tt').value=c.toDataURL('image/png');});
}
</script>
{% endblock %}"""

TEMPLATES["selesai.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-lg-6">
 <div class="card border-success border-3 shadow-sm text-center"><div class="card-body py-4">
  <div style="font-size:4rem">✅</div>
  <h3 class="text-success">Pengambilan Disahkan</h3>
  <p class="mb-1"><b>No. Siri:</b> {{ s.no_siri }}</p>
  <p class="mb-1">{{ s.perkara }}</p>
  <p class="text-muted">{{ masa.strftime('%d/%m/%Y %I:%M %p') }}</p>
  <div class="alert alert-light border small">Tunjukkan skrin ini kepada petugas kaunter.</div>
  <a class="btn btn-outline-primary" href="{{ url_for('index') }}">Halaman Utama</a>
 </div></div>
</div></div>
{% endblock %}"""

TEMPLATES["slip.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-lg-6">
 <div class="alert alert-warning d-print-none">
   <b>Simpan / cetak slip ini sekarang.</b> PIN hanya dipaparkan sekali dan tidak boleh dilihat semula.
   Jika hilang, tekan butang 🔑 PIN pada rekod untuk jana PIN baharu.</div>
 <div class="card shadow-sm"><div class="card-body text-center">
  {% if ADA_LOGO %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="" height="55" class="mb-2"><br>{% endif %}
  <h5 class="mb-0">SLIP PIN PENGAMBILAN SURAT</h5>
  <div class="small mb-3">Pejabat Rektor UiTM Cawangan Sabah</div>
  <table class="table table-sm text-start">
    <tr><th style="width:35%">No. Siri</th><td>{{ s.no_siri }}</td></tr>
    <tr><th>Perkara</th><td>{{ s.perkara }}</td></tr>
    <tr><th>Penerima</th><td>{{ s.nama }}</td></tr>
    <tr><th>No. Pelajar/Staf</th><td>{{ s.no_id }}</td></tr>
  </table>
  <div class="pin-kotak">
    <div class="small text-muted">PIN ANDA</div>
    <div class="pin-nombor">{{ pin }}</div>
    <div class="small"><b>Jangan kongsi PIN ini.</b></div>
  </div>
  <p class="small mt-3 mb-1">Apabila surat siap, ambil di kaunter bersama kad matrik / kad staf,
    kemudian layari:</p>
  <p class="fw-bold mb-1">{{ laman }}</p>
  <img src="{{ url_for('qr_laman') }}" alt="QR" width="130">
  <p class="small text-muted mb-0">Masukkan No. Pelajar/Staf + PIN untuk mengesahkan pengambilan.</p>
 </div></div>
 <div class="d-print-none mt-3 d-flex gap-2">
   <button class="btn btn-primary" data-print>🖨️ Cetak Slip</button>
   <a class="btn btn-outline-secondary" href="{{ url_for('admin') }}">Selesai</a>
   <a class="btn btn-outline-primary" href="{{ url_for('admin_tambah') }}">➕ Tambah Lagi</a>
 </div>
</div></div>
{% endblock %}"""

TEMPLATES["poster.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-lg-6 text-center">
 <div class="card shadow-sm"><div class="card-body py-4">
  {% if ADA_LOGO %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="" height="80" class="mb-2"><br>{% endif %}
  <h4>SEMAK &amp; SAHKAN SURAT ANDA</h4>
  <div class="mb-2">Pejabat Rektor UiTM Cawangan Sabah</div>
  <img src="{{ url_for('qr_laman') }}" alt="QR" width="260">
  <p class="fw-bold mt-2 mb-1">{{ laman }}</p>
  <p class="small text-muted mb-0">Imbas QR, masukkan No. Pelajar/Staf untuk semak status.<br>
    Ambil surat? Sediakan PIN pada slip anda.</p>
 </div></div>
 <button class="btn btn-primary mt-3 d-print-none" data-print>🖨️ Cetak Poster</button>
</div></div>
{% endblock %}"""

TEMPLATES["login.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-md-5 col-lg-4">
 <div class="card shadow-sm"><div class="card-body">
  <h4>🔐 Log Masuk Admin</h4>
  <form method="post">
    <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
    <input type="password" name="password" class="form-control mb-3" placeholder="Kata laluan"
           required autofocus autocomplete="current-password">
    <button class="btn btn-primary w-100">Log Masuk</button>
  </form>
 </div></div></div></div>
{% endblock %}"""

TEMPLATES["admin.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="d-flex flex-wrap gap-2 mb-3 align-items-center">
  <h4 class="me-auto mb-0">Rekod Surat Keluar 2026</h4>
  <a class="btn btn-primary" href="{{ url_for('admin_tambah') }}">➕ Tambah Surat</a>
  <a class="btn btn-outline-dark" target="_blank"
     href="{{ url_for('admin_cetak', q=q, status=st) }}">🖨️ Cetak Laporan</a>
</div>

<div class="row g-2 mb-3">
 {% for k in STATUS_LIST %}
  <div class="col-6 col-md-3"><a class="text-decoration-none" href="{{ url_for('admin', status=k) }}">
   <div class="card text-center shadow-sm"><div class="card-body py-2">
    {{ badge(k) }}<div class="fs-4 fw-bold text-dark">{{ kira.get(k,0) }}</div>
   </div></div></a></div>
 {% endfor %}
</div>

<form class="row g-2 mb-3" method="get">
  <div class="col-md-6"><input class="form-control" name="q" value="{{ q }}" maxlength="100"
       placeholder="Cari no. siri, rujukan, perkara, nama, no. pelajar/staf"></div>
  <div class="col-md-3"><select class="form-select" name="status">
    <option value="">Semua status</option>
    {% for k in STATUS_LIST %}<option {{ 'selected' if k==st }}>{{ k }}</option>{% endfor %}
  </select></div>
  <div class="col-md-3 d-grid d-md-block">
    <button class="btn btn-secondary">Tapis</button>
    <a class="btn btn-link" href="{{ url_for('admin') }}">Reset</a></div>
</form>

<div class="card shadow-sm"><div class="table-responsive">
<table class="table table-hover table-bordered mb-0 align-middle">
 <thead class="table-dark"><tr>
  <th>No. Siri</th><th>No. Rujukan &amp; Tarikh</th><th>Perkara / Tajuk Surat</th>
  <th>Nama &amp; No. Pelajar/Staf</th><th>Status Surat</th><th>Tarikh Kemaskini</th>
  <th>Pengesahan Penerima</th><th class="d-print-none">Tindakan</th></tr></thead>
 <tbody>
 {% for s in rows %}
 <tr>
  <td><b>{{ s.no_siri }}</b></td>
  <td>{{ s.no_rujukan }}<br><small class="text-muted">{{ s.tarikh_surat|tgl }}</small></td>
  <td>{{ s.perkara }}
    {% if s.gambar %}<br><a class="small" target="_blank" rel="noopener"
       href="{{ url_for('admin_gambar', sid=s.id) }}">📷 Lihat gambar surat</a>{% endif %}</td>
  <td>{{ s.nama }}<br><small class="text-muted">({{ s.jenis }}: {{ s.no_id }})</small></td>
  <td style="min-width:170px">
    {{ badge(s.status) }}
    <form method="post" action="{{ url_for('admin_status', sid=s.id) }}" class="mt-1">
      <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
      <select name="status" class="form-select form-select-sm auto-submit">
        {% for k in STATUS_LIST %}<option {{ 'selected' if k==s.status }}>{{ k }}</option>{% endfor %}
      </select></form></td>
  <td>{{ s.tarikh_kemaskini|tgl }}</td>
  <td>
    {% if s.tarikh_ambil %}
      ✅ {{ s.tarikh_ambil|tgl_masa }}<br>
      <small class="text-muted">oleh {{ s.disahkan_oleh }}</small>
      {% if s.tandatangan %}<br><img src="{{ s.tandatangan }}" alt="tandatangan" style="height:40px;border:1px solid #ddd;background:#fff">{% endif %}
    {% elif s.status == 'Surat Keluar' %}
      <span class="text-primary">Menunggu pengesahan</span><br>
      {% if s.ada_pin %}<small class="text-success">🔑 PIN telah dijana</small>
      {% else %}<small class="text-danger">⚠️ Tiada PIN, tekan 🔑 PIN</small>{% endif %}
      <div class="small text-muted">Semak kad matrik/staf penerima</div>
    {% else %}
      <span class="text-muted">-</span>
      {% if not s.ada_pin %}<br><small class="text-danger">⚠️ Tiada PIN</small>{% endif %}
    {% endif %}
  </td>
  <td class="text-nowrap">
    <a class="btn btn-sm btn-outline-primary" href="{{ url_for('admin_edit', sid=s.id) }}">Edit</a>
    {% if s.status != 'Selesai' %}
    <form class="d-inline" method="post" action="{{ url_for('admin_pin', sid=s.id) }}"
          data-confirm="Jana PIN baharu untuk surat {{ s.no_siri }}? PIN lama tidak sah lagi.">
      <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
      <button class="btn btn-sm btn-outline-warning">🔑 PIN</button></form>
    {% endif %}
    <form class="d-inline" method="post" action="{{ url_for('admin_padam', sid=s.id) }}"
          data-confirm="Padam rekod {{ s.no_siri }}?">
      <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
      <button class="btn btn-sm btn-outline-danger">Padam</button></form>
  </td>
 </tr>
 {% else %}<tr><td colspan="8" class="text-center text-muted py-4">Tiada rekod.</td></tr>
 {% endfor %}
 </tbody></table></div></div>
<p class="small text-muted mt-2">Memaparkan maksimum 300 rekod terkini. Gunakan carian untuk rekod lama.</p>
{% endblock %}"""

TEMPLATES["borang.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="row justify-content-center"><div class="col-lg-7">
<div class="card shadow-sm"><div class="card-body">
 <h4>{{ tajuk }}</h4>
 {% if not sid %}<div class="alert alert-info py-2 small">Selepas simpan, <b>PIN {{ PIN_PANJANG }} digit</b>
   dijana automatik dan <b>slip PIN</b> dipaparkan untuk diserahkan kepada pemohon.</div>{% endif %}
 <form method="post" enctype="multipart/form-data" class="row g-3">
  <input type="hidden" name="_csrf" value="{{ csrf_token() }}">
  <div class="col-md-4"><label class="form-label">No. Siri *</label>
    <input name="no_siri" class="form-control" value="{{ s.no_siri }}" required maxlength="30"></div>
  <div class="col-md-8"><label class="form-label">No. Rujukan *</label>
    <input name="no_rujukan" class="form-control" value="{{ s.no_rujukan }}" maxlength="100"
           placeholder="100-UITMCSH (KAB/100/01)" required></div>
  <div class="col-md-4"><label class="form-label">Tarikh Surat *</label>
    <input type="date" name="tarikh_surat" class="form-control" value="{{ s.tarikh_surat }}" required></div>
  <div class="col-md-8"><label class="form-label">Perkara / Tajuk *</label>
    <input name="perkara" class="form-control" value="{{ s.perkara }}" required maxlength="500"></div>
  <div class="col-md-6"><label class="form-label">Nama Penerima *</label>
    <input name="nama" class="form-control" value="{{ s.nama }}" required maxlength="150"></div>
  <div class="col-md-3"><label class="form-label">Jenis *</label>
    <select name="jenis" class="form-select">
      {% for j in ['Staf','Pelajar'] %}<option {{ 'selected' if s.jenis==j }}>{{ j }}</option>{% endfor %}
    </select></div>
  <div class="col-md-3"><label class="form-label">No. Pelajar/Staf *</label>
    <input name="no_id" class="form-control" value="{{ s.no_id }}" required maxlength="30"></div>
  <div class="col-md-6"><label class="form-label">Status</label>
    <select name="status" class="form-select">
      {% for k in STATUS_LIST %}<option {{ 'selected' if s.status==k }}>{{ k }}</option>{% endfor %}
    </select></div>

  <div class="col-12"><hr class="my-1"></div>
  <div class="col-12">
    <label class="form-label">📷 Gambar / Imbasan Surat (pilihan)</label>
    {% if STORAGE_ON %}
      {% if sid and s.gambar %}
        <div class="mb-2">
          <a target="_blank" rel="noopener" href="{{ url_for('admin_gambar', sid=sid) }}">
            <img src="{{ url_for('admin_gambar', sid=sid) }}" alt="gambar surat"
                 class="img-thumbnail" style="max-height:200px"></a>
          <div class="form-check mt-1">
            <input class="form-check-input" type="checkbox" name="buang_gambar" value="1" id="bg">
            <label class="form-check-label" for="bg">Buang gambar ini</label></div>
        </div>
      {% endif %}
      <input type="file" name="gambar" class="form-control"
             accept="image/jpeg,image/png,image/webp">
      <div class="form-text">JPG / PNG / WEBP, maksimum 6 MB. Disimpan secara peribadi; hanya admin boleh melihat.
        {% if sid and s.gambar %}Memilih fail baharu akan menggantikan gambar lama.{% endif %}</div>
    {% else %}
      <div class="alert alert-secondary py-2 mb-0 small">Storan gambar belum dikonfigurasi
        (tetapkan SUPABASE_URL &amp; SUPABASE_SERVICE_KEY).</div>
    {% endif %}
  </div>

  <div class="col-12 d-flex gap-2">
    <button class="btn btn-primary">💾 Simpan</button>
    <a class="btn btn-outline-secondary" href="{{ url_for('admin') }}">Batal</a></div>
 </form>
</div></div></div></div>
{% endblock %}"""

TEMPLATES["cetak.html"] = """{% extends 'base.html' %}
{% block content %}
<div class="text-center mb-3">
  {% if ADA_LOGO %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="" height="60" class="mb-2"><br>{% endif %}
  <h5 class="mb-0">REKOD SURAT DOKUMEN KELUAR</h5>
  <h5>PEJABAT REKTOR UiTM CAWANGAN SABAH TAHUN 2026</h5>
  <small>Dicetak: {{ tarikh.strftime('%d/%m/%Y %I:%M %p') }}</small>
  <div class="d-print-none mt-2"><button class="btn btn-primary" data-print>🖨️ Cetak</button></div>
</div>
<table class="table table-bordered table-sm">
 <thead><tr><th>No. Siri</th><th>No. Rujukan &amp; Tarikh</th><th>Perkara</th>
 <th>Nama &amp; No. Pelajar/Staf</th><th>Status</th><th>Tarikh Kemaskini</th>
 <th>Pengesahan Penerima</th></tr></thead><tbody>
 {% for s in rows %}<tr>
  <td>{{ s.no_siri }}</td>
  <td>{{ s.no_rujukan }}<br>{{ s.tarikh_surat|tgl }}</td>
  <td>{{ s.perkara }}</td>
  <td>{{ s.nama }}<br>({{ s.no_id }})</td>
  <td>{{ STATUS_META[s.status].emoji }} {{ s.status }}</td>
  <td>{{ s.tarikh_kemaskini|tgl }}</td>
  <td>{% if s.tarikh_ambil %}{{ s.tarikh_ambil|tgl_masa }}
      {% if s.tandatangan %}<br><img src="{{ s.tandatangan }}" alt="" style="height:35px">{% endif %}{% endif %}</td>
 </tr>{% endfor %}</tbody></table>
{% endblock %}"""

TEMPLATES["log.html"] = """{% extends 'base.html' %}
{% block content %}
<h4>Log Audit (200 terkini)</h4>
<div class="card shadow-sm"><div class="table-responsive">
<table class="table table-sm table-striped mb-0">
 <thead class="table-dark"><tr><th>Masa</th><th>Tindakan</th><th>Butiran</th><th>IP</th></tr></thead><tbody>
 {% for r in rows %}<tr><td>{{ r.masa|tgl_masa }}</td><td>{{ r.tindakan }}</td>
   <td>{{ r.butiran }}</td><td>{{ r.ip }}</td></tr>
 {% else %}<tr><td colspan="4" class="text-center text-muted">Tiada log.</td></tr>{% endfor %}
 </tbody></table></div></div>
{% endblock %}"""

app.jinja_loader = DictLoader(TEMPLATES)

# ----------------------------------------------------------------------
init_db()                 # dijalankan juga oleh gunicorn
storage_ensure_bucket()   # cipta bucket peribadi jika belum ada

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=DEBUG)
