# -*- coding: utf-8 -*-
"""Sistem Rekod Surat Dokumen Keluar - Pejabat Rektor 2026 (UiTM Cawangan Sabah)
Flask + Supabase (Postgres + Storage) + arkib Google Drive (pilihan, Apps Script)."""
import os, io, re, hmac, hashlib, secrets, base64, threading, logging, uuid, time
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from functools import wraps

import requests
import qrcode
import psycopg2, psycopg2.pool
from psycopg2.extras import RealDictCursor
from PIL import Image, ImageOps
from dotenv import load_dotenv
from flask import (Flask, request, session, redirect, url_for, render_template,
                   abort, flash, g, Response)
from jinja2 import DictLoader
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash

load_dotenv()
logging.basicConfig(level=logging.INFO)
lg = logging.getLogger("surat")


def env(k, d=""):
    return os.environ.get(k, d).strip()


PROD = bool(os.environ.get("RENDER")) or env("PRODUCTION") == "1"
DATABASE_URL = env("DATABASE_URL")
SECRET_KEY = env("SECRET_KEY")
if PROD and len(SECRET_KEY) < 32:
    raise RuntimeError("SECRET_KEY mesti sekurang-kurangnya 32 aksara")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
ADMIN_HASH = env("ADMIN_PASSWORD_HASH")
ADMIN_PLAIN = env("ADMIN_PASSWORD") or ("" if PROD else "admin12345678")
if PROD and not ADMIN_HASH and len(ADMIN_PLAIN) < 12:
    raise RuntimeError("Set ADMIN_PASSWORD_HASH (atau ADMIN_PASSWORD >= 12 aksara)")
SUPABASE_URL = env("SUPABASE_URL").rstrip("/")
SUPABASE_KEY = env("SUPABASE_SERVICE_KEY")
BUCKET = env("SUPABASE_BUCKET", "surat-gambar")
try:
    PIN_LEN = min(8, max(4, int(env("PIN_PANJANG", "4"))))
except ValueError:
    PIN_LEN = 4
ALLOW_INDEX = env("ALLOW_INDEX") == "1"
try:
    PROXY_HOPS = int(env("PROXY_HOPS", "1" if PROD else "0"))
except ValueError:
    PROXY_HOPS = 1
ARKIB_URL = env("ARKIB_URL")
ARKIB_KEY = env("ARKIB_KEY")

STATUS = ["Direkod", "Tindakan", "Surat Keluar", "Selesai"]
MYT = timezone(timedelta(hours=8))
Image.MAX_IMAGE_PIXELS = 25_000_000

app = Flask(__name__, static_folder="static")
if PROXY_HOPS:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=PROXY_HOPS, x_proto=1, x_host=1)
app.config.update(
    SECRET_KEY=SECRET_KEY,
    SESSION_COOKIE_NAME="__Host-session" if PROD else "session",
    SESSION_COOKIE_SECURE=PROD,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=2),
    MAX_CONTENT_LENGTH=8 * 1024 * 1024,
)

_img = os.path.join(app.static_folder or "static", "img")
LOGO_ADA = os.path.exists(os.path.join(_img, "logo.png"))
BANNER_FAIL = next((f for f in ("banner.jpg", "banner.png")
                    if os.path.exists(os.path.join(_img, f))), "")

# ------------------------------------------------------------------ DB
_pool = None
_pool_lock = threading.Lock()
_db_ready = False
_db_lock = threading.Lock()


def get_pool():
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = psycopg2.pool.ThreadedConnectionPool(
                1, 8, DATABASE_URL, connect_timeout=10, sslmode="require")
        return _pool


@contextmanager
def db():
    pool = get_pool()
    c = pool.getconn()
    try:
        yield c
        c.commit()
    except Exception:
        try:
            c.rollback()
        except Exception:
            pass
        raise
    finally:
        pool.putconn(c, close=bool(c.closed))


def run(sql, args=(), fetch="all"):
    for t in (1, 2):
        try:
            with db() as c:
                with c.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql, args)
                    if fetch == "all":
                        return cur.fetchall()
                    if fetch == "one":
                        return cur.fetchone()
                    return cur.rowcount
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if t == 2:
                raise


SCHEMA = [
    """CREATE TABLE IF NOT EXISTS rekod(
        id SERIAL PRIMARY KEY,
        no_siri TEXT NOT NULL,
        no_id TEXT NOT NULL,
        nama TEXT NOT NULL,
        jenis TEXT NOT NULL DEFAULT 'Pelajar',
        perkara TEXT NOT NULL,
        rujukan TEXT DEFAULT '',
        tarikh_surat DATE,
        status TEXT NOT NULL DEFAULT 'Direkod',
        gambar TEXT DEFAULT '',
        pin_hash TEXT,
        tandatangan TEXT DEFAULT '',
        disahkan_oleh TEXT DEFAULT '',
        selesai_pada TIMESTAMPTZ,
        arkib TEXT DEFAULT '',
        dicipta TIMESTAMPTZ DEFAULT now(),
        dikemas TIMESTAMPTZ DEFAULT now())""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_rekod_siri ON rekod(no_siri)",
    "CREATE INDEX IF NOT EXISTS ix_rekod_id ON rekod(no_id)",
    # PIN unik bagi setiap ID (surat aktif sahaja; PIN dipadam bila Selesai)
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_rekod_pin ON rekod(no_id, pin_hash) WHERE pin_hash IS NOT NULL",
    """CREATE TABLE IF NOT EXISTS had(
        k TEXT PRIMARY KEY, n INT NOT NULL, mula TIMESTAMPTZ NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS log_audit(
        id SERIAL PRIMARY KEY, masa TIMESTAMPTZ DEFAULT now(),
        siapa TEXT, aksi TEXT, butiran TEXT, ip TEXT)""",
    "ALTER TABLE rekod ENABLE ROW LEVEL SECURITY",
    "ALTER TABLE had ENABLE ROW LEVEL SECURITY",
    "ALTER TABLE log_audit ENABLE ROW LEVEL SECURITY",
]


def init_db():
    with db() as c:
        with c.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(2026001)")
            for s in SCHEMA:
                cur.execute(s)


def ensure_db():
    global _db_ready
    if _db_ready:
        return
    with _db_lock:
        if not _db_ready:
            init_db()
            _db_ready = True


# ------------------------------------------------------------------ Had cubaan
def had_sekat(k, maks, saat):
    r = run("SELECT n FROM had WHERE k=%s AND mula > now() - (%s * interval '1 second')",
            (k, saat), "one")
    return bool(r and r["n"] >= maks)


def had_tambah(k, saat):
    run("""INSERT INTO had(k,n,mula) VALUES(%s,1,now())
           ON CONFLICT (k) DO UPDATE SET
             n = CASE WHEN had.mula < now() - (%s * interval '1 second') THEN 1 ELSE had.n+1 END,
             mula = CASE WHEN had.mula < now() - (%s * interval '1 second') THEN now() ELSE had.mula END""",
        (k, saat, saat), "count")


def had_reset(k):
    run("DELETE FROM had WHERE k=%s", (k,), "count")


def audit(aksi, butiran=""):
    try:
        run("INSERT INTO log_audit(siapa,aksi,butiran,ip) VALUES(%s,%s,%s,%s)",
            ("admin" if session.get("admin") else "awam", aksi, (butiran or "")[:500],
             request.remote_addr), "count")
    except Exception:
        lg.exception("audit gagal")


# ------------------------------------------------------------------ Util
ID_RE = re.compile(r"^[A-Z0-9][A-Z0-9/.\-]{2,29}$")


def norm_id(s):
    s = (s or "").strip().upper().replace(" ", "")
    return s if ID_RE.match(s) else ""


def bersih(s, maks):
    return re.sub(r"[\x00-\x1f\x7f]", "", s or "").strip()[:maks]


def jana_pin():
    return str(secrets.randbelow(10 ** PIN_LEN)).zfill(PIN_LEN)


def hash_pin(no_id, pin):
    return hmac.new(SECRET_KEY.encode(), f"pin:{no_id}:{pin}".encode(),
                    hashlib.sha256).hexdigest()


def pin_unik(no_id):
    for _ in range(60):
        p = jana_pin()
        h = hash_pin(no_id, p)
        if not run("SELECT 1 FROM rekod WHERE no_id=%s AND pin_hash=%s", (no_id, h), "one"):
            return p, h
    raise RuntimeError("Gagal jana PIN unik")


def ralat(kod, mesej):
    return render_template("ralat.html", kod=kod, mesej=mesej), kod


# ------------------------------------------------------------------ Storan Supabase
_bucket_ok = False


def _sh():
    return {"Authorization": f"Bearer {SUPABASE_KEY}", "apikey": SUPABASE_KEY}


def storage_ready():
    global _bucket_ok
    if _bucket_ok:
        return
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise ValueError("Storan gambar belum dikonfigurasi (SUPABASE_URL / SUPABASE_SERVICE_KEY).")
    r = requests.post(f"{SUPABASE_URL}/storage/v1/bucket", headers=_sh(),
                      json={"id": BUCKET, "name": BUCKET, "public": False}, timeout=15)
    if r.status_code in (200, 201, 409) or "exist" in r.text.lower():
        _bucket_ok = True
    else:
        raise RuntimeError("Gagal cipta bucket: " + r.text[:200])


def storage_put(path, data, ctype):
    storage_ready()
    h = _sh()
    h.update({"Content-Type": ctype, "x-upsert": "false"})
    r = requests.post(f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{path}",
                      headers=h, data=data, timeout=30)
    if not r.ok:
        raise RuntimeError("Upload gagal: " + r.text[:200])


def storage_get(path):
    try:
        r = requests.get(f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{path}",
                         headers=_sh(), timeout=30)
        return r.content if r.ok else None
    except Exception:
        return None


def storage_del(path):
    if not path:
        return
    try:
        requests.delete(f"{SUPABASE_URL}/storage/v1/object/{BUCKET}",
                        headers=_sh(), json={"prefixes": [path]}, timeout=15)
    except Exception:
        lg.exception("padam fail gagal")


def simpan_gambar(fs):
    data = fs.read(6 * 1024 * 1024 + 1)
    if len(data) > 6 * 1024 * 1024:
        raise ValueError("Gambar terlalu besar (maks 6 MB).")
    try:
        im = Image.open(io.BytesIO(data))
        if im.format not in ("JPEG", "PNG", "WEBP"):
            raise ValueError("Format gambar mesti JPG, PNG atau WEBP.")
        im.verify()
        im = Image.open(io.BytesIO(data))
        im = ImageOps.exif_transpose(im).convert("RGB")
    except ValueError:
        raise
    except Exception:
        raise ValueError("Fail bukan gambar yang sah.")
    im.thumbnail((1600, 1600))
    out = io.BytesIO()
    im.save(out, "JPEG", quality=85, optimize=True)  # EXIF dibuang
    path = f"g/{uuid.uuid4().hex}.jpg"
    storage_put(path, out.getvalue(), "image/jpeg")
    return path


def terima_tt(data_url):
    """Sahkan tandatangan PNG, kembalikan bytes atau None."""
    if not data_url or not data_url.startswith("data:image/png;base64,"):
        return None
    try:
        raw = base64.b64decode(data_url.split(",", 1)[1], validate=True)
        if len(raw) > 400 * 1024:
            return None
        im = Image.open(io.BytesIO(raw))
        if im.format != "PNG":
            return None
        im.load()
        im.thumbnail((800, 400))
        out = io.BytesIO()
        im.save(out, "PNG", optimize=True)
        return out.getvalue()
    except Exception:
        return None


# ------------------------------------------------------------------ Arkib Google Drive
def hantar_arkib(rid):
    if not (ARKIB_URL and ARKIB_KEY):
        return
    try:
        r = run("SELECT * FROM rekod WHERE id=%s", (rid,), "one")
        if not r:
            return
        b64 = lambda b: base64.b64encode(b).decode() if b else ""
        tt = storage_get(r["tandatangan"]) if r["tandatangan"] else None
        gb = storage_get(r["gambar"]) if r["gambar"] else None
        sp = r["selesai_pada"].astimezone(MYT).strftime("%d/%m/%Y %H:%M") if r["selesai_pada"] else ""
        payload = {
            "kunci": ARKIB_KEY, "no_siri": r["no_siri"], "no_id": r["no_id"],
            "nama": r["nama"], "jenis": r["jenis"], "perkara": r["perkara"],
            "rujukan": r["rujukan"] or "",
            "tarikh_surat": str(r["tarikh_surat"] or ""),
            "selesai_pada": sp, "disahkan_oleh": r["disahkan_oleh"] or "",
            "tt": b64(tt), "gambar": b64(gb),
        }
        resp = requests.post(ARKIB_URL, json=payload, timeout=90)
        ok = False
        try:
            ok = bool(resp.json().get("ok"))
        except Exception:
            pass
        run("UPDATE rekod SET arkib=%s WHERE id=%s",
            ("OK " + datetime.now(MYT).strftime("%d/%m/%Y %H:%M") if ok else "GAGAL", rid), "count")
    except Exception:
        lg.exception("arkib gagal")
        try:
            run("UPDATE rekod SET arkib='GAGAL' WHERE id=%s", (rid,), "count")
        except Exception:
            pass


def mula_arkib(rid):
    if ARKIB_URL and ARKIB_KEY:
        threading.Thread(target=hantar_arkib, args=(rid,), daemon=True).start()


# ------------------------------------------------------------------ Keselamatan
def csrf_token():
    if "csrf" not in session:
        session["csrf"] = secrets.token_urlsafe(32)
    return session["csrf"]


@app.before_request
def sebelum():
    g.nonce = secrets.token_urlsafe(16)
    if request.endpoint in ("static", "healthz", None):
        return
    if request.method == "POST":
        t = session.get("csrf", "")
        f = request.form.get("csrf", "")
        if not t or not hmac.compare_digest(t, f):
            abort(400)
    try:
        ensure_db()
    except Exception:
        lg.exception("DB tidak dapat dihubungi")
        return ralat(503, "Pangkalan data tidak dapat dihubungi. Cuba sebentar lagi.")


@app.after_request
def selepas(resp):
    n = getattr(g, "nonce", "")
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self'; img-src 'self' data:; "
        f"script-src 'self' 'nonce-{n}'; "
        f"style-src 'self' 'nonce-{n}' https://cdn.jsdelivr.net; style-src-attr 'unsafe-inline'; "
        "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; "
        "form-action 'self'; object-src 'none'")
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "same-origin"
    resp.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if PROD:
        resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    if request.endpoint != "static":
        resp.headers["Cache-Control"] = "no-store"
    return resp


@app.context_processor
def konteks():
    return dict(nonce=getattr(g, "nonce", ""), csrf_token=csrf_token,
                logo_ada=LOGO_ADA, banner_fail=BANNER_FAIL, allow_index=ALLOW_INDEX,
                pin_len=PIN_LEN, is_admin=bool(session.get("admin")),
                arkib_aktif=bool(ARKIB_URL and ARKIB_KEY), statuses=STATUS)


@app.template_filter("tarikh")
def f_tarikh(v):
    if not v:
        return "-"
    if isinstance(v, datetime):
        v = v.astimezone(MYT)
    return v.strftime("%d/%m/%Y")


@app.template_filter("waktu")
def f_waktu(v):
    if not v:
        return "-"
    if isinstance(v, datetime):
        v = v.astimezone(MYT)
    return v.strftime("%d/%m/%Y %H:%M")


@app.errorhandler(400)
def e400(e):
    return ralat(400, "Permintaan tidak sah atau sesi tamat. Muat semula halaman dan cuba lagi.")


@app.errorhandler(404)
def e404(e):
    return ralat(404, "Halaman tidak dijumpai.")


@app.errorhandler(413)
def e413(e):
    return ralat(413, "Fail terlalu besar.")


@app.errorhandler(500)
def e500(e):
    return ralat(500, "Ralat dalaman. Cuba lagi.")


def admin_required(f):
    @wraps(f)
    def w(*a, **k):
        if not session.get("admin"):
            return redirect(url_for("login"))
        if time.time() - session.get("last", 0) > 1800:
            session.clear()
            flash("Sesi tamat kerana tidak aktif. Sila log masuk semula.", "warning")
            return redirect(url_for("login"))
        session["last"] = time.time()
        return f(*a, **k)
    return w


# ------------------------------------------------------------------ Awam
@app.route("/healthz")
def healthz():
    return "ok"


@app.route("/robots.txt")
def robots():
    if ALLOW_INDEX:
        body = "User-agent: *\nDisallow: /admin\nDisallow: /sahkan\nDisallow: /lihat\n"
    else:
        body = "User-agent: *\nDisallow: /\n"
    return Response(body, mimetype="text/plain")


@app.route("/qr.png")
def qr_png():
    img = qrcode.make(url_for("index", _external=True), box_size=12, border=2)
    buf = io.BytesIO()
    img.save(buf)
    return Response(buf.getvalue(), mimetype="image/png")


@app.route("/")
def index():
    q = request.args.get("id", "")
    rows, qn = None, ""
    if q.strip():
        qn = norm_id(q)
        if not qn:
            flash("Format No. Pelajar / No. Staf tidak sah.", "warning")
        else:
            k = f"cari:{request.remote_addr}"
            if had_sekat(k, 30, 300):
                return ralat(429, "Terlalu banyak semakan. Cuba lagi dalam beberapa minit.")
            had_tambah(k, 300)
            rows = run("""SELECT id, no_siri, status, dikemas, (pin_hash IS NOT NULL) AS ada_pin
                          FROM rekod WHERE no_id=%s ORDER BY id DESC LIMIT 50""", (qn,))
    return render_template("index.html", rows=rows, q=qn or q.strip()[:30])


@app.route("/lihat", methods=["POST"])
def lihat():
    ip = request.remote_addr
    no_id = norm_id(request.form.get("no_id"))
    pin = re.sub(r"\D", "", request.form.get("pin", ""))
    try:
        rid = int(request.form.get("id", "0"))
    except ValueError:
        rid = 0
    k_ip, k_id = f"lihat-ip:{ip}", f"lihat-id:{no_id or 'x'}:{rid}"
    if had_sekat(k_ip, 10, 900) or had_sekat(k_id, 5, 1800):
        return ralat(429, "Terlalu banyak cubaan salah. Cuba lagi dalam 30 minit.")
    r = None
    if no_id and rid and len(pin) == PIN_LEN:
        r = run("""SELECT no_siri, perkara, rujukan, tarikh_surat, status, dikemas FROM rekod
                   WHERE id=%s AND no_id=%s AND pin_hash=%s AND status<>'Selesai'""",
                (rid, no_id, hash_pin(no_id, pin)), "one")
    if not r:
        had_tambah(k_ip, 900)
        had_tambah(k_id, 1800)
        audit("lihat_gagal", f"{no_id}#{rid}")
        return ralat(403, "PIN tidak sah atau surat tidak lagi boleh dibuka.")
    had_reset(k_id)
    return render_template("lihat.html", r=r)


@app.route("/sahkan", methods=["GET", "POST"])
def sahkan():
    if request.method == "GET":
        return render_template("sahkan.html")
    ip = request.remote_addr
    no_id = norm_id(request.form.get("no_id"))
    pin = re.sub(r"\D", "", request.form.get("pin", ""))
    k_ip, k_id = f"sahkan-ip:{ip}", f"sahkan-id:{no_id or 'x'}"
    if had_sekat(k_ip, 10, 900) or had_sekat(k_id, 5, 1800):
        return ralat(429, "Terlalu banyak cubaan salah. Cuba lagi dalam 30 minit atau hubungi pejabat.")
    r = None
    if no_id and len(pin) == PIN_LEN:
        r = run("""UPDATE rekod SET status='Selesai', pin_hash=NULL, selesai_pada=now(),
                   dikemas=now(), disahkan_oleh='Penerima (PIN)'
                   WHERE no_id=%s AND pin_hash=%s AND status='Surat Keluar'
                   RETURNING id, no_siri, nama, no_id, perkara""",
                (no_id, hash_pin(no_id, pin)), "one")
    if not r:
        had_tambah(k_ip, 900)
        had_tambah(k_id, 1800)
        audit("sahkan_gagal", no_id)
        flash("ID atau PIN tidak sah, atau surat belum berstatus 'Surat Keluar'.", "danger")
        return render_template("sahkan.html"), 400
    had_reset(k_id)
    tt = terima_tt(request.form.get("tt", ""))
    if tt:
        try:
            path = f"t/{uuid.uuid4().hex}.png"
            storage_put(path, tt, "image/png")
            run("UPDATE rekod SET tandatangan=%s WHERE id=%s", (path, r["id"]), "count")
        except Exception:
            lg.exception("simpan tandatangan gagal")
    audit("sahkan_ok", f"{r['no_siri']} {r['no_id']}")
    mula_arkib(r["id"])
    return render_template("selesai.html", r=r, masa=datetime.now(timezone.utc))


# ------------------------------------------------------------------ Admin
def semak_admin(pw):
    if ADMIN_HASH:
        return check_password_hash(ADMIN_HASH, pw)
    return bool(ADMIN_PLAIN) and hmac.compare_digest(pw.encode(), ADMIN_PLAIN.encode())


@app.route("/admin/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        k = f"login:{request.remote_addr}"
        if had_sekat(k, 5, 600):
            return ralat(429, "Terlalu banyak cubaan log masuk. Cuba lagi dalam 10 minit.")
        if semak_admin(request.form.get("password", "")):
            had_reset(k)
            session.clear()
            session["admin"] = True
            session["last"] = time.time()
            session.permanent = True
            audit("login_ok")
            return redirect(url_for("admin_home"))
        had_tambah(k, 600)
        audit("login_gagal")
        flash("Kata laluan salah.", "danger")
    return render_template("login.html")


@app.route("/admin/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/admin")
@admin_required
def admin_home():
    q = bersih(request.args.get("q", ""), 40)
    s = request.args.get("s", "")
    sql, args = "SELECT * FROM rekod WHERE 1=1", []
    if q:
        like = f"%{q}%"
        sql += " AND (no_id ILIKE %s OR no_siri ILIKE %s OR nama ILIKE %s)"
        args += [like, like, like]
    if s in STATUS:
        sql += " AND status=%s"
        args.append(s)
    sql += " ORDER BY id DESC LIMIT 300"
    rows = run(sql, args)
    kira = {x["status"]: x["n"] for x in run("SELECT status, count(*) n FROM rekod GROUP BY status")}
    return render_template("admin.html", rows=rows, q=q, s=s, kira=kira)


def baca_borang():
    f = request.form
    d = {
        "no_siri": bersih(f.get("no_siri"), 30),
        "no_id": norm_id(f.get("no_id")),
        "nama": bersih(f.get("nama"), 100),
        "jenis": f.get("jenis") if f.get("jenis") in ("Pelajar", "Staf") else "Pelajar",
        "perkara": bersih(f.get("perkara"), 300),
        "rujukan": bersih(f.get("rujukan"), 100),
        "tarikh_surat": None,
    }
    try:
        d["tarikh_surat"] = datetime.strptime(f.get("tarikh_surat", ""), "%Y-%m-%d").date()
    except ValueError:
        pass
    if not d["no_siri"] or not d["nama"] or not d["perkara"]:
        return d, "No. Siri, Nama dan Perkara wajib diisi."
    if not d["no_id"]:
        return d, "No. Pelajar/Staf tidak sah (3-30 aksara: huruf, nombor, / . -)."
    return d, None


@app.route("/admin/tambah", methods=["GET", "POST"])
@admin_required
def tambah():
    if request.method == "GET":
        return render_template("borang.html", r={}, baru=True)
    d, err = baca_borang()
    if err:
        flash(err, "danger")
        return render_template("borang.html", r=request.form, baru=True)
    gambar = ""
    fs = request.files.get("gambar")
    if fs and fs.filename:
        try:
            gambar = simpan_gambar(fs)
        except ValueError as e:
            flash(str(e), "danger")
            return render_template("borang.html", r=request.form, baru=True)
        except Exception:
            lg.exception("upload gambar")
            flash("Gagal memuat naik gambar. Semak tetapan Supabase.", "danger")
            return render_template("borang.html", r=request.form, baru=True)
    pin, h = pin_unik(d["no_id"])
    try:
        row = run("""INSERT INTO rekod(no_siri,no_id,nama,jenis,perkara,rujukan,tarikh_surat,gambar,pin_hash)
                     VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                  (d["no_siri"], d["no_id"], d["nama"], d["jenis"], d["perkara"], d["rujukan"],
                   d["tarikh_surat"], gambar, h), "one")
    except psycopg2.IntegrityError:
        storage_del(gambar)
        flash("No. Siri itu sudah wujud.", "danger")
        return render_template("borang.html", r=request.form, baru=True)
    audit("tambah", f"{d['no_siri']} {d['no_id']}")
    return render_template("slip.html", r=d, pin=pin, rid=row["id"])


@app.route("/admin/edit/<int:rid>", methods=["GET", "POST"])
@admin_required
def edit(rid):
    r = run("SELECT * FROM rekod WHERE id=%s", (rid,), "one") or abort(404)
    if request.method == "GET":
        return render_template("borang.html", r=r, baru=False)
    d, err = baca_borang()
    if err:
        flash(err, "danger")
        return render_template("borang.html", r=dict(r, **request.form), baru=False)
    gambar = r["gambar"] or ""
    lama = ""
    fs = request.files.get("gambar")
    if fs and fs.filename:
        try:
            baru = simpan_gambar(fs)
            lama, gambar = gambar, baru
        except ValueError as e:
            flash(str(e), "danger")
            return render_template("borang.html", r=dict(r, **request.form), baru=False)
        except Exception:
            lg.exception("upload gambar")
            flash("Gagal memuat naik gambar.", "danger")
            return render_template("borang.html", r=dict(r, **request.form), baru=False)
    pin, h = None, r["pin_hash"]
    if d["no_id"] != r["no_id"] and r["status"] != "Selesai":
        pin, h = pin_unik(d["no_id"])   # hash PIN terikat pada ID, jadi PIN baharu
    try:
        run("""UPDATE rekod SET no_siri=%s,no_id=%s,nama=%s,jenis=%s,perkara=%s,rujukan=%s,
               tarikh_surat=%s,gambar=%s,pin_hash=%s,dikemas=now() WHERE id=%s""",
            (d["no_siri"], d["no_id"], d["nama"], d["jenis"], d["perkara"], d["rujukan"],
             d["tarikh_surat"], gambar, h, rid), "count")
    except psycopg2.IntegrityError:
        if lama:
            storage_del(gambar)
        flash("No. Siri itu sudah wujud.", "danger")
        return render_template("borang.html", r=dict(r, **request.form), baru=False)
    storage_del(lama)
    audit("edit", f"{d['no_siri']}")
    if pin:
        return render_template("slip.html", r=d, pin=pin, rid=rid)
    flash("Rekod dikemas kini.", "success")
    return redirect(url_for("admin_home"))


@app.route("/admin/status/<int:rid>", methods=["POST"])
@admin_required
def ubah_status(rid):
    s = request.form.get("status")
    if s not in STATUS:
        abort(400)
    r = run("SELECT * FROM rekod WHERE id=%s", (rid,), "one") or abort(404)
    if s == "Selesai":
        run("""UPDATE rekod SET status='Selesai', pin_hash=NULL, selesai_pada=COALESCE(selesai_pada, now()),
               disahkan_oleh=CASE WHEN disahkan_oleh='' THEN 'Admin' ELSE disahkan_oleh END,
               dikemas=now() WHERE id=%s""", (rid,), "count")
        audit("status", f"{r['no_siri']} -> Selesai")
        if r["status"] != "Selesai":
            mula_arkib(rid)
        flash(f"Surat {r['no_siri']} ditanda Selesai.", "success")
        return redirect(url_for("admin_home"))
    if r["pin_hash"] is None:   # dibuka semula dari Selesai -> PIN baharu
        pin, h = pin_unik(r["no_id"])
        run("UPDATE rekod SET status=%s, pin_hash=%s, selesai_pada=NULL, dikemas=now() WHERE id=%s",
            (s, h, rid), "count")
        audit("status", f"{r['no_siri']} -> {s} (PIN baharu)")
        return render_template("slip.html", r=r, pin=pin, rid=rid)
    run("UPDATE rekod SET status=%s, dikemas=now() WHERE id=%s", (s, rid), "count")
    audit("status", f"{r['no_siri']} -> {s}")
    flash(f"Status surat {r['no_siri']}: {s}.", "success")
    return redirect(url_for("admin_home"))


@app.route("/admin/pin/<int:rid>", methods=["POST"])
@admin_required
def jana_semula_pin(rid):
    r = run("SELECT * FROM rekod WHERE id=%s", (rid,), "one") or abort(404)
    if r["status"] == "Selesai":
        flash("Surat sudah Selesai; PIN tidak diperlukan.", "warning")
        return redirect(url_for("admin_home"))
    pin, h = pin_unik(r["no_id"])
    run("UPDATE rekod SET pin_hash=%s, dikemas=now() WHERE id=%s", (h, rid), "count")
    audit("pin_baharu", r["no_siri"])
    return render_template("slip.html", r=r, pin=pin, rid=rid)


@app.route("/admin/padam/<int:rid>", methods=["POST"])
@admin_required
def padam(rid):
    r = run("DELETE FROM rekod WHERE id=%s RETURNING no_siri, gambar, tandatangan", (rid,), "one")
    if r:
        storage_del(r["gambar"])
        storage_del(r["tandatangan"])
        audit("padam", r["no_siri"])
        flash(f"Surat {r['no_siri']} dipadam.", "success")
    return redirect(url_for("admin_home"))


@app.route("/admin/arkib/<int:rid>", methods=["POST"])
@admin_required
def arkib_semula(rid):
    r = run("SELECT status FROM rekod WHERE id=%s", (rid,), "one") or abort(404)
    if r["status"] == "Selesai":
        mula_arkib(rid)
        flash("Arkib dihantar ke Google Drive. Muat semula halaman untuk lihat status.", "info")
    return redirect(url_for("admin_home"))


@app.route("/admin/fail/<jenis>/<int:rid>")
@admin_required
def fail_admin(jenis, rid):
    if jenis not in ("gambar", "tt"):
        abort(404)
    r = run("SELECT gambar, tandatangan FROM rekod WHERE id=%s", (rid,), "one")
    path = (r["gambar"] if jenis == "gambar" else r["tandatangan"]) if r else ""
    if not path:
        abort(404)
    data = storage_get(path)
    if data is None:
        abort(404)
    return Response(data, mimetype="image/jpeg" if jenis == "gambar" else "image/png",
                    headers={"X-Content-Type-Options": "nosniff"})


@app.route("/admin/cetak")
@admin_required
def cetak():
    s = request.args.get("s", "")
    if s in STATUS:
        rows = run("SELECT * FROM rekod WHERE status=%s ORDER BY id DESC LIMIT 1000", (s,))
    else:
        rows = run("SELECT * FROM rekod ORDER BY id DESC LIMIT 1000")
    return render_template("cetak.html", rows=rows, s=s)


@app.route("/admin/poster")
@admin_required
def poster():
    return render_template("poster.html")


@app.route("/admin/log")
@admin_required
def log_view():
    rows = run("SELECT * FROM log_audit ORDER BY id DESC LIMIT 200")
    return render_template("log.html", rows=rows)


# ------------------------------------------------------------------ Templates
TEMPLATES = {}

TEMPLATES["base.html"] = """<!doctype html>
<html lang="ms"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
{% block robots %}<meta name="robots" content="noindex,nofollow">{% endblock %}
<title>{% block title %}Rekod Surat Keluar{% endblock %} | UiTM Sabah</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
<style nonce="{{ nonce }}">
body{background:#f4f5f9}
.navbar{background:#4b1d6b}
.navbar-brand{color:#fff!important;font-weight:600;line-height:1.2}
.navbar img{height:42px;margin-right:12px}
.banner{width:100%;max-height:230px;object-fit:cover;display:block}
.kad{border-left:6px solid #adb5bd}
.kad.Direkod{border-left-color:#fd7e14}
.kad.Tindakan{border-left-color:#0d6efd}
.kad.SuratKeluar{border-left-color:#198754}
.kad.Selesai{border-left-color:#6c757d}
summary{cursor:pointer;list-style:none}
summary::-webkit-details-marker{display:none}
.pinbesar{font-size:3.2rem;letter-spacing:.6rem;font-weight:700;font-family:monospace}
#pad{border:2px dashed #999;border-radius:8px;width:100%;height:180px;touch-action:none;background:#fff}
@media print{.noprint{display:none!important}body{background:#fff}}
</style></head><body>
<nav class="navbar noprint"><div class="container">
<a class="navbar-brand d-flex align-items-center" href="{{ url_for('index') }}">
{% if logo_ada %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="UiTM">{% endif %}
<span>Rekod Surat Keluar<br><small class="fw-normal">Pejabat Rektor &middot; UiTM Cawangan Sabah</small></span></a>
{% if is_admin %}<a class="btn btn-sm btn-light" href="{{ url_for('admin_home') }}">Admin</a>{% endif %}
</div></nav>
{% block banner %}{% endblock %}
<main class="container py-4">
{% for c,m in get_flashed_messages(with_categories=true) %}<div class="alert alert-{{ c }}">{{ m }}</div>{% endfor %}
{% block content %}{% endblock %}
</main>
<footer class="text-center text-muted small pb-4 noprint">&copy; 2026 Pejabat Rektor, UiTM Cawangan Sabah</footer>
<script nonce="{{ nonce }}">
document.addEventListener('submit',function(e){var m=e.target.getAttribute('data-confirm');if(m&&!confirm(m))e.preventDefault();});
document.addEventListener('click',function(e){if(e.target.closest('[data-print]'))window.print();});
</script>
{% block js %}{% endblock %}
</body></html>"""

TEMPLATES["ralat.html"] = """{% extends "base.html" %}
{% block content %}<div class="card shadow-sm"><div class="card-body text-center py-5">
<div class="display-4">{{ kod }}</div><p class="lead">{{ mesej }}</p>
<a class="btn btn-primary" href="{{ url_for('index') }}">Kembali</a></div></div>{% endblock %}"""

TEMPLATES["index.html"] = """{% extends "base.html" %}
{% block robots %}{% if not allow_index %}<meta name="robots" content="noindex,nofollow">{% endif %}{% endblock %}
{% block banner %}{% if banner_fail %}<img class="banner" src="{{ url_for('static', filename='img/'+banner_fail) }}" alt="UiTM Cawangan Sabah">{% endif %}{% endblock %}
{% block content %}
{% set warna = {'Direkod':'bg-warning text-dark','Tindakan':'bg-primary','Surat Keluar':'bg-success','Selesai':'bg-secondary'} %}
<div class="card shadow-sm mb-4"><div class="card-body">
<h1 class="h4">Semakan Surat Keluar</h1>
<p class="text-muted">Masukkan <b>No. Pelajar</b> atau <b>No. Staf</b>. Tiada akaun diperlukan.</p>
<form method="get" class="input-group input-group-lg">
<input name="id" value="{{ q }}" class="form-control" placeholder="Contoh: 2021123456" maxlength="30" autocomplete="off" required>
<button class="btn btn-primary">&#128269; Semak</button></form>
<a class="btn btn-outline-success btn-sm mt-3" href="{{ url_for('sahkan') }}">&#9997;&#65039; Sudah ambil surat? Sahkan pengambilan (guna PIN)</a>
</div></div>
{% if rows is not none %}
{% if rows %}<h2 class="h5 mb-3">{{ rows|length }} surat untuk <b>{{ q }}</b></h2>
{% for r in rows %}
<div class="card kad {{ r.status|replace(' ','') }} shadow-sm mb-3"><div class="card-body">
<div class="d-flex justify-content-between align-items-start">
<div><small class="text-muted">No. Siri</small> <b>{{ r.no_siri }}</b></div>
<span class="badge rounded-pill {{ warna[r.status] }}">{{ r.status }}</span></div>
{% if r.status == 'Surat Keluar' %}<div class="alert alert-success py-2 my-2 small">Surat sudah keluar. Sila ambil di kaunter dengan <b>kad pelajar/staf</b> dan <b>PIN</b> anda.</div>{% endif %}
{% if r.ada_pin %}
<details class="mt-2"><summary class="btn btn-outline-secondary btn-sm">&#128274; Perkara sulit &ndash; klik untuk lihat</summary>
<form method="post" action="{{ url_for('lihat') }}" class="row g-2 mt-1">
<input type="hidden" name="csrf" value="{{ csrf_token() }}">
<input type="hidden" name="id" value="{{ r.id }}"><input type="hidden" name="no_id" value="{{ q }}">
<div class="col"><input name="pin" class="form-control" inputmode="numeric" pattern="[0-9]*" maxlength="8" autocomplete="off" placeholder="PIN {{ pin_len }} digit" required></div>
<div class="col-auto"><button class="btn btn-primary">Buka</button></div></form></details>
{% elif r.status == 'Selesai' %}<div class="text-muted small mt-2">&#10004; Surat telah diambil. Butiran tidak lagi dipaparkan.</div>
{% else %}<div class="text-muted small mt-2">PIN belum dijana. Sila hubungi pejabat.</div>{% endif %}
<div class="small text-muted mt-2">Kemas kini: {{ r.dikemas|tarikh }}</div>
</div></div>
{% endfor %}
{% else %}<div class="alert alert-info">Tiada surat direkodkan untuk ID ini.</div>{% endif %}
{% endif %}
{% endblock %}"""

TEMPLATES["lihat.html"] = """{% extends "base.html" %}
{% block content %}<div class="card shadow-sm"><div class="card-body">
<h1 class="h5">&#128275; Butiran Surat No. Siri {{ r.no_siri }}</h1>
<dl class="row mb-3">
<dt class="col-sm-3">Perkara</dt><dd class="col-sm-9">{{ r.perkara }}</dd>
<dt class="col-sm-3">Rujukan</dt><dd class="col-sm-9">{{ r.rujukan or '-' }}</dd>
<dt class="col-sm-3">Tarikh surat</dt><dd class="col-sm-9">{{ r.tarikh_surat|tarikh }}</dd>
<dt class="col-sm-3">Status</dt><dd class="col-sm-9">{{ r.status }}</dd></dl>
<p class="text-muted small">Halaman ini tidak disimpan. Jangan kongsi PIN anda.</p>
<a class="btn btn-secondary" href="{{ url_for('index') }}">Kembali</a></div></div>{% endblock %}"""

TEMPLATES["sahkan.html"] = """{% extends "base.html" %}
{% block banner %}{% if banner_fail %}<img class="banner" src="{{ url_for('static', filename='img/'+banner_fail) }}" alt="UiTM Cawangan Sabah">{% endif %}{% endblock %}
{% block content %}<div class="card shadow-sm"><div class="card-body">
<h1 class="h4">&#9997;&#65039; Sahkan Pengambilan Surat</h1>
<p class="text-muted">Masukkan No. Pelajar/Staf dan PIN pada slip anda. Tunjukkan skrin hijau kepada staf kaunter.</p>
<form method="post" id="sahkanform" autocomplete="off">
<input type="hidden" name="csrf" value="{{ csrf_token() }}"><input type="hidden" name="tt" id="tt">
<div class="mb-3"><label class="form-label">No. Pelajar / No. Staf</label>
<input name="no_id" class="form-control form-control-lg" maxlength="30" required></div>
<div class="mb-3"><label class="form-label">PIN ({{ pin_len }} digit)</label>
<input name="pin" class="form-control form-control-lg" inputmode="numeric" pattern="[0-9]*" maxlength="8" required></div>
<div class="mb-2"><label class="form-label">Tandatangan (pilihan)</label><canvas id="pad"></canvas>
<button type="button" id="padam" class="btn btn-sm btn-outline-secondary mt-1">Padam tandatangan</button></div>
<button class="btn btn-success btn-lg w-100 mt-3">&#10004; Sahkan Saya Telah Ambil Surat</button>
</form></div></div>{% endblock %}
{% block js %}<script nonce="{{ nonce }}">
(function(){
var c=document.getElementById('pad'),f=document.getElementById('sahkanform'),h=document.getElementById('tt'),x=c.getContext('2d'),d=false,ada=false;
var r0=c.getBoundingClientRect();c.width=r0.width;c.height=r0.height;x.lineWidth=2.5;x.lineCap='round';x.strokeStyle='#111';
function p(e){var r=c.getBoundingClientRect();return [e.clientX-r.left,e.clientY-r.top];}
c.addEventListener('pointerdown',function(e){d=true;ada=true;var a=p(e);x.beginPath();x.moveTo(a[0],a[1]);c.setPointerCapture(e.pointerId);});
c.addEventListener('pointermove',function(e){if(!d)return;var a=p(e);x.lineTo(a[0],a[1]);x.stroke();});
c.addEventListener('pointerup',function(){d=false;});
document.getElementById('padam').addEventListener('click',function(){x.clearRect(0,0,c.width,c.height);ada=false;});
f.addEventListener('submit',function(){h.value=ada?c.toDataURL('image/png'):'';});
})();
</script>{% endblock %}"""

TEMPLATES["selesai.html"] = """{% extends "base.html" %}
{% block content %}<div class="card border-success border-3 shadow"><div class="card-body text-center py-5">
<div class="display-1 text-success">&#10004;</div>
<h1 class="text-success">PENGAMBILAN DISAHKAN</h1>
<p class="fs-4 mb-1"><b>{{ r.nama }}</b></p>
<p class="mb-1">No. ID: <b>{{ r.no_id }}</b> &nbsp;|&nbsp; No. Siri: <b>{{ r.no_siri }}</b></p>
<p class="mb-1">Perkara: {{ r.perkara }}</p>
<p class="text-muted">{{ masa|waktu }}</p>
<div class="alert alert-warning d-inline-block">Staf kaunter: semak <b>kad pelajar/staf</b> sebelum menyerahkan surat.</div>
</div></div>{% endblock %}"""

TEMPLATES["slip.html"] = """{% extends "base.html" %}
{% block content %}<div class="card shadow-sm"><div class="card-body text-center">
{% if logo_ada %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="UiTM" height="70" class="mb-2">{% endif %}
<h1 class="h5">SLIP PIN SURAT &mdash; UiTM Cawangan Sabah</h1>
<p class="mb-1">No. Siri: <b>{{ r.no_siri }}</b></p>
<p class="mb-1">No. ID: <b>{{ r.no_id }}</b> &nbsp; Nama: <b>{{ r.nama }}</b></p>
<p class="mt-3 mb-0 text-muted">PIN anda</p>
<div class="pinbesar">{{ pin }}</div>
<ul class="text-start small mx-auto mt-3" style="max-width:480px">
<li>Simpan PIN ini. <b>PIN hanya dipaparkan sekali</b>.</li>
<li>Gunakan No. ID + PIN untuk melihat tajuk surat dan mengesahkan pengambilan.</li>
<li>Semak status di: {{ url_for('index', _external=True) }}</li>
<li>Jangan kongsi PIN dengan orang lain.</li></ul>
<div class="noprint mt-3"><button class="btn btn-primary" data-print>&#128424;&#65039; Cetak</button>
<a class="btn btn-secondary" href="{{ url_for('admin_home') }}">Kembali ke senarai</a></div>
</div></div>{% endblock %}"""

TEMPLATES["poster.html"] = """{% extends "base.html" %}
{% block content %}<div class="noprint mb-3"><button class="btn btn-primary" data-print>&#128424;&#65039; Cetak poster</button>
<a class="btn btn-secondary" href="{{ url_for('admin_home') }}">Kembali</a></div>
<div class="card shadow-sm"><div class="card-body text-center py-5">
{% if logo_ada %}<img src="{{ url_for('static', filename='img/logo.png') }}" alt="UiTM" height="90" class="mb-3">{% endif %}
<h1>Semak Status Surat Anda</h1>
<p class="lead">Imbas kod QR atau layari pautan di bawah</p>
<img src="{{ url_for('qr_png') }}" alt="QR" width="320" height="320">
<p class="fs-5 mt-3">{{ url_for('index', _external=True) }}</p>
<p>Pejabat Rektor &middot; UiTM Cawangan Sabah</p></div></div>{% endblock %}"""

TEMPLATES["login.html"] = """{% extends "base.html" %}
{% block banner %}{% if banner_fail %}<img class="banner" src="{{ url_for('static', filename='img/'+banner_fail) }}" alt="UiTM Cawangan Sabah">{% endif %}{% endblock %}
{% block content %}<div class="card shadow-sm mx-auto" style="max-width:420px"><div class="card-body">
<h1 class="h5">Log Masuk Admin</h1>
<form method="post"><input type="hidden" name="csrf" value="{{ csrf_token() }}">
<input type="password" name="password" class="form-control mb-3" placeholder="Kata laluan" autocomplete="current-password" required>
<button class="btn btn-primary w-100">Log Masuk</button></form></div></div>{% endblock %}"""

TEMPLATES["admin.html"] = """{% extends "base.html" %}
{% block content %}
<div class="d-flex flex-wrap gap-2 mb-3 noprint">
<a class="btn btn-primary" href="{{ url_for('tambah') }}">&#10133; Tambah Surat</a>
<a class="btn btn-outline-dark" href="{{ url_for('cetak') }}">&#128424;&#65039; Cetak Senarai</a>
<a class="btn btn-outline-dark" href="{{ url_for('poster') }}">&#128241; Poster QR</a>
<a class="btn btn-outline-dark" href="{{ url_for('log_view') }}">&#128220; Log</a>
<form method="post" action="{{ url_for('logout') }}"><input type="hidden" name="csrf" value="{{ csrf_token() }}"><button class="btn btn-outline-danger">Log Keluar</button></form>
</div>
<div class="mb-3">{% for s in statuses %}<a class="badge bg-secondary text-decoration-none me-1" href="{{ url_for('admin_home', s=s) }}">{{ s }}: {{ kira.get(s,0) }}</a>{% endfor %}
<a class="small" href="{{ url_for('admin_home') }}">Semua</a></div>
<form class="input-group mb-3" method="get"><input name="q" value="{{ q }}" class="form-control" placeholder="Cari No. ID / No. Siri / Nama"><button class="btn btn-outline-primary">Cari</button></form>
<div class="table-responsive"><table class="table table-sm table-bordered align-middle bg-white">
<thead><tr><th>Siri</th><th>ID / Nama</th><th>Perkara</th><th>Status</th><th>PIN</th><th>Fail</th><th></th></tr></thead><tbody>
{% for r in rows %}<tr>
<td>{{ r.no_siri }}</td>
<td>{{ r.no_id }}<br><small>{{ r.nama }} ({{ r.jenis }})</small></td>
<td>{{ r.perkara }}<br><small class="text-muted">{{ r.rujukan }}</small></td>
<td><form method="post" action="{{ url_for('ubah_status', rid=r.id) }}" class="d-flex gap-1">
<input type="hidden" name="csrf" value="{{ csrf_token() }}">
<select name="status" class="form-select form-select-sm">{% for s in statuses %}<option {{ 'selected' if s==r.status }}>{{ s }}</option>{% endfor %}</select>
<button class="btn btn-sm btn-outline-primary">OK</button></form></td>
<td>{% if r.pin_hash %}&#128273;{% elif r.status=='Selesai' %}&mdash;{% else %}&#9888;&#65039;{% endif %}</td>
<td>{% if r.gambar %}<a href="{{ url_for('fail_admin', jenis='gambar', rid=r.id) }}" target="_blank">&#128444;&#65039;</a>{% endif %}
{% if r.tandatangan %}<a href="{{ url_for('fail_admin', jenis='tt', rid=r.id) }}" target="_blank">&#9997;&#65039;</a>{% endif %}
{% if r.status=='Selesai' %}{% if (r.arkib or '').startswith('OK') %}<span title="{{ r.arkib }}">&#128452;&#65039;&#10004;</span>
{% elif arkib_aktif %}<form method="post" action="{{ url_for('arkib_semula', rid=r.id) }}" class="d-inline"><input type="hidden" name="csrf" value="{{ csrf_token() }}"><button class="btn btn-sm btn-outline-secondary">&#128452;&#65039; Arkib</button></form>{% endif %}{% endif %}</td>
<td class="text-nowrap">
<a class="btn btn-sm btn-outline-secondary" href="{{ url_for('edit', rid=r.id) }}">Edit</a>
{% if r.status != 'Selesai' %}<form method="post" action="{{ url_for('jana_semula_pin', rid=r.id) }}" class="d-inline" data-confirm="Jana PIN baharu? PIN lama tidak sah lagi."><input type="hidden" name="csrf" value="{{ csrf_token() }}"><button class="btn btn-sm btn-outline-warning">&#128273; PIN</button></form>{% endif %}
<form method="post" action="{{ url_for('padam', rid=r.id) }}" class="d-inline" data-confirm="Padam surat {{ r.no_siri }}?"><input type="hidden" name="csrf" value="{{ csrf_token() }}"><button class="btn btn-sm btn-outline-danger">Padam</button></form></td>
</tr>{% else %}<tr><td colspan="7" class="text-center text-muted">Tiada rekod.</td></tr>{% endfor %}
</tbody></table></div>{% endblock %}"""

TEMPLATES["borang.html"] = """{% extends "base.html" %}
{% block content %}<div class="card shadow-sm"><div class="card-body">
<h1 class="h5">{{ 'Tambah Surat Baharu' if baru else 'Edit Surat' }}</h1>
<form method="post" enctype="multipart/form-data" class="row g-3">
<input type="hidden" name="csrf" value="{{ csrf_token() }}">
<div class="col-md-4"><label class="form-label">No. Siri *</label><input name="no_siri" class="form-control" value="{{ r.get('no_siri') or '' }}" maxlength="30" required></div>
<div class="col-md-4"><label class="form-label">No. Pelajar / Staf *</label><input name="no_id" class="form-control" value="{{ r.get('no_id') or '' }}" maxlength="30" required></div>
<div class="col-md-4"><label class="form-label">Jenis</label><select name="jenis" class="form-select">{% for j in ['Pelajar','Staf'] %}<option {{ 'selected' if r.get('jenis')==j }}>{{ j }}</option>{% endfor %}</select></div>
<div class="col-md-6"><label class="form-label">Nama *</label><input name="nama" class="form-control" value="{{ r.get('nama') or '' }}" maxlength="100" required></div>
<div class="col-md-6"><label class="form-label">Rujukan</label><input name="rujukan" class="form-control" value="{{ r.get('rujukan') or '' }}" maxlength="100"></div>
<div class="col-md-9"><label class="form-label">Perkara (tajuk surat) *</label><input name="perkara" class="form-control" value="{{ r.get('perkara') or '' }}" maxlength="300" required></div>
<div class="col-md-3"><label class="form-label">Tarikh surat</label><input type="date" name="tarikh_surat" class="form-control" value="{{ r.get('tarikh_surat') or '' }}"></div>
<div class="col-12"><label class="form-label">Gambar surat (JPG/PNG/WEBP, maks 6 MB){% if not baru and r.get('gambar') %} &mdash; sudah ada; muat naik untuk ganti{% endif %}</label>
<input type="file" name="gambar" class="form-control" accept="image/jpeg,image/png,image/webp"></div>
<div class="col-12"><button class="btn btn-primary">Simpan</button> <a class="btn btn-secondary" href="{{ url_for('admin_home') }}">Batal</a></div>
</form>{% if baru %}<p class="text-muted small mt-3 mb-0">PIN {{ pin_len }} digit dijana automatik dan dipaparkan pada slip selepas simpan.</p>{% endif %}
</div></div>{% endblock %}"""

TEMPLATES["cetak.html"] = """{% extends "base.html" %}
{% block content %}<div class="noprint mb-3"><button class="btn btn-primary" data-print>&#128424;&#65039; Cetak</button>
<a class="btn btn-secondary" href="{{ url_for('admin_home') }}">Kembali</a></div>
<h1 class="h5">Senarai Surat Keluar 2026{% if s %} &ndash; {{ s }}{% endif %}</h1>
<table class="table table-sm table-bordered bg-white"><thead><tr><th>Siri</th><th>ID</th><th>Nama</th><th>Perkara</th><th>Status</th><th>Kemas kini</th></tr></thead><tbody>
{% for r in rows %}<tr><td>{{ r.no_siri }}</td><td>{{ r.no_id }}</td><td>{{ r.nama }}</td><td>{{ r.perkara }}</td><td>{{ r.status }}</td><td>{{ r.dikemas|tarikh }}</td></tr>{% endfor %}
</tbody></table>{% endblock %}"""

TEMPLATES["log.html"] = """{% extends "base.html" %}
{% block content %}<h1 class="h5">Log Audit (200 terkini)</h1>
<div class="table-responsive"><table class="table table-sm table-bordered bg-white"><thead><tr><th>Masa</th><th>Siapa</th><th>Aksi</th><th>Butiran</th><th>IP</th></tr></thead><tbody>
{% for r in rows %}<tr><td>{{ r.masa|waktu }}</td><td>{{ r.siapa }}</td><td>{{ r.aksi }}</td><td>{{ r.butiran }}</td><td>{{ r.ip }}</td></tr>{% endfor %}
</tbody></table></div>{% endblock %}"""

app.jinja_loader = DictLoader(TEMPLATES)

if __name__ == "__main__":
    app.run(debug=not PROD, port=5000)
