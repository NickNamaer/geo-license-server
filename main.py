import os
import json
import hmac
import uuid
import sqlite3
import hashlib
import secrets
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Request, Response, Cookie
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

APP_NAME = "GeoTivity License Server"
from pathlib import Path

DEFAULT_DB_PATH = "data/licenses.db"

DB_PATH = os.environ.get("GEOTIVITY_DB_PATH", DEFAULT_DB_PATH)
DB_PATH = str(Path(DB_PATH))

print(f"[startup] DB_PATH = {DB_PATH}")
SECRET = os.environ.get("GEOTIVITY_SECRET", "CHANGE_THIS_SECRET")
TRIAL_DAYS = int(os.environ.get("GEOTIVITY_TRIAL_DAYS", "30"))
TRIAL_AREA_LIMIT_HA = float(os.environ.get("GEOTIVITY_TRIAL_AREA_LIMIT_HA", "5.0"))
ADMIN_TOKEN = os.environ.get("GEOTIVITY_ADMIN_TOKEN", "CHANGE_THIS_ADMIN_TOKEN")
SESSION_SECRET = os.environ.get("GEOTIVITY_SESSION_SECRET", "CHANGE_THIS_SESSION_SECRET")
SESSION_COOKIE_NAME = os.environ.get("GEOTIVITY_SESSION_COOKIE_NAME", "geotivity_admin_session")
SESSION_TTL_HOURS = int(os.environ.get("GEOTIVITY_SESSION_TTL_HOURS", "12"))
PRODUCT_NAME = "GeoTivity"

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "http://192.168.1.12:5173",
        "http://192.168.1.12:5174",
        "http://172.20.240.1:5173",
        "http://172.20.240.1:5174",
        "https://geotivity.jp",
        "https://www.geotivity.jp",
        "https://admin.geotivity.jp",
        "https://geotivity-license-admin.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/admin/licenses")
def get_admin_licenses(
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                license_key,
                license_type,
                status,
                max_machines,
                current_machine_count,
                plan_code,
                support_enabled,
                support_until
            FROM licenses
            ORDER BY id DESC
            """
        ).fetchall()

        result = []
        for row in rows:
            result.append(
                {
                    "license_key": row["license_key"],
                    "license_type": row["license_type"],
                    "status": row["status"],
                    "max_machines": row["max_machines"],
                    "current_machine_count": row["current_machine_count"],
                    "plan_code": row["plan_code"],
                    "support_enabled": bool(row["support_enabled"]),
                    "support_until": row["support_until"],
                }
            )
        return result
    finally:
        conn.close()

# =========================
# DB
# =========================

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    db_dir = Path(DB_PATH).parent
    db_dir.mkdir(parents=True, exist_ok=True)

    print(f"[startup] DB_DIR = {db_dir}")

    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS licenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                license_key TEXT NOT NULL UNIQUE,
                license_type TEXT NOT NULL,           -- trial / full
                status TEXT NOT NULL,                 -- active / inactive / expired
                issued_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                area_limit_ha REAL NOT NULL DEFAULT 0.0,
                machine_id TEXT,
                note TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        migrate_schema_step1(conn)
        conn.commit()

def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        if row["name"] == column_name:
            return True
    return False


def add_column_if_not_exists(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    column_sql: str,
) -> None:
    if not column_exists(conn, table_name, column_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")


def migrate_schema_step1(conn: sqlite3.Connection) -> None:
    # ==================================================
    # licenses table expansion
    # ==================================================
    add_column_if_not_exists(conn, "licenses", "customer_id", "INTEGER")
    add_column_if_not_exists(conn, "licenses", "product_code", "TEXT NOT NULL DEFAULT 'GeoTivity'")
    add_column_if_not_exists(conn, "licenses", "plan_code", "TEXT NOT NULL DEFAULT 'trial'")
    add_column_if_not_exists(conn, "licenses", "max_machines", "INTEGER NOT NULL DEFAULT 1")
    add_column_if_not_exists(conn, "licenses", "current_machine_count", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_not_exists(conn, "licenses", "last_verified_at", "TEXT")
    add_column_if_not_exists(conn, "licenses", "next_verify_due_at", "TEXT")
    add_column_if_not_exists(conn, "licenses", "offline_grace_until", "TEXT")
    add_column_if_not_exists(conn, "licenses", "support_until", "TEXT")
    add_column_if_not_exists(conn, "licenses", "support_enabled", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_not_exists(conn, "licenses", "is_perpetual", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_not_exists(conn, "licenses", "disabled_reason", "TEXT DEFAULT ''")
    add_column_if_not_exists(conn, "licenses", "issued_to_name", "TEXT DEFAULT ''")
    add_column_if_not_exists(conn, "licenses", "issued_to_email", "TEXT DEFAULT ''")
    add_column_if_not_exists(conn, "licenses", "metadata_json", "TEXT DEFAULT '{}'")

    # ==================================================
    # customers
    # ==================================================
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            company_name TEXT DEFAULT '',
            email TEXT DEFAULT '',
            note TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # ==================================================
    # machines
    # 1 license に複数マシンを持てるようにする
    # ただし現行 API は licenses.machine_id も残して互換維持
    # ==================================================
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS machines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_id INTEGER NOT NULL,
            machine_id TEXT NOT NULL,
            machine_name TEXT DEFAULT '',
            os_info TEXT DEFAULT '',
            app_version TEXT DEFAULT '',
            first_verified_at TEXT NOT NULL,
            last_verified_at TEXT NOT NULL,
            released_at TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(license_id, machine_id),
            FOREIGN KEY (license_id) REFERENCES licenses(id)
        )
        """
    )

    # ==================================================
    # verify logs
    # ==================================================
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS license_verifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_id INTEGER,
            license_key TEXT NOT NULL,
            machine_id TEXT DEFAULT '',
            product TEXT DEFAULT '',
            version TEXT DEFAULT '',
            client_time TEXT,
            server_time TEXT NOT NULL,
            success INTEGER NOT NULL DEFAULT 0,
            error_code TEXT DEFAULT '',
            message TEXT DEFAULT '',
            remote_addr TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY (license_id) REFERENCES licenses(id)
        )
        """
    )

    # ==================================================
    # support contracts
    # ==================================================
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS support_contracts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            license_id INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            note TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (license_id) REFERENCES licenses(id)
        )
        """
    )

    # ==================================================
    # admin users
    # ==================================================
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            display_name TEXT DEFAULT '',
            role TEXT NOT NULL DEFAULT 'admin',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    # ==================================================
    # admin audit logs
    # ==================================================
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_username TEXT NOT NULL,
            action TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_key TEXT DEFAULT '',
            detail_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        """
    )

    # ==================================================
    # indexes
    # ==================================================
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_licenses_license_key
        ON licenses(license_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_licenses_status
        ON licenses(status)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_machines_license_id
        ON machines(license_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_machines_machine_id
        ON machines(machine_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_license_verifications_license_key
        ON license_verifications(license_key)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_license_verifications_created_at
        ON license_verifications(created_at)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_support_contracts_license_id
        ON support_contracts(license_id)
        """
    )

    # ==================================================
    # backfill licenses
    # ==================================================
    now = today_utc_str()

    conn.execute(
        """
        UPDATE licenses
        SET product_code = COALESCE(NULLIF(product_code, ''), 'GeoTivity')
        """
    )

    conn.execute(
        """
        UPDATE licenses
        SET plan_code = CASE
            WHEN license_type = 'trial' THEN 'trial'
            WHEN license_type = 'full' THEN 'full_perpetual'
            ELSE COALESCE(NULLIF(plan_code, ''), 'trial')
        END
        """
    )

    conn.execute(
        """
        UPDATE licenses
        SET is_perpetual = CASE
            WHEN license_type = 'full' THEN 1
            ELSE 0
        END
        """
    )

    conn.execute(
        """
        UPDATE licenses
        SET support_enabled = CASE
            WHEN license_type = 'full' THEN 1
            ELSE COALESCE(support_enabled, 0)
        END
        """
    )

    conn.execute(
        """
        UPDATE licenses
        SET max_machines = CASE
            WHEN max_machines IS NULL OR max_machines <= 0 THEN 1
            ELSE max_machines
        END
        """
    )

    conn.execute(
        """
        UPDATE licenses
        SET current_machine_count = CASE
            WHEN machine_id IS NOT NULL AND TRIM(machine_id) <> '' THEN 1
            ELSE 0
        END
        """
    )

    conn.execute(
        """
        UPDATE licenses
        SET last_verified_at = COALESCE(last_verified_at, updated_at, created_at, ?)
        """,
        (now,)
    )

    rows = conn.execute(
        """
        SELECT id, license_type, expires_at, last_verified_at
        FROM licenses
        """
    ).fetchall()

    for row in rows:
        license_id = int(row["id"])
        license_type = str(row["license_type"] or "")
        expires_at = str(row["expires_at"] or "")
        last_verified_at = str(row["last_verified_at"] or now)

        if license_type == "trial":
            next_verify_due_at = expires_at
        else:
            try:
                next_verify_due_at = add_days_str(last_verified_at, 30)
            except Exception:
                next_verify_due_at = add_days_str(now, 30)

        conn.execute(
            """
            UPDATE licenses
            SET next_verify_due_at = COALESCE(next_verify_due_at, ?)
            WHERE id = ?
            """,
            (next_verify_due_at, license_id),
        )

    # 既存 machine_id を machines に反映
    license_rows = conn.execute(
        """
        SELECT id, machine_id, created_at, updated_at
        FROM licenses
        WHERE machine_id IS NOT NULL AND TRIM(machine_id) <> ''
        """
    ).fetchall()

    for row in license_rows:
        license_id = int(row["id"])
        machine_id = str(row["machine_id"]).strip()
        created_at = str(row["created_at"] or now)
        updated_at = str(row["updated_at"] or now)

        exists = conn.execute(
            """
            SELECT id
            FROM machines
            WHERE license_id = ? AND machine_id = ?
            """,
            (license_id, machine_id),
        ).fetchone()

        if exists is None:
            conn.execute(
                """
                INSERT INTO machines (
                    license_id, machine_id, machine_name, os_info, app_version,
                    first_verified_at, last_verified_at, released_at, is_active,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    license_id,
                    machine_id,
                    "",
                    "",
                    "",
                    created_at,
                    updated_at,
                    None,
                    1,
                    created_at,
                    updated_at,
                ),
            )

    # 初期 admin ユーザーの暫定作成
    # username: admin
    # password_hash: 平文ではなく最低限 SHA256 を暫定利用
    # 次ステップで bcrypt/passlib に置き換える
    admin_exists = conn.execute(
        """
        SELECT id FROM admin_users WHERE username = 'admin'
        """
    ).fetchone()

    if admin_exists is None:
        seed_password = os.environ.get("GEOTIVITY_INITIAL_ADMIN_PASSWORD", "admin1234!")
        password_hash = hashlib.sha256(seed_password.encode("utf-8")).hexdigest()

        conn.execute(
            """
            INSERT INTO admin_users (
                username, password_hash, display_name, role, is_active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "admin",
                password_hash,
                "Default Admin",
                "admin",
                1,
                now,
                now,
            ),
        )

def get_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "").strip()
    if xff:
        return xff.split(",")[0].strip()
    client = getattr(request, "client", None)
    if client and getattr(client, "host", None):
        return str(client.host)
    return ""


def log_verification(
    conn: sqlite3.Connection,
    *,
    license_id: Optional[int],
    license_key: str,
    machine_id: str,
    product: str,
    version: str,
    client_time: Optional[str],
    success: bool,
    error_code: str,
    message: str,
    remote_addr: str,
) -> None:
    now = today_utc_str()
    conn.execute(
        """
        INSERT INTO license_verifications (
            license_id,
            license_key,
            machine_id,
            product,
            version,
            client_time,
            server_time,
            success,
            error_code,
            message,
            remote_addr,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            license_id,
            license_key,
            machine_id,
            product,
            version,
            client_time,
            now,
            1 if success else 0,
            error_code,
            message,
            remote_addr,
            now,
        ),
    )


def upsert_machine_binding(
    conn: sqlite3.Connection,
    *,
    license_id: int,
    machine_id: str,
    product_version: str,
) -> None:
    if not machine_id.strip():
        return

    now = today_utc_str()

    row = conn.execute(
        """
        SELECT id, is_active
        FROM machines
        WHERE license_id = ? AND machine_id = ?
        """,
        (license_id, machine_id),
    ).fetchone()

    if row is None:
        conn.execute(
            """
            INSERT INTO machines (
                license_id,
                machine_id,
                machine_name,
                os_info,
                app_version,
                first_verified_at,
                last_verified_at,
                released_at,
                is_active,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                license_id,
                machine_id,
                "",
                "",
                product_version,
                now,
                now,
                None,
                1,
                now,
                now,
            ),
        )
    else:
        conn.execute(
            """
            UPDATE machines
            SET app_version = ?,
                last_verified_at = ?,
                released_at = NULL,
                is_active = 1,
                updated_at = ?
            WHERE id = ?
            """,
            (
                product_version,
                now,
                now,
                int(row["id"]),
            ),
        )

    active_count_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM machines
        WHERE license_id = ? AND is_active = 1
        """
    ,
        (license_id,),
    ).fetchone()

    active_count = int(active_count_row[0] or 0)

    conn.execute(
        """
        UPDATE licenses
        SET current_machine_count = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (active_count, now, license_id),
    )


def update_license_verify_timestamps(
    conn: sqlite3.Connection,
    *,
    license_id: int,
    license_type: str,
    expires_at: Optional[str],
) -> None:
    now = today_utc_str()

    if (license_type or "").lower() == "trial":
        next_due = expires_at or now
    else:
        next_due = add_days_str(now, 30)

    conn.execute(
        """
        UPDATE licenses
        SET last_verified_at = ?,
            next_verify_due_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (now, next_due, now, license_id),
    )


@app.on_event("startup")
def startup() -> None:
    init_db()


# =========================
# Utils
# =========================

def today_utc_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def add_days_str(base_date_str: str, days: int) -> str:
    dt = datetime.strptime(base_date_str, "%Y-%m-%d")
    return (dt + timedelta(days=days)).strftime("%Y-%m-%d")


def compute_signature_payload(state: dict) -> str:
    return "|".join([
        str(state.get("license_key", "") or ""),
        str(state.get("type", "") or ""),
        str(state.get("status", "") or ""),
        str(state.get("issued_at", "") or ""),
        str(state.get("expires_at", "") or ""),
        str(state.get("area_limit_ha", 0.0)),
        str(state.get("last_verified_at", "") or ""),
        str(state.get("next_verify_due_at", "") or ""),
        str(state.get("machine_id", "") or ""),
    ])


def calculate_signature(state: dict) -> str:
    payload = compute_signature_payload(state)
    return hashlib.sha256((SECRET + payload).encode("utf-8")).hexdigest()


def make_response_state(
    *,
    license_key: str,
    license_type: str,
    status: str,
    issued_at: str,
    expires_at: str,
    area_limit_ha: float,
    machine_id: str,
    support_enabled: int = 0,
    support_until: str = "",
    support_active: bool = False,
) -> dict:
    last_verified_at = today_utc_str()
    next_verify_due_at = expires_at if license_type == "trial" else add_days_str(last_verified_at, 30)

    state = {
        "license_key": license_key,
        "type": license_type,
        "status": status,
        "issued_at": issued_at,
        "expires_at": expires_at,
        "area_limit_ha": area_limit_ha,
        "last_verified_at": last_verified_at,
        "next_verify_due_at": next_verify_due_at,
        "machine_id": machine_id,
        "support_enabled": int(support_enabled),
        "support_until": support_until or "",
        "support_active": bool(support_active),
    }
    state["signature"] = calculate_signature(state)
    return state


def get_license_by_key(license_key: str) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM licenses
            WHERE license_key = ?
            """,
            (license_key,)
        ).fetchone()
    return row


def upsert_trial_license(license_key: str, machine_id: str) -> sqlite3.Row:
    now = today_utc_str()
    row = get_license_by_key(license_key)

    with db_connect() as conn:
        if row is None:
            issued_at = now
            expires_at = add_days_str(now, TRIAL_DAYS)
            conn.execute(
                """
                INSERT INTO licenses (
                    license_key, license_type, status, issued_at, expires_at,
                    area_limit_ha, machine_id, note, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    license_key,
                    "trial",
                    "active",
                    issued_at,
                    expires_at,
                    TRIAL_AREA_LIMIT_HA,
                    machine_id,
                    "auto-created trial",
                    now,
                    now,
                ),
            )
            conn.commit()

        row = conn.execute(
            """
            SELECT *
            FROM licenses
            WHERE license_key = ?
            """,
            (license_key,)
        ).fetchone()

    return row


def normalize_machine_id(machine_id: str) -> str:
    return str(machine_id or "").strip()


def get_trial_by_machine_id(machine_id: str) -> Optional[sqlite3.Row]:
    normalized_machine_id = normalize_machine_id(machine_id)
    if not normalized_machine_id:
        return None

    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM licenses
            WHERE license_type = 'trial'
              AND TRIM(COALESCE(machine_id, '')) = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized_machine_id,),
        ).fetchone()
    return row


def generate_trial_license_key() -> str:
    while True:
        candidate = "TRIAL-" + secrets.token_hex(8).upper()
        if get_license_by_key(candidate) is None:
            return candidate


def create_machine_trial_license(
    *,
    machine_id: str,
    product_name: str = "",
    plugin_version: str = "",
) -> sqlite3.Row:
    normalized_machine_id = normalize_machine_id(machine_id)
    if not normalized_machine_id:
        raise HTTPException(status_code=400, detail="machine_id is required")

    now = today_utc_str()

    note_parts = ["plugin auto-issued trial"]
    if product_name.strip():
        note_parts.append(f"product={product_name.strip()}")
    if plugin_version.strip():
        note_parts.append(f"plugin_version={plugin_version.strip()}")
    note = " | ".join(note_parts)

    with db_connect() as conn:
        existing_row = conn.execute(
            """
            SELECT *
            FROM licenses
            WHERE license_type = 'trial'
              AND TRIM(COALESCE(machine_id, '')) = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized_machine_id,),
        ).fetchone()

        if existing_row is not None:
            return existing_row

        license_key = generate_trial_license_key()
        expires_at = add_days_str(now, TRIAL_DAYS)

        conn.execute(
            """
            INSERT INTO licenses (
                license_key, license_type, status, issued_at, expires_at,
                area_limit_ha, machine_id, note, created_at, updated_at,
                product_code, plan_code, max_machines, current_machine_count,
                support_enabled, is_perpetual
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                license_key,
                "trial",
                "active",
                now,
                expires_at,
                TRIAL_AREA_LIMIT_HA,
                normalized_machine_id,
                note,
                now,
                now,
                PRODUCT_NAME,
                "trial",
                1,
                1,
                0,
                0,
            ),
        )
        conn.commit()

        row = conn.execute(
            """
            SELECT *
            FROM licenses
            WHERE license_key = ?
            """,
            (license_key,),
        ).fetchone()

    return row


def issue_full_license(
    *,
    license_key: str,
    expires_at: str,
    area_limit_ha: float,
    note: str,
    max_machines: int = 1,
) -> None:
    now = today_utc_str()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO licenses (
                license_key, license_type, status, issued_at, expires_at,
                area_limit_ha, machine_id, note, created_at, updated_at,
                product_code, plan_code, max_machines, current_machine_count,
                support_enabled, is_perpetual
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(license_key) DO UPDATE SET
                license_type = excluded.license_type,
                status = excluded.status,
                issued_at = excluded.issued_at,
                expires_at = excluded.expires_at,
                area_limit_ha = excluded.area_limit_ha,
                note = excluded.note,
                updated_at = excluded.updated_at,
                product_code = excluded.product_code,
                plan_code = excluded.plan_code,
                max_machines = excluded.max_machines,
                support_enabled = excluded.support_enabled,
                is_perpetual = excluded.is_perpetual
            """,
            (
                license_key,
                "full",
                "active",
                now,
                expires_at,
                area_limit_ha,
                None,
                note,
                now,
                now,
                "GeoTivity",
                "full_perpetual",
                max(1, int(max_machines)),
                0,
                1,
                1,
            ),
        )
        conn.commit()

def hash_password_sha256(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _session_sign(payload: str) -> str:
    return hmac.new(
        SESSION_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_admin_session_value(username: str, expires_at: str) -> str:
    payload = f"{username}|{expires_at}"
    sig = _session_sign(payload)
    return f"{payload}|{sig}"


def parse_admin_session_value(session_value: str) -> Optional[dict]:
    try:
        username, expires_at, sig = session_value.split("|", 2)
    except Exception:
        return None

    payload = f"{username}|{expires_at}"
    expected = _session_sign(payload)

    if not hmac.compare_digest(sig, expected):
        return None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return None

    if datetime.utcnow() > expires_dt:
        return None

    return {
        "username": username,
        "expires_at": expires_at,
    }


def get_admin_user_by_username(conn: sqlite3.Connection, username: str):
    return conn.execute(
        """
        SELECT id, username, password_hash, display_name, role, is_active
        FROM admin_users
        WHERE username = ?
        """,
        (username,),
    ).fetchone()


def get_admin_username_from_request(request: Request) -> Optional[str]:
    session_value = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_value:
        return None

    parsed = parse_admin_session_value(session_value)
    if not parsed:
        return None

    return str(parsed["username"])


def require_admin_access(request: Request, x_admin_token: Optional[str]) -> str:
    username = get_admin_username_from_request(request)
    if username:
        return username

    if x_admin_token and hmac.compare_digest(x_admin_token, ADMIN_TOKEN):
        return "token_admin"

    raise HTTPException(status_code=403, detail="admin auth required")

def require_admin_token(x_admin_token: Optional[str]) -> None:
    if not x_admin_token or not hmac.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="invalid admin token")


def release_machine_binding(
    *,
    license_key: str,
    machine_id: str,
) -> dict:
    now = today_utc_str()

    with db_connect() as conn:
        license_row = conn.execute(
            """
            SELECT *
            FROM licenses
            WHERE license_key = ?
            """,
            (license_key,),
        ).fetchone()

        if license_row is None:
            raise HTTPException(status_code=404, detail="license not found")

        license_id = int(license_row["id"])

        machine_row = conn.execute(
            """
            SELECT *
            FROM machines
            WHERE license_id = ? AND machine_id = ? AND is_active = 1
            """,
            (license_id, machine_id),
        ).fetchone()

        if machine_row is None:
            raise HTTPException(status_code=404, detail="active machine binding not found")

        conn.execute(
            """
            UPDATE machines
            SET is_active = 0,
                released_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (now, now, int(machine_row["id"])),
        )

        active_count_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM machines
            WHERE license_id = ? AND is_active = 1
            """,
            (license_id,),
        ).fetchone()

        active_count = int(active_count_row[0] or 0)

        updates = [
            "current_machine_count = ?",
            "updated_at = ?",
        ]
        params = [active_count, now]

        if str(license_row["license_type"] or "").lower() == "trial":
            updates.append("machine_id = NULL")

        params.append(license_id)

        conn.execute(
            f"""
            UPDATE licenses
            SET {', '.join(updates)}
            WHERE id = ?
            """,
            tuple(params),
        )

        conn.commit()

    return {
        "ok": True,
        "license_key": license_key,
        "machine_id": machine_id,
        "current_machine_count": active_count,
        "message": "machine released",
    }

def is_support_active(row: sqlite3.Row) -> bool:
    support_enabled = int(row["support_enabled"] or 0)
    if support_enabled != 1:
        return False

    support_until = str(row["support_until"] or "").strip()
    if not support_until:
        return True

    try:
        today = datetime.utcnow().date()
        support_until_date = datetime.strptime(support_until, "%Y-%m-%d").date()
        return today <= support_until_date
    except Exception:
        return False


def write_admin_audit_log(
    conn: sqlite3.Connection,
    *,
    admin_username: str,
    action: str,
    target_type: str,
    target_key: str,
    detail: dict,
) -> None:
    conn.execute(
        """
        INSERT INTO admin_audit_logs (
            admin_username,
            action,
            target_type,
            target_key,
            detail_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            admin_username,
            action,
            target_type,
            target_key,
            json.dumps(detail, ensure_ascii=False),
            today_utc_str(),
        ),
    )

# =========================
# Schemas
# =========================

class VerifyRequest(BaseModel):
    license_key: str
    machine_id: str
    product: str
    version: str
    client_time: Optional[str] = None

class AdminLoginRequest(BaseModel):
    username: str
    password: str


class AdminMeResponse(BaseModel):
    username: str
    display_name: str
    role: str


class IssueFullRequest(BaseModel):
    license_key: str
    expires_at: str
    area_limit_ha: float = 0.0
    note: str = ""
    max_machines: int = 1


class DeactivateRequest(BaseModel):
    license_key: str


class ReleaseMachineRequest(BaseModel):
    license_key: str
    machine_id: str


class IssueTrialRequest(BaseModel):
    machine_id: str
    plugin_version: Optional[str] = None
    product_name: Optional[str] = None


# =========================
# Public API
# =========================

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": APP_NAME,
        "time": datetime.utcnow().isoformat() + "Z",
    }

@app.post("/api/admin/login")
def admin_login(payload: AdminLoginRequest, response: Response):
    username = str(payload.username or "").strip()
    password = str(payload.password or "")

    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password are required")

    with db_connect() as conn:
        user = get_admin_user_by_username(conn, username)
        if user is None:
            raise HTTPException(status_code=401, detail="invalid credentials")

        if int(user["is_active"] or 0) != 1:
            raise HTTPException(status_code=403, detail="admin user is inactive")

        password_hash = hash_password_sha256(password)
        if not hmac.compare_digest(password_hash, str(user["password_hash"] or "")):
            raise HTTPException(status_code=401, detail="invalid credentials")

        expires_dt = datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS)
        expires_at = expires_dt.isoformat()
        session_value = build_admin_session_value(username, expires_at)

        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=session_value,
            httponly=True,
            secure=True,
            samesite="none",
            max_age=SESSION_TTL_HOURS * 3600,
            path="/",
        )

        return {
            "ok": True,
            "username": username,
            "display_name": str(user["display_name"] or ""),
            "role": str(user["role"] or "admin"),
            "expires_at": expires_at,
        }


@app.post("/api/admin/logout")
def admin_logout(response: Response):
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        samesite="none",
        secure=True,
    )
    return {"ok": True}


@app.get("/api/admin/me", response_model=AdminMeResponse)
def admin_me(request: Request):
    username = get_admin_username_from_request(request)
    if not username:
        raise HTTPException(status_code=401, detail="not logged in")

    with db_connect() as conn:
        user = get_admin_user_by_username(conn, username)
        if user is None or int(user["is_active"] or 0) != 1:
            raise HTTPException(status_code=401, detail="not logged in")

        return {
            "username": str(user["username"]),
            "display_name": str(user["display_name"] or ""),
            "role": str(user["role"] or "admin"),
        }


@app.post("/api/license/issue-trial")
def issue_trial(req: IssueTrialRequest):
    machine_id = normalize_machine_id(req.machine_id)
    product_name = (req.product_name or PRODUCT_NAME).strip() or PRODUCT_NAME
    plugin_version = (req.plugin_version or "").strip()

    if not machine_id:
        return {
            "ok": False,
            "error_code": "EMPTY_MACHINE_ID",
            "message": "Machine ID is empty.",
        }

    row = get_trial_by_machine_id(machine_id)

    if row is None:
        row = create_machine_trial_license(
            machine_id=machine_id,
            product_name=product_name,
            plugin_version=plugin_version,
        )
        message = "trial issued"
    else:
        message = "existing trial"

    expires_at = str(row["expires_at"])
    today = datetime.utcnow().date()

    try:
        expires_date = datetime.strptime(expires_at, "%Y-%m-%d").date()
        days_left = max(0, (expires_date - today).days)
    except Exception:
        return {
            "ok": False,
            "error_code": "INVALID_EXPIRES_AT",
            "message": "Invalid expires_at on server.",
        }

    return {
        "ok": True,
        "license_key": str(row["license_key"]),
        "license_type": str(row["license_type"]),
        "days_left": days_left,
        "area_limit": float(row["area_limit_ha"] or 0.0),
        "message": message,
    }


@app.post("/api/license/verify")
def verify_license(req: VerifyRequest, request: Request):
    now = today_utc_str()
    remote_addr = get_client_ip(request)
    conn = db_connect()

    if req.product != "GeoTivity":
        log_verification(
            conn,
            license_id=None,
            license_key=req.license_key,
            machine_id=req.machine_id,
            product=req.product,
            version=req.version,
            client_time=req.client_time,
            success=False,
            error_code="INVALID_PRODUCT",
            message="Invalid product.",
            remote_addr=remote_addr,
        )
        conn.commit()
        return {
            "ok": False,
            "error_code": "INVALID_PRODUCT",
            "message": "Invalid product.",
        }

    license_key = str(req.license_key or "").strip()
    expires_at = str(req.expires_at or "").strip()
    note = str(req.note or "")

    if not license_key:
        raise HTTPException(status_code=400, detail="license_key is required")

    if not expires_at:
        raise HTTPException(status_code=400, detail="expires_at is required")

    if not license_key:
        log_verification(
            conn,
            license_id=None,
            license_key=req.license_key,
            machine_id=req.machine_id,
            product=req.product,
            version=req.version,
            client_time=req.client_time,
            success=False,
            error_code="EMPTY_LICENSE_KEY",
            message="License key is empty.",
            remote_addr=remote_addr,
        )
        conn.commit()
        return {
            "ok": False,
            "error_code": "EMPTY_LICENSE_KEY",
            "message": "License key is empty.",
        }

    if not machine_id:
        log_verification(
            conn,
            license_id=None,
            license_key=license_key,
            machine_id=req.machine_id,
            product=req.product,
            version=req.version,
            client_time=req.client_time,
            success=False,
            error_code="EMPTY_MACHINE_ID",
            message="Machine ID is empty.",
            remote_addr=remote_addr,
        )
        conn.commit()
        return {
            "ok": False,
            "error_code": "EMPTY_MACHINE_ID",
            "message": "Machine ID is empty.",
        }

    if license_key.upper() == "TRIAL":
        row = upsert_trial_license("TRIAL", machine_id)
    else:
        row = get_license_by_key(license_key)
        if row is None:
            log_verification(
                conn,
                license_id=None,
                license_key=license_key,
                machine_id=machine_id,
                product=req.product,
                version=req.version,
                client_time=req.client_time,
                success=False,
                error_code="LICENSE_NOT_FOUND",
                message="License key not found.",
                remote_addr=remote_addr,
            )
            conn.commit()
            return {
                "ok": False,
                "error_code": "LICENSE_NOT_FOUND",
                "message": "License key not found.",
            }

    license_id = int(row["id"])
    row_status = str(row["status"])
    row_type = str(row["license_type"])
    issued_at = str(row["issued_at"])
    expires_at = str(row["expires_at"])
    area_limit_ha = float(row["area_limit_ha"] or 0.0)
    bound_machine_id = (row["machine_id"] or "").strip()

    today = datetime.utcnow().date()
    try:
        expires_date = datetime.strptime(expires_at, "%Y-%m-%d").date()
    except Exception:
        log_verification(
            conn,
            license_id=license_id,
            license_key=license_key,
            machine_id=machine_id,
            product=req.product,
            version=req.version,
            client_time=req.client_time,
            success=False,
            error_code="INVALID_EXPIRES_AT",
            message="Invalid expires_at on server.",
            remote_addr=remote_addr,
        )
        conn.commit()
        return {
            "ok": False,
            "error_code": "INVALID_EXPIRES_AT",
            "message": "Invalid expires_at on server.",
        }
        
    if today > expires_date:
        with db_connect() as conn:
            conn.execute(
                """
                UPDATE licenses
                SET status = ?, updated_at = ?
                WHERE license_key = ?
                """,
                ("expired", today_utc_str(), license_key),
            )
            conn.commit()

        log_verification(
            conn,
            license_id=license_id,
            license_key=license_key,
            machine_id=machine_id,
            product=req.product,
            version=req.version,
            client_time=req.client_time,
            success=False,
            error_code="LICENSE_EXPIRED",
            message="License expired.",
            remote_addr=remote_addr,
        )
        conn.commit()
        return {
            "ok": False,
            "error_code": "LICENSE_EXPIRED",
            "message": "License expired.",
        }

    if row_status != "active":
        log_verification(
            conn,
            license_id=license_id,
            license_key=license_key,
            machine_id=machine_id,
            product=req.product,
            version=req.version,
            client_time=req.client_time,
            success=False,
            error_code="LICENSE_INACTIVE",
            message="License is inactive.",
            remote_addr=remote_addr,
        )
        conn.commit()
        return {
            "ok": False,
            "error_code": "LICENSE_INACTIVE",
            "message": "License is inactive.",
        }

    if row_type == "trial":
        # trial は初回アクセスの machine_id で固定
        if not bound_machine_id:
            conn.execute(
                """
                UPDATE licenses
                SET machine_id = ?, updated_at = ?
                WHERE license_key = ?
                """,
                (machine_id, now, license_key),
            )
            conn.commit()
            bound_machine_id = machine_id

        if bound_machine_id != machine_id:
            log_verification(
                conn,
                license_id=license_id,
                license_key=license_key,
                machine_id=machine_id,
                product=req.product,
                version=req.version,
                client_time=req.client_time,
                success=False,
                error_code="MACHINE_MISMATCH",
                message="License machine mismatch.",
                remote_addr=remote_addr,
            )
            conn.commit()
            return {
                "ok": False,
                "error_code": "MACHINE_MISMATCH",
                "message": "License machine mismatch.",
            }

    else:
        max_machines = int(row["max_machines"] or 1)

        existing_machine = conn.execute(
            """
            SELECT id
            FROM machines
            WHERE license_id = ? AND machine_id = ? AND is_active = 1
            """,
            (license_id, machine_id),
        ).fetchone()

        active_count_row = conn.execute(
            """
            SELECT COUNT(*)
            FROM machines
            WHERE license_id = ? AND is_active = 1
            """,
            (license_id,),
        ).fetchone()

        active_count = int(active_count_row[0] or 0)

        if existing_machine is None and active_count >= max_machines:
            log_verification(
                conn,
                license_id=license_id,
                license_key=license_key,
                machine_id=machine_id,
                product=req.product,
                version=req.version,
                client_time=req.client_time,
                success=False,
                error_code="MAX_MACHINES_REACHED",
                message="maximum active machines reached",
                remote_addr=remote_addr,
            )
            conn.commit()
            return {
                "ok": False,
                "error_code": "MAX_MACHINES_REACHED",
                "message": "Maximum active machines reached.",
            }

    update_license_verify_timestamps(
        conn,
        license_id=license_id,
        license_type=row_type,
        expires_at=expires_at,
    )

    upsert_machine_binding(
        conn,
        license_id=license_id,
        machine_id=machine_id,
        product_version=req.version,
    )

    active_count_row = conn.execute(
        """
        SELECT COUNT(*)
        FROM machines
        WHERE license_id = ? AND is_active = 1
        """,
        (license_id,),
    ).fetchone()

    active_count = int(active_count_row[0] or 0)

    conn.execute(
        """
        UPDATE licenses
        SET current_machine_count = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (active_count, today_utc_str(), license_id),
    )

    log_verification(
        conn,
        license_id=license_id,
        license_key=license_key,
        machine_id=machine_id,
        product=req.product,
        version=req.version,
        client_time=req.client_time,
        success=True,
        error_code="",
        message="ok",
        remote_addr=remote_addr,
    )

    conn.commit()

    support_enabled = int(row["support_enabled"] or 0)
    support_until = str(row["support_until"] or "")
    support_active = is_support_active(row)

    response_state = make_response_state(
        license_key=license_key,
        license_type=row_type,
        status="active",
        issued_at=issued_at,
        expires_at=expires_at,
        area_limit_ha=area_limit_ha,
        machine_id=machine_id,
        support_enabled=support_enabled,
        support_until=support_until,
        support_active=support_active,
    )

    response_state["ok"] = True
    return response_state


# =========================
# Admin API
# =========================

@app.get("/api/admin/machines")
def get_admin_licenses(
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT
                m.id,
                m.license_id,
                l.license_key AS license_key,
                m.machine_id,
                m.is_active,
                m.app_version,
                m.created_at
            FROM machines m
            LEFT JOIN licenses l
            ON l.id = m.license_id
            ORDER BY m.id DESC
            """
        ).fetchall()

    result = []
    for row in rows:
        result.append(
            {
                "id": row["id"],
                "license_id": row["license_id"],
                "license_key": row["license_key"],
                "machine_id": row["machine_id"],
                "is_active": bool(row["is_active"]),
                "app_version": row["app_version"],
                "created_at": row["created_at"],
            }
        )
    return result


@app.get("/api/admin/admin_audit_logs")
def api_get_admin_audit_logs(
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT
                id,
                admin_username,
                action,
                target_type,
                target_key,
                detail_json,
                created_at
            FROM admin_audit_logs
            ORDER BY id DESC
            """
        ).fetchall()

    result = []
    for row in rows:
        result.append(
            {
                "id": row["id"],
                "admin_username": row["admin_username"],
                "action": row["action"],
                "target_type": row["target_type"],
                "target_key": row["target_key"],
                "detail_json": row["detail_json"],
                "created_at": row["created_at"],
            }
        )
    return result

@app.post("/api/admin/issue_full")
def api_issue_full(
    req: IssueFullRequest,
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    license_key = str(req.license_key or "").strip()
    expires_at = str(req.expires_at or "").strip()
    note = str(req.note or "")

    if not license_key:
        raise HTTPException(status_code=400, detail="license_key is required")

    if not expires_at:
        raise HTTPException(status_code=400, detail="expires_at is required")

    try:
        datetime.strptime(expires_at, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="expires_at must be YYYY-MM-DD")

    issue_full_license(
        license_key=license_key,
        expires_at=expires_at,
        area_limit_ha=float(req.area_limit_ha or 0),
        note=note,
        max_machines=int(req.max_machines or 1),
    )

    with db_connect() as conn:
        write_admin_audit_log(
            conn,
            admin_username="admin",
            action="issue_full",
            target_type="license",
            target_key=license_key,
            detail={
                "expires_at": expires_at,
                "area_limit_ha": float(req.area_limit_ha),
                "note": req.note,
                "max_machines": int(req.max_machines),
            },
        )
        conn.commit()

    return {
        "ok": True,
        "message": "full license issued",
        "license_key": license_key,
    }


@app.post("/api/admin/deactivate")
def api_deactivate(
    req: DeactivateRequest,
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    now = today_utc_str()
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE licenses
            SET status = ?, updated_at = ?
            WHERE license_key = ?
            """,
            ("inactive", now, req.license_key.strip()),
        )
        conn.commit()

    with db_connect() as conn:
        write_admin_audit_log(
            conn,
            admin_username="admin",
            action="deactivate_license",
            target_type="license",
            target_key=req.license_key.strip(),
            detail={},
        )
        conn.commit()

    return {
        "ok": True,
        "message": "license deactivated",
        "license_key": req.license_key.strip(),
    }


@app.post("/api/admin/release_machine")
def api_release_machine(
    req: ReleaseMachineRequest,
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)
    result = release_machine_binding(
        license_key=req.license_key.strip(),
        machine_id=req.machine_id.strip(),
    )

    with db_connect() as conn:
        write_admin_audit_log(
            conn,
            admin_username="admin",
            action="release_machine",
            target_type="machine",
            target_key=f"{req.license_key.strip()}:{req.machine_id.strip()}",
            detail={},
        )
        conn.commit()

    return result

@app.get("/api/admin/license/{license_key}")
def api_get_license(
    license_key: str,
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    row = get_license_by_key(license_key.strip())
    if row is None:
        raise HTTPException(status_code=404, detail="license not found")

    return {
        "ok": True,
        "license": dict(row),
    }


@app.post("/api/admin/create_trial")
def api_create_trial(
    request: Request,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_access(request, x_admin_token)

    trial_key = "TRIAL"
    now = today_utc_str()

    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM licenses WHERE license_key = ?",
            (trial_key,),
        ).fetchone()

        if row is None:
            conn.execute(
                """
                INSERT INTO licenses (
                    license_key, license_type, status, issued_at, expires_at,
                    area_limit_ha, machine_id, note, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trial_key,
                    "trial",
                    "active",
                    now,
                    add_days_str(now, TRIAL_DAYS),
                    TRIAL_AREA_LIMIT_HA,
                    None,
                    "shared trial key",
                    now,
                    now,
                ),
            )
            conn.commit()
    with db_connect() as conn:
        write_admin_audit_log(
            conn,
            admin_username="admin",
            action="create_trial",
            target_type="license",
            target_key="TRIAL",
            detail={},
        )
        conn.commit()

    return {
        "ok": True,
        "license_key": "TRIAL",
        "message": "trial license prepared",
    }
