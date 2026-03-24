import os
import json
import hmac
import uuid
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel


load_dotenv()

APP_NAME = "GeoTivity License Server"
DB_PATH = os.environ.get("GEOTIVITY_DB_PATH", "./data/licenses.db")
SECRET = os.environ.get("GEOTIVITY_SECRET", "CHANGE_THIS_SECRET")
TRIAL_DAYS = int(os.environ.get("GEOTIVITY_TRIAL_DAYS", "30"))
TRIAL_AREA_LIMIT_HA = float(os.environ.get("GEOTIVITY_TRIAL_AREA_LIMIT_HA", "5.0"))
ADMIN_TOKEN = os.environ.get("GEOTIVITY_ADMIN_TOKEN", "CHANGE_THIS_ADMIN_TOKEN")
PRODUCT_NAME = "GeoTivity"

app = FastAPI(title=APP_NAME)


# =========================
# DB
# =========================

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

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
        conn.commit()


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


def issue_full_license(
    *,
    license_key: str,
    expires_at: str,
    area_limit_ha: float,
    note: str,
) -> None:
    now = today_utc_str()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO licenses (
                license_key, license_type, status, issued_at, expires_at,
                area_limit_ha, machine_id, note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(license_key) DO UPDATE SET
                license_type = excluded.license_type,
                status = excluded.status,
                issued_at = excluded.issued_at,
                expires_at = excluded.expires_at,
                area_limit_ha = excluded.area_limit_ha,
                note = excluded.note,
                updated_at = excluded.updated_at
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
            ),
        )
        conn.commit()


def require_admin_token(x_admin_token: Optional[str]) -> None:
    if not x_admin_token or not hmac.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="invalid admin token")


# =========================
# Schemas
# =========================

class VerifyRequest(BaseModel):
    license_key: str
    machine_id: str
    product: str
    version: str
    client_time: Optional[str] = None


class IssueFullRequest(BaseModel):
    license_key: str
    expires_at: str
    area_limit_ha: float = 0.0
    note: str = ""


class DeactivateRequest(BaseModel):
    license_key: str


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


@app.post("/api/license/verify")
def verify_license(req: VerifyRequest):
    if req.product != PRODUCT_NAME:
        return {
            "ok": False,
            "error_code": "INVALID_PRODUCT",
            "message": "Invalid product.",
        }

    license_key = req.license_key.strip()
    machine_id = req.machine_id.strip()

    if not license_key:
        return {
            "ok": False,
            "error_code": "EMPTY_LICENSE_KEY",
            "message": "License key is empty.",
        }

    if not machine_id:
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
            return {
                "ok": False,
                "error_code": "LICENSE_NOT_FOUND",
                "message": "License key not found.",
            }

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

        return {
            "ok": False,
            "error_code": "LICENSE_EXPIRED",
            "message": "License expired.",
        }

    if row_status != "active":
        return {
            "ok": False,
            "error_code": "LICENSE_INACTIVE",
            "message": "License is inactive.",
        }

    # trial は初回アクセスの machine_id で固定
    # full も初回認証時に固定
    if not bound_machine_id:
        with db_connect() as conn:
            conn.execute(
                """
                UPDATE licenses
                SET machine_id = ?, updated_at = ?
                WHERE license_key = ?
                """,
                (machine_id, today_utc_str(), license_key),
            )
            conn.commit()
        bound_machine_id = machine_id

    if bound_machine_id != machine_id:
        return {
            "ok": False,
            "error_code": "MACHINE_MISMATCH",
            "message": "License machine mismatch.",
        }

    response_state = make_response_state(
        license_key=license_key,
        license_type=row_type,
        status="active",
        issued_at=issued_at,
        expires_at=expires_at,
        area_limit_ha=area_limit_ha,
        machine_id=machine_id,
    )

    response_state["ok"] = True
    return response_state


# =========================
# Admin API
# =========================

@app.post("/api/admin/issue_full")
def api_issue_full(
    req: IssueFullRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_token(x_admin_token)

    issue_full_license(
        license_key=req.license_key.strip(),
        expires_at=req.expires_at.strip(),
        area_limit_ha=float(req.area_limit_ha),
        note=req.note,
    )
    return {
        "ok": True,
        "message": "full license issued",
        "license_key": req.license_key.strip(),
    }


@app.post("/api/admin/deactivate")
def api_deactivate(
    req: DeactivateRequest,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_token(x_admin_token)

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

    return {
        "ok": True,
        "message": "license deactivated",
        "license_key": req.license_key.strip(),
    }


@app.get("/api/admin/license/{license_key}")
def api_get_license(
    license_key: str,
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_token(x_admin_token)

    row = get_license_by_key(license_key.strip())
    if row is None:
        raise HTTPException(status_code=404, detail="license not found")

    return {
        "ok": True,
        "license": dict(row),
    }


@app.post("/api/admin/create_trial")
def api_create_trial(
    x_admin_token: Optional[str] = Header(default=None),
):
    require_admin_token(x_admin_token)

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

    return {
        "ok": True,
        "license_key": "TRIAL",
        "message": "trial license prepared",
    }